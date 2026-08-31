#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Process boundary for the Ratatoskr Claude Archive service: the runtime
//! lifecycle facts readiness is computed from, and the loopback operator
//! router serving liveness, readiness, metrics, and version.
//!
//! Every admin response carries `Cache-Control: no-store`: a cached "ready"
//! is a routing decision made from stale data.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http_body_util::BodyExt as _;
use ratatoskr_claude_archive::{
    ArchiveIdentity, BlobStore, Database, MediaType, PlatformOperation, StoreError, TenantClaim,
};
use serde::Serialize;
use tokio::io::AsyncWriteExt as _;
use tokio_util::io::SyncIoBridge;
use uuid::Uuid;

pub mod lifecycle_commands;
pub mod operator_commands;

/// No probe of this dependency has answered yet.
const COMPONENT_ABSENT: u8 = 0;
/// The last probe answered.
const COMPONENT_UP: u8 = 1;
/// The last probe did not answer.
const COMPONENT_DOWN: u8 = 2;

/// The deployable role this process serves, one of one.
pub const ROLE: &str = "archive";

/// Shared process lifecycle used by readiness computation.
///
/// Readiness itself is startup and drain only; a dependency that flaps must
/// not flap a process that is still accepting work correctly. What the last
/// probe of each configured dependency found is REPORTED in the check list
/// instead.
#[derive(Debug, Default)]
pub struct RuntimeState {
    startup_complete: AtomicBool,
    draining: AtomicBool,
    database: AtomicU8,
    blob_store: AtomicU8,
    operation_report_publisher: AtomicU8,
    initial_import_worker: AtomicU8,
}

impl RuntimeState {
    /// A process that has bound nothing yet: readiness fails, liveness does not.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configuration validated, telemetry installed, every configured
    /// listener bound. Set exactly once.
    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(true, Ordering::Release);
    }

    /// A shutdown signal arrived. Readiness fails immediately; the listener
    /// stays open through the drain window.
    pub fn begin_draining(&self) {
        self.draining.store(true, Ordering::Release);
    }

    /// Record what the latest database probe found.
    ///
    /// Called by the prober, not by a request: a readiness probe must never
    /// open a connection, or a saturated pool would make the health check the
    /// thing that finishes it off.
    pub fn set_database_reachable(&self, reachable: bool) {
        self.database.store(
            if reachable {
                COMPONENT_UP
            } else {
                COMPONENT_DOWN
            },
            Ordering::Release,
        );
    }

    /// Record what the latest blob-store writability probe found.
    pub fn set_blob_store_writable(&self, writable: bool) {
        self.blob_store.store(
            if writable {
                COMPONENT_UP
            } else {
                COMPONENT_DOWN
            },
            Ordering::Release,
        );
    }

    /// Records whether the configured operation-report publisher currently
    /// has authenticated broker authority.
    pub fn set_operation_report_publisher_ready(&self, ready: bool) {
        self.operation_report_publisher.store(
            if ready { COMPONENT_UP } else { COMPONENT_DOWN },
            Ordering::Release,
        );
    }

    /// Records whether the restart-safe initial import loop completed its latest pass.
    pub fn set_initial_import_worker_ready(&self, ready: bool) {
        self.initial_import_worker.store(
            if ready { COMPONENT_UP } else { COMPONENT_DOWN },
            Ordering::Release,
        );
    }

    /// Whether new work may be routed to this process.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let dependencies_up = |probe: &AtomicU8| {
            matches!(
                probe.load(Ordering::Acquire),
                COMPONENT_ABSENT | COMPONENT_UP
            )
        };
        self.startup_complete.load(Ordering::Acquire)
            && !self.draining.load(Ordering::Acquire)
            && dependencies_up(&self.database)
            && dependencies_up(&self.blob_store)
            && dependencies_up(&self.operation_report_publisher)
            && dependencies_up(&self.initial_import_worker)
    }

    /// The readiness checks, sorted by name so two consecutive bodies are
    /// byte-identical and `diff` stays usable at 03:00.
    ///
    /// A dependency that was never probed reports no check at all: a passing
    /// check for something that does not exist is the readiness equivalent of
    /// an always-zero metric.
    #[must_use]
    pub fn checks(&self) -> Vec<Check> {
        let draining = self.draining.load(Ordering::Acquire);
        let started = self.startup_complete.load(Ordering::Acquire);
        let mut checks = vec![
            Check {
                name: CheckName::Drain,
                state: state(!draining),
                reason: draining.then_some(CheckReason::ShutdownRequested),
            },
            Check {
                name: CheckName::Startup,
                state: state(started),
                reason: (!started).then_some(CheckReason::StartupIncomplete),
            },
        ];

        for (probe, name) in [
            (&self.blob_store, CheckName::BlobStore),
            (&self.database, CheckName::Database),
            (
                &self.operation_report_publisher,
                CheckName::OperationReportPublisher,
            ),
            (&self.initial_import_worker, CheckName::InitialImportWorker),
        ] {
            if probe.load(Ordering::Acquire) == COMPONENT_ABSENT {
                continue;
            }
            let up = probe.load(Ordering::Acquire) == COMPONENT_UP;
            checks.push(Check {
                name,
                state: state(up),
                reason: (!up).then_some(CheckReason::DependencyUnavailable),
            });
        }

        checks.sort_unstable_by_key(|check| check.name);
        checks
    }
}

fn state(subject: bool) -> CheckState {
    if subject {
        CheckState::Pass
    } else {
        CheckState::Fail
    }
}

/// One readiness check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Check {
    /// The logical name of the subject.
    pub name: CheckName,
    /// Whether the subject passes.
    pub state: CheckState,
    /// Why it does not, when it does not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<CheckReason>,
}

/// A logical token from a closed set. Never a hostname, port, DSN or driver
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckName {
    /// The blob store root accepted its latest write probe.
    BlobStore,
    /// The database answers. Present only once one has been probed.
    Database,
    /// No shutdown signal has arrived.
    Drain,
    /// The restart-safe initial import loop completed its latest bounded pass.
    InitialImportWorker,
    /// The authenticated terminal-report publisher can use its subject.
    OperationReportPublisher,
    /// Configuration, telemetry and every configured listener are up.
    Startup,
}

/// Whether one check passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckState {
    /// The subject is healthy.
    Pass,
    /// The subject is not healthy.
    Fail,
}

/// A closed set of failure reasons. NEVER a formatted dependency error: a
/// driver message can carry a host, a port and sometimes a password.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckReason {
    /// The process has not finished binding its listeners.
    StartupIncomplete,
    /// A shutdown signal arrived and this instance is draining.
    ShutdownRequested,
    /// The last probe did not answer.
    DependencyUnavailable,
}

/// The Prometheus text exposition format the `metrics` crate renders.
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";
/// The HTTP stream cannot outrun the archive reader by more than this bound.
const RECEIPT_PIPE_BYTES: usize = 64 * 1024;

/// Verified local dependencies used only by the Platform receipt route.
#[derive(Debug, Clone)]
pub struct PlatformReceiptState {
    database: Database,
    blob_store: BlobStore,
    platform_accounts: HashMap<Uuid, Uuid>,
    max_archive_bytes: u64,
}

impl PlatformReceiptState {
    /// Creates the state used by a loopback Platform receipt listener.
    #[must_use]
    pub fn new(
        database: Database,
        blob_store: BlobStore,
        platform_accounts: &[(Uuid, Uuid)],
        max_archive_bytes: u64,
    ) -> Self {
        Self {
            database,
            blob_store,
            platform_accounts: platform_accounts.iter().copied().collect(),
            max_archive_bytes,
        }
    }
}

struct AdminState {
    runtime: Arc<RuntimeState>,
    render_metrics: Box<dyn Fn() -> String + Send + Sync>,
}

/// Builds the loopback operator router.
pub fn admin_router(
    state: Arc<RuntimeState>,
    render_metrics: impl Fn() -> String + Send + Sync + 'static,
) -> Router {
    let state = Arc::new(AdminState {
        runtime: state,
        render_metrics: Box::new(render_metrics),
    });
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/version", get(version))
        .with_state(state)
        .layer(middleware::from_fn(no_store))
}

/// Combines operator probes with the private Platform receipt listener.
pub fn service_router(
    runtime: Arc<RuntimeState>,
    render_metrics: impl Fn() -> String + Send + Sync + 'static,
    receipt: PlatformReceiptState,
) -> Router {
    admin_router(runtime, render_metrics).merge(
        Router::new()
            .route("/v1/ai-archives/receipt", post(platform_receipt))
            .with_state(Arc::new(receipt))
            .layer(middleware::from_fn(no_store)),
    )
}

/// *This process's async runtime is scheduling tasks and the server answers.*
///
/// It consults nothing external, ever, and it answers 200 from bind until
/// exit INCLUDING throughout drain. Wiring liveness to a dependency converts
/// one database blip into a restart loop.
async fn live() -> Json<Liveness> {
    Json(Liveness {
        state: "live",
        role: ROLE,
    })
}

/// *Route new work to me.*
async fn ready(State(state): State<Arc<AdminState>>) -> Response {
    let ready = state.runtime.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(Readiness {
            state: if ready { "ready" } else { "not_ready" },
            role: ROLE,
            checks: state.runtime.checks(),
        }),
    )
        .into_response()
}

/// Prometheus pull. One route calling the renderer closure: no second HTTP
/// server and no push gateway.
async fn metrics(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        (state.render_metrics)(),
    )
}

/// The build identity, kept on the operator plane so a build fingerprint is
/// never public.
async fn version() -> Json<Version> {
    Json(Version {
        service: ratatoskr_claude_archive::telemetry::SERVICE_NAME,
        role: ROLE,
        version: ratatoskr_claude_archive::telemetry::VERSION,
        git_sha: ratatoskr_claude_archive::telemetry::GIT_SHA,
        rust_version: ratatoskr_claude_archive::telemetry::RUST_VERSION,
    })
}

/// Receives a Platform-forwarded archive through the bounded streaming bridge.
async fn platform_receipt(
    State(state): State<Arc<PlatformReceiptState>>,
    headers: HeaderMap,
    mut body: Body,
) -> StatusCode {
    if headers.contains_key(header::AUTHORIZATION) {
        return StatusCode::UNAUTHORIZED;
    }
    let Some((claim, identity, operation)) = platform_claims(&state, &headers) else {
        return StatusCode::UNAUTHORIZED;
    };
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some("application/zip")
    {
        return StatusCode::BAD_REQUEST;
    }

    let scope = match ratatoskr_claude_archive::receipt::authenticate_claim(&state.database, &claim)
        .await
    {
        Ok(scope) => scope,
        Err(ratatoskr_claude_archive::ReceiptError::UnknownTenant) => {
            return StatusCode::UNAUTHORIZED;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    let Ok(media_type) = MediaType::parse("application/zip") else {
        return StatusCode::INTERNAL_SERVER_ERROR;
    };
    let (reader, mut writer) = tokio::io::duplex(RECEIPT_PIPE_BYTES);
    let blob_store = state.blob_store.clone();
    let maximum = state.max_archive_bytes;
    let receiver = tokio::task::spawn_blocking(move || {
        blob_store.store_stream_with_identity(
            media_type,
            SyncIoBridge::new(reader),
            maximum,
            &identity.sha256,
            identity.byte_size,
        )
    });

    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            receiver.abort();
            return StatusCode::BAD_REQUEST;
        };
        if let Ok(bytes) = frame.into_data()
            && writer.write_all(&bytes).await.is_err()
        {
            receiver.abort();
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    if writer.shutdown().await.is_err() {
        receiver.abort();
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    match receiver.await {
        Ok(Ok(blob_ref)) => {
            match ratatoskr_claude_archive::receipt::record_verified_platform_archive(
                &state.database,
                scope,
                blob_ref,
                operation,
            )
            .await
            {
                Ok(_) => StatusCode::ACCEPTED,
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
        Ok(Err(StoreError::DeclaredIdentityMismatch)) => StatusCode::BAD_REQUEST,
        Ok(Err(_)) | Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn platform_claims(
    state: &PlatformReceiptState,
    headers: &HeaderMap,
) -> Option<(TenantClaim, ArchiveIdentity, PlatformOperation)> {
    let header = |name: &'static str| headers.get(name)?.to_str().ok();
    let user_id = header("x-ratatoskr-user-id")?.parse::<Uuid>().ok()?;
    let _device_id = header("x-ratatoskr-device-id")?.parse::<Uuid>().ok()?;
    let correlation = header("x-correlation-id")?;
    let operation_id = header("x-ratatoskr-operation-id")?.parse::<Uuid>().ok()?;
    let sha256 = header("x-ratatoskr-archive-sha256")?;
    let byte_size = header("x-ratatoskr-archive-byte-size")?
        .parse::<u64>()
        .ok()?;
    if correlation.is_empty()
        || correlation.len() > 200
        || byte_size == 0
        || sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let account = *state.platform_accounts.get(&user_id)?;
    Some((
        TenantClaim {
            account: Some(account),
            organization: None,
        },
        ArchiveIdentity {
            sha256: sha256.to_owned(),
            byte_size,
        },
        PlatformOperation { operation_id },
    ))
}

/// `Cache-Control: no-store` on every admin response, including bare 404s.
async fn no_store(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// `GET /health/live`.
#[derive(Serialize)]
struct Liveness {
    /// Always `live`. The property is `state`, not `status`.
    state: &'static str,
    /// The deployable role.
    role: &'static str,
}

/// `GET /health/ready`.
#[derive(Serialize)]
struct Readiness {
    /// `ready` | `not_ready`.
    state: &'static str,
    /// The deployable role.
    role: &'static str,
    /// Name-sorted, never a map, so two consecutive bodies are identical.
    checks: Vec<Check>,
}

/// `GET /version`.
#[allow(
    clippy::struct_field_names,
    reason = "the member names are the operator-facing JSON shape, not a naming choice"
)]
#[derive(Serialize)]
struct Version {
    /// The one wire identity of this bounded context.
    service: &'static str,
    /// The deployable role.
    role: &'static str,
    /// The crate version.
    version: &'static str,
    /// The build's git commit, or `unknown` outside a container build.
    git_sha: &'static str,
    /// The declared toolchain.
    rust_version: &'static str,
}
