//! Structured telemetry: the JSON log pipeline and the Prometheus registry.
//!
//! Installed exactly once per process. A second installation attempt is a
//! refusal, not a reset: two subscribers or two recorders would split every
//! observation after startup.

use metrics::gauge;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::config::TelemetryConfig;

/// The one wire identity of this bounded context.
pub const SERVICE_NAME: &str = "ratatoskr-claude-archive";

/// The deployable role this binary serves. One process, one role.
pub const ROLE: &str = "archive";

/// The crate version, compiled in.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The build's git commit, provided by the container build, or `unknown`
/// outside one — the first thing anyone checks when a deployment misbehaves.
pub const GIT_SHA: &str = match option_env!("RATATOSKR_GIT_SHA") {
    Some(sha) => sha,
    None => "unknown",
};

/// The declared toolchain.
pub const RUST_VERSION: &str = env!("CARGO_PKG_RUST_VERSION");

/// The build-identity gauge: one series, labelled with the compiled identity.
const BUILD_INFO_METRIC: &str = "claude_build_info";

/// Telemetry bootstrap failure.
#[derive(Debug, thiserror::Error)]
#[error("telemetry could not be initialized")]
pub struct TelemetryError(#[source] Box<dyn std::error::Error + Send + Sync>);

/// Owns the telemetry runtime for the life of the process.
#[derive(Debug)]
pub struct TelemetryGuard {
    /// The text exposition renderer of the installed recorder.
    metrics_handle: PrometheusHandle,
}

impl TelemetryGuard {
    /// A cloneable renderer of the installed recorder, handed to whatever
    /// surface serves the exposition text.
    #[must_use]
    pub fn metrics_handle(&self) -> PrometheusHandle {
        self.metrics_handle.clone()
    }

    /// Releases telemetry resources before exit.
    pub fn shutdown(self) {}
}

/// Builds the event filter from the configured expression.
///
/// # Errors
///
/// Returns [`TelemetryError`] when the expression does not parse.
pub fn build_filter(expression: &str) -> Result<EnvFilter, TelemetryError> {
    EnvFilter::try_new(expression).map_err(|error| TelemetryError(Box::new(error)))
}

/// Installs the process-wide structured telemetry once.
///
/// # Errors
///
/// Returns [`TelemetryError`] when the filter expression is invalid, a global
/// subscriber is already installed, or the Prometheus recorder cannot be
/// installed.
pub fn init_telemetry(config: &TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter = build_filter(&config.log_filter)?;

    let metrics_handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|error| TelemetryError(Box::new(error)))?;

    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .finish();
    subscriber.try_init().map_err(|error| {
        // The recorder is already global at this point; nothing can uninstall
        // it, so the caller must treat this failure as fatal rather than retry
        // into a half-installed state.
        TelemetryError(Box::new(error))
    })?;

    gauge!(BUILD_INFO_METRIC,
        "service" => SERVICE_NAME,
        "role" => ROLE,
        "version" => VERSION,
        "git_sha" => GIT_SHA,
        "rust_version" => RUST_VERSION,
    )
    .set(1.0);

    Ok(TelemetryGuard { metrics_handle })
}
