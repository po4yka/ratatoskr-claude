//! The operator router contract: liveness, readiness with named sorted
//! checks, and `Cache-Control: no-store` on every response.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]
#![expect(clippy::panic, reason = "integration tests fail loudly by design")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ratatoskr_ai_archive_contracts::platform_receipt::{
    ARCHIVE_MEDIA_TYPE, CAPABILITIES_PATH, HEADER_ARCHIVE_BYTE_SIZE, HEADER_ARCHIVE_SHA256,
    HEADER_CORRELATION_ID, HEADER_DEVICE_ID, HEADER_OPERATION_ID, HEADER_USER_ID, RECEIPT_PATH,
    receipt_capability_document,
};
use ratatoskr_claude_archive::BlobStore;
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive_service::{PlatformReceiptState, RuntimeState};
use ratatoskr_error_contracts::ErrorEnvelope;
use serde_json::Value;
use tower::ServiceExt as _;

async fn get(router: axum::Router, path: &str) -> (StatusCode, Value, Option<String>) {
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .expect("the probe request is well-formed"),
        )
        .await
        .expect("the in-memory service answers");

    let status = response.status();
    let cache_control = response
        .headers()
        .get("cache-control")
        .map(|value| value.to_str().expect("header is ASCII").to_owned());
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("the body collects")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body, cache_control)
}

async fn post_receipt(router: axum::Router) -> StatusCode {
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/ai-archives/receipt")
                .header("content-type", "application/zip")
                .header(
                    "x-ratatoskr-user-id",
                    "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb56",
                )
                .header(
                    "x-ratatoskr-device-id",
                    "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb57",
                )
                .header("x-correlation-id", "archive-receipt-test")
                .header(
                    "x-ratatoskr-operation-id",
                    "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb58",
                )
                .header(
                    "x-ratatoskr-archive-sha256",
                    "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881",
                )
                .header("x-ratatoskr-archive-byte-size", "1")
                .body(Body::from("x"))
                .expect("the receipt request is well-formed"),
        )
        .await
        .expect("the in-memory service answers")
        .status()
}

fn check<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["checks"]
        .as_array()
        .expect("readiness carries a checks array")
        .iter()
        .find(|check| check["name"] == name)
        .unwrap_or_else(|| panic!("readiness names the {name} check"))
}

#[test]
fn live_reports_process_health() {
    let runtime = Arc::new(RuntimeState::new());
    let router = ratatoskr_claude_archive_service::admin_router(runtime, || "stub".to_owned());

    let body = tokio::runtime::Runtime::new()
        .expect("the test runtime starts")
        .block_on(async { get(router, "/health/live").await });

    let (status, body, _) = body;
    assert_eq!(status, StatusCode::OK, "liveness succeeds while serving");
    assert_eq!(body["state"], "live");
}

#[test]
fn ready_lists_failing_component() {
    let runtime = Arc::new(RuntimeState::new());
    // The prober has answered at least once and found the database down.
    runtime.set_database_reachable(false);
    let router =
        ratatoskr_claude_archive_service::admin_router(Arc::clone(&runtime), || "stub".to_owned());

    let (status, body, _) = tokio::runtime::Runtime::new()
        .expect("the test runtime starts")
        .block_on(async { get(router, "/health/ready").await });

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["state"], "not_ready");

    let database = check(&body, "database");
    assert_eq!(database["state"], "fail");
    assert_eq!(database["reason"], "dependency_unavailable");
}

#[test]
fn ready_succeeds_when_all_checks_pass() {
    let runtime = Arc::new(RuntimeState::new());
    runtime.set_database_reachable(true);
    runtime.mark_startup_complete();
    let router =
        ratatoskr_claude_archive_service::admin_router(Arc::clone(&runtime), || "stub".to_owned());

    let (status, body, _) = tokio::runtime::Runtime::new()
        .expect("the test runtime starts")
        .block_on(async { get(router, "/health/ready").await });

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "ready");

    let checks = body["checks"].as_array().expect("a checks array");
    let names: Vec<&str> = checks
        .iter()
        .map(|check| check["name"].as_str().expect("names are strings"))
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(
        names, sorted,
        "checks are sorted by name for diff stability"
    );
    assert!(
        names.contains(&"database") && names.contains(&"startup"),
        "configured components appear in the list"
    );
    for entry in checks {
        assert_eq!(entry["state"], "pass");
        assert!(
            entry.get("reason").is_none(),
            "passing checks carry no reason"
        );
    }
}

#[test]
fn report_publisher_permission_controls_readiness_and_recovers() {
    let runtime = Arc::new(RuntimeState::new());
    runtime.set_database_reachable(true);
    runtime.mark_startup_complete();
    runtime.set_operation_report_publisher_ready(false);
    let router =
        ratatoskr_claude_archive_service::admin_router(Arc::clone(&runtime), || "stub".to_owned());
    let harness = tokio::runtime::Runtime::new().expect("the test runtime starts");

    let (status, body, _) = harness.block_on(async { get(router.clone(), "/health/ready").await });
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(check(&body, "operation_report_publisher")["state"], "fail");

    runtime.set_operation_report_publisher_ready(true);
    let (status, body, _) = harness.block_on(async { get(router, "/health/ready").await });
    assert_eq!(status, StatusCode::OK);
    assert_eq!(check(&body, "operation_report_publisher")["state"], "pass");
}

#[test]
fn draining_reports_not_ready() {
    let runtime = Arc::new(RuntimeState::new());
    runtime.set_database_reachable(true);
    runtime.mark_startup_complete();
    runtime.begin_draining();
    let router =
        ratatoskr_claude_archive_service::admin_router(Arc::clone(&runtime), || "stub".to_owned());

    let (status, body, _) = tokio::runtime::Runtime::new()
        .expect("the test runtime starts")
        .block_on(async { get(router, "/health/ready").await });

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let drain = check(&body, "drain");
    assert_eq!(drain["reason"], "shutdown_requested");
}

#[test]
fn responses_carry_no_store() {
    let runtime = Arc::new(RuntimeState::new());
    let router = ratatoskr_claude_archive_service::admin_router(runtime, || "stub".to_owned());

    let (_, _, cache_control) = tokio::runtime::Runtime::new()
        .expect("the test runtime starts")
        .block_on(async { get(router, "/health/live").await });

    assert_eq!(cache_control.as_deref(), Some("no-store"));
}

#[tokio::test]
async fn platform_archive_receipt_route_is_available() {
    let database = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let platform_user = uuid::Uuid::parse_str("018f5b4a-7c6f-7ab1-95d6-86ebc16bbb56")
        .expect("the fixture Platform user is a UUID");
    let archive_account = uuid::Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(archive_account)
    .bind(format!("account-{archive_account}"))
    .execute(database.database.pool())
    .await
    .expect("the mapped archive account inserts");
    let root = temp_root("platform-receipt-route");
    let store = BlobStore::open(&root).expect("the temporary blob store opens");
    let router = ratatoskr_claude_archive_service::receipt_router(PlatformReceiptState::new(
        database.database.clone(),
        store,
        &[(platform_user, archive_account)],
        1024,
    ));

    let status = post_receipt(router).await;

    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "the Platform receipt endpoint accepts its trusted envelope"
    );
    remove(&root);
    database
        .cleanup()
        .await
        .expect("the disposable database cleans up");
}

const PLATFORM_USER: &str = "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb56";
const RECEIPT_LIMIT: u64 = 1024;

struct ReceiptFixture {
    database: TestDatabase,
    root: std::path::PathBuf,
    router: axum::Router,
}

impl ReceiptFixture {
    async fn open(label: &str) -> Self {
        let database = TestDatabase::create()
            .await
            .expect("a disposable database applies the definition");
        let platform_user =
            uuid::Uuid::parse_str(PLATFORM_USER).expect("the fixture Platform user is a UUID");
        let archive_account = uuid::Uuid::now_v7();
        sqlx::query(
            "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
        )
        .bind(archive_account)
        .bind(format!("account-{archive_account}"))
        .execute(database.database.pool())
        .await
        .expect("the mapped archive account inserts");
        let root = temp_root(label);
        let store = BlobStore::open(&root).expect("the temporary blob store opens");
        let router = ratatoskr_claude_archive_service::receipt_router(PlatformReceiptState::new(
            database.database.clone(),
            store,
            &[(platform_user, archive_account)],
            RECEIPT_LIMIT,
        ));
        Self {
            database,
            root,
            router,
        }
    }

    async fn close(self) {
        remove(&self.root);
        self.database
            .cleanup()
            .await
            .expect("the disposable database cleans up");
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    use std::fmt::Write as _;
    sha2::Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// The trusted claims Edge mints for `body`, as `(name, value)` pairs a case can edit.
fn receipt_headers(body: &[u8]) -> Vec<(String, String)> {
    vec![
        ("content-type".to_owned(), ARCHIVE_MEDIA_TYPE.to_owned()),
        (HEADER_USER_ID.to_owned(), PLATFORM_USER.to_owned()),
        (
            HEADER_DEVICE_ID.to_owned(),
            "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb57".to_owned(),
        ),
        (
            HEADER_CORRELATION_ID.to_owned(),
            "archive-receipt-test".to_owned(),
        ),
        (
            HEADER_OPERATION_ID.to_owned(),
            "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb58".to_owned(),
        ),
        (HEADER_ARCHIVE_SHA256.to_owned(), sha256_hex(body)),
        (HEADER_ARCHIVE_BYTE_SIZE.to_owned(), body.len().to_string()),
    ]
}

struct Refusal {
    label: &'static str,
    method: &'static str,
    body: Vec<u8>,
    edit: fn(&mut Vec<(String, String)>),
    status: StatusCode,
    code: &'static str,
}

fn refusals() -> Vec<Refusal> {
    vec![
        Refusal {
            label: "missing claims",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| headers.retain(|(name, _)| name == "content-type"),
            status: StatusCode::UNAUTHORIZED,
            code: "claude.auth.unauthenticated",
        },
        Refusal {
            label: "an Authorization header",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| headers.push(("authorization".to_owned(), "Bearer x".to_owned())),
            status: StatusCode::UNAUTHORIZED,
            code: "claude.auth.unauthenticated",
        },
        Refusal {
            label: "an unknown tenant",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| {
                headers.retain(|(name, _)| name != HEADER_USER_ID);
                headers.push((
                    HEADER_USER_ID.to_owned(),
                    "018f5b4a-7c6f-7ab1-95d6-86ebc16bbb99".to_owned(),
                ));
            },
            status: StatusCode::UNAUTHORIZED,
            code: "claude.auth.unauthenticated",
        },
        Refusal {
            label: "a wrong content type",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| {
                headers.retain(|(name, _)| name != "content-type");
                headers.push(("content-type".to_owned(), "text/plain".to_owned()));
            },
            status: StatusCode::BAD_REQUEST,
            code: "claude.request.invalid",
        },
        Refusal {
            label: "a missing content type",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| headers.retain(|(name, _)| name != "content-type"),
            status: StatusCode::BAD_REQUEST,
            code: "claude.request.invalid",
        },
        Refusal {
            label: "a declared identity mismatch",
            method: "POST",
            body: b"x".to_vec(),
            edit: |headers| {
                headers.retain(|(name, _)| name != HEADER_ARCHIVE_SHA256);
                headers.push((HEADER_ARCHIVE_SHA256.to_owned(), sha256_hex(b"y")));
            },
            status: StatusCode::BAD_REQUEST,
            code: "claude.request.invalid",
        },
        Refusal {
            label: "a body over the limit",
            method: "POST",
            body: vec![b'z'; 2048],
            edit: |_| {},
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "claude.request.too_large",
        },
        Refusal {
            label: "the wrong method",
            method: "PUT",
            body: b"x".to_vec(),
            edit: |_| {},
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: "claude.request.method_not_allowed",
        },
    ]
}

#[tokio::test]
async fn capabilities_document_is_served_without_claims() {
    let fixture = ReceiptFixture::open("receipt-capabilities").await;

    let (status, body, cache_control) = get(fixture.router.clone(), CAPABILITIES_PATH).await;

    assert_eq!(
        (status, body, cache_control.as_deref()),
        (
            StatusCode::OK,
            receipt_capability_document("claude"),
            Some("no-store")
        ),
        "the capability probe needs no claim and answers the contract document"
    );
    fixture.close().await;
}

#[tokio::test]
async fn receipt_refusals_are_error_envelopes() {
    let fixture = ReceiptFixture::open("receipt-refusals").await;

    for refusal in refusals() {
        let mut headers = receipt_headers(&refusal.body);
        (refusal.edit)(&mut headers);
        let mut request = Request::builder().method(refusal.method).uri(RECEIPT_PATH);
        for (name, value) in &headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let response = fixture
            .router
            .clone()
            .oneshot(
                request
                    .body(Body::from(refusal.body.clone()))
                    .expect("the refusal request is well-formed"),
            )
            .await
            .expect("the in-memory service answers");
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .map(|value| value.to_str().expect("header is ASCII").to_owned());
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("the body collects")
            .to_bytes();
        let envelope = serde_json::from_slice::<ErrorEnvelope>(&bytes);

        assert_eq!(status, refusal.status, "{}: status", refusal.label);
        assert_eq!(
            content_type.as_deref(),
            Some("application/json"),
            "{}: content type",
            refusal.label
        );
        let envelope = envelope
            .unwrap_or_else(|_| panic!("{}: the body parses as an ErrorEnvelope", refusal.label));
        assert_eq!(
            envelope.code.as_str(),
            refusal.code,
            "{}: code",
            refusal.label
        );
        assert!(!envelope.retryable, "{}: not retryable", refusal.label);
    }
    fixture.close().await;
}

/// The receipt listener (loopback 8097) and the operator listener (loopback 9086) are separate
/// sockets, so each router must serve its own surface and none of the other's (XR-021
/// CONTRACTS.md S05): the operator plane on the receipt port, or the receipt on the operator
/// port, would defeat the allocation.
#[tokio::test]
async fn the_operator_router_serves_no_receipt_surface() {
    let runtime = Arc::new(RuntimeState::new());
    let router = ratatoskr_claude_archive_service::admin_router(runtime, || "stub".to_owned());

    let (capabilities, _, _) = get(router.clone(), CAPABILITIES_PATH).await;
    let receipt = post_receipt(router).await;

    assert_eq!(
        capabilities,
        StatusCode::NOT_FOUND,
        "the capability probe is advertised only where the receipt works"
    );
    assert_eq!(
        receipt,
        StatusCode::NOT_FOUND,
        "the operator listener never accepts an archive"
    );
}

#[tokio::test]
async fn the_receipt_router_serves_no_operator_surface() {
    let fixture = ReceiptFixture::open("receipt-no-operator").await;

    let mut statuses = Vec::new();
    for path in ["/health/live", "/health/ready", "/metrics", "/version"] {
        let (status, _, _) = get(fixture.router.clone(), path).await;
        statuses.push((path, status));
    }
    fixture.close().await;

    assert!(
        statuses
            .iter()
            .all(|(_, status)| *status == StatusCode::NOT_FOUND),
        "the receipt port exposes no operator route: {statuses:?}"
    );
}
