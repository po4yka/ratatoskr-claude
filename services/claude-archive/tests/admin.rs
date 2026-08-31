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
use ratatoskr_claude_archive::BlobStore;
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive_service::{PlatformReceiptState, RuntimeState};
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
    let body: Value = serde_json::from_slice(&bytes).expect("admin bodies are JSON");
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
    let runtime = Arc::new(RuntimeState::new());
    let router = ratatoskr_claude_archive_service::service_router(
        runtime,
        || "stub".to_owned(),
        PlatformReceiptState::new(
            database.database.clone(),
            store,
            &[(platform_user, archive_account)],
            1024,
        ),
    );

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
