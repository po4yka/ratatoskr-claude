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
use ratatoskr_claude_archive_service::RuntimeState;
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
