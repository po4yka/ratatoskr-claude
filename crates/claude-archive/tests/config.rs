//! The strictness contract of configuration loading.
//!
//! Every test drives `Config::from_environment` directly so the loader is
//! observed against explicit entries rather than whatever this process
//! happens to have inherited.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use ratatoskr_claude_archive::Config;

/// The smallest environment that must produce working configuration: the
/// two required values and nothing else.
fn minimal_environment() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "RATATOSKR__STORAGE__BLOB_ROOT",
            "/var/lib/ratatoskr-claude/blobs",
        ),
        (
            "RATATOSKR__STORAGE__DATABASE_URL",
            "postgres://claude:claude@127.0.0.1:5438/claude",
        ),
    ]
}

#[test]
fn minimal_environment_loads_with_defaults() {
    let config =
        Config::from_environment(minimal_environment()).expect("the minimal environment is valid");

    assert_eq!(
        config.admin.listen_address,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9084),
        "the operator listener default is loopback port 9084"
    );
    assert_eq!(config.telemetry.log_filter, "info");
    assert_eq!(config.limits.database_connections, 8);
    assert_eq!(config.limits.database_acquire_timeout_ms, 5_000);
    assert_eq!(config.limits.shutdown_timeout_ms, 10_000);
    assert_eq!(
        config.storage.blob_root,
        Some(PathBuf::from("/var/lib/ratatoskr-claude/blobs")),
        "the required blob root is taken from the environment"
    );
}

#[test]
fn missing_required_value_names_field() {
    let environment: Vec<(&str, &str)> = minimal_environment()
        .into_iter()
        .filter(|(key, _)| *key != "RATATOSKR__STORAGE__BLOB_ROOT")
        .collect();

    let error = Config::from_environment(environment)
        .expect_err("configuration must refuse to load without the blob root");

    let violation = error
        .violations
        .iter()
        .find(|violation| violation.key == "RATATOSKR__STORAGE__BLOB_ROOT")
        .expect("the refusal names the missing field");
    assert!(
        violation.rule.contains("required"),
        "the rule states why the field is needed, got: {}",
        violation.rule
    );
}

#[test]
fn malformed_value_names_field_and_hides_value() {
    let secret_marker = "S3CR3T-MARKER-should-never-be-echoed";
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__ADMIN__LISTEN_ADDRESS", secret_marker));

    let error = Config::from_environment(environment)
        .expect_err("an unparsable listen address must refuse to load");

    let rendered = error.to_string();
    let violation = error
        .violations
        .iter()
        .find(|violation| violation.key == "RATATOSKR__ADMIN__LISTEN_ADDRESS")
        .expect("the refusal names the offending field");
    assert!(
        !rendered.contains(secret_marker),
        "the failure text must never echo the supplied value"
    );
    assert!(!violation.rule.is_empty());
}

#[test]
fn unknown_key_rejected() {
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__NOT_A_SECTION__KEY", "1"));

    let error = Config::from_environment(environment)
        .expect_err("unknown keys are violations, never silently ignored");

    let violation = error
        .violations
        .iter()
        .find(|violation| violation.key == "RATATOSKR__NOT_A_SECTION__KEY")
        .expect("the refusal names the unknown key");
    assert!(
        violation.rule.contains("not recognized"),
        "the rule explains the key is unknown, got: {}",
        violation.rule
    );
}
