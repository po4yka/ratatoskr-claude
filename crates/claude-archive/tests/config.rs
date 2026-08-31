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

#[test]
fn max_archive_bytes_defaults_to_ten_gibibytes() {
    let config =
        Config::from_environment(minimal_environment()).expect("the minimal environment is valid");

    assert_eq!(
        config.limits.max_archive_bytes,
        10 * 1024 * 1024 * 1024,
        "the archive cap defaults to 10 GiB without configuration"
    );
}

#[test]
fn max_archive_bytes_accepts_configured_value() {
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES", "1024"));

    let config = Config::from_environment(environment)
        .expect("a positive archive cap is a valid configuration");

    assert_eq!(
        config.limits.max_archive_bytes, 1024,
        "the configured cap replaces the default"
    );
}

#[test]
fn max_archive_bytes_rejects_non_positive_value() {
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES", "0"));

    let error = Config::from_environment(environment)
        .expect_err("an archive cap of zero must refuse to load");

    let violation = error
        .violations
        .iter()
        .find(|violation| violation.key == "RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES")
        .expect("the refusal names the offending field");
    assert!(
        violation.rule.contains("positive"),
        "the rule states the value must be positive, got: {}",
        violation.rule
    );
}

#[test]
fn archive_inspection_limits_default_to_safe_bounds() {
    let config =
        Config::from_environment(minimal_environment()).expect("the minimal environment is valid");

    assert_eq!(config.limits.max_archive_entries, 50_000);
    assert_eq!(config.limits.max_entry_bytes, 1024 * 1024 * 1024);
    assert_eq!(
        config.limits.max_total_extracted_bytes,
        10 * 1024 * 1024 * 1024
    );
    assert_eq!(config.limits.max_compression_ratio, 100);
}

#[test]
fn archive_inspection_limits_accept_valid_environment_overrides() {
    let mut environment = minimal_environment();
    environment.extend([
        ("RATATOSKR__LIMITS__MAX_ARCHIVE_ENTRIES", "12"),
        ("RATATOSKR__LIMITS__MAX_ENTRY_BYTES", "1024"),
        ("RATATOSKR__LIMITS__MAX_TOTAL_EXTRACTED_BYTES", "4096"),
        ("RATATOSKR__LIMITS__MAX_COMPRESSION_RATIO", "8"),
    ]);

    let config = Config::from_environment(environment)
        .expect("positive internally consistent inspection limits are valid");

    assert_eq!(config.limits.max_archive_entries, 12);
    assert_eq!(config.limits.max_entry_bytes, 1024);
    assert_eq!(config.limits.max_total_extracted_bytes, 4096);
    assert_eq!(config.limits.max_compression_ratio, 8);
}

#[test]
fn archive_inspection_limits_reject_zero_or_inconsistent_values() {
    let mut environment = minimal_environment();
    environment.extend([
        ("RATATOSKR__LIMITS__MAX_ARCHIVE_ENTRIES", "0"),
        ("RATATOSKR__LIMITS__MAX_ENTRY_BYTES", "4096"),
        ("RATATOSKR__LIMITS__MAX_TOTAL_EXTRACTED_BYTES", "1024"),
        ("RATATOSKR__LIMITS__MAX_COMPRESSION_RATIO", "0"),
    ]);

    let error = Config::from_environment(environment)
        .expect_err("zero or inconsistent inspection limits must refuse to load");

    assert!(error.violations.iter().any(|violation| {
        violation.key == "RATATOSKR__LIMITS__MAX_ARCHIVE_ENTRIES"
            && violation.rule.contains("positive")
    }));
    assert!(error.violations.iter().any(|violation| {
        violation.key == "RATATOSKR__LIMITS__MAX_TOTAL_EXTRACTED_BYTES"
            && violation.rule.contains("at least")
    }));
    assert!(error.violations.iter().any(|violation| {
        violation.key == "RATATOSKR__LIMITS__MAX_COMPRESSION_RATIO"
            && violation.rule.contains("positive")
    }));
}

#[test]
fn event_bus_url_requires_nkey_seed_path() {
    let endpoint = "nats://operator-secret@127.0.0.1:4222";
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__RECEIPT__EVENT_BUS_URL", endpoint));

    let error = Config::from_environment(environment)
        .expect_err("an operation-report endpoint without credentials must fail closed");

    assert!(error.violations.iter().any(|violation| {
        violation.key == "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH"
            && violation.rule == "is required when the event bus URL is configured"
    }));
    assert!(!format!("{error}").contains(endpoint));
}

#[test]
fn platform_receipt_mapping_requires_report_bus_and_nkey() {
    let mut environment = minimal_environment();
    environment.push((
        "RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS",
        "018f0000-0000-7000-8000-000000000001=018f0000-0000-7000-8000-000000000002",
    ));

    let error = Config::from_environment(environment)
        .expect_err("Platform receipt must not start without its terminal report bus");
    assert!(
        error
            .violations
            .iter()
            .any(|violation| violation.key == "RATATOSKR__RECEIPT__EVENT_BUS_URL")
    );
    assert!(
        error
            .violations
            .iter()
            .any(|violation| { violation.key == "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH" })
    );
}

#[test]
fn nkey_event_bus_configuration_redacts_endpoint_and_path() {
    let endpoint = "nats://operator-secret@127.0.0.1:4222";
    let mut environment = minimal_environment();
    environment.extend([
        ("RATATOSKR__RECEIPT__EVENT_BUS_URL", endpoint),
        (
            "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH",
            "/run/credentials/claude.nkey",
        ),
    ]);

    let config =
        Config::from_environment(environment).expect("credentialed operation reporting must load");
    assert_eq!(
        config.receipt.event_bus_nkey_seed_path,
        Some(std::path::PathBuf::from("/run/credentials/claude.nkey"))
    );
    let rendered = format!("{config:?}");
    assert!(!rendered.contains(endpoint));
    assert!(!rendered.contains("claude.nkey"));
}
