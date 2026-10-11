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
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9086),
        "the operator listener default is loopback port 9086"
    );
    assert_eq!(
        config.receipt.listen_address,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8097),
        "the Platform receipt listener default is loopback port 8097, apart from the operator plane"
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
fn receipt_listener_address_is_configurable_and_loopback() {
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__RECEIPT__LISTEN_ADDRESS", "127.0.0.1:18097"));
    let config = Config::from_environment(environment).expect("a loopback address is valid");
    assert_eq!(
        config.receipt.listen_address,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18097)
    );

    for refused in ["0.0.0.0:8097", "127.0.0.1:0", "not-an-address"] {
        let mut environment = minimal_environment();
        environment.push(("RATATOSKR__RECEIPT__LISTEN_ADDRESS", refused));
        let error = Config::from_environment(environment)
            .expect_err("a non-loopback or malformed receipt address must refuse to load");
        assert!(
            error
                .violations
                .iter()
                .any(|violation| violation.key == "RATATOSKR__RECEIPT__LISTEN_ADDRESS"),
            "the refusal names the receipt listener field for {refused}"
        );
        assert!(
            !error.to_string().contains(refused),
            "the failure text never echoes the supplied value"
        );
    }
}

#[test]
fn receipt_and_operator_listeners_may_not_share_an_address() {
    let mut environment = minimal_environment();
    environment.push(("RATATOSKR__ADMIN__LISTEN_ADDRESS", "127.0.0.1:8097"));

    let error = Config::from_environment(environment)
        .expect_err("the operator plane must not be exposed on the receipt port");

    assert!(
        error.violations.iter().any(|violation| {
            violation.key == "RATATOSKR__RECEIPT__LISTEN_ADDRESS"
                && violation.rule.contains("differ")
        }),
        "the refusal says the two listeners must differ, got: {:?}",
        error.violations
    );
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

const USER_ONE: &str = "018f0000-0000-7000-8000-000000000001";
const USER_TWO: &str = "018f0000-0000-7000-8000-000000000002";
const ACCOUNT_ONE: &str = "018f0000-0000-7000-8000-0000000000a1";
const ACCOUNT_TWO: &str = "018f0000-0000-7000-8000-0000000000a2";

/// A complete environment carrying `mapping` as the Platform account mapping.
fn mapped_environment(mapping: &str) -> Vec<(&'static str, String)> {
    let mut environment: Vec<(&'static str, String)> = minimal_environment()
        .into_iter()
        .map(|(key, value)| (key, value.to_owned()))
        .collect();
    environment.extend([
        ("RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS", mapping.to_owned()),
        (
            "RATATOSKR__RECEIPT__EVENT_BUS_URL",
            "nats://127.0.0.1:4222".to_owned(),
        ),
        (
            "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH",
            "/run/credentials/claude.nkey".to_owned(),
        ),
    ]);
    environment
}

#[test]
fn distinct_platform_account_pairs_are_accepted() {
    let mapping = format!("{USER_ONE}={ACCOUNT_ONE},{USER_TWO}={ACCOUNT_TWO}");

    let config = Config::from_environment(mapped_environment(&mapping))
        .expect("two users mapped to two accounts are valid");

    assert_eq!(config.receipt.platform_accounts.len(), 2);
}

#[test]
fn two_platform_users_may_not_share_one_account() {
    let mapping = format!("{USER_ONE}={ACCOUNT_ONE},{USER_TWO}={ACCOUNT_ONE}");

    let error = Config::from_environment(mapped_environment(&mapping))
        .expect_err("one account owned by two users would give every fact to an arbitrary one");

    assert!(
        error
            .violations
            .iter()
            .any(|violation| violation.key == "RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS"),
        "the violation names the mapping: {error}"
    );
}

#[test]
fn one_platform_user_may_not_map_to_two_accounts() {
    let mapping = format!("{USER_ONE}={ACCOUNT_ONE},{USER_ONE}={ACCOUNT_TWO}");

    let error = Config::from_environment(mapped_environment(&mapping))
        .expect_err("a user with two accounts has no single archive account");

    assert!(
        error
            .violations
            .iter()
            .any(|violation| violation.key == "RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS"),
        "the violation names the mapping: {error}"
    );
}

#[test]
fn the_event_size_guard_defaults_to_eight_million_bytes() {
    let config =
        Config::from_environment(minimal_environment()).expect("the minimal environment is valid");

    assert_eq!(
        config.receipt.max_event_bytes, 8_000_000,
        "XR-021 CONTRACTS.md R2-04: the enqueue guard is 8000000 bytes unless configured"
    );
}

#[test]
fn the_event_size_guard_accepts_only_its_documented_range() {
    for accepted in ["65536", "8000000", "8388608"] {
        let mut environment = minimal_environment();
        environment.push(("RATATOSKR__RECEIPT__MAX_EVENT_BYTES", accepted));
        let config = Config::from_environment(environment)
            .unwrap_or_else(|_| panic!("{accepted} is inside 65536..=8388608"));
        assert_eq!(
            config.receipt.max_event_bytes,
            accepted.parse::<usize>().expect("a test literal"),
            "the configured guard is used as given"
        );
    }
    for refused in ["65535", "8388609", "0", "-1", "eight", ""] {
        let mut environment = minimal_environment();
        environment.push(("RATATOSKR__RECEIPT__MAX_EVENT_BYTES", refused));
        let error = Config::from_environment(environment)
            .expect_err("a value outside 65536..=8388608 must be refused");
        assert!(
            error
                .violations
                .iter()
                .any(|violation| violation.key == "RATATOSKR__RECEIPT__MAX_EVENT_BYTES"),
            "{refused:?} is refused by naming its key"
        );
    }
}

/// The value of `key` in the shipped operator example, when the example ships and sets it.
fn example_value(key: &str) -> Option<String> {
    let example = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/systemd/claude.conf.example"
    ))
    .ok()?;
    example
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        .map(str::to_owned)
}

/// XR-021 CONTRACTS.md R3-11: a deployment that follows the docs can start the service, so the
/// role and database the example names are the ones the checked-in provisioning file creates, on
/// the port the target runs `PostgreSQL` on.
#[test]
fn the_example_role_file_exists() {
    let url = example_value("RATATOSKR__STORAGE__DATABASE_URL")
        .expect("the operator example sets the database URL");
    let rest = url
        .strip_prefix("postgres://")
        .expect("the example names a postgres:// URL");
    let (credentials, location) = rest
        .split_once('@')
        .expect("the example URL names credentials and a host");
    let role = credentials
        .split_once(':')
        .map_or(credentials, |(role, _placeholder)| role);
    let (host_and_port, database) = location
        .split_once('/')
        .expect("the example URL names a database");
    let port = host_and_port
        .rsplit_once(':')
        .map(|(_host, port)| port)
        .expect("the example URL names a port");

    let provisioning = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/postgres/role.sql"
    ))
    .expect("deploy/postgres/role.sql provisions the role the example names");

    assert_eq!(
        (role, database, port),
        ("ratatoskr_claude", "ratatoskr", "5432"),
        "the example connects as the service role to the one database on the target's port"
    );
    for statement in [
        format!("create role {role} login;"),
        format!("alter role {role} nosuperuser nocreatedb nocreaterole noreplication nobypassrls;"),
        format!("grant connect, create on database {database} to {role};"),
        format!("\\connect {database}"),
        format!("revoke all on schema public from {role};"),
    ] {
        assert!(
            provisioning.contains(&statement),
            "role.sql lacks `{statement}`"
        );
    }
    assert!(
        !provisioning.to_ascii_lowercase().contains("password '"),
        "no credential is checked in; the password is set with \\password"
    );
}
