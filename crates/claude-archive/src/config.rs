//! Strict process configuration: a finite typed structure loaded from the
//! `RATATOSKR__` environment, where every entry is examined, unknown keys are
//! violations, and no failure text ever echoes a supplied value.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use secrecy::SecretString;
use serde::Serialize;
use uuid::Uuid;

const ENV_PREFIX: &str = "RATATOSKR__";

/// Process configuration with finite built-in limits.
#[derive(Debug, Clone, Serialize)]
pub struct Config {
    /// Operator listener configuration.
    pub admin: AdminConfig,
    /// Owned durable storage configuration.
    pub storage: StorageConfig,
    /// Telemetry pipeline configuration.
    pub telemetry: TelemetryConfig,
    /// Resource and shutdown limits.
    pub limits: Limits,
    /// Trusted Platform receipt and terminal-report configuration.
    pub receipt: ReceiptConfig,
    /// Re-sending of deletion tombstones until Knowledge acknowledges them.
    pub tombstone: TombstoneConfig,
}

/// How often a tombstone Knowledge has not acknowledged is published again (XR-021 CONTRACTS.md
/// R5-11). The events stream collapses a repeated message id inside its 120 second duplicate
/// window, so a resend sooner than that is a no-op at the broker.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TombstoneConfig {
    /// Seconds a published tombstone waits for its acknowledgement, and the first wait between
    /// two resends.
    pub resend_after_seconds: u64,
    /// The wait doubles after each resend up to this many seconds.
    pub resend_cap_seconds: u64,
}

impl Default for TombstoneConfig {
    fn default() -> Self {
        Self {
            resend_after_seconds: 600,
            resend_cap_seconds: 21_600,
        }
    }
}

/// Private receipt integration configuration.
#[derive(Clone, Serialize)]
pub struct ReceiptConfig {
    /// Socket address of the private Platform receipt listener. XR-021 CONTRACTS.md S05 allocates
    /// loopback port 8097 to it, separate from the operator plane.
    pub listen_address: SocketAddr,
    /// Platform user identifiers mapped to known Claude archive account IDs.
    pub platform_accounts: Vec<(Uuid, Uuid)>,
    /// NATS `JetStream` endpoint for durable terminal operation reports.
    #[serde(skip_serializing)]
    pub event_bus_url: Option<SecretString>,
    /// Absolute `NATS` `NKey` seed path for the Claude service identity.
    #[serde(skip_serializing)]
    pub event_bus_nkey_seed_path: Option<PathBuf>,
    /// Largest serialized `ai_archive.*` event envelope the import enqueues, in bytes. A fact
    /// over it is not enqueued and the import is reported as partial (XR-021 CONTRACTS.md R2-04).
    pub max_event_bytes: usize,
}

/// The default of [`ReceiptConfig::max_event_bytes`]: below the 8 MiB broker payload limit that
/// Platform's NATS configuration sets.
pub const DEFAULT_MAX_EVENT_BYTES: usize = 8_000_000;
/// The smallest accepted [`ReceiptConfig::max_event_bytes`].
const MIN_EVENT_BYTES: usize = 65_536;
/// The largest accepted [`ReceiptConfig::max_event_bytes`]: the 8 MiB broker payload limit.
const MAX_EVENT_BYTES: usize = 8_388_608;
/// The accepted range of [`TombstoneConfig::resend_after_seconds`]. The 30 second floor exists
/// for tests; the events stream makes any resend inside its 120 second window a no-op.
const MIN_RESEND_AFTER_SECONDS: u64 = 30;
const MAX_RESEND_AFTER_SECONDS: u64 = 86_400;
/// The largest accepted [`TombstoneConfig::resend_cap_seconds`]: seven days, the stream retention.
const MAX_RESEND_CAP_SECONDS: u64 = 604_800;

impl std::fmt::Debug for ReceiptConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiptConfig")
            .field("listen_address", &self.listen_address)
            .field("platform_accounts", &self.platform_accounts.len())
            .field("event_bus_url", &"[REDACTED]")
            .field("event_bus_nkey_seed_path", &"[REDACTED]")
            .field("max_event_bytes", &self.max_event_bytes)
            .finish()
    }
}

/// Loopback-only operator listener configuration.
#[derive(Debug, Clone, Serialize)]
pub struct AdminConfig {
    /// Socket address for health, metrics, and version routes.
    pub listen_address: SocketAddr,
}

/// Durable storage locations owned by this service.
#[derive(Clone, Serialize)]
pub struct StorageConfig {
    /// Root directory of the content-addressed blob store. Required; there
    /// is deliberately no default that would scatter archive bytes somewhere
    /// the operator did not choose.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_root: Option<PathBuf>,
    /// Archive `PostgreSQL` connection URL. Required; there is deliberately
    /// no default that is not either wrong or a secret in the source tree.
    #[serde(skip_serializing)]
    pub database_url: Option<SecretString>,
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageConfig")
            .field("blob_root", &self.blob_root)
            .field("database_url", &"[REDACTED]")
            .finish()
    }
}

/// Telemetry pipeline configuration.
#[derive(Debug, Clone, Serialize)]
pub struct TelemetryConfig {
    /// Structured log filter expression.
    pub log_filter: String,
}

/// Finite limits used by the process foundation.
#[derive(Debug, Clone, Serialize)]
pub struct Limits {
    /// Maximum database connections.
    pub database_connections: u32,
    /// Maximum wait for a database connection.
    pub database_acquire_timeout_ms: u64,
    /// Maximum graceful shutdown duration.
    pub shutdown_timeout_ms: u64,
    /// Maximum accepted archive size in bytes. Receipt refuses a stream that
    /// exceeds it mid-flight, before anything durable is written.
    pub max_archive_bytes: u64,
    /// Maximum ZIP entries accepted from one raw archive.
    pub max_archive_entries: u32,
    /// Maximum decompressed bytes accepted from one ZIP entry.
    pub max_entry_bytes: u64,
    /// Maximum decompressed bytes accepted across one ZIP archive.
    pub max_total_extracted_bytes: u64,
    /// Maximum declared ZIP compression ratio accepted from one entry.
    pub max_compression_ratio: u32,
}

/// One configuration violation. The offending key and the rule it broke, and
/// never the supplied value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The environment variable key.
    pub key: String,
    /// The rule the value violated.
    pub rule: &'static str,
}

/// Configuration loading failure carrying every violation found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// Every violation found, in first-seen order.
    pub violations: Vec<Violation>,
}

impl ConfigError {
    fn new(key: &str, rule: &'static str) -> Self {
        Self {
            violations: vec![Violation {
                key: key.to_owned(),
                rule,
            }],
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "configuration is invalid")?;
        for violation in &self.violations {
            write!(formatter, "\n  {} {}", violation.key, violation.rule)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Loads the current process environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] carrying every violation found.
    pub fn load() -> Result<Self, ConfigError> {
        let mut entries = Vec::new();
        for (key, value) in std::env::vars_os() {
            let Some(key) = key.into_string().ok() else {
                continue;
            };
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            let Ok(value) = value.into_string() else {
                return Err(ConfigError::new(&key, "must contain Unicode text"));
            };
            entries.push((key, value));
        }

        Self::from_environment(entries)
    }

    /// Loads configuration from prefixed environment entries.
    ///
    /// Every entry under [`ENV_PREFIX`] must name a known key and carry a
    /// valid value; nothing is silently ignored. All entries are examined so
    /// one load reports every violation found, never only the first.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] carrying every violation found.
    pub fn from_environment<I, K, V>(entries: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut violations = Vec::new();
        let mut config = Self::default();
        for (key, value) in entries {
            let key = key.as_ref();
            if !key.starts_with(ENV_PREFIX) {
                continue;
            }
            apply_entry(&mut config, key, value.as_ref(), &mut violations);
        }

        if config.storage.blob_root.is_none() {
            violations.push(Violation {
                key: "RATATOSKR__STORAGE__BLOB_ROOT".to_owned(),
                rule: "is required: the content-addressed blob store needs an explicit root",
            });
        }
        if config.storage.database_url.is_none() {
            violations.push(Violation {
                key: "RATATOSKR__STORAGE__DATABASE_URL".to_owned(),
                rule: "is required: the archive database has no default",
            });
        }
        if !config.receipt.platform_accounts.is_empty()
            && config.receipt.event_bus_url.is_none()
            && !violations
                .iter()
                .any(|violation| violation.key == "RATATOSKR__RECEIPT__EVENT_BUS_URL")
        {
            violations.push(Violation {
                key: "RATATOSKR__RECEIPT__EVENT_BUS_URL".to_owned(),
                rule: "is required when Platform account mappings are configured",
            });
        }
        if (config.receipt.event_bus_url.is_some() || !config.receipt.platform_accounts.is_empty())
            && config.receipt.event_bus_nkey_seed_path.is_none()
            && !violations
                .iter()
                .any(|violation| violation.key == "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH")
        {
            violations.push(Violation {
                key: "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH".to_owned(),
                rule: "is required when the event bus URL is configured",
            });
        }
        if config.tombstone.resend_cap_seconds < config.tombstone.resend_after_seconds
            && !violations
                .iter()
                .any(|violation| violation.key.starts_with("RATATOSKR__TOMBSTONE__"))
        {
            violations.push(Violation {
                key: "RATATOSKR__TOMBSTONE__RESEND_CAP_SECONDS".to_owned(),
                rule: "must be at least RATATOSKR__TOMBSTONE__RESEND_AFTER_SECONDS",
            });
        }
        if config.receipt.listen_address == config.admin.listen_address
            && !violations
                .iter()
                .any(|violation| violation.key.ends_with("__LISTEN_ADDRESS"))
        {
            violations.push(Violation {
                key: "RATATOSKR__RECEIPT__LISTEN_ADDRESS".to_owned(),
                rule: "must differ from RATATOSKR__ADMIN__LISTEN_ADDRESS: the operator plane is never served on the receipt port",
            });
        }
        validate_inspection_limits(&config.limits, &mut violations);

        if violations.is_empty() {
            Ok(config)
        } else {
            Err(ConfigError { violations })
        }
    }
}

fn apply_entry(config: &mut Config, key: &str, value: &str, violations: &mut Vec<Violation>) {
    let refused = |rule: &'static str| Violation {
        key: key.to_owned(),
        rule,
    };
    match key {
        "RATATOSKR__ADMIN__LISTEN_ADDRESS" => match value.parse::<SocketAddr>() {
            Ok(address) if address.ip().is_loopback() && address.port() != 0 => {
                config.admin.listen_address = address;
            }
            Ok(_) => violations.push(refused("must be a loopback address with a port")),
            Err(_) => violations.push(refused("must be a socket address")),
        },
        "RATATOSKR__RECEIPT__LISTEN_ADDRESS" => match value.parse::<SocketAddr>() {
            Ok(address) if address.ip().is_loopback() && address.port() != 0 => {
                config.receipt.listen_address = address;
            }
            Ok(_) => violations.push(refused("must be a loopback address with a port")),
            Err(_) => violations.push(refused("must be a socket address")),
        },
        "RATATOSKR__STORAGE__BLOB_ROOT" => match value.parse::<PathBuf>() {
            Ok(path) if path.as_os_str().is_empty() => {
                violations.push(refused("must be a non-empty filesystem path"));
            }
            Ok(path) => {
                config.storage.blob_root = Some(path);
            }
            Err(_) => violations.push(refused("must be a filesystem path")),
        },
        "RATATOSKR__STORAGE__DATABASE_URL" => {
            match value.parse::<sqlx::postgres::PgConnectOptions>() {
                Ok(_) => {
                    config.storage.database_url = Some(SecretString::from(value.to_owned()));
                }
                Err(_) => violations.push(refused(
                    "must be a PostgreSQL connection URL naming user, password, host, and database",
                )),
            }
        }
        "RATATOSKR__TELEMETRY__LOG_FILTER" => {
            if value.trim().is_empty() {
                violations.push(refused("must be a non-empty tracing filter expression"));
            } else {
                value.clone_into(&mut config.telemetry.log_filter);
            }
        }
        "RATATOSKR__RECEIPT__EVENT_BUS_URL" => {
            if value.starts_with("nats://") || value.starts_with("tls://") {
                config.receipt.event_bus_url = Some(SecretString::from(value.to_owned()));
            } else {
                violations.push(refused("must be a nats:// or tls:// endpoint"));
            }
        }
        "RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH" => {
            let path = PathBuf::from(value);
            if path.is_absolute() && !path.as_os_str().is_empty() {
                config.receipt.event_bus_nkey_seed_path = Some(path);
            } else {
                violations.push(refused("must be an absolute file path"));
            }
        }
        "RATATOSKR__RECEIPT__MAX_EVENT_BYTES" => match value.parse::<usize>() {
            Ok(parsed) if (MIN_EVENT_BYTES..=MAX_EVENT_BYTES).contains(&parsed) => {
                config.receipt.max_event_bytes = parsed;
            }
            _ => violations.push(refused("must be an integer from 65536 to 8388608")),
        },
        "RATATOSKR__TOMBSTONE__RESEND_AFTER_SECONDS" => match value.parse::<u64>() {
            Ok(parsed)
                if (MIN_RESEND_AFTER_SECONDS..=MAX_RESEND_AFTER_SECONDS).contains(&parsed) =>
            {
                config.tombstone.resend_after_seconds = parsed;
            }
            _ => violations.push(refused("must be an integer from 30 to 86400")),
        },
        "RATATOSKR__TOMBSTONE__RESEND_CAP_SECONDS" => match value.parse::<u64>() {
            Ok(parsed) if parsed <= MAX_RESEND_CAP_SECONDS => {
                config.tombstone.resend_cap_seconds = parsed;
            }
            _ => violations.push(refused("must be an integer of at most 604800")),
        },
        "RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS" => match parse_platform_accounts(value) {
            Ok(accounts) => config.receipt.platform_accounts = accounts,
            Err(rule) => violations.push(refused(rule)),
        },
        _ => match apply_limit_entry(&mut config.limits, key, value) {
            Some(Ok(())) => {}
            Some(Err(rule)) => violations.push(refused(rule)),
            None => violations.push(refused("is not recognized")),
        },
    }
}

/// Applies one `RATATOSKR__LIMITS__*` entry; `None` when `key` is not a limit.
fn apply_limit_entry(
    limits: &mut Limits,
    key: &str,
    value: &str,
) -> Option<Result<(), &'static str>> {
    let applied = match key {
        "RATATOSKR__LIMITS__DATABASE_CONNECTIONS" => {
            parse_positive::<u32>(value).map(|parsed| limits.database_connections = parsed)
        }
        "RATATOSKR__LIMITS__DATABASE_ACQUIRE_TIMEOUT_MS" => {
            parse_positive::<u64>(value).map(|parsed| limits.database_acquire_timeout_ms = parsed)
        }
        "RATATOSKR__LIMITS__SHUTDOWN_TIMEOUT_MS" => {
            parse_positive::<u64>(value).map(|parsed| limits.shutdown_timeout_ms = parsed)
        }
        "RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES" => {
            parse_positive::<u64>(value).map(|parsed| limits.max_archive_bytes = parsed)
        }
        "RATATOSKR__LIMITS__MAX_ARCHIVE_ENTRIES" => {
            parse_positive::<u32>(value).map(|parsed| limits.max_archive_entries = parsed)
        }
        "RATATOSKR__LIMITS__MAX_ENTRY_BYTES" => {
            parse_positive::<u64>(value).map(|parsed| limits.max_entry_bytes = parsed)
        }
        "RATATOSKR__LIMITS__MAX_TOTAL_EXTRACTED_BYTES" => {
            parse_positive::<u64>(value).map(|parsed| limits.max_total_extracted_bytes = parsed)
        }
        "RATATOSKR__LIMITS__MAX_COMPRESSION_RATIO" => {
            parse_positive::<u32>(value).map(|parsed| limits.max_compression_ratio = parsed)
        }
        _ => return None,
    };
    Some(applied)
}

/// Parses `user=account` pairs. The owner of a fact is looked up from its account, so a user or an
/// account that appears twice is refused: an account with two users has no single owner.
fn parse_platform_accounts(value: &str) -> Result<Vec<(Uuid, Uuid)>, &'static str> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let pairs: Vec<(Uuid, Uuid)> = value
        .split(',')
        .map(|pair| {
            let (user, account) = pair.split_once('=')?;
            Some((user.parse().ok()?, account.parse().ok()?))
        })
        .collect::<Option<_>>()
        .ok_or("must map Platform and archive UUIDs as user=account pairs")?;
    let users: HashSet<Uuid> = pairs.iter().map(|(user, _)| *user).collect();
    let accounts: HashSet<Uuid> = pairs.iter().map(|(_, account)| *account).collect();
    if users.len() != pairs.len() || accounts.len() != pairs.len() {
        return Err("must map each Platform user and each archive account at most once");
    }
    Ok(pairs)
}

fn validate_inspection_limits(limits: &Limits, violations: &mut Vec<Violation>) {
    if limits.max_total_extracted_bytes < limits.max_entry_bytes {
        violations.push(Violation {
            key: "RATATOSKR__LIMITS__MAX_TOTAL_EXTRACTED_BYTES".to_owned(),
            rule: "must be at least RATATOSKR__LIMITS__MAX_ENTRY_BYTES",
        });
    }
}

fn parse_positive<T>(value: &str) -> Result<T, &'static str>
where
    T: std::str::FromStr + Default + PartialOrd,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| "must be a positive integer")?;
    if parsed <= T::default() {
        return Err("must be a positive integer");
    }
    Ok(parsed)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            admin: AdminConfig {
                listen_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9086),
            },
            storage: StorageConfig {
                blob_root: None,
                database_url: None,
            },
            telemetry: TelemetryConfig {
                log_filter: "info".to_owned(),
            },
            limits: Limits {
                database_connections: 8,
                database_acquire_timeout_ms: 5_000,
                shutdown_timeout_ms: 10_000,
                max_archive_bytes: 10 * 1024 * 1024 * 1024,
                max_archive_entries: 50_000,
                max_entry_bytes: 1024 * 1024 * 1024,
                max_total_extracted_bytes: 10 * 1024 * 1024 * 1024,
                max_compression_ratio: 100,
            },
            receipt: ReceiptConfig {
                listen_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8097),
                platform_accounts: Vec::new(),
                event_bus_url: None,
                event_bus_nkey_seed_path: None,
                max_event_bytes: DEFAULT_MAX_EVENT_BYTES,
            },
            tombstone: TombstoneConfig::default(),
        }
    }
}
