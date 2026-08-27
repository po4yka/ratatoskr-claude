//! Manual operator lifecycle command boundary.

use std::ffi::OsString;
use std::path::PathBuf;

use serde_json::json;

const PORTABLE_USAGE: &[u8] = b"usage: portable-export --tenant TENANT --output PATH [--project ID] [--observed-from RFC3339] [--observed-to RFC3339]\n";
const PRIVACY_USAGE: &[u8] = b"usage: privacy-delete plan --tenant TENANT --request-key KEY --scope tenant|export|conversation [--target UUID]\n";
const PRIVACY_EXECUTE_USAGE: &[u8] =
    b"usage: privacy-delete execute --tenant TENANT --request-id UUID --confirm\n";
const REPARSE_USAGE: &[u8] =
    b"usage: reparse --tenant UUID --archive UUID --parser NAME@VERSION [--dry-run]\n";
const PARSER_MIGRATE_USAGE: &[u8] =
    b"usage: parser-migrate --tenant UUID --operation-key KEY --parser NAME@VERSION [--dry-run]\n";

/// Parsed `portable-export` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableExportCommand {
    /// Required authenticated tenant identity.
    pub tenant: String,
    /// Destination path, retained as an operating-system path.
    pub output: PathBuf,
    /// Optional exact provider project identity.
    pub project: Option<String>,
    /// Optional inclusive lower RFC 3339 observation bound.
    pub observed_from: Option<String>,
    /// Optional inclusive upper RFC 3339 observation bound.
    pub observed_to: Option<String>,
}

/// Parsed privacy-deletion scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivacyDeletionScope {
    /// Delete every archive-owned record for the authenticated tenant.
    Tenant,
    /// Delete one raw provider export and its unretained projections.
    Export,
    /// Delete one conversation and every raw archive containing it.
    Conversation,
}

/// Parsed `privacy-delete` planning invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivacyDeletePlanCommand {
    /// Required authenticated tenant identity.
    pub tenant: String,
    /// Required idempotency key for the durable deletion request.
    pub request_key: String,
    /// Exactly one requested deletion scope.
    pub scope: PrivacyDeletionScope,
    /// Export or conversation target identity when required by the scope.
    pub target: Option<String>,
}

/// Result supplied by the privacy-delete planning boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivacyDeletePlanExecution {
    /// A deterministic deletion inventory was produced.
    Planned {
        /// Opaque durable deletion request identity.
        request_id: String,
        /// Exact number of planned inventory items.
        item_count: u64,
    },
    /// Planning failed after invocation validation.
    Failed {
        /// Stable non-sensitive operational error code.
        error_code: String,
    },
}

/// Parsed confirmed `privacy-delete execute` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivacyDeleteExecuteCommand {
    /// Required authenticated tenant identity.
    pub tenant: String,
    /// Exact durable deletion request identity to execute.
    pub request_id: uuid::Uuid,
}

/// Result supplied by the confirmed privacy-delete execution boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivacyDeleteExecuteExecution {
    /// The durable deletion request completed.
    Completed {
        /// Exact completed request identity.
        request_id: uuid::Uuid,
    },
    /// Execution failed after invocation validation.
    Failed {
        /// Stable non-sensitive operational error code.
        error_code: String,
    },
}

/// Result supplied by the portable-export execution boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortableExportExecution {
    /// The requested archive was published.
    Completed {
        /// SHA-256 of the completed portable archive.
        archive_sha256: String,
        /// Exact archive byte length.
        byte_size: u64,
    },
    /// Execution failed after invocation validation.
    Failed {
        /// Stable non-sensitive operational error code.
        error_code: String,
    },
}

/// Parsed exact reparse invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReparseCommand {
    /// Authenticated tenant identity.
    pub tenant_id: uuid::Uuid,
    /// Selected portable archive identity.
    pub archive_id: uuid::Uuid,
    /// Exact parser name.
    pub parser_name: String,
    /// Exact parser version.
    pub parser_version: String,
    /// Whether to return the plan without writes.
    pub dry_run: bool,
}

/// Result supplied by the reparse execution boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReparseExecution {
    /// Planning or apply completed with a stable report.
    Completed(serde_json::Value),
    /// Execution failed after argument validation.
    Failed {
        /// Stable content-free error code.
        error_code: String,
    },
}

/// Parsed tenant parser-migration invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserMigrateCommand {
    /// Authenticated tenant identity.
    pub tenant_id: uuid::Uuid,
    /// Stable operator idempotency key.
    pub operation_key: String,
    /// Exact parser name.
    pub parser_name: String,
    /// Exact parser version.
    pub parser_version: String,
    /// Whether to classify without applying.
    pub dry_run: bool,
}

/// Result supplied by the parser migration boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserMigrateExecution {
    /// Planning or apply completed with a stable report and partial marker.
    Completed {
        /// Stable report.
        report: serde_json::Value,
        /// Whether at least one eligible archive failed.
        partial: bool,
    },
    /// Execution failed.
    Failed {
        /// Stable content-free error code.
        error_code: String,
    },
}

/// Captured process-facing command outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleCommandResult {
    /// Portable process exit status: zero, one, or two.
    pub exit_code: u8,
    /// Machine-readable standard output bytes.
    pub stdout: Vec<u8>,
    /// Redacted diagnostic standard error bytes.
    pub stderr: Vec<u8>,
}

/// Parses and executes one `portable-export` invocation.
///
/// This seam keeps paths as [`OsString`] values until the filesystem boundary
/// and lets the deployable process supply archive execution separately.
pub fn run_portable_export_command<I, S, F>(arguments: I, execute: F) -> LifecycleCommandResult
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
    F: FnOnce(&PortableExportCommand) -> PortableExportExecution,
{
    let arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(command) = parse_portable_export(&arguments) else {
        return usage_result();
    };
    render_portable_result(execute(&command))
}

/// Parses and executes one exact reparse command.
pub fn run_reparse_command<I, S, F>(arguments: I, execute: F) -> LifecycleCommandResult
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
    F: FnOnce(&ReparseCommand) -> ReparseExecution,
{
    let arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(command) = parse_reparse(&arguments) else {
        return reparse_usage_result();
    };
    render_reparse_result(execute(&command))
}

/// Parses and executes one tenant parser migration.
pub fn run_parser_migrate_command<I, S, F>(arguments: I, execute: F) -> LifecycleCommandResult
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
    F: FnOnce(&ParserMigrateCommand) -> ParserMigrateExecution,
{
    let arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(command) = parse_parser_migrate(&arguments) else {
        return parser_migrate_usage_result();
    };
    render_parser_migrate_result(execute(&command))
}

fn parse_parser_migrate(arguments: &[OsString]) -> Option<ParserMigrateCommand> {
    let mut arguments = arguments.iter();
    if arguments.next()?.to_str()? != "parser-migrate" {
        return None;
    }
    let mut tenant = None;
    let mut operation_key = None;
    let mut parser = None;
    let mut dry_run = false;
    while let Some(flag) = arguments.next() {
        match flag.to_str()? {
            "--tenant" => {
                let value = arguments.next()?.to_str()?;
                if tenant.is_some() {
                    return None;
                }
                tenant = Some(uuid::Uuid::parse_str(value).ok()?);
            }
            "--operation-key" => set_text(&mut operation_key, arguments.next()?)?,
            "--parser" => {
                let value = arguments.next()?.to_str()?;
                if parser.is_some() {
                    return None;
                }
                parser = Some(parse_parser_identity(value)?);
            }
            "--dry-run" if !dry_run => dry_run = true,
            _ => return None,
        }
    }
    let (parser_name, parser_version) = parser?;
    Some(ParserMigrateCommand {
        tenant_id: tenant?,
        operation_key: operation_key?,
        parser_name,
        parser_version,
        dry_run,
    })
}

fn parse_reparse(arguments: &[OsString]) -> Option<ReparseCommand> {
    let mut arguments = arguments.iter();
    if arguments.next()?.to_str()? != "reparse" {
        return None;
    }
    let mut tenant = None;
    let mut archive = None;
    let mut parser = None;
    let mut dry_run = false;
    while let Some(flag) = arguments.next() {
        match flag.to_str()? {
            "--tenant" => {
                let value = arguments.next()?.to_str()?;
                if tenant.is_some() {
                    return None;
                }
                tenant = Some(uuid::Uuid::parse_str(value).ok()?);
            }
            "--archive" => {
                let value = arguments.next()?.to_str()?;
                if archive.is_some() {
                    return None;
                }
                archive = Some(uuid::Uuid::parse_str(value).ok()?);
            }
            "--parser" => {
                let value = arguments.next()?.to_str()?;
                if parser.is_some() {
                    return None;
                }
                parser = Some(parse_parser_identity(value)?);
            }
            "--dry-run" if !dry_run => dry_run = true,
            _ => return None,
        }
    }
    let (parser_name, parser_version) = parser?;
    Some(ReparseCommand {
        tenant_id: tenant?,
        archive_id: archive?,
        parser_name,
        parser_version,
        dry_run,
    })
}

fn parse_parser_identity(value: &str) -> Option<(String, String)> {
    let mut parts = value.split('@');
    let name = parts.next()?;
    let version = parts.next()?;
    if parts.next().is_some()
        || name.is_empty()
        || version.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || !version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return None;
    }
    Some((name.to_owned(), version.to_owned()))
}

fn render_reparse_result(execution: ReparseExecution) -> LifecycleCommandResult {
    let (exit_code, value, stderr) = match execution {
        ReparseExecution::Completed(value) => (0, value, Vec::new()),
        ReparseExecution::Failed { error_code } => {
            let code = safe_error_code(&error_code);
            (
                1,
                json!({"command":"reparse","error_code":code,"status":"failed"}),
                format!("reparse failed: {code}\n").into_bytes(),
            )
        }
    };
    let mut stdout = serde_json::to_vec(&value).unwrap_or_else(|_| {
        b"{\"command\":\"reparse\",\"error_code\":\"encoding_failed\",\"status\":\"failed\"}"
            .to_vec()
    });
    stdout.push(b'\n');
    LifecycleCommandResult {
        exit_code,
        stdout,
        stderr,
    }
}

/// Parses and executes one `privacy-delete` planning invocation.
///
/// The deployable process supplies authorization and inventory planning
/// separately from this process-facing argument boundary.
pub fn run_privacy_delete_plan_command<I, S, F>(arguments: I, execute: F) -> LifecycleCommandResult
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
    F: FnOnce(&PrivacyDeletePlanCommand) -> PrivacyDeletePlanExecution,
{
    let arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(command) = parse_privacy_delete_plan(&arguments) else {
        return privacy_usage_result();
    };
    render_privacy_result(execute(&command))
}

/// Parses and executes one explicitly confirmed durable deletion request.
pub fn run_privacy_delete_execute_command<I, S, F>(
    arguments: I,
    execute: F,
) -> LifecycleCommandResult
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
    F: FnOnce(&PrivacyDeleteExecuteCommand) -> PrivacyDeleteExecuteExecution,
{
    let arguments = arguments.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(command) = parse_privacy_delete_execute(&arguments) else {
        return privacy_execute_usage_result();
    };
    render_privacy_execute_result(execute(&command))
}

fn parse_privacy_delete_execute(arguments: &[OsString]) -> Option<PrivacyDeleteExecuteCommand> {
    let mut arguments = arguments.iter();
    if arguments.next()?.to_str()? != "privacy-delete" || arguments.next()?.to_str()? != "execute" {
        return None;
    }
    let mut tenant = None;
    let mut request_id = None;
    let mut confirmed = false;
    while let Some(flag) = arguments.next() {
        match flag.to_str()? {
            "--tenant" => set_text(&mut tenant, arguments.next()?)?,
            "--request-id" => {
                let value = arguments.next()?.to_str()?;
                if request_id.is_some() || value.starts_with("--") {
                    return None;
                }
                request_id = Some(uuid::Uuid::parse_str(value).ok()?);
            }
            "--confirm" if !confirmed => confirmed = true,
            _ => return None,
        }
    }
    confirmed.then_some(PrivacyDeleteExecuteCommand {
        tenant: tenant?,
        request_id: request_id?,
    })
}

fn parse_privacy_delete_plan(arguments: &[OsString]) -> Option<PrivacyDeletePlanCommand> {
    let mut arguments = arguments.iter();
    if arguments.next()?.to_str()? != "privacy-delete" || arguments.next()?.to_str()? != "plan" {
        return None;
    }
    let mut tenant = None;
    let mut request_key = None;
    let mut scope = None;
    let mut target = None;
    while let Some(flag) = arguments.next() {
        match flag.to_str()? {
            "--tenant" => set_text(&mut tenant, arguments.next()?)?,
            "--request-key" => set_text(&mut request_key, arguments.next()?)?,
            "--scope" => {
                let value = arguments.next()?.to_str()?;
                if scope.is_some() {
                    return None;
                }
                scope = Some(match value {
                    "tenant" => PrivacyDeletionScope::Tenant,
                    "export" => PrivacyDeletionScope::Export,
                    "conversation" => PrivacyDeletionScope::Conversation,
                    _ => return None,
                });
            }
            "--target" => {
                let value = arguments.next()?.to_str()?;
                if target.is_some() || value.starts_with("--") {
                    return None;
                }
                target = Some(uuid::Uuid::parse_str(value).ok()?.to_string());
            }
            _ => return None,
        }
    }
    let scope = scope?;
    if matches!(scope, PrivacyDeletionScope::Tenant) != target.is_none() {
        return None;
    }
    Some(PrivacyDeletePlanCommand {
        tenant: tenant?,
        request_key: request_key?,
        scope,
        target,
    })
}

fn parse_portable_export(arguments: &[OsString]) -> Option<PortableExportCommand> {
    let mut arguments = arguments.iter();
    if arguments.next()?.to_str()? != "portable-export" {
        return None;
    }
    let mut tenant = None;
    let mut output = None;
    let mut project = None;
    let mut observed_from = None;
    let mut observed_to = None;
    while let Some(flag) = arguments.next() {
        match flag.to_str()? {
            "--tenant" => set_text(&mut tenant, arguments.next()?)?,
            "--output" => set_path(&mut output, arguments.next()?)?,
            "--project" => set_text(&mut project, arguments.next()?)?,
            "--observed-from" => set_time(&mut observed_from, arguments.next()?)?,
            "--observed-to" => set_time(&mut observed_to, arguments.next()?)?,
            _ => return None,
        }
    }
    if matches!((&observed_from, &observed_to), (Some(from), Some(to)) if from > to) {
        return None;
    }
    Some(PortableExportCommand {
        tenant: tenant?,
        output: output?,
        project,
        observed_from,
        observed_to,
    })
}

fn set_text(target: &mut Option<String>, value: &OsString) -> Option<()> {
    let value = value.to_str()?;
    if target.is_some() || value.is_empty() || value.starts_with("--") {
        return None;
    }
    *target = Some(value.to_owned());
    Some(())
}

fn set_path(target: &mut Option<PathBuf>, value: &OsString) -> Option<()> {
    if target.is_some()
        || value.is_empty()
        || value.to_str().is_some_and(|text| text.starts_with("--"))
    {
        return None;
    }
    *target = Some(PathBuf::from(value));
    Some(())
}

fn set_time(target: &mut Option<String>, value: &OsString) -> Option<()> {
    let value = value.to_str()?;
    if target.is_some() || !is_canonical_utc_rfc3339(value) {
        return None;
    }
    *target = Some(value.to_owned());
    Some(())
}

fn is_canonical_utc_rfc3339(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 20
        && matches!(bytes.get(4), Some(b'-'))
        && matches!(bytes.get(7), Some(b'-'))
        && matches!(bytes.get(10), Some(b'T'))
        && matches!(bytes.get(13), Some(b':'))
        && matches!(bytes.get(16), Some(b':'))
        && matches!(bytes.get(19), Some(b'Z'))
        && bytes.iter().enumerate().all(|(index, byte)| {
            matches!(index, 4 | 7 | 10 | 13 | 16 | 19) || byte.is_ascii_digit()
        })
        && valid_date_time_fields(value)
}

fn valid_date_time_fields(value: &str) -> bool {
    let field = |range: std::ops::Range<usize>| value.get(range)?.parse::<u32>().ok();
    let (Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        field(5..7),
        field(8..10),
        field(11..13),
        field(14..16),
        field(17..19),
    ) else {
        return false;
    };
    (1..=12).contains(&month)
        && (1..=31).contains(&day)
        && hour <= 23
        && minute <= 59
        && second <= 60
}

fn render_portable_result(execution: PortableExportExecution) -> LifecycleCommandResult {
    let (exit_code, value, stderr) = match execution {
        PortableExportExecution::Completed {
            archive_sha256,
            byte_size,
        } => (
            0,
            json!({
                "archive_sha256": archive_sha256,
                "byte_size": byte_size,
                "command": "portable-export",
                "status": "completed",
            }),
            Vec::new(),
        ),
        PortableExportExecution::Failed { error_code } => {
            let error_code = safe_error_code(&error_code);
            (
                1,
                json!({
                    "command": "portable-export",
                    "error_code": error_code,
                    "status": "failed",
                }),
                format!("portable-export failed: {error_code}\n").into_bytes(),
            )
        }
    };
    let mut stdout = match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(_) => b"{\"command\":\"portable-export\",\"error_code\":\"encoding_failed\",\"status\":\"failed\"}".to_vec(),
    };
    stdout.push(b'\n');
    LifecycleCommandResult {
        exit_code,
        stdout,
        stderr,
    }
}

fn render_privacy_result(execution: PrivacyDeletePlanExecution) -> LifecycleCommandResult {
    let (exit_code, value, stderr) = match execution {
        PrivacyDeletePlanExecution::Planned {
            request_id,
            item_count,
        } => (
            0,
            json!({
                "command": "privacy-delete",
                "item_count": item_count,
                "request_id": request_id,
                "status": "planned",
            }),
            Vec::new(),
        ),
        PrivacyDeletePlanExecution::Failed { error_code } => {
            let error_code = safe_error_code(&error_code);
            (
                1,
                json!({
                    "command": "privacy-delete",
                    "error_code": error_code,
                    "status": "failed",
                }),
                format!("privacy-delete failed: {error_code}\n").into_bytes(),
            )
        }
    };
    let mut stdout = match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(_) => b"{\"command\":\"privacy-delete\",\"error_code\":\"encoding_failed\",\"status\":\"failed\"}".to_vec(),
    };
    stdout.push(b'\n');
    LifecycleCommandResult {
        exit_code,
        stdout,
        stderr,
    }
}

fn render_privacy_execute_result(
    execution: PrivacyDeleteExecuteExecution,
) -> LifecycleCommandResult {
    let (exit_code, value, stderr) = match execution {
        PrivacyDeleteExecuteExecution::Completed { request_id } => (
            0,
            json!({
                "command": "privacy-delete",
                "mode": "execute",
                "request_id": request_id,
                "status": "completed",
            }),
            Vec::new(),
        ),
        PrivacyDeleteExecuteExecution::Failed { error_code } => {
            let error_code = safe_error_code(&error_code);
            (
                1,
                json!({
                    "command": "privacy-delete",
                    "error_code": error_code,
                    "mode": "execute",
                    "status": "failed",
                }),
                format!("privacy-delete execute failed: {error_code}\n").into_bytes(),
            )
        }
    };
    let mut stdout = match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(_) => b"{\"command\":\"privacy-delete\",\"error_code\":\"encoding_failed\",\"mode\":\"execute\",\"status\":\"failed\"}".to_vec(),
    };
    stdout.push(b'\n');
    LifecycleCommandResult {
        exit_code,
        stdout,
        stderr,
    }
}

fn safe_error_code(value: &str) -> &str {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        value
    } else {
        "operation_failed"
    }
}

fn usage_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: Vec::new(),
        stderr: PORTABLE_USAGE.to_vec(),
    }
}

fn privacy_usage_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: b"{\"command\":\"privacy-delete\",\"error_code\":\"invalid_arguments\",\"status\":\"failed\"}\n".to_vec(),
        stderr: PRIVACY_USAGE.to_vec(),
    }
}

fn privacy_execute_usage_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: b"{\"command\":\"privacy-delete\",\"error_code\":\"invalid_arguments\",\"mode\":\"execute\",\"status\":\"failed\"}\n".to_vec(),
        stderr: PRIVACY_EXECUTE_USAGE.to_vec(),
    }
}

fn reparse_usage_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: Vec::new(),
        stderr: REPARSE_USAGE.to_vec(),
    }
}

fn parser_migrate_usage_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: Vec::new(),
        stderr: PARSER_MIGRATE_USAGE.to_vec(),
    }
}

fn render_parser_migrate_result(execution: ParserMigrateExecution) -> LifecycleCommandResult {
    let (exit_code, value, stderr) = match execution {
        ParserMigrateExecution::Completed { report, partial } => {
            (u8::from(partial), report, Vec::new())
        }
        ParserMigrateExecution::Failed { error_code } => {
            let code = safe_error_code(&error_code);
            (
                1,
                json!({"command":"parser-migrate","error_code":code,"status":"failed"}),
                format!("parser-migrate failed: {code}\n").into_bytes(),
            )
        }
    };
    let mut stdout = serde_json::to_vec(&value).unwrap_or_else(|_| {
        b"{\"command\":\"parser-migrate\",\"error_code\":\"encoding_failed\",\"status\":\"failed\"}".to_vec()
    });
    stdout.push(b'\n');
    LifecycleCommandResult {
        exit_code,
        stdout,
        stderr,
    }
}
