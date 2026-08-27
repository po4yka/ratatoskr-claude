#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Ratatoskr Claude Archive service process.
//!
//! Sequence, in this order and no other: load configuration, install
//! telemetry, refuse to start without a database, connect, apply the schema,
//! prepare the blob store, bind the operator listener, mark readiness — then
//! serve until SIGTERM or SIGINT and drain within the configured bound.
//!
//! Exit codes: `0` clean run; `1` runtime startup failure; `78`
//! (`EX_CONFIG`) configuration unreadable or invalid.

use std::future::IntoFuture as _;
use std::io::Write as _;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_claude_archive::OperationReportOutbox;
use ratatoskr_claude_archive::blob_store::BlobStore;
use ratatoskr_claude_archive::telemetry::SERVICE_NAME;
use ratatoskr_claude_archive::{Config, Database};
use ratatoskr_claude_archive_service::RuntimeState;
use ratatoskr_claude_archive_service::lifecycle_commands::{
    LifecycleCommandResult, ParserMigrateExecution, PortableExportExecution,
    PrivacyDeleteExecuteExecution, PrivacyDeletePlanExecution, ReparseExecution,
    run_parser_migrate_command, run_portable_export_command, run_privacy_delete_execute_command,
    run_privacy_delete_plan_command, run_reparse_command,
};
use ratatoskr_claude_archive_service::operator_commands::OperatorContext;
use secrecy::ExposeSecret as _;

/// How often the prober copies the dependency answers into the readiness
/// facts.
///
/// Long enough that the probe is not itself load; short enough that a
/// readiness state is never more than one scrape interval stale.
const PROBE_INTERVAL: Duration = Duration::from_secs(5);
/// Terminal reports are retried gently; each pass has its own finite broker cap.
const OPERATION_REPORT_INTERVAL: Duration = Duration::from_secs(2);

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("check-config") {
        return check_config();
    }
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .and_then(|value| value.to_str())
        .is_some_and(|command| {
            matches!(
                command,
                "portable-export"
                    | "privacy-delete"
                    | "reparse"
                    | "parser-migrate"
                    | "fixture-admit"
            )
        })
    {
        return operator_command(arguments);
    }
    match tokio_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(exit) => exit,
    }
}

fn operator_command(arguments: Vec<std::ffi::OsString>) -> ExitCode {
    if arguments.first().and_then(|value| value.to_str()) == Some("fixture-admit") {
        return fixture_admit(&arguments);
    }
    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    else {
        return ExitCode::FAILURE;
    };
    let first = arguments.first().and_then(|value| value.to_str());
    let second = arguments.get(1).and_then(|value| value.to_str());
    let result =
        match (first, second) {
            (Some("portable-export"), _) => run_portable_export_command(arguments, |command| {
                match runtime.block_on(OperatorContext::open()) {
                    Ok(context) => runtime.block_on(context.portable_export(command)),
                    Err(error_code) => PortableExportExecution::Failed { error_code },
                }
            }),
            (Some("privacy-delete"), Some("plan")) => {
                run_privacy_delete_plan_command(arguments, |command| {
                    match runtime.block_on(OperatorContext::open()) {
                        Ok(context) => runtime.block_on(context.privacy_plan(command)),
                        Err(error_code) => PrivacyDeletePlanExecution::Failed { error_code },
                    }
                })
            }
            (Some("privacy-delete"), Some("execute")) => {
                run_privacy_delete_execute_command(arguments, |command| {
                    match runtime.block_on(OperatorContext::open()) {
                        Ok(context) => runtime.block_on(context.privacy_execute(command)),
                        Err(error_code) => PrivacyDeleteExecuteExecution::Failed { error_code },
                    }
                })
            }
            (Some("reparse"), _) => run_reparse_command(arguments, |command| {
                match runtime.block_on(OperatorContext::open()) {
                    Ok(context) => runtime.block_on(context.reparse(command)),
                    Err(error_code) => ReparseExecution::Failed { error_code },
                }
            }),
            (Some("parser-migrate"), _) => run_parser_migrate_command(arguments, |command| {
                match runtime.block_on(OperatorContext::open()) {
                    Ok(context) => runtime.block_on(context.parser_migrate(command)),
                    Err(error_code) => ParserMigrateExecution::Failed { error_code },
                }
            }),
            _ => LifecycleCommandResult {
                exit_code: 2,
                stdout: Vec::new(),
                stderr: b"invalid lifecycle command\n".to_vec(),
            },
        };
    runtime.shutdown_timeout(Duration::from_secs(1));
    emit_command_result(&result)
}

fn fixture_admit(arguments: &[std::ffi::OsString]) -> ExitCode {
    let candidate = match arguments {
        [command, flag, path]
            if command.to_str() == Some("fixture-admit")
                && flag.to_str() == Some("--candidate") =>
        {
            std::path::Path::new(path)
        }
        _ => {
            eprintln!("usage: fixture-admit --candidate PATH");
            return ExitCode::from(2);
        }
    };
    let Ok(report) =
        ratatoskr_claude_archive::fixture_admission::FixtureAdmission::inspect(candidate)
    else {
        let _ = std::io::stdout().write_all(
            b"{\"case_id\":null,\"findings\":[\"candidate_unreadable\"],\"status\":\"rejected\"}\n",
        );
        return ExitCode::FAILURE;
    };
    let admitted = report.status
        == ratatoskr_claude_archive::fixture_admission::FixtureAdmissionStatus::Admitted;
    match serde_json::to_string(&report) {
        Ok(json) => {
            let _ = std::io::stdout().write_all(json.as_bytes());
            let _ = std::io::stdout().write_all(b"\n");
        }
        Err(_) => return ExitCode::FAILURE,
    }
    if admitted {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn emit_command_result(result: &LifecycleCommandResult) -> ExitCode {
    let _ = std::io::stdout().write_all(&result.stdout);
    let _ = std::io::stderr().write_all(&result.stderr);
    ExitCode::from(result.exit_code)
}

/// `<binary> check-config`: load and validate without binding anything.
///
/// Both outputs go to stderr: no subscriber exists yet, and a stray line on
/// stdout could be mistaken for a log record. The effective configuration is
/// safe to render because every secret member is redacted by type.
fn check_config() -> ExitCode {
    match Config::load() {
        Ok(config) => {
            eprintln!("{SERVICE_NAME}: configuration is valid.\n{config:#?}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{SERVICE_NAME}: {error}");
            ExitCode::from(78)
        }
    }
}

#[tokio::main]
async fn tokio_main() -> Result<(), ExitCode> {
    let config = match Config::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{SERVICE_NAME}: {error}");
            return Err(ExitCode::from(78));
        }
    };

    let guard = match ratatoskr_claude_archive::init_telemetry(&config.telemetry) {
        Ok(guard) => guard,
        Err(error) => {
            eprintln!("{SERVICE_NAME}: refusing to start; telemetry failed: {error}");
            return Err(ExitCode::FAILURE);
        }
    };
    tracing::info!(
        service_name = SERVICE_NAME,
        version = ratatoskr_claude_archive::telemetry::VERSION,
        git_sha = ratatoskr_claude_archive::telemetry::GIT_SHA,
        config = ?config,
        "startup"
    );

    // Refusing to start without a database is deliberate: every capability
    // this binary will ever offer reads the archive database, and a process
    // that started anyway would report itself ready and fail everything. The
    // same holds for the blob root: raw-first archiving has nowhere to land
    // without it.
    let Some(database_url) = config.storage.database_url.as_ref() else {
        eprintln!("{SERVICE_NAME}: refusing to start without RATATOSKR__STORAGE__DATABASE_URL");
        return Err(ExitCode::FAILURE);
    };
    let Some(blob_root) = config.storage.blob_root.as_ref() else {
        eprintln!("{SERVICE_NAME}: refusing to start without RATATOSKR__STORAGE__BLOB_ROOT");
        return Err(ExitCode::FAILURE);
    };

    let database = Database::connect(
        database_url.expose_secret(),
        config.limits.database_connections,
        Duration::from_millis(config.limits.database_acquire_timeout_ms),
    )
    .await
    .map_err(|error| {
        tracing::error!(%error, "the database could not be reached");
        ExitCode::FAILURE
    })?;
    database.apply_schema().await.map_err(|error| {
        tracing::error!(%error, "the schema could not be applied");
        ExitCode::FAILURE
    })?;

    let blob_store = BlobStore::open(blob_root).map_err(|error| {
        tracing::error!(
            %error,
            root = %blob_root.display(),
            "the blob store root could not be prepared"
        );
        ExitCode::FAILURE
    })?;

    let runtime = Arc::new(RuntimeState::new());
    let listener = tokio::net::TcpListener::bind(config.admin.listen_address)
        .await
        .map_err(|error| {
            tracing::error!(
                bind = %config.admin.listen_address,
                %error,
                "the operator listener could not bind"
            );
            ExitCode::FAILURE
        })?;

    // The first probes happen before readiness flips, so the process never
    // reports itself ready over unverified dependencies.
    let prober = spawn_probers(database.clone(), blob_store.clone(), Arc::clone(&runtime));
    let operation_reporter = config.receipt.event_bus_url.as_ref().map(|endpoint| {
        spawn_operation_reporter(
            OperationReportOutbox::new(database.pool().clone()),
            endpoint.expose_secret().to_owned(),
        )
    });
    runtime.mark_startup_complete();
    tracing::info!(admin = %config.admin.listen_address, "startup complete");

    let metrics_handle = guard.metrics_handle();
    let serve_result = serve_admin(
        listener,
        Arc::clone(&runtime),
        database,
        blob_store,
        config.receipt.platform_accounts,
        config.limits.max_archive_bytes,
        move || metrics_handle.render(),
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await;

    prober.abort();
    if let Some(reporter) = operation_reporter {
        reporter.abort();
    }

    match serve_result {
        Ok(()) => {
            guard.shutdown();
            Ok(())
        }
        Err(error) => {
            tracing::error!(%error, "the operator server failed");
            Err(ExitCode::FAILURE)
        }
    }
}

/// Delivers a bounded batch of durable terminal reports on each interval.
///
/// A failed pass is intentionally quiet about broker details: the outbox row
/// remains pending and the next pass retries it with the same message ID.
fn spawn_operation_reporter(
    outbox: OperationReportOutbox,
    endpoint: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(OPERATION_REPORT_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if outbox.publish_pending_once(&endpoint).await.is_err() {
                tracing::warn!("terminal operation report publication deferred");
            }
        }
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "startup owns each independently drained runtime dependency"
)]
async fn serve_admin(
    listener: tokio::net::TcpListener,
    runtime: Arc<RuntimeState>,
    database: Database,
    blob_store: BlobStore,
    platform_accounts: Vec<(uuid::Uuid, uuid::Uuid)>,
    max_archive_bytes: u64,
    render_metrics: impl Fn() -> String + Send + Sync + 'static,
    shutdown_timeout: Duration,
) -> Result<(), String> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(
        listener,
        ratatoskr_claude_archive_service::service_router(
            runtime.clone(),
            render_metrics,
            ratatoskr_claude_archive_service::PlatformReceiptState::new(
                database.clone(),
                blob_store,
                &platform_accounts,
                max_archive_bytes,
            ),
        ),
    )
    .with_graceful_shutdown(async move {
        let _ignored = shutdown_rx.await;
    })
    .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            database.close().await;
            result.map_err(|error| error.to_string())
        }
        result = shutdown_signal() => {
            result.map_err(|error| error.to_string())?;
            // Readiness fails immediately; the listener stays open through
            // the drain window so in-flight requests finish.
            runtime.begin_draining();
            let _ignored = shutdown_tx.send(());
            if tokio::time::timeout(shutdown_timeout, &mut server).await.is_err() {
                database.close().await;
                return Err("the operator server did not stop within the shutdown bound".to_owned());
            }
            database.close().await;
            Ok(())
        }
    }
}

/// Copies the dependency answers into readiness forever.
///
/// A separate loop because it answers a different question at a different
/// cadence than any request: this keeps `/health/ready` honest while adding
/// almost no load — one round trip per dependency per interval.
fn spawn_probers(
    database: Database,
    blob_store: BlobStore,
    runtime: Arc<RuntimeState>,
) -> tokio::task::JoinHandle<()> {
    // The blocking filesystem probe runs off the async workers.
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(PROBE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            runtime.set_database_reachable(database.ping().await.is_ok());
            let writable = tokio::task::spawn_blocking({
                let blob_store = blob_store.clone();
                move || blob_store.writable_probe()
            })
            .await
            .unwrap_or(false);
            runtime.set_blob_store_writable(writable);
        }
    })
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = terminate.recv() => Ok(()),
        result = tokio::signal::ctrl_c() => result,
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
