//! A permanently failed import is reported for every operation bound to it, including a later
//! upload of the same bytes, and is logged with a class (XR-021 CONTRACTS.md R3-04 rule 2 and
//! R3-07 rule 2).
//!
//! A separate file from `receipt.rs`, which sits at the 850 line cap.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::sync::{Arc, Mutex, OnceLock};

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, Database, InitialImportWorker, Limits,
    ParserRegistry, PlatformOperation, PlatformUsers, ReceiptOutcome, TenantClaim,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// Bytes the inspector refuses, so the import fails permanently.
const NOT_AN_ARCHIVE: &[u8] = b"not a zip archive";

fn worker(database: &Database, store: &BlobStore) -> InitialImportWorker {
    InitialImportWorker::new(
        database.pool().clone(),
        store.clone(),
        Arc::new(ParserRegistry::runtime().expect("runtime parser registry is valid")),
        Limits {
            database_connections: 2,
            database_acquire_timeout_ms: 5_000,
            shutdown_timeout_ms: 5_000,
            max_archive_bytes: 1_048_576,
            max_archive_entries: 32,
            max_entry_bytes: 1_048_576,
            max_total_extracted_bytes: 2_097_152,
            max_compression_ratio: 100,
        },
        PlatformUsers::default(),
    )
}

async fn seed_account(pool: &sqlx::PgPool) -> TenantClaim {
    let account_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account_id)
    .bind(format!("acct-import-failure-{account_id}"))
    .execute(pool)
    .await
    .expect("the account inserts");
    TenantClaim {
        account: Some(account_id),
        organization: None,
    }
}

async fn upload(
    database: &Database,
    store: &BlobStore,
    claim: &TenantClaim,
    operation_id: Uuid,
) -> ReceiptOutcome {
    ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        database,
        store,
        claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(NOT_AN_ARCHIVE),
        1_048_576,
        &ArchiveIdentity {
            sha256: format!("{:x}", Sha256::digest(NOT_AN_ARCHIVE)),
            byte_size: u64::try_from(NOT_AN_ARCHIVE.len()).expect("fixture length fits"),
        },
        PlatformOperation { operation_id },
    )
    .await
    .expect("the archive is stored durably, whatever it contains")
}

/// The `failed` reports of one operation: `(status, code, retryable)` per outbox row.
async fn reports(pool: &sqlx::PgPool, operation_id: Uuid) -> Vec<(String, String, bool)> {
    let envelopes: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1' and aggregate_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_all(pool)
    .await
    .expect("the reports are readable");
    let text = |envelope: &serde_json::Value, pointer: &str| {
        envelope
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    envelopes
        .iter()
        .map(|envelope| {
            (
                text(envelope, "/payload/status"),
                text(envelope, "/payload/error/code"),
                envelope
                    .pointer("/payload/error/retryable")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_second_upload_of_bytes_that_failed_permanently_is_reported_failed() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let claim = seed_account(db.database.pool()).await;
    let root = temp_root("import-failure-duplicate");
    let store = BlobStore::open(&root).expect("the store opens");
    let worker = worker(&db.database, &store);
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();

    upload(&db.database, &store, &claim, first).await;
    let first_pass = worker.process_pending_once().await;
    let outcome = upload(&db.database, &store, &claim, second).await;
    let second_pass = worker.process_pending_once().await;
    let third_pass = worker.process_pending_once().await;
    let first_reports = reports(db.database.pool(), first).await;
    let second_reports = reports(db.database.pool(), second).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(first_pass, Ok(1)),
        "the import fails: {first_pass:?}"
    );
    assert!(
        matches!(outcome, ReceiptOutcome::Duplicate { .. }),
        "the same bytes are a duplicate: {outcome:?}"
    );
    assert!(
        matches!(second_pass, Ok(1)),
        "the duplicate's operation is picked up although its run already failed: {second_pass:?}"
    );
    assert!(
        matches!(third_pass, Ok(0)),
        "nothing is left to report on a further pass: {third_pass:?}"
    );
    let failed = vec![(
        "failed".to_owned(),
        "claude.archive.invalid".to_owned(),
        false,
    )];
    assert_eq!(
        first_reports, failed,
        "the first operation is reported once"
    );
    assert_eq!(
        second_reports, failed,
        "the second operation is reported failed exactly once"
    );
}

/// A writer the subscriber fills and the test reads back.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("the log buffer is not poisoned")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Installs the process-wide subscriber once and returns the buffer it fills.
///
/// It is global on purpose. Tests of one binary run on separate threads, and a thread-local
/// subscriber leaves the `warn!` callsite of `mark_permanent_failure` cached as disabled whenever
/// another test reaches it first: the log assertion then fails depending on the schedule. The
/// events of every test land in the one buffer and each assertion filters by its own run id.
fn captured_log() -> &'static Captured {
    static LOG: OnceLock<Captured> = OnceLock::new();
    LOG.get_or_init(|| {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other subscriber is installed in this test binary");
        captured
    })
}

#[tokio::test]
async fn a_permanent_import_failure_is_logged_with_a_class() {
    let captured = captured_log();
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let claim = seed_account(db.database.pool()).await;
    let root = temp_root("import-failure-logged");
    let store = BlobStore::open(&root).expect("the store opens");
    upload(&db.database, &store, &claim, Uuid::now_v7()).await;

    worker(&db.database, &store)
        .process_pending_once()
        .await
        .expect("a permanent failure is handled");
    let run_id: Uuid = sqlx::query_scalar("select run_id from claude_archive.import_runs")
        .fetch_one(db.database.pool())
        .await
        .expect("the failed run exists");
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let log = String::from_utf8(
        captured
            .0
            .lock()
            .expect("the log buffer is not poisoned")
            .clone(),
    )
    .expect("the log is UTF-8");
    let failure = log
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|event| event["level"] == "WARN" && event["fields"]["run_id"] == run_id.to_string())
        .unwrap_or_else(|| panic!("no WARN event names the failed run in: {log}"));
    assert_eq!(
        failure["fields"]["class"], "archive_invalid",
        "the class names why the archive failed"
    );
    assert!(
        !log.contains("not a zip archive"),
        "the log never carries archive content: {log}"
    );
}
