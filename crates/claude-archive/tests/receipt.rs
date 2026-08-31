//! Authenticated tenant-scoped archive receipt: verification before storage,
//! streamed digest and size accounting under a cap, write-once raw placement,
//! and explicit stored/duplicate outcomes.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, ImportRunStore, OperationReportOutbox,
    PlatformOperation, ReceiptError, ReceiptOutcome, TenantClaim,
};
use sqlx::Row as _;
use uuid::Uuid;

/// A deterministic payload several internal chunks long plus a ragged
/// remainder, so incremental hashing crosses chunk boundaries unevenly.
fn multi_chunk_payload(chunks: usize, remainder: usize) -> Vec<u8> {
    let length = chunks * 65_536 + remainder;
    (0..length)
        .map(|index| u8::try_from(index % 251).unwrap_or(0))
        .collect()
}

fn open_store(label: &str) -> (std::path::PathBuf, BlobStore) {
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the store opens against a fresh directory");
    (root, store)
}

fn synthetic_zip() -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("synthetic export entry starts");
    writer
        .write_all(include_bytes!("fixtures/synthetic_consumer_export.json"))
        .expect("synthetic export entry writes");
    writer
        .finish()
        .expect("synthetic archive closes")
        .into_inner()
}

fn initial_worker(
    database: &ratatoskr_claude_archive::Database,
    store: &BlobStore,
) -> ratatoskr_claude_archive::InitialImportWorker {
    ratatoskr_claude_archive::InitialImportWorker::new(
        database.pool().clone(),
        store.clone(),
        Arc::new(
            ratatoskr_claude_archive::ParserRegistry::runtime()
                .expect("runtime parser registry is valid"),
        ),
        ratatoskr_claude_archive::Limits {
            database_connections: 2,
            database_acquire_timeout_ms: 5_000,
            shutdown_timeout_ms: 5_000,
            max_archive_bytes: 1_048_576,
            max_archive_entries: 32,
            max_entry_bytes: 1_048_576,
            max_total_extracted_bytes: 2_097_152,
            max_compression_ratio: 100,
        },
    )
}

async fn seed_account(pool: &sqlx::PgPool) -> Uuid {
    let account_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account_id)
    .bind(format!("acct-receipt-{account_id}"))
    .execute(pool)
    .await
    .expect("the account inserts");
    account_id
}

async fn seed_organization(pool: &sqlx::PgPool) -> Uuid {
    let organization_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.organizations (organization_id, external_organization_id)
         values ($1, $2)",
    )
    .bind(organization_id)
    .bind(format!("org-receipt-{organization_id}"))
    .execute(pool)
    .await
    .expect("the organization inserts");
    organization_id
}

async fn export_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query("select count(*) as n from claude_archive.exports")
        .fetch_one(pool)
        .await
        .expect("the count query succeeds")
        .get::<i64, _>("n")
}

/// Every regular file beneath the store's digest tree.
fn object_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let buckets = fs::read_dir(root.join("sha256")).expect("the digest bucket layer exists");
    for bucket in buckets {
        let bucket = bucket.expect("bucket entries are readable");
        if !bucket.file_type().expect("entry type is readable").is_dir() {
            continue;
        }
        for object in fs::read_dir(bucket.path()).expect("objects are listable") {
            let object = object.expect("object entries are readable");
            if object.file_type().expect("type is readable").is_file() {
                found.push(object.path());
            }
        }
    }
    found
}

#[tokio::test]
async fn authenticated_stored_receipt_records_export_run_and_readable_bytes() {
    use sha2::{Digest as _, Sha256};

    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-stored");
    let payload = multi_chunk_payload(3, 137);

    let outcome = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect("an authenticated delivery is accepted");

    let (export_id, run_id, blob_ref) = match &outcome {
        ReceiptOutcome::Stored {
            export_id,
            run_id,
            blob_ref,
        } => (*export_id, *run_id, blob_ref),
        ReceiptOutcome::Duplicate {
            existing_export_id: _,
        } => panic!("expected a stored outcome, got duplicate: {outcome:?}"),
    };

    // The export row records exactly what arrived.
    let row = sqlx::query(
        "select acquisition, archive_hash, byte_size,
                (received_at is not null) as has_received_at
         from claude_archive.exports where export_id = $1",
    )
    .bind(export_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the export row exists");
    assert_eq!(
        row.get::<String, _>("acquisition"),
        "consumer_export",
        "the acquisition mode is recorded"
    );
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let expected_digest = hasher.finalize().to_vec();
    assert_eq!(
        row.get::<Vec<u8>, _>("archive_hash"),
        expected_digest,
        "the recorded digest matches an independent one-shot hash"
    );
    assert_eq!(
        row.get::<i64, _>("byte_size"),
        i64::try_from(payload.len()).expect("length fits"),
        "the recorded size counts every delivered byte"
    );
    assert!(
        row.get::<bool, _>("has_received_at"),
        "received_at carries the delivery moment"
    );

    // The initial import run exists and reads back through its own store.
    let runs = ImportRunStore::new(&db.database);
    assert_eq!(
        runs.current(run_id).await.expect("the run reads back"),
        ratatoskr_claude_archive::ImportState::Received,
        "the fresh run begins at received"
    );

    // The raw bytes survive verbatim behind their reference.
    let read_back = store.read(blob_ref).expect("the reference resolves");
    assert_eq!(
        read_back, payload,
        "the stored bytes equal the delivered archive"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn duplicate_receipt_names_existing_export_without_second_row_or_object() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-duplicate");
    let payload = multi_chunk_payload(1, 999);

    let first = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect("the first delivery is stored");

    let second = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect("the second delivery reports a verdict rather than failing");

    let first_export_id = match &first {
        ReceiptOutcome::Stored { export_id, .. } => *export_id,
        ReceiptOutcome::Duplicate {
            existing_export_id: _,
        } => panic!("expected the first attempt to store, got duplicate: {first:?}"),
    };
    assert_eq!(
        second,
        ReceiptOutcome::Duplicate {
            existing_export_id: first_export_id
        },
        "the duplicate names the existing export"
    );
    assert_eq!(
        export_count(db.database.pool()).await,
        1,
        "exactly one export row exists for the digest"
    );
    assert_eq!(
        object_files(&blob_root).len(),
        1,
        "exactly one raw object holds the bytes"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn equal_digest_in_two_tenants_creates_two_isolated_imports() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let first_account = seed_account(db.database.pool()).await;
    let second_account = seed_account(db.database.pool()).await;
    let (blob_root, store) = open_store("receipt-cross-tenant-digest");
    let payload = multi_chunk_payload(2, 7);

    for account in [first_account, second_account] {
        let outcome = ratatoskr_claude_archive::receipt::receive_archive(
            &db.database,
            &store,
            &TenantClaim {
                account: Some(account),
                organization: None,
            },
            AcquisitionMode::ConsumerExport,
            std::io::Cursor::new(&payload),
            10 * 1024 * 1024 * 1024,
        )
        .await
        .expect("equal bytes remain isolated by owning tenant");
        assert!(matches!(outcome, ReceiptOutcome::Stored { .. }));
    }

    assert_eq!(export_count(db.database.pool()).await, 2);
    assert_eq!(
        object_files(&blob_root).len(),
        1,
        "content-addressed raw bytes may deduplicate without sharing tenant import state"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn distinct_archives_yield_distinct_exports() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let organization_id = seed_organization(db.database.pool()).await;
    let claim = TenantClaim {
        account: None,
        organization: Some(organization_id),
    };
    let (blob_root, store) = open_store("receipt-distinct");

    let first = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::OrganizationExport,
        std::io::Cursor::new(multi_chunk_payload(1, 11)),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect("the first archive stores");
    let second = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::OrganizationExport,
        std::io::Cursor::new(multi_chunk_payload(1, 22)),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect("the second archive stores");

    assert!(matches!(first, ReceiptOutcome::Stored { .. }));
    assert!(matches!(second, ReceiptOutcome::Stored { .. }));
    assert_ne!(
        first, second,
        "different digests produce different stored outcomes"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn unknown_tenant_principal_is_refused_before_any_storage() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let claim = TenantClaim {
        account: Some(Uuid::now_v7()), // no such account row
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-unknown-tenant");

    let refused = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(multi_chunk_payload(1, 33)),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect_err("an unresolvable principal must be refused");

    assert!(
        matches!(refused, ReceiptError::UnknownTenant),
        "expected an unknown-tenant refusal, got: {refused}"
    );
    assert_eq!(
        export_count(db.database.pool()).await,
        0,
        "nothing was recorded for the refused delivery"
    );
    assert_eq!(
        object_files(&blob_root).len(),
        0,
        "no bytes were stored for the refused delivery"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn ambiguous_scope_principal_is_refused() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let organization_id = seed_organization(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: Some(organization_id),
    };
    let (blob_root, store) = open_store("receipt-ambiguous");

    let refused = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(multi_chunk_payload(1, 44)),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect_err("a claim on both scopes must be refused");

    assert!(
        matches!(refused, ReceiptError::AmbiguousScope),
        "expected an ambiguous-scope refusal, got: {refused}"
    );
    assert_eq!(
        export_count(db.database.pool()).await,
        0,
        "nothing was recorded for the refused delivery"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn oversized_stream_is_refused_with_cap_error_and_no_durable_state() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-cap");
    let payload = multi_chunk_payload(1, 5001);

    let refused = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        1024,
    )
    .await
    .expect_err("an oversized delivery must be refused mid-stream");

    match &refused {
        ReceiptError::ArchiveTooLarge { limit_bytes } => {
            assert_eq!(*limit_bytes, 1024, "the refusal names the cap");
        }
        other => panic!("expected an archive-too-large refusal, got: {other}"),
    }
    assert_eq!(
        export_count(db.database.pool()).await,
        0,
        "no export row survives a refused stream"
    );
    let staging_entries = fs::read_dir(blob_root.join("staging")).expect("staging is listable");
    assert_eq!(
        staging_entries.count(),
        0,
        "no staged file survives the refusal"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn empty_stream_is_refused() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-empty");
    let empty: Vec<u8> = Vec::new();

    let refused = ratatoskr_claude_archive::receipt::receive_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&empty),
        10 * 1024 * 1024 * 1024,
    )
    .await
    .expect_err("an empty delivery has nothing worth archiving");

    assert!(
        matches!(refused, ReceiptError::EmptyDelivery),
        "expected an empty-delivery refusal, got: {refused}"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn platform_archive_receipt_refuses_mismatched_digest_without_storing() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-platform-mismatch");
    let payload = b"platform receipt identity mismatch";

    let refused = ratatoskr_claude_archive::receipt::receive_platform_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(payload),
        10 * 1024 * 1024 * 1024,
        &ArchiveIdentity {
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            byte_size: u64::try_from(payload.len()).expect("the fixture length fits"),
        },
    )
    .await
    .expect_err("a Platform identity mismatch must be refused");

    assert!(
        matches!(refused, ReceiptError::DeclaredIdentityMismatch),
        "the refusal identifies the supplied digest/length contract, got: {refused}"
    );
    assert_eq!(
        export_count(db.database.pool()).await,
        0,
        "no export row survives a declared identity mismatch"
    );
    assert!(
        object_files(&blob_root).is_empty(),
        "no raw object is published for a declared identity mismatch"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn raw_receipt_is_nonterminal_until_restart_safe_import_completes() {
    use sha2::{Digest as _, Sha256};

    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-platform-terminal-report");
    let payload = synthetic_zip();
    let digest = format!("{:x}", Sha256::digest(&payload));
    let operation_id = Uuid::now_v7();

    ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        10 * 1024 * 1024 * 1024,
        &ArchiveIdentity {
            sha256: digest,
            byte_size: u64::try_from(payload.len()).expect("the fixture length fits"),
        },
        PlatformOperation { operation_id },
    )
    .await
    .expect("a verified Platform archive is stored");

    let reports = sqlx::query(
        "select envelope from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1' and aggregate_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_all(db.database.pool())
    .await
    .expect("the outbox query succeeds");
    assert!(
        reports.is_empty(),
        "raw persistence is nonterminal; parser/import completion owns the report"
    );

    let worker = initial_worker(&db.database, &store);
    assert_eq!(
        worker
            .process_pending_once()
            .await
            .expect("restart worker imports the durable ZIP"),
        1
    );
    let envelope: serde_json::Value = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1' and aggregate_id = $1",
    )
    .bind(operation_id.to_string())
    .fetch_one(db.database.pool())
    .await
    .expect("terminal report is durable after import");
    assert_eq!(envelope["producer"], "ratatoskr-claude");
    assert_eq!(envelope["event_type"], "platform.operation.reported.v1");
    assert!(envelope["event_id"].as_str().is_some());
    let summary = &envelope["payload"]["results"][0]["ai_archive_import_summary"];
    assert_eq!(summary["conversation_count"], 1);
    assert!(
        summary["message_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn permanent_import_failure_is_terminal_and_reported_once() {
    use sha2::{Digest as _, Sha256};

    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-platform-invalid-terminal");
    let payload = b"not a zip archive";
    let operation_id = Uuid::now_v7();
    ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        &db.database,
        &store,
        &claim,
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(payload),
        10 * 1024 * 1024 * 1024,
        &ArchiveIdentity {
            sha256: format!("{:x}", Sha256::digest(payload)),
            byte_size: u64::try_from(payload.len()).expect("fixture length fits"),
        },
        PlatformOperation { operation_id },
    )
    .await
    .expect("the invalid archive remains durable for terminal classification");

    let worker = initial_worker(&db.database, &store);
    assert_eq!(
        worker
            .process_pending_once()
            .await
            .expect("a permanent parser failure is handled"),
        1
    );
    let state: String = sqlx::query_scalar(
        "select r.state from claude_archive.import_runs r
         join claude_archive.platform_operation_imports p on p.import_run_id=r.run_id
         where p.operation_id=$1",
    )
    .bind(operation_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the failed run remains queryable");
    assert_eq!(state, "failed");
    let envelope: serde_json::Value = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type='platform.operation.reported.v1' and aggregate_id=$1",
    )
    .bind(operation_id.to_string())
    .fetch_one(db.database.pool())
    .await
    .expect("one safe terminal report is durable");
    assert_eq!(envelope["payload"]["status"], "failed");
    assert_eq!(envelope["payload"]["error"]["retryable"], false);
    assert_eq!(
        envelope["payload"]["error"]["code"],
        "claude.archive.invalid"
    );

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn duplicate_digest_reports_terminal_result_for_each_bound_operation() {
    use sha2::{Digest as _, Sha256};

    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account_id = seed_account(db.database.pool()).await;
    let claim = TenantClaim {
        account: Some(account_id),
        organization: None,
    };
    let (blob_root, store) = open_store("receipt-platform-duplicate-operations");
    let payload = synthetic_zip();
    let digest = format!("{:x}", Sha256::digest(&payload));
    let operations = [Uuid::now_v7(), Uuid::now_v7()];

    for operation_id in operations {
        ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
            &db.database,
            &store,
            &claim,
            AcquisitionMode::ConsumerExport,
            std::io::Cursor::new(&payload),
            10 * 1024 * 1024 * 1024,
            &ArchiveIdentity {
                sha256: digest.clone(),
                byte_size: u64::try_from(payload.len()).expect("fixture length fits"),
            },
            PlatformOperation { operation_id },
        )
        .await
        .expect("duplicate receipt must retain its operation binding");
    }

    assert_eq!(export_count(db.database.pool()).await, 1);
    let bound: i64 =
        sqlx::query_scalar("select count(*) from claude_archive.platform_operation_imports")
            .fetch_one(db.database.pool())
            .await
            .expect("operation bindings are queryable");
    assert_eq!(bound, 2, "both Platform operations remain bound");
    let worker = initial_worker(&db.database, &store);
    assert_eq!(
        worker
            .process_pending_once()
            .await
            .expect("one import processes both bound operations"),
        1
    );
    let reports: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1'
           and aggregate_id = any($1)",
    )
    .bind(operations.map(|operation| operation.to_string()).to_vec())
    .fetch_one(db.database.pool())
    .await
    .expect("operation reports are queryable");
    assert_eq!(reports, 2, "every bound operation receives one result");

    remove(&blob_root);
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn failed_operation_report_publication_leaves_the_outbox_row_pending() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let event_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.outbox_events
             (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
              correlation_id, occurred_at)
         values ($1, 'platform.operation.reported.v1', 'operation', $2, '{}'::jsonb,
                 decode(repeat('00', 32), 'hex'), $3, now())",
    )
    .bind(event_id)
    .bind(Uuid::now_v7().to_string())
    .bind(format!("event:{event_id}"))
    .execute(db.database.pool())
    .await
    .expect("the pending report inserts");

    let publisher = OperationReportOutbox::new(db.database.pool().clone());
    let missing_credentials = std::path::Path::new("/missing/claude-operation-report.nkey");
    let failure = publisher
        .publish_pending_once("nats://127.0.0.1:1", missing_credentials)
        .await
        .expect_err("an unreachable broker leaves the durable report pending");
    assert!(
        failure.to_string().contains("broker"),
        "the bounded publisher reports the broker class"
    );
    let was_published: bool = sqlx::query_scalar(
        "select published_at is not null from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the row remains readable");
    assert!(!was_published, "the failed report remains pending");

    db.cleanup().await.expect("cleanup succeeds");
}
