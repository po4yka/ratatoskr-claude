//! Shared setup for the integration tests that drive the real receipt and import path with the
//! documented synthetic export. Each test file declares `mod synthetic_import;` and owns its own
//! assertions.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::io::Write as _;
use std::sync::Arc;

use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, InitialImportWorker, Limits, ParserRegistry,
    PlatformOperation, PlatformUsers, TenantClaim,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub(crate) fn limits() -> Limits {
    Limits {
        database_connections: 2,
        database_acquire_timeout_ms: 5_000,
        shutdown_timeout_ms: 5_000,
        max_archive_bytes: 1_048_576,
        max_archive_entries: 32,
        max_entry_bytes: 1_048_576,
        max_total_extracted_bytes: 2_097_152,
        max_compression_ratio: 100,
    }
}

pub(crate) fn versions() -> serde_json::Value {
    serde_json::json!([{
        "id": "react-component-v1",
        "media_type": "text/markdown",
        "sha256": "04e8d1a2038648c55e814e55637df9468bc304abfa046ff461ed511a7f4773cf",
        "bytes": [35, 32, 71, 97, 114, 100, 101, 110, 10]
    }])
}

/// The synthetic export with `artifacts` and, when `second_conversation` is set, a second
/// conversation; `note` makes each export a new archive.
pub(crate) fn zip(note: &str, artifacts: serde_json::Value, second_conversation: bool) -> Vec<u8> {
    let mut export: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/synthetic_consumer_export.json"))
            .expect("the base fixture is JSON");
    let object = export
        .as_object_mut()
        .expect("the base fixture is an object");
    object.insert("artifacts".to_owned(), artifacts);
    if second_conversation {
        object
            .get_mut("conversations")
            .and_then(serde_json::Value::as_array_mut)
            .expect("the base fixture lists conversations")
            .push(serde_json::json!({
                "id": "conversation-pruning",
                "title": "Pruning",
                "created_at": "2026-08-02T08:00:00Z",
                "messages": [{
                    "id": "message-user-2",
                    "role": "user",
                    "content": [{"type": "text", "text": "When should roses be pruned?"}]
                }]
            }));
    }
    object
        .get_mut("projects")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|projects| projects.get_mut(1))
        .and_then(serde_json::Value::as_object_mut)
        .expect("the second project exists")
        .insert("description".to_owned(), serde_json::json!(note));
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("the export entry starts");
    writer
        .write_all(&serde_json::to_vec(&export).expect("the export serializes"))
        .expect("the export entry writes");
    writer.finish().expect("the archive closes").into_inner()
}

pub(crate) async fn account(db: &TestDatabase) -> Uuid {
    let account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account)
    .bind(format!("tenancy-{account}"))
    .execute(db.database.pool())
    .await
    .expect("the account inserts");
    account
}

pub(crate) async fn receive(db: &TestDatabase, store: &BlobStore, account: Uuid, archive: &[u8]) {
    ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        &db.database,
        store,
        &TenantClaim {
            account: Some(account),
            organization: None,
        },
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(archive),
        10 * 1024 * 1024 * 1024,
        &ArchiveIdentity {
            sha256: format!("{:x}", Sha256::digest(archive)),
            byte_size: u64::try_from(archive.len()).expect("the fixture length fits"),
        },
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await
    .expect("the archive is received");
}

pub(crate) fn worker(
    db: &TestDatabase,
    store: &BlobStore,
    users: PlatformUsers,
) -> InitialImportWorker {
    InitialImportWorker::new(
        db.database.pool().clone(),
        store.clone(),
        Arc::new(ParserRegistry::runtime().expect("the runtime registry is valid")),
        limits(),
        users,
    )
}
