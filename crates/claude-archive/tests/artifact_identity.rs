//! An Artifact has one identity per account: a second export that contains it again observes the
//! existing row instead of inserting a duplicate, whatever the Artifact's scope.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::io::Write as _;
use std::sync::Arc;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, InitialImportWorker, Limits, ParserRegistry,
    PlatformOperation, PlatformUsers, TenantClaim,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

fn limits() -> Limits {
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

/// One Artifact per scope the schema distinguishes, each with verified bytes.
fn artifacts() -> serde_json::Value {
    let version = |id: &str| {
        serde_json::json!([{
            "id": id,
            "media_type": "text/markdown",
            "sha256": "04e8d1a2038648c55e814e55637df9468bc304abfa046ff461ed511a7f4773cf",
            "bytes": [35, 32, 71, 97, 114, 100, 101, 110, 10]
        }])
    };
    serde_json::json!([
        {"id": "artifact-in-project", "type": "text/markdown", "project_id": "project-garden",
         "versions": version("artifact-in-project-v1")},
        {"id": "artifact-in-conversation", "type": "text/markdown",
         "conversation_id": "conversation-planting",
         "versions": version("artifact-in-conversation-v1")},
        {"id": "artifact-in-message", "type": "text/markdown",
         "conversation_id": "conversation-planting", "message_id": "message-assistant-1",
         "versions": version("artifact-in-message-v1")},
        {"id": "artifact-unscoped", "type": "text/markdown",
         "versions": version("artifact-unscoped-v1")}
    ])
}

/// The synthetic export with `artifacts`, made unique by `note` so each export is a new archive.
fn zip(note: &str) -> Vec<u8> {
    let mut export: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/synthetic_consumer_export.json"))
            .expect("the base fixture is JSON");
    let object = export
        .as_object_mut()
        .expect("the base fixture is an object");
    object.insert("artifacts".to_owned(), artifacts());
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

async fn receive(db: &TestDatabase, store: &BlobStore, account: Uuid, archive: &[u8]) {
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

#[tokio::test]
async fn a_second_export_of_the_same_account_does_not_duplicate_an_artifact() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account)
    .bind(format!("identity-{account}"))
    .execute(db.database.pool())
    .await
    .expect("the account inserts");
    let root = temp_root("artifact-identity");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = InitialImportWorker::new(
        db.database.pool().clone(),
        store.clone(),
        Arc::new(ParserRegistry::runtime().expect("the runtime registry is valid")),
        limits(),
        PlatformUsers::default(),
    );

    for note in ["first export", "second export"] {
        receive(&db, &store, account, &zip(note)).await;
        assert_eq!(
            worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1
        );
    }
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "select external_artifact_id, count(*), max((select count(*)
                 from claude_archive.export_observations o
                 where o.subject_kind = 'artifact' and o.subject_id = a.artifact_id))
         from claude_archive.artifacts a group by external_artifact_id
         order by external_artifact_id",
    )
    .fetch_all(db.database.pool())
    .await
    .expect("the Artifact rows are readable");
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        rows.iter()
            .map(|(external, rows, observations)| (external.as_str(), *rows, *observations))
            .collect::<Vec<_>>(),
        vec![
            ("artifact-in-conversation", 1, 2),
            ("artifact-in-message", 1, 2),
            ("artifact-in-project", 1, 2),
            ("artifact-unscoped", 1, 2),
        ],
        "each Artifact is one row observed by both exports"
    );
}
