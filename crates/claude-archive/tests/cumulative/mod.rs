//! Shared setup for the integration tests that import several exports of one account through the
//! real receipt and import path. Each test file declares `mod cumulative;` and owns its
//! assertions.
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
use sqlx::PgPool;
use uuid::Uuid;

/// The Platform user the account is mapped to.
pub(crate) const USER: &str = "018f0000-0000-7000-8000-0000000000c1";

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

/// The documented synthetic export: two projects, one conversation, no Artifact.
pub(crate) fn base_export() -> serde_json::Value {
    serde_json::from_str(include_str!("../fixtures/synthetic_consumer_export.json"))
        .expect("the base fixture is JSON")
}

/// A ZIP archive holding `export` as its one entry.
pub(crate) fn zip_of(export: &serde_json::Value) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("the export entry starts");
    writer
        .write_all(&serde_json::to_vec(export).expect("the export serializes"))
        .expect("the export entry writes");
    writer.finish().expect("the archive closes").into_inner()
}

/// A new archive account.
pub(crate) async fn add_account(db: &TestDatabase) -> Uuid {
    let account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account)
    .bind(format!("series-{account}"))
    .execute(db.database.pool())
    .await
    .expect("the account inserts");
    account
}

/// An import worker over the series' database and blob store.
pub(crate) fn worker_over(
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

/// One account whose exports are received in order and imported by the real worker.
pub(crate) struct Series {
    pub(crate) db: TestDatabase,
    pub(crate) store: BlobStore,
    pub(crate) root: std::path::PathBuf,
    pub(crate) account: Uuid,
    pub(crate) worker: InitialImportWorker,
}

impl Series {
    /// A disposable database, a blob store and an account mapped to [`USER`].
    pub(crate) async fn open(label: &str) -> Self {
        let db = TestDatabase::create()
            .await
            .expect("a disposable database applies the definition");
        let account = add_account(&db).await;
        let root = temp_root(label);
        let store = BlobStore::open(&root).expect("the blob store opens");
        let users = PlatformUsers::new(&[(Uuid::parse_str(USER).expect("user UUID"), account)]);
        let worker = worker_over(&db, &store, users);
        Self {
            db,
            store,
            root,
            account,
            worker,
        }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        self.db.database.pool()
    }

    /// Receives `export` as a new archive of this series' account and returns its operation id
    /// without importing it.
    pub(crate) async fn receive(&self, export: &serde_json::Value) -> Uuid {
        self.receive_as(self.account, export).await
    }

    /// Receives `export` as a new archive of `account`.
    pub(crate) async fn receive_as(&self, account: Uuid, export: &serde_json::Value) -> Uuid {
        let archive = zip_of(export);
        let operation_id = Uuid::now_v7();
        ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
            &self.db.database,
            &self.store,
            &TenantClaim {
                account: Some(account),
                organization: None,
            },
            AcquisitionMode::ConsumerExport,
            std::io::Cursor::new(&archive),
            10 * 1024 * 1024 * 1024,
            &ArchiveIdentity {
                sha256: format!("{:x}", Sha256::digest(&archive)),
                byte_size: u64::try_from(archive.len()).expect("the fixture length fits"),
            },
            PlatformOperation { operation_id },
        )
        .await
        .expect("the archive is received");
        operation_id
    }

    /// Receives and imports `export`; returns its export id and operation id.
    pub(crate) async fn import(&self, export: &serde_json::Value) -> (Uuid, Uuid) {
        let operation = self.receive(export).await;
        assert_eq!(
            self.worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1,
            "one pass imports the archive"
        );
        let export_id: Uuid = sqlx::query_scalar(
            "select e.export_id from claude_archive.exports e
             join claude_archive.platform_operation_imports p on p.export_id = e.export_id
             where p.operation_id = $1",
        )
        .bind(operation)
        .fetch_one(self.pool())
        .await
        .expect("the operation is bound to its export");
        (export_id, operation)
    }

    pub(crate) async fn finish(self) {
        remove(&self.root);
        self.db.cleanup().await.expect("cleanup succeeds");
    }
}
