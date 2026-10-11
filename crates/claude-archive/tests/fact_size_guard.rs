//! A fact larger than the event limit is not enqueued, and the import says so (XR-021 CONTRACTS.md R2-04 rule 4): `ChatGPT` and Claude do not truncate or split conversations, so the user is told the truth instead of seeing a success whose conversation Knowledge never receives.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;

use ratatoskr_ai_archive_contracts::platform_receipt::{
    INCOMPLETE_IMPORT_WARNING_CODE, incomplete_import_warning,
};
use ratatoskr_ai_archive_contracts::{AiArchiveImport, AiConversationAdded};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, InitialImportWorker, Limits, ParserRegistry,
    PlatformOperation, PlatformUsers, TenantClaim,
};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_operation_contracts::{OperationReported, OperationStatus};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const USER: &str = "018f0000-0000-7000-8000-0000000000c1";
/// The smallest limit configuration accepts.
const LIMIT: usize = 65_536;

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

async fn account(db: &TestDatabase) -> Uuid {
    let account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account)
    .bind(format!("size-guard-{account}"))
    .execute(db.database.pool())
    .await
    .expect("the account inserts");
    account
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

fn worker(db: &TestDatabase, store: &BlobStore, users: PlatformUsers) -> InitialImportWorker {
    InitialImportWorker::new(
        db.database.pool().clone(),
        store.clone(),
        Arc::new(ParserRegistry::runtime().expect("the runtime registry is valid")),
        limits(),
        users,
    )
}

/// `length` hexadecimal characters of a hash chain: archive inspection refuses a compression ratio a run of one letter would exceed.
fn incompressible(length: usize) -> String {
    let mut text = String::new();
    let mut block = Sha256::digest(b"fact-size-guard");
    while text.len() < length {
        write!(text, "{block:x}").expect("writing to a String cannot fail");
        block = Sha256::digest(block);
    }
    text.truncate(length);
    text
}

/// The synthetic export plus one conversation of three 30 000 character messages, which no 65 536 byte envelope can hold.
fn zip_with_a_huge_conversation() -> Vec<u8> {
    let mut export: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/synthetic_consumer_export.json"))
            .expect("the base fixture is JSON");
    let text = incompressible(30_000);
    let messages: Vec<serde_json::Value> = (0..3)
        .map(|index| {
            serde_json::json!({
                "id": format!("huge-message-{index}"),
                "role": if index % 2 == 0 { "user" } else { "assistant" },
                "content": [{"type": "text", "text": text}]
            })
        })
        .collect();
    export
        .get_mut("conversations")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the base fixture lists conversations")
        .push(serde_json::json!({
            "id": "conversation-huge",
            "title": "Huge",
            "created_at": "2026-08-02T08:00:00Z",
            "messages": messages
        }));
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("the export entry starts");
    writer
        .write_all(&serde_json::to_vec(&export).expect("the export serializes"))
        .expect("the export entry writes");
    writer.finish().expect("the archive closes").into_inner()
}

async fn envelopes(db: &TestDatabase, like: &str) -> Vec<EventEnvelope> {
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type like $1 order by occurred_at, event_id",
    )
    .bind(like)
    .fetch_all(db.database.pool())
    .await
    .expect("the outbox is readable");
    rows.into_iter()
        .map(|raw| serde_json::from_value(raw).expect("a complete envelope"))
        .collect()
}

#[tokio::test]
async fn a_fact_over_the_event_limit_is_not_enqueued_and_the_import_is_partial() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = account(&db).await;
    let root = temp_root("fact-size-guard");
    let store = BlobStore::open(&root).expect("the blob store opens");
    receive(&db, &store, account, &zip_with_a_huge_conversation()).await;
    let users = PlatformUsers::new(&[(Uuid::parse_str(USER).expect("user UUID"), account)]);
    let imported = worker(&db, &store, users)
        .with_max_event_bytes(LIMIT)
        .process_pending_once()
        .await;
    assert_eq!(imported.expect("the import completes"), 1);

    let conversations = envelopes(&db, "ai_archive.conversation.added.v1").await;
    let heads = envelopes(&db, "ai_archive.archive.imported.v1").await;
    let reports = envelopes(&db, "platform.operation.reported.v1").await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let announced: Vec<String> = conversations
        .iter()
        .map(|envelope| {
            envelope
                .payload_as::<AiConversationAdded>()
                .expect("the payload decodes")
                .conversation
                .external_conversation_id
                .map(|id| id.to_string())
                .unwrap_or_default()
        })
        .collect();
    assert!(
        !announced.iter().any(|id| id == "conversation-huge"),
        "the conversation no 65 536 byte envelope can hold is not enqueued: {announced:?}"
    );
    assert_eq!(
        announced.len(),
        1,
        "the other conversation is enqueued: {announced:?}"
    );
    for envelope in &conversations {
        assert!(
            serde_json::to_vec(envelope)
                .expect("an envelope serializes")
                .len()
                <= LIMIT,
            "an enqueued fact fits the limit"
        );
    }

    let head = heads
        .first()
        .expect("the import head is enqueued")
        .payload_as::<AiArchiveImport>()
        .expect("the head decodes");
    assert!(
        !head.completeness_report.completeness.is_complete(),
        "an archive that lost a conversation is never announced as complete"
    );

    let [report] = <[EventEnvelope; 1]>::try_from(reports).expect("one operation report");
    let report = report
        .payload_as::<OperationReported>()
        .expect("the report decodes");
    report
        .validate()
        .expect("the report satisfies the operation contract");
    assert_eq!(
        report.status,
        OperationStatus::PartiallySucceeded,
        "the user is not told the import succeeded"
    );
    let codes: Vec<String> = report
        .warnings
        .iter()
        .map(|warning| warning.code.to_string())
        .collect();
    assert!(
        codes
            .iter()
            .any(|code| code == "ai_archive.facts.too_large"),
        "the report names the too-large facts: {codes:?}"
    );
    assert_eq!(
        codes
            .iter()
            .filter(|code| code.as_str() == INCOMPLETE_IMPORT_WARNING_CODE)
            .count(),
        1,
        "exactly one incomplete-import warning: {codes:?}"
    );
    assert!(
        report.warnings.contains(&incomplete_import_warning()),
        "the incomplete-import warning is the contract's"
    );
}
