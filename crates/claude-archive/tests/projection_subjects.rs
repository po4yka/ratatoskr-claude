//! The added/updated/unchanged decision of an Artifact fact looks only at the facts of the same
//! tenant and the same scope (XR-021 CONTRACTS.md S07): provider Artifact ids such as
//! `react-component` recur across tenants and across the conversations of one tenant, and one
//! subject's outbox content must never steer another's events. A project the contract cannot
//! name is counted as a gap on the import head, never an error that retries the import forever.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::io::Write as _;

mod synthetic_import;

use ratatoskr_ai_archive_contracts::{AiArchiveImport, AiArtifactAdded, AiArtifactUpdated};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{BlobStore, PlatformUsers};
use ratatoskr_event_envelope::EventEnvelope;
use synthetic_import::{account, receive, versions, worker, zip};
use uuid::Uuid;

const USER_ONE: &str = "018f0000-0000-7000-8000-0000000000c1";
const USER_TWO: &str = "018f0000-0000-7000-8000-0000000000c2";
const SHARED_ID: &str = "react-component";

/// Every Artifact fact in the outbox, in publication order, as its event type and envelope.
async fn artifact_facts(db: &TestDatabase) -> Vec<(String, EventEnvelope)> {
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type like 'ai_archive.artifact.%' order by occurred_at, event_id",
    )
    .fetch_all(db.database.pool())
    .await
    .expect("the Artifact facts are readable");
    rows.into_iter()
        .map(|raw| {
            let envelope: EventEnvelope = serde_json::from_value(raw).expect("a complete envelope");
            (envelope.event_type.to_wire(), envelope)
        })
        .collect()
}

fn owner_of(envelope: &EventEnvelope, event_type: &str) -> String {
    if event_type == "ai_archive.artifact.added.v1" {
        envelope
            .payload_as::<AiArtifactAdded>()
            .expect("the payload decodes")
            .artifact
            .owner
            .to_string()
    } else {
        envelope
            .payload_as::<AiArtifactUpdated>()
            .expect("the payload decodes")
            .artifact
            .owner
            .to_string()
    }
}

#[tokio::test]
async fn the_same_artifact_id_in_two_tenants_is_added_for_each() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let (first, second) = (account(&db).await, account(&db).await);
    let users = PlatformUsers::new(&[
        (Uuid::parse_str(USER_ONE).expect("user UUID"), first),
        (Uuid::parse_str(USER_TWO).expect("user UUID"), second),
    ]);
    let root = temp_root("tenancy-two-tenants");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users);
    let artifacts = serde_json::json!([{
        "id": SHARED_ID, "type": "text/markdown", "project_id": "project-garden",
        "versions": versions()
    }]);
    for (account, note) in [(first, "tenant one"), (second, "tenant two")] {
        receive(&db, &store, account, &zip(note, artifacts.clone(), false)).await;
        assert_eq!(
            worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1
        );
    }
    let facts = artifact_facts(&db).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let summary: Vec<(&str, String)> = facts
        .iter()
        .map(|(event_type, envelope)| (event_type.as_str(), owner_of(envelope, event_type)))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("ai_archive.artifact.added.v1", format!("user:{USER_ONE}")),
            ("ai_archive.artifact.added.v1", format!("user:{USER_TWO}")),
        ],
        "each tenant's first observation is an addition, whatever another tenant published"
    );
}

/// An Artifact with no conversation, project or message has the narrowest scope, so only the
/// tenant separates one tenant's Artifact from another's of the same provider id.
#[tokio::test]
async fn an_unscoped_artifact_id_in_two_tenants_is_added_for_each() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let (first, second) = (account(&db).await, account(&db).await);
    let users = PlatformUsers::new(&[
        (Uuid::parse_str(USER_ONE).expect("user UUID"), first),
        (Uuid::parse_str(USER_TWO).expect("user UUID"), second),
    ]);
    let root = temp_root("tenancy-unscoped");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users);
    let artifacts = serde_json::json!([{
        "id": SHARED_ID, "type": "text/markdown", "versions": versions()
    }]);
    for (account, note) in [(first, "tenant one"), (second, "tenant two")] {
        receive(&db, &store, account, &zip(note, artifacts.clone(), false)).await;
        assert_eq!(
            worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1
        );
    }
    let facts = artifact_facts(&db).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let summary: Vec<(&str, String)> = facts
        .iter()
        .map(|(event_type, envelope)| (event_type.as_str(), owner_of(envelope, event_type)))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("ai_archive.artifact.added.v1", format!("user:{USER_ONE}")),
            ("ai_archive.artifact.added.v1", format!("user:{USER_TWO}")),
        ],
        "an Artifact with no scope is still new to a tenant that never published it"
    );
}

/// The import identifies an Artifact by account, scope and provider id, so one export cannot hold
/// two Artifacts of one id; two exports of one account can, one per conversation.
#[tokio::test]
async fn the_same_artifact_id_in_two_conversations_is_added_for_each_and_stays_quiet_on_reimport() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = account(&db).await;
    let users = PlatformUsers::new(&[(Uuid::parse_str(USER_ONE).expect("user UUID"), account)]);
    let root = temp_root("tenancy-two-conversations");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users);
    let in_planting = serde_json::json!([
        {"id": SHARED_ID, "type": "text/markdown", "conversation_id": "conversation-planting",
         "versions": versions()},
        {"id": "react-message", "type": "text/markdown",
         "conversation_id": "conversation-planting", "message_id": "message-assistant-1",
         "versions": versions()}
    ]);
    let in_pruning = serde_json::json!([
        {"id": SHARED_ID, "type": "text/markdown", "conversation_id": "conversation-pruning",
         "versions": versions()},
        {"id": "react-message", "type": "text/markdown",
         "conversation_id": "conversation-pruning", "message_id": "message-user-2",
         "versions": versions()}
    ]);
    for (note, artifacts, both_conversations) in [
        ("first export", &in_planting, false),
        ("second export", &in_pruning, true),
        ("first content again", &in_planting, true),
        ("second content again", &in_pruning, true),
    ] {
        receive(
            &db,
            &store,
            account,
            &zip(note, artifacts.clone(), both_conversations),
        )
        .await;
        assert_eq!(
            worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1
        );
    }
    let facts = artifact_facts(&db).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let types: Vec<&str> = facts
        .iter()
        .map(|(event_type, _)| event_type.as_str())
        .collect();
    assert_eq!(
        types,
        vec!["ai_archive.artifact.added.v1"; 4],
        "Artifacts of one id in two conversations, directly or through a message, are four \
         additions, and re-importing either conversation's content adds no update"
    );
}

#[tokio::test]
async fn a_project_the_contract_cannot_name_is_a_gap_and_does_not_fail_the_import() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = account(&db).await;
    let users = PlatformUsers::new(&[(Uuid::parse_str(USER_ONE).expect("user UUID"), account)]);
    let root = temp_root("projection-unrepresentable-project");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users);
    let mut export: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/synthetic_consumer_export.json"))
            .expect("the base fixture is JSON");
    export
        .get_mut("projects")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the base fixture lists projects")
        .push(serde_json::json!({
            "id": "bad\u{7}id",
            "name": "bad\u{7}name",
            "knowledge_files": []
        }));
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("the export entry starts");
    writer
        .write_all(&serde_json::to_vec(&export).expect("the export serializes"))
        .expect("the export entry writes");
    let archive = writer.finish().expect("the archive closes").into_inner();
    receive(&db, &store, account, &archive).await;

    let processed = worker.process_pending_once().await;
    let state: String = sqlx::query_scalar("select state from claude_archive.import_runs")
        .fetch_one(db.database.pool())
        .await
        .expect("the import run is readable");
    let heads: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type = 'ai_archive.archive.imported.v1'",
    )
    .fetch_all(db.database.pool())
    .await
    .expect("the heads are readable");
    let projects: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'ai_archive.project.added.v1'",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("the projects are countable");
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        processed.expect("the import is not an error that would be retried forever"),
        1
    );
    assert!(
        matches!(state.as_str(), "completed" | "partial"),
        "the import finishes instead of failing: {state}"
    );
    assert_eq!(
        projects, 2,
        "the two projects the contract can name are announced"
    );
    let head: EventEnvelope =
        serde_json::from_value(heads.into_iter().next().expect("one import head"))
            .expect("a complete envelope");
    let head = head
        .payload_as::<AiArchiveImport>()
        .expect("the head decodes");
    head.validate().expect("the head satisfies its invariants");
    let gap = head
        .completeness_report
        .gaps
        .iter()
        .find(|gap| gap.gap_kind.as_str() == "unprojected_project")
        .expect("the project the contract cannot name is a gap");
    assert_eq!(gap.affected_count, Some(1));
    assert!(!head.completeness_report.completeness.is_complete());
}
