//! Published AI-archive event conformance and durable outbox tests.

#![allow(clippy::expect_used, reason = "fixture assertions")]

use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    ArchiveEventFact, ArchiveOutbox, KnowledgeLinkError, KnowledgeLinkOutcome, KnowledgeLinkStore,
};
use ratatoskr_event_envelope::EventEnvelope;

// Exact published `ai_archive.archive.imported.v1` valid fixture, kept with the producer test so
// an updated contract cannot be accepted accidentally through an ad-hoc local shape.
const IMPORTED: &str = r#"{
  "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
  "provider":"chatgpt",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "source_export":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":2097152},
  "imported_at":"2026-08-18T11:30:00Z",
  "parser_name":"chatgpt_export",
  "parser_version":"2026.08.1",
  "completeness_report":{"completeness":"structurally_partial","conversation_count":1,"message_count":3,"asset_count":1,"gap_count":1,"gaps":[{"gap_kind":"undecodable_record","detail":"One attachment record could not be decoded.","affected_count":1}]},
  "warnings":[{"code":"ai_archive.export_unlisted_file_skipped","message":"A non-blocking problem was recorded."}]
}"#;

const ARTIFACT_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "artifact":{"external_artifact_id":"artifact-001","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","title":"Hello","artifact_kind":"artifact","content_blob":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"text/plain","length_bytes":5},"content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

// Exact published `ai_archive.project.added.v1` fixture. The sibling update fixture differs
// only in its current state, so the test below constructs that distinct event type from this
// contract shape and ensures both remain serializable without a local compatibility layer.
const PROJECT_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "project":{"ai_project_id":"018f0000-0000-7000-8000-000000000402","provider":"claude","title":"Archive project","parser_name":"claude_export","parser_version":"2026.08.1"},
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
}"#;

// The minimal current-state form from the published conversation fixture. It deliberately keeps
// the complete provenance and canonical content digest that Knowledge requires to index a revision.
const CONVERSATION_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "conversation":{"ai_conversation_id":"018f0000-0000-7000-8000-000000000403","provider":"claude","external_conversation_id":"conversation-001","owner":"user:018f0000-0000-7000-8000-000000000005","messages":[],"content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

const ARTIFACT_COMPLETION: &str = r#"{
  "ai_archive_id":"018f0000-0000-7000-8000-000000000401",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "subject":{"subject_kind":"artifact","external_artifact_id":"artifact-001"},
  "content_digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
  "completed_at":"2026-08-27T08:01:00Z"
}"#;

const CONVERSATION_TOMBSTONE: &str = r#"{
  "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
  "provider":"chatgpt",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "subject":{"subject_kind":"conversation","ai_conversation_id":"018f0000-0000-7000-8000-000000000403"},
  "reason":"provider_deletion_event",
  "evidence_ref":{"owner_service":"ratatoskr-chatgpt","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/json","length_bytes":512},
  "observed_at":"2026-08-27T06:00:00Z"
}"#;

#[test]
fn published_import_fixture_round_trips_without_dropping_provenance()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture: ratatoskr_ai_archive_contracts::AiArchiveImport = serde_json::from_str(IMPORTED)?;
    fixture.validate()?;
    assert_eq!(
        serde_json::to_value(&fixture)?,
        serde_json::from_str::<serde_json::Value>(IMPORTED)?
    );
    Ok(())
}

#[test]
fn published_lifecycle_fixture_shapes_cover_every_add_and_update_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let project_added: ratatoskr_ai_archive_contracts::AiProjectAdded =
        serde_json::from_str(PROJECT_ADDED)?;
    let project_updated: ratatoskr_ai_archive_contracts::AiProjectUpdated =
        serde_json::from_value(serde_json::to_value(&project_added)?)?;
    let conversation_added: ratatoskr_ai_archive_contracts::AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED)?;
    conversation_added.validate()?;
    let conversation_updated: ratatoskr_ai_archive_contracts::AiConversationUpdated =
        serde_json::from_value(serde_json::to_value(&conversation_added)?)?;
    conversation_updated.validate()?;
    let artifact_added: ratatoskr_ai_archive_contracts::AiArtifactAdded =
        serde_json::from_str(ARTIFACT_ADDED)?;
    let artifact_updated: ratatoskr_ai_archive_contracts::AiArtifactUpdated =
        serde_json::from_value(serde_json::to_value(&artifact_added)?)?;
    let tombstone: ratatoskr_ai_archive_contracts::AiArchiveTombstone =
        serde_json::from_str(CONVERSATION_TOMBSTONE)?;

    assert_eq!(project_added.project.title.as_str(), "Archive project");
    assert_eq!(
        project_updated.project.ai_project_id,
        project_added.project.ai_project_id
    );
    assert_eq!(
        conversation_updated.conversation.content_digest,
        conversation_added.conversation.content_digest
    );
    assert_eq!(
        artifact_updated.artifact.content_digest,
        artifact_added.artifact.content_digest
    );
    assert!(matches!(
        tombstone.subject,
        ratatoskr_ai_archive_contracts::AiArchiveTombstoneSubject::Conversation { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn import_fact_persists_the_complete_contract_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let fixture: ratatoskr_ai_archive_contracts::AiArchiveImport = serde_json::from_str(IMPORTED)?;
    let event = ArchiveOutbox::new(&database.database)
        .enqueue(
            &ArchiveEventFact::Imported(fixture.clone()),
            "2026-08-18T11:30:01Z".parse()?,
        )
        .await?;
    let persisted: serde_json::Value =
        sqlx::query_scalar("select envelope from claude_archive.outbox_events where event_id = $1")
            .bind(event.envelope.event_id.0)
            .fetch_one(database.database.pool())
            .await?;
    let replayed: ratatoskr_event_envelope::EventEnvelope = serde_json::from_value(persisted)?;
    assert_eq!(replayed, event.envelope);
    assert_eq!(
        replayed.payload_as::<ratatoskr_ai_archive_contracts::AiArchiveImport>()?,
        fixture
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn knowledge_completion_links_only_the_exact_published_artifact_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let artifact: ratatoskr_ai_archive_contracts::AiArtifactAdded =
        serde_json::from_str(ARTIFACT_ADDED)?;
    ArchiveOutbox::new(&database.database)
        .enqueue(
            &ArchiveEventFact::ArtifactAdded(artifact),
            "2026-08-27T08:00:01Z".parse()?,
        )
        .await?;
    let completion: ratatoskr_ai_archive_contracts::AiArchiveAnalysisCompleted =
        serde_json::from_str(ARTIFACT_COMPLETION)?;
    let links = KnowledgeLinkStore::new(&database.database);
    let completion_event_id = uuid::Uuid::now_v7();
    assert_eq!(
        links.accept(completion_event_id, &completion).await?,
        KnowledgeLinkOutcome::Linked
    );
    assert_eq!(
        links.accept(completion_event_id, &completion).await?,
        KnowledgeLinkOutcome::Duplicate
    );
    let wrong: ratatoskr_ai_archive_contracts::AiArchiveAnalysisCompleted = serde_json::from_value(
        serde_json::json!({
            "ai_archive_id":"018f0000-0000-7000-8000-000000000401",
            "owner":"user:018f0000-0000-7000-8000-000000000005",
            "subject":{"subject_kind":"artifact","external_artifact_id":"artifact-001"},
            "content_digest":{"algorithm":"sha256","hex":"1111111111111111111111111111111111111111111111111111111111111111"},
            "completed_at":"2026-08-27T08:01:00Z"
        }),
    )?;
    assert!(matches!(
        links.accept(uuid::Uuid::now_v7(), &wrong).await,
        Err(KnowledgeLinkError::RevisionNotPublished)
    ));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn knowledge_completion_envelope_links_the_exact_published_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let artifact: ratatoskr_ai_archive_contracts::AiArtifactAdded =
        serde_json::from_str(ARTIFACT_ADDED)?;
    ArchiveOutbox::new(&database.database)
        .enqueue(
            &ArchiveEventFact::ArtifactAdded(artifact),
            "2026-08-27T08:00:01Z".parse()?,
        )
        .await?;
    let completion: ratatoskr_ai_archive_contracts::AiArchiveAnalysisCompleted =
        serde_json::from_str(ARTIFACT_COMPLETION)?;
    let mut envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
        "event_id": uuid::Uuid::now_v7(),
        "event_type": "knowledge.ai_archive_analysis.completed.v1",
        "occurred_at": "2026-08-27T08:01:00Z",
        "producer": "ratatoskr-knowledge",
        "aggregate_id": "artifact:artifact-001",
        "correlation_id": format!("event:{}", uuid::Uuid::now_v7()),
        "tenant_id": "user:018f0000-0000-7000-8000-000000000005",
        "schema_version": 1,
        "payload": completion,
    }))?;
    // Parsing and re-emitting must retain the exact typed contract payload.
    envelope.set_payload(&completion)?;
    assert_eq!(
        KnowledgeLinkStore::new(&database.database)
            .accept_envelope(&envelope)
            .await?,
        KnowledgeLinkOutcome::Linked
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn completed_import_publishes_its_normalized_subjects_in_one_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let import: ratatoskr_ai_archive_contracts::AiArchiveImport = serde_json::from_str(IMPORTED)?;
    let mut artifact: ratatoskr_ai_archive_contracts::AiArtifactAdded =
        serde_json::from_str(ARTIFACT_ADDED)?;
    artifact.import_provenance.ai_archive_id = import.ai_archive_id;
    assert!(matches!(
        ArchiveOutbox::new(&database.database)
            .publish_import(
                &import,
                &[ArchiveEventFact::ArtifactAdded(artifact.clone())],
                "2026-08-27T08:00:01Z".parse()?,
            )
            .await,
        Err(ratatoskr_claude_archive::ArchiveEventError::ProvenanceMismatch)
    ));
    let mut matching_import = import;
    matching_import.ai_archive_id = artifact.import_provenance.ai_archive_id;
    matching_import.provider = artifact.import_provenance.provider.clone();
    matching_import.owner = artifact.import_provenance.owner;
    matching_import.source_export = artifact.import_provenance.source_export.clone();
    matching_import.imported_at = artifact.import_provenance.imported_at;
    matching_import.parser_name = artifact.import_provenance.parser_name.clone();
    matching_import.parser_version = artifact.import_provenance.parser_version.clone();
    let events = ArchiveOutbox::new(&database.database)
        .publish_import(
            &matching_import,
            &[ArchiveEventFact::ArtifactAdded(artifact)],
            "2026-08-27T08:00:01Z".parse()?,
        )
        .await?;
    assert_eq!(events.len(), 2);
    let count: i64 = sqlx::query_scalar("select count(*) from claude_archive.outbox_events")
        .fetch_one(database.database.pool())
        .await?;
    assert_eq!(count, 2);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_tombstone_is_idempotent_and_no_snapshot_absence_is_published()
-> Result<(), Box<dyn std::error::Error>> {
    let database = TestDatabase::create().await?;
    let tombstone: ratatoskr_ai_archive_contracts::AiArchiveTombstone =
        serde_json::from_str(CONVERSATION_TOMBSTONE)?;
    let outbox = ArchiveOutbox::new(&database.database);
    let first = outbox
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone.clone()),
            "2026-08-27T06:00:00Z".parse()?,
        )
        .await?;
    let replay = outbox
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone),
            "2026-08-27T06:00:01Z".parse()?,
        )
        .await?;
    assert_eq!(first.envelope, replay.envelope);
    let types: Vec<String> = sqlx::query_scalar(
        "select event_type from claude_archive.outbox_events order by event_type",
    )
    .fetch_all(database.database.pool())
    .await?;
    assert_eq!(types, ["ai_archive.subject.tombstoned.v1"]);
    database.cleanup().await?;
    Ok(())
}
