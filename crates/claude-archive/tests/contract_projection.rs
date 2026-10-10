//! The persisted import is projected into `ai_archive.*` facts, in the transaction that persists
//! it, owned by the Platform user of the account (XR-021 CONTRACTS.md S07).
//!
//! Verified only against the documented synthetic export schema, never a real Claude export.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::io::Write as _;
use std::sync::Arc;

use ratatoskr_ai_archive_contracts::{
    AiArchiveImport, AiArtifactAdded, AiAuthorRole, AiContentPart, AiConversationAdded,
    AiConversationUpdated, AiProjectAdded, AiText, AiTitle,
};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobStore, Database, InitialImportWorker, Limits,
    OutboxPublisher, ParsedExport, ParserCapability, ParserDescriptor, ParserExecutionError,
    ParserExecutionInput, ParserExecutor, ParserIdentity, ParserRegistry, PlatformOperation,
    PlatformUsers, ReparseEngine, TenantClaim,
};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::TenantRef;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const PLATFORM_USER: &str = "018f0000-0000-7000-8000-0000000000c1";
const SCHEMA: &str = "claude-export-2026-08-synthetic";
const PARSER: &str = "claude-synthetic-consumer-export";

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

/// The documented synthetic export: two projects, one conversation and one project Artifact.
fn synthetic_zip() -> Vec<u8> {
    let artifacts: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/synthetic_consumer_export_artifact_v1.json"
    ))
    .expect("the Artifact fixture is JSON");
    zip_with_artifacts(
        artifacts
            .get("artifacts")
            .cloned()
            .expect("the Artifact fixture lists artifacts"),
    )
}

/// The synthetic export carrying exactly `artifacts`.
fn zip_with_artifacts(artifacts: serde_json::Value) -> Vec<u8> {
    let mut export: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/synthetic_consumer_export.json"))
            .expect("the base fixture is JSON");
    export
        .as_object_mut()
        .expect("the base fixture is an object")
        .insert("artifacts".to_owned(), artifacts);
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("export.json", zip::write::SimpleFileOptions::default())
        .expect("the export entry starts");
    writer
        .write_all(&serde_json::to_vec(&export).expect("the export serializes"))
        .expect("the export entry writes");
    writer.finish().expect("the archive closes").into_inner()
}

/// Re-runs the real parser and optionally retitles every conversation, as a newer parser might.
#[derive(Debug)]
struct NewerParser {
    version: &'static str,
    retitle: bool,
}

impl ParserExecutor for NewerParser {
    fn execute(
        &self,
        input: ParserExecutionInput<'_>,
    ) -> Result<ParsedExport, ParserExecutionError> {
        let mut parsed = ratatoskr_claude_archive::ConsumerExportParser::parse(input.evidence)
            .map_err(|_| ParserExecutionError::Failed)?;
        self.version.clone_into(&mut parsed.parser.parser_version);
        for conversation in &mut parsed.conversations {
            self.version
                .clone_into(&mut conversation.parser.parser_version);
            if self.retitle {
                conversation.title = format!("{} (revised)", conversation.title);
            }
        }
        Ok(parsed)
    }
}

/// The parser set of the running service: one parser, so schema selection is unambiguous.
fn import_registry() -> ParserRegistry {
    ParserRegistry::runtime().expect("the runtime registry is valid")
}

/// The service parser plus two newer releases of it, the targets of a reparse.
fn reparse_registry() -> ParserRegistry {
    let mut registry = import_registry();
    for (version, retitle) in [("2026-09-01", false), ("2026-09-02", true)] {
        registry
            .register_compiled(
                ParserDescriptor::new(
                    PARSER,
                    version,
                    vec![AcquisitionMode::ConsumerExport],
                    vec![SCHEMA.to_owned()],
                    vec![ParserCapability::Conversations],
                ),
                Arc::new(NewerParser { version, retitle }),
            )
            .expect("the newer parser versions are distinct");
    }
    registry
}

struct Imported {
    db: TestDatabase,
    store: BlobStore,
    root: std::path::PathBuf,
    account: Uuid,
    archive: Uuid,
}

async fn import_synthetic_export(label: &str, mapped: bool) -> Imported {
    import_export(label, mapped, synthetic_zip()).await
}

async fn import_export(label: &str, mapped: bool, archive_bytes: Vec<u8>) -> Imported {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account)
    .bind(format!("projection-{account}"))
    .execute(db.database.pool())
    .await
    .expect("the account inserts");
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the blob store opens");
    ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        &db.database,
        &store,
        &TenantClaim {
            account: Some(account),
            organization: None,
        },
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&archive_bytes),
        10 * 1024 * 1024 * 1024,
        &ArchiveIdentity {
            sha256: format!("{:x}", Sha256::digest(&archive_bytes)),
            byte_size: u64::try_from(archive_bytes.len()).expect("the fixture length fits"),
        },
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await
    .expect("the archive is received");
    let users = if mapped {
        PlatformUsers::new(&[(Uuid::parse_str(PLATFORM_USER).expect("user UUID"), account)])
    } else {
        PlatformUsers::default()
    };
    let worker = InitialImportWorker::new(
        db.database.pool().clone(),
        store.clone(),
        Arc::new(import_registry()),
        limits(),
        users,
    );
    assert_eq!(
        worker
            .process_pending_once()
            .await
            .expect("the worker imports the durable archive"),
        1
    );
    let state: String = sqlx::query_scalar("select state from claude_archive.import_runs")
        .fetch_one(db.database.pool())
        .await
        .expect("the import run is readable");
    assert!(
        matches!(state.as_str(), "completed" | "partial"),
        "the synthetic export must import, not fail: {state}"
    );
    let archive: Uuid =
        sqlx::query_scalar("select ai_archive_id from claude_archive.exports limit 1")
            .fetch_one(db.database.pool())
            .await
            .expect("the export is readable");
    Imported {
        db,
        store,
        root,
        account,
        archive,
    }
}

/// The unpublished archive facts in publication order.
async fn facts(database: &Database) -> Vec<EventEnvelope> {
    let pending = OutboxPublisher::new(database.pool().clone())
        .pending_event_ids()
        .await
        .expect("the pending queue is readable");
    let mut envelopes = Vec::new();
    for event_id in pending {
        let raw: serde_json::Value = sqlx::query_scalar(
            "select envelope from claude_archive.outbox_events where event_id = $1",
        )
        .bind(event_id)
        .fetch_one(database.pool())
        .await
        .expect("the envelope is readable");
        let envelope: EventEnvelope = serde_json::from_value(raw).expect("a complete envelope");
        if envelope.event_type.to_wire().starts_with("ai_archive.") {
            envelopes.push(envelope);
        }
    }
    envelopes
}

fn types(envelopes: &[EventEnvelope]) -> Vec<String> {
    envelopes
        .iter()
        .map(|envelope| envelope.event_type.to_wire())
        .collect()
}

fn payload<T: ratatoskr_event_envelope::EventPayload>(
    envelopes: &[EventEnvelope],
    event_type: &str,
) -> Vec<T> {
    envelopes
        .iter()
        .filter(|envelope| envelope.event_type.to_wire() == event_type)
        .map(|envelope| envelope.payload_as::<T>().expect("the payload decodes"))
        .collect()
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one projection proof keeps every fact class visible"
)]
async fn the_persisted_import_is_projected_into_archive_facts() {
    let imported = import_synthetic_export("projection-import", true).await;
    let owner = TenantRef::parse(&format!("user:{PLATFORM_USER}")).expect("a user reference");

    let emitted = facts(&imported.db.database).await;
    let head: Vec<AiArchiveImport> = payload(&emitted, "ai_archive.archive.imported.v1");
    let projects: Vec<AiProjectAdded> = payload(&emitted, "ai_archive.project.added.v1");
    let conversations: Vec<AiConversationAdded> =
        payload(&emitted, "ai_archive.conversation.added.v1");
    let artifacts: Vec<AiArtifactAdded> = payload(&emitted, "ai_archive.artifact.added.v1");
    let blob_exists = artifacts.iter().all(|added| {
        let blob = &added.artifact.content_blob;
        let reference = ratatoskr_claude_archive::BlobRef {
            owner_service: blob.owner_service.to_string(),
            algorithm: ratatoskr_claude_archive::DigestAlgorithm::Sha256,
            digest_hex: blob.digest.hex.to_string(),
            media_type: ratatoskr_claude_archive::MediaType::parse(blob.media_type.as_str())
                .expect("media type"),
            length_bytes: blob.length_bytes,
        };
        imported.store.verify(&reference).is_ok()
    });
    remove(&imported.root);
    let account = imported.account;
    let archive = imported.archive;
    imported.db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        types(&emitted),
        vec![
            "ai_archive.archive.imported.v1",
            "ai_archive.project.added.v1",
            "ai_archive.project.added.v1",
            "ai_archive.conversation.added.v1",
            "ai_archive.artifact.added.v1",
        ],
        "the import head comes first, then each project, conversation and Artifact"
    );
    let head = head.first().expect("one import head");
    head.validate()
        .expect("the import head satisfies its invariants");
    assert_eq!(
        head.owner, owner,
        "the owner is the Platform user of the account"
    );
    assert_eq!(head.ai_archive_id.0, archive);
    assert_eq!(head.completeness_report.conversation_count, 1);
    assert_eq!(head.completeness_report.message_count, 2);
    assert_eq!(
        head.completeness_report.gap_count,
        u32::try_from(head.completeness_report.gaps.len()).expect("gap count fits"),
        "the gap count counts the gap entries"
    );
    let titles: Vec<&str> = projects
        .iter()
        .map(|added| added.project.title.as_str())
        .collect();
    assert_eq!(
        titles,
        vec!["Garden plans", "Recipes"],
        "projects are projected by title"
    );
    let garden = projects.first().expect("the garden project");
    assert_eq!(
        garden.project.description.as_ref().map(AiText::as_str),
        Some("Seasonal notes")
    );
    assert_eq!(
        garden.project.instructions.as_ref().map(AiText::as_str),
        Some("Use metric units.")
    );
    garden
        .validate()
        .expect("the project fact agrees with its provenance");
    let conversation = conversations.first().expect("one conversation fact");
    conversation
        .validate()
        .expect("AiConversationAdded::validate passes: provenance and content digest agree");
    assert_eq!(conversation.conversation.owner, owner);
    assert_eq!(
        conversation.conversation.project_ref,
        Some(garden.project.ai_project_id.as_entity_ref()),
        "the conversation points at its project"
    );
    assert_eq!(
        conversation
            .conversation
            .title
            .as_ref()
            .map(AiTitle::as_str),
        Some("Planting calendar")
    );
    let roles: Vec<AiAuthorRole> = conversation
        .conversation
        .messages
        .iter()
        .map(|message| message.author_role)
        .collect();
    assert_eq!(roles, vec![AiAuthorRole::User, AiAuthorRole::Assistant]);
    let first_parts = &conversation
        .conversation
        .messages
        .first()
        .expect("the first message")
        .parts;
    assert!(
        matches!(
            first_parts.as_slice(),
            [AiContentPart::Text { .. }, AiContentPart::Markdown { .. }]
        ),
        "text and markdown parts keep their order: {first_parts:?}"
    );
    let artifact = artifacts.first().expect("one Artifact fact");
    assert_eq!(
        artifact.artifact.external_artifact_id.as_str(),
        "artifact-garden-plan"
    );
    assert_eq!(artifact.artifact.owner, owner);
    assert!(
        blob_exists,
        "the Artifact content blob exists in the blob store"
    );
    let _ = account;
}

#[tokio::test]
async fn an_unmapped_account_emits_no_archive_facts_but_still_reports() {
    let imported = import_synthetic_export("projection-unmapped", false).await;

    let emitted = facts(&imported.db.database).await;
    let reports: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1'",
    )
    .fetch_one(imported.db.database.pool())
    .await
    .expect("the reports are countable");
    remove(&imported.root);
    imported.db.cleanup().await.expect("cleanup succeeds");

    assert!(
        emitted.is_empty(),
        "an account without a Platform user has no Knowledge tenant: {:?}",
        types(&emitted)
    );
    assert_eq!(reports, 1, "the operation report still flows");
}

async fn publish_all(database: &Database) {
    sqlx::query(
        "update claude_archive.outbox_events set published_at = now() where published_at is null",
    )
    .execute(database.pool())
    .await
    .expect("the queue is drained");
}

#[tokio::test]
async fn reparse_adds_nothing_for_identical_content_and_one_update_for_changed_content() {
    let imported = import_synthetic_export("projection-reparse", true).await;
    let initial_head: Vec<AiArchiveImport> = payload(
        &facts(&imported.db.database).await,
        "ai_archive.archive.imported.v1",
    );
    publish_all(&imported.db.database).await;
    let users = PlatformUsers::new(&[(
        Uuid::parse_str(PLATFORM_USER).expect("user UUID"),
        imported.account,
    )]);
    let engine = ReparseEngine::new(
        imported.db.database.pool().clone(),
        imported.store.clone(),
        Arc::new(reparse_registry()),
        limits(),
        users,
    );

    let identical = engine
        .plan(
            imported.account,
            imported.archive,
            ParserIdentity::new(PARSER, "2026-09-01"),
        )
        .await
        .expect("the identical reparse is planned");
    engine
        .apply(&identical)
        .await
        .expect("the identical reparse applies");
    let after_identical = facts(&imported.db.database).await;
    let changed = engine
        .plan(
            imported.account,
            imported.archive,
            ParserIdentity::new(PARSER, "2026-09-02"),
        )
        .await
        .expect("the changed reparse is planned");
    engine
        .apply(&changed)
        .await
        .expect("the changed reparse applies");
    let after_changed = facts(&imported.db.database).await;
    let updates: Vec<AiConversationUpdated> =
        payload(&after_changed, "ai_archive.conversation.updated.v1");
    let reparse_head: Vec<AiArchiveImport> =
        payload(&after_changed, "ai_archive.archive.imported.v1");
    remove(&imported.root);
    imported.db.cleanup().await.expect("cleanup succeeds");

    assert!(
        after_identical.is_empty(),
        "identical content adds nothing: {:?}",
        types(&after_identical)
    );
    assert_eq!(
        types(&after_changed)
            .into_iter()
            .filter(|event_type| event_type == "ai_archive.conversation.updated.v1")
            .count(),
        1,
        "changed content adds exactly one conversation update: {:?}",
        types(&after_changed)
    );
    let update = updates.first().expect("the update decodes");
    update
        .validate()
        .expect("AiConversationUpdated::validate passes");
    assert_eq!(
        update.conversation.title.as_ref().map(AiTitle::as_str),
        Some("Planting calendar (revised)")
    );
    let initial_head = initial_head.first().expect("the import announced a head");
    let reparse_head = reparse_head
        .first()
        .expect("the changed reparse re-announced a head");
    assert_eq!(
        reparse_head.completeness_report, initial_head.completeness_report,
        "a reparse of a changed conversation does not change what Knowledge is told about the \
         completeness of the archive: status, counts, asset count and gaps stay those of the import"
    );
    assert_eq!(
        reparse_head.warnings, initial_head.warnings,
        "the import warnings are carried over, not dropped"
    );
}

/// Every subject that cannot be projected is named on the head instead of vanishing (XR-021
/// CONTRACTS.md S07): Knowledge must be able to tell that its fact set is not the whole archive.
#[tokio::test]
async fn an_artifact_without_verified_bytes_is_named_as_a_gap_on_the_head() {
    let archive = zip_with_artifacts(serde_json::json!([{
        "id": "artifact-reference-only",
        "type": "text/markdown",
        "title": "Reference only",
        "project_id": "project-garden",
        "versions": [{"id": "artifact-reference-only-v1", "media_type": "text/markdown"}]
    }]));
    let imported = import_export("projection-reference-artifact", true, archive).await;

    let emitted = facts(&imported.db.database).await;
    remove(&imported.root);
    imported.db.cleanup().await.expect("cleanup succeeds");

    let head: Vec<AiArchiveImport> = payload(&emitted, "ai_archive.archive.imported.v1");
    let head = head.first().expect("one import head");
    head.validate().expect("the head satisfies its invariants");
    let gap = head
        .completeness_report
        .gaps
        .iter()
        .find(|gap| gap.gap_kind.as_str() == "unprojected_artifact")
        .expect("the omitted Artifact is named as a gap");
    assert_eq!(
        gap.affected_count,
        Some(1),
        "exactly the one Artifact is omitted"
    );
    assert_eq!(
        u32::try_from(head.completeness_report.gaps.len()).expect("gap count fits"),
        head.completeness_report.gap_count
    );
    assert!(
        !head.completeness_report.completeness.is_complete(),
        "an archive with an unprojected subject is not announced as complete"
    );
    assert!(
        payload::<AiArtifactAdded>(&emitted, "ai_archive.artifact.added.v1").is_empty(),
        "no fact is invented for the Artifact that has no verified bytes"
    );
}

#[tokio::test]
async fn a_conversation_that_cannot_be_projected_is_named_as_a_gap_on_the_reparse_head() {
    let imported = import_synthetic_export("projection-unprojectable", true).await;
    publish_all(&imported.db.database).await;
    sqlx::query("update claude_archive.messages set parser_name = null, parser_version = null")
        .execute(imported.db.database.pool())
        .await
        .expect("the message stamps are removed");
    let engine = ReparseEngine::new(
        imported.db.database.pool().clone(),
        imported.store.clone(),
        Arc::new(reparse_registry()),
        limits(),
        PlatformUsers::new(&[(
            Uuid::parse_str(PLATFORM_USER).expect("user UUID"),
            imported.account,
        )]),
    );
    let changed = engine
        .plan(
            imported.account,
            imported.archive,
            ParserIdentity::new(PARSER, "2026-09-02"),
        )
        .await
        .expect("the changed reparse is planned");
    engine
        .apply(&changed)
        .await
        .expect("the changed reparse applies");

    let emitted = facts(&imported.db.database).await;
    publish_all(&imported.db.database).await;
    let again = engine
        .plan(
            imported.account,
            imported.archive,
            ParserIdentity::new(PARSER, "2026-09-01"),
        )
        .await
        .expect("the next reparse is planned");
    engine
        .apply(&again)
        .await
        .expect("the next reparse applies");
    let repeated = facts(&imported.db.database).await;
    remove(&imported.root);
    imported.db.cleanup().await.expect("cleanup succeeds");

    assert!(
        repeated.is_empty(),
        "a reparse that loses the same subject again does not repeat the same head: {:?}",
        types(&repeated)
    );
    assert!(
        payload::<AiConversationUpdated>(&emitted, "ai_archive.conversation.updated.v1").is_empty(),
        "an unstamped conversation is not announced"
    );
    let head: Vec<AiArchiveImport> = payload(&emitted, "ai_archive.archive.imported.v1");
    let head = head
        .first()
        .expect("an omission alone is reason enough to re-announce the head");
    let gap = head
        .completeness_report
        .gaps
        .iter()
        .find(|gap| gap.gap_kind.as_str() == "unprojected_conversation")
        .expect("the omitted conversation is named as a gap");
    assert_eq!(gap.affected_count, Some(1));
    assert!(!head.completeness_report.completeness.is_complete());
}
