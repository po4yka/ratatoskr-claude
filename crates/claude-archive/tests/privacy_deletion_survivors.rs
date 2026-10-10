//! Deleting a raw export must not tell Knowledge to drop a subject that a retained export still
//! holds, and must not leave the deleted export's import head in the outbox (XR-021 CONTRACTS.md
//! S07).
//!
//! The facts of a subject were announced under the archive id of the export that first saw it, and
//! a later export that sees the same record announces nothing. The tombstone of the deleted archive
//! therefore reaches the survivor too, so the deletion re-announces every survivor under the
//! earliest retained export that observed it.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod synthetic_import;

use ratatoskr_ai_archive_contracts::{
    AiArchiveProvenance, AiArtifactAdded, AiArtifactUpdated, AiConversationAdded,
    AiConversationUpdated, AiProjectAdded, AiProjectUpdated,
};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::privacy_deletion::{
    ConversationDeletionPlanRequest, PrivacyDeletionExecutor, PrivacyDeletionPlanner,
    RawExportDeletionPlanRequest, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{BlobStore, MediaType, PlatformUsers};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::AiArchiveId;
use synthetic_import::{account, receive, versions, worker, zip};
use uuid::Uuid;

const USER: &str = "018f0000-0000-7000-8000-0000000000c1";
const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";
const HEAD: &str = "ai_archive.archive.imported.v1";

/// Three cumulative exports of one account: each observes the garden project, the planting
/// conversation and the project Artifact, and only the first announced them.
struct Imported {
    db: TestDatabase,
    store: BlobStore,
    root: std::path::PathBuf,
    account: Uuid,
    /// `(export_id, ai_archive_id)` in the order the exports were received.
    exports: [(Uuid, Uuid); 3],
}

async fn import_three_exports(label: &str) -> Imported {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let account = account(&db).await;
    let users = PlatformUsers::new(&[(Uuid::parse_str(USER).expect("user UUID"), account)]);
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users);
    let artifacts = serde_json::json!([{
        "id": "react-component", "type": "text/markdown", "project_id": "project-garden",
        "versions": versions()
    }]);
    for note in ["first", "second", "third"] {
        receive(&db, &store, account, &zip(note, artifacts.clone(), false)).await;
        assert_eq!(
            worker
                .process_pending_once()
                .await
                .expect("the worker imports the durable archive"),
            1
        );
    }
    let exports: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "select export_id, ai_archive_id from claude_archive.exports order by received_at",
    )
    .fetch_all(db.database.pool())
    .await
    .expect("the exports are readable");
    let exports = <[(Uuid, Uuid); 3]>::try_from(exports).expect("three exports were received");
    Imported {
        db,
        store,
        root,
        account,
        exports,
    }
}

impl Imported {
    async fn finish(self) {
        remove(&self.root);
        self.db.cleanup().await.expect("cleanup succeeds");
    }

    fn tenant_ref() -> String {
        format!("user:{USER}")
    }

    async fn execute(&self, request: Uuid) {
        let evidence = self
            .store
            .store(
                MediaType::parse("application/json").expect("canonical media type"),
                b"{}",
            )
            .expect("the evidence blob is stored");
        PrivacyDeletionExecutor::new(self.db.database.pool().clone())
            .finalize(request, &evidence, None)
            .await
            .expect("the deletion completes");
    }

    fn planner(&self) -> PrivacyDeletionPlanner {
        PrivacyDeletionPlanner::new(self.db.database.pool().clone())
    }

    async fn delete_export(&self, export_id: Uuid) {
        let request = Uuid::now_v7();
        self.planner()
            .plan_raw_export(&RawExportDeletionPlanRequest {
                tenant_ref: Self::tenant_ref(),
                account_id: self.account,
                request_id: request,
                request_key: "survivors-export".to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
                export_id,
            })
            .await
            .expect("the export deletion is planned");
        self.execute(request).await;
    }

    async fn delete_conversation(&self) {
        let request = Uuid::now_v7();
        let conversation: Uuid = sqlx::query_scalar(
            "select conversation_id from claude_archive.conversations
             where external_conversation_id = 'conversation-planting'",
        )
        .fetch_one(self.db.database.pool())
        .await
        .expect("the planting conversation exists");
        self.planner()
            .plan_conversation(&ConversationDeletionPlanRequest {
                tenant_ref: Self::tenant_ref(),
                account_id: self.account,
                request_id: request,
                request_key: "survivors-conversation".to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
                conversation_id: conversation,
            })
            .await
            .expect("the conversation deletion is planned");
        self.execute(request).await;
    }

    async fn delete_tenant(&self) {
        let request = Uuid::now_v7();
        self.planner()
            .plan_tenant(&TenantDeletionPlanRequest {
                tenant_ref: Self::tenant_ref(),
                account_id: self.account,
                request_id: request,
                request_key: "survivors-tenant".to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
            })
            .await
            .expect("the tenant deletion is planned");
        self.execute(request).await;
    }

    /// Every archive fact in the order the pump publishes it.
    async fn facts(&self) -> Vec<EventEnvelope> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "select envelope from claude_archive.outbox_events
             where event_type like 'ai_archive.%' order by occurred_at, event_id",
        )
        .fetch_all(self.db.database.pool())
        .await
        .expect("the archive facts are readable");
        rows.into_iter()
            .map(|raw| serde_json::from_value(raw).expect("a complete envelope"))
            .collect()
    }

    /// How many import heads each export's archive has in the outbox, in receipt order.
    async fn head_rows(&self) -> [i64; 3] {
        let mut counts = [0; 3];
        for (count, (_, archive)) in counts.iter_mut().zip(self.exports) {
            *count = sqlx::query_scalar(
                "select count(*) from claude_archive.outbox_events
                 where event_type = $1 and aggregate_type = 'archive' and aggregate_id = $2",
            )
            .bind(HEAD)
            .bind(AiArchiveId(archive).as_entity_ref().to_string())
            .fetch_one(self.db.database.pool())
            .await
            .expect("the head rows are countable");
        }
        counts
    }
}

/// The facts published after the last tombstone, which are the ones that follow the deletion.
fn after_the_tombstones(facts: &[EventEnvelope]) -> Vec<EventEnvelope> {
    let last = facts
        .iter()
        .rposition(|fact| fact.event_type.to_wire() == TOMBSTONED)
        .expect("the deletion enqueued a tombstone");
    facts
        .get(last + 1..)
        .expect("the tombstone is inside the list")
        .to_vec()
}

/// Every fact payload that carries provenance names it under the same key.
#[derive(serde::Deserialize)]
struct Stamped {
    import_provenance: AiArchiveProvenance,
}

fn provenance_of(fact: &EventEnvelope) -> AiArchiveProvenance {
    serde_json::from_value::<Stamped>(serde_json::Value::Object(fact.payload.clone()))
        .expect("the payload names its provenance")
        .import_provenance
}

fn types(facts: &[EventEnvelope]) -> Vec<String> {
    facts.iter().map(|fact| fact.event_type.to_wire()).collect()
}

/// The first fact of `event_type` the imports announced.
fn announced(facts: &[EventEnvelope], event_type: &str) -> EventEnvelope {
    facts
        .iter()
        .find(|fact| fact.event_type.to_wire() == event_type)
        .expect("the import announced the subject")
        .clone()
}

#[tokio::test]
async fn a_subject_a_retained_export_holds_is_re_announced_under_the_earliest_such_export() {
    let imported = import_three_exports("survivors-reannounce").await;
    let [(deleted, _), (retained, earliest_retained), _] = imported.exports;
    let first_facts = imported.facts().await;
    imported.delete_export(deleted).await;
    let facts = imported.facts().await;
    let follow_up = after_the_tombstones(&facts);
    let retained_hash: String = sqlx::query_scalar(
        "select encode(archive_hash, 'hex') from claude_archive.exports where export_id = $1",
    )
    .bind(retained)
    .fetch_one(imported.db.database.pool())
    .await
    .expect("the retained export is readable");
    imported.finish().await;

    assert_eq!(
        types(&follow_up),
        vec![
            "ai_archive.project.updated.v1",
            "ai_archive.conversation.updated.v1",
            "ai_archive.artifact.updated.v1",
        ],
        "the garden project, the planting conversation and the Artifact were announced only \
         under the deleted archive, so each is announced again after its tombstone; the recipes \
         project already carries a retained archive and is not"
    );
    for fact in &follow_up {
        let provenance = provenance_of(fact);
        assert_eq!(
            provenance.ai_archive_id.0, earliest_retained,
            "the earliest retained observing export, not the latest"
        );
        assert_eq!(
            provenance.source_export.digest.hex.to_string(),
            retained_hash,
            "the evidence is the retained raw export"
        );
    }
    let [project, conversation, artifact] =
        <[EventEnvelope; 3]>::try_from(follow_up).expect("three facts follow the tombstone");
    let project_now = project
        .payload_as::<AiProjectUpdated>()
        .expect("the payload decodes");
    project_now
        .validate()
        .expect("the re-announced project satisfies its invariants");
    let project_then = announced(&first_facts, "ai_archive.project.added.v1")
        .payload_as::<AiProjectAdded>()
        .expect("the payload decodes");
    assert_eq!(project_now.project, project_then.project);
    let conversation_now = conversation
        .payload_as::<AiConversationUpdated>()
        .expect("the payload decodes");
    conversation_now
        .validate()
        .expect("the re-announced conversation satisfies its invariants");
    let conversation_then = announced(&first_facts, "ai_archive.conversation.added.v1")
        .payload_as::<AiConversationAdded>()
        .expect("the payload decodes");
    assert_eq!(
        conversation_now.conversation, conversation_then.conversation,
        "the record is the one the survivor still has"
    );
    let artifact_now = artifact
        .payload_as::<AiArtifactUpdated>()
        .expect("the payload decodes");
    let artifact_then = announced(&first_facts, "ai_archive.artifact.added.v1")
        .payload_as::<AiArtifactAdded>()
        .expect("the payload decodes");
    assert_eq!(artifact_now.artifact, artifact_then.artifact);
}

#[tokio::test]
async fn only_the_subjects_the_deleted_export_last_announced_are_re_announced() {
    let imported = import_three_exports("survivors-last-announced").await;
    // The newest export changed the recipes project description, so it holds the latest fact of
    // that project and of nothing else: the garden project, the conversation and the Artifact are
    // still announced under the first export, which stays.
    let [
        (earliest_retained_export, earliest_retained),
        _,
        (deleted, _),
    ] = imported.exports;
    let _ = earliest_retained_export;
    imported.delete_export(deleted).await;
    let facts = imported.facts().await;
    let follow_up = after_the_tombstones(&facts);
    imported.finish().await;

    assert_eq!(types(&follow_up), vec!["ai_archive.project.updated.v1"]);
    let project = follow_up.first().expect("one fact follows the tombstone");
    assert_eq!(
        provenance_of(project).ai_archive_id.0,
        earliest_retained,
        "the earliest retained observing export"
    );
}

#[tokio::test]
async fn deleting_an_export_removes_its_import_head_from_the_outbox() {
    let imported = import_three_exports("survivors-head-export").await;
    imported.delete_export(imported.exports[0].0).await;
    let heads = imported.head_rows().await;
    imported.finish().await;

    assert_eq!(
        heads,
        [0, 1, 1],
        "the deleted export's head names a source export whose bytes are erased; the retained \
         exports keep theirs"
    );
}

#[tokio::test]
async fn deleting_a_conversation_removes_the_heads_of_the_exports_that_held_it() {
    let imported = import_three_exports("survivors-head-conversation").await;
    imported.delete_conversation().await;
    let heads = imported.head_rows().await;
    imported.finish().await;

    assert_eq!(heads, [0, 0, 0], "every export held the conversation");
}

#[tokio::test]
async fn deleting_a_tenant_removes_every_import_head() {
    let imported = import_three_exports("survivors-head-tenant").await;
    imported.delete_tenant().await;
    let heads = imported.head_rows().await;
    imported.finish().await;

    assert_eq!(heads, [0, 0, 0]);
}
