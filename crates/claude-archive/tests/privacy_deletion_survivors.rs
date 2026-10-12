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
    AiConversationUpdated, AiProjectAdded, AiProjectUpdated, AiText,
};
use ratatoskr_claude_archive::blob_protocol::{self, BlobRegistrationError};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::privacy_deletion::{
    ConversationDeletionPlanRequest, EraseGate, FinalizationFault, PrivacyDeletionExecutionError,
    PrivacyDeletionExecutor, PrivacyDeletionPlanner, RawExportDeletionPlanRequest,
    ResolvedDeletionBlob, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::receipt::{PlatformOperation, ReceiptError, TenantScope};
use ratatoskr_claude_archive::test_support::{TestDatabase, database_url};
use ratatoskr_claude_archive::{BlobRef, BlobStore, Database, MediaType, PlatformUsers};
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
    // Every export changed the recipes project description ("first", "second", "third"), so the
    // newest export holds the latest fact of that project and of nothing else: the garden project,
    // the conversation and the Artifact are still announced under the first export, which stays.
    // The deletion puts the recipes project back to the description of the export before it, and
    // that is the content the survivor fact carries, under the export that presented it.
    let [_, (_, presenter), (deleted, _)] = imported.exports;
    imported.delete_export(deleted).await;
    let facts = imported.facts().await;
    let follow_up = after_the_tombstones(&facts);
    imported.finish().await;

    assert_eq!(types(&follow_up), vec!["ai_archive.project.updated.v1"]);
    let project = follow_up.first().expect("one fact follows the tombstone");
    assert_eq!(
        provenance_of(project).ai_archive_id.0,
        presenter,
        "the earliest retained export that presented the restored content"
    );
    let announced = project
        .payload_as::<AiProjectUpdated>()
        .expect("the payload decodes");
    assert_eq!(
        announced.project.description.as_ref().map(AiText::as_str),
        Some("second"),
        "the survivor fact carries the description of the retained export, not the deleted one's"
    );
    assert!(
        !serde_json::to_string(&facts)
            .expect("the facts serialize")
            .contains("third"),
        "no fact in the outbox carries the deleted export's description"
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

/// A raw export whose rows are already removed: the request is `purging` and only the byte phase
/// is left, with a second archive account ready to register the same bytes.
struct Purging {
    db: TestDatabase,
    /// A pool large enough for an eraser, a registrar and the test's own queries at once.
    wide: Database,
    store: BlobStore,
    root: std::path::PathBuf,
    request: Uuid,
    resolved: Vec<ResolvedDeletionBlob>,
    raw: BlobRef,
    bytes: Vec<u8>,
    account: Uuid,
    other_account: Uuid,
}

async fn purging_request(label: &str) -> Purging {
    purging_request_of(label, false).await
}

/// `import` runs the importer on the received archive first, so the export has its evidence blob.
async fn purging_request_of(label: &str, import: bool) -> Purging {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let wide = Database::connect(
        &database_url(db.name()),
        6,
        std::time::Duration::from_secs(10),
    )
    .await
    .expect("a wider pool connects");
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the blob store opens");
    let account = account(&db).await;
    let other_account = synthetic_import::account(&db).await;
    let bytes = zip("erase", serde_json::json!([]), false);
    receive(&db, &store, account, &bytes).await;
    if import {
        let users = PlatformUsers::new(&[(Uuid::parse_str(USER).expect("user UUID"), account)]);
        let imported = worker(&db, &store, users).process_pending_once().await;
        assert_eq!(imported.expect("the archive is imported"), 1);
    }
    let export: Uuid = sqlx::query_scalar("select export_id from claude_archive.exports")
        .fetch_one(db.database.pool())
        .await
        .expect("the export was received");
    let request = Uuid::now_v7();
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: format!("user:{USER}"),
            account_id: account,
            request_id: request,
            request_key: "erase-race".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: export,
        })
        .await
        .expect("the export deletion is planned");
    let keys: Vec<String> = sqlx::query_scalar(
        "select blob_ref from claude_archive.privacy_deletion_items
         where request_id = $1 and action = 'erase_blob' and blob_ref is not null",
    )
    .bind(request)
    .fetch_all(db.database.pool())
    .await
    .expect("the erase items are readable");
    let resolved: Vec<ResolvedDeletionBlob> = keys
        .into_iter()
        .map(|key| {
            let digest = key
                .strip_prefix("sha256/")
                .expect("an inventory key is a content address");
            ResolvedDeletionBlob {
                reference: store.locate(digest).expect("the blob is stored"),
                key,
            }
        })
        .collect();
    let raw = resolved
        .first()
        .expect("the raw archive is an erase item")
        .reference
        .clone();
    let purging = Purging {
        db,
        wide,
        store,
        root,
        request,
        resolved,
        raw,
        bytes,
        account,
        other_account,
    };
    let stopped = purging
        .executor()
        .finalize_with_blob_store(
            request,
            &purging.store,
            &purging.resolved,
            Some(FinalizationFault::AfterRowsCommitted),
        )
        .await;
    assert!(
        matches!(
            stopped,
            Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterRowsCommitted
            ))
        ),
        "the deletion stops after its rows are committed and before any byte is erased"
    );
    purging
}

impl Purging {
    fn executor(&self) -> PrivacyDeletionExecutor {
        PrivacyDeletionExecutor::new(self.wide.pool().clone())
    }

    async fn finish(self) {
        remove(&self.root);
        self.wide.close().await;
        self.db.cleanup().await.expect("cleanup succeeds");
    }

    async fn export_rows_of_the_other_account(&self) -> i64 {
        sqlx::query_scalar("select count(*) from claude_archive.exports where account_ref = $1")
            .bind(self.other_account)
            .fetch_one(self.wide.pool())
            .await
            .expect("the exports are countable")
    }

    /// Resumes the byte phase on its own task, so a test can watch it from outside.
    fn resume(&self, gate: &EraseGate) -> tokio::task::JoinHandle<()> {
        let executor = self.executor().with_erase_gate(gate.clone());
        let (request, store, resolved) = (self.request, self.store.clone(), self.resolved.clone());
        tokio::spawn(async move {
            executor
                .finalize_with_blob_store(request, &store, &resolved, None)
                .await
                .expect("the byte phase completes");
        })
    }
}

/// Whether `task` is still running after a pause long enough for an unblocked task to finish.
async fn is_blocked<T>(task: &tokio::task::JoinHandle<T>) -> bool {
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    !task.is_finished()
}

#[tokio::test]
async fn an_import_landing_between_row_removal_and_erase_keeps_its_blob() {
    let purging = purging_request("erase-race-between").await;
    // Another tenant imports identical bytes after the rows left and before the bytes do.
    let registered = ratatoskr_claude_archive::receipt::record_verified_platform_archive(
        &purging.wide,
        &purging.store,
        TenantScope::Account(purging.other_account),
        purging.raw.clone(),
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await;
    assert!(registered.is_ok(), "the second tenant registers the blob");

    purging
        .executor()
        .finalize_with_blob_store(purging.request, &purging.store, &purging.resolved, None)
        .await
        .expect("the deletion completes");

    let stored = purging.store.verify(&purging.raw);
    let rows = purging.export_rows_of_the_other_account().await;
    purging.finish().await;
    assert_eq!(rows, 1, "the second tenant's export row is committed");
    assert!(
        stored.is_ok(),
        "the eraser re-checks references and keeps bytes a committed row references"
    );
}

#[tokio::test]
async fn a_registrar_blocked_by_the_eraser_re_stores_the_blob() {
    let purging = purging_request("erase-race-restore").await;
    let gate = EraseGate::new();
    let eraser = purging.resume(&gate);
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.reached())
        .await
        .expect("the eraser reaches the gate");

    let (wide, store, raw, bytes, account) = (
        purging.wide.clone(),
        purging.store.clone(),
        purging.raw.clone(),
        purging.bytes.clone(),
        purging.other_account,
    );
    let registrar = tokio::spawn(async move {
        let mut tx = wide.pool().begin().await.expect("a transaction begins");
        blob_protocol::register(&mut tx, &store, &[&raw], &[bytes.as_slice()])
            .await
            .expect("the registrar finds or restores the blob");
        sqlx::query(
            "insert into claude_archive.exports
             (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
              byte_size, received_at)
             values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(account)
        .bind(&raw.digest_hex)
        .bind(format!("sha256/{}", raw.digest_hex))
        .bind(i64::try_from(raw.length_bytes).expect("the length fits"))
        .execute(&mut *tx)
        .await
        .expect("the referencing row inserts");
        tx.commit().await.expect("the registration commits");
    });
    let blocked = is_blocked(&registrar).await;
    gate.release();
    eraser.await.expect("the eraser task ends");
    registrar.await.expect("the registrar task ends");

    let restored = purging.store.verify(&purging.raw);
    let rows = purging.export_rows_of_the_other_account().await;
    purging.finish().await;
    assert!(
        blocked,
        "the registrar waits for the digest lock the eraser holds"
    );
    assert_eq!(rows, 1, "the registration committed");
    assert!(
        restored.is_ok(),
        "the registrar stored the bytes again after the eraser removed them"
    );
}

#[tokio::test]
async fn a_registrar_that_cannot_re_store_fails_retryably_and_registers_nothing() {
    let purging = purging_request("erase-race-no-bytes").await;
    let gate = EraseGate::new();
    let eraser = purging.resume(&gate);
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.reached())
        .await
        .expect("the eraser reaches the gate");

    let (wide, store, raw, account) = (
        purging.wide.clone(),
        purging.store.clone(),
        purging.raw.clone(),
        purging.other_account,
    );
    let registrar = tokio::spawn(async move {
        ratatoskr_claude_archive::receipt::record_verified_platform_archive(
            &wide,
            &store,
            TenantScope::Account(account),
            raw,
            PlatformOperation {
                operation_id: Uuid::now_v7(),
            },
        )
        .await
    });
    let blocked = is_blocked(&registrar).await;
    gate.release();
    eraser.await.expect("the eraser task ends");
    let outcome = registrar.await.expect("the registrar task ends");

    let rows = purging.export_rows_of_the_other_account().await;
    let present = purging.store.verify(&purging.raw).is_ok();
    purging.finish().await;
    assert!(
        blocked,
        "the registrar waits for the digest lock the eraser holds"
    );
    assert!(
        matches!(
            outcome,
            Err(ReceiptError::Registration(
                BlobRegistrationError::Erased { .. }
            ))
        ),
        "a receipt holds no bytes, so it fails with the retryable erased class: {outcome:?}"
    );
    assert_eq!(rows, 0, "nothing was registered");
    assert!(!present, "the eraser's removal stands");
}

#[tokio::test]
async fn an_import_blocked_by_the_eraser_stores_its_parser_evidence_again() {
    let purging = purging_request_of("erase-race-import", true).await;
    // The deleted export's parser evidence is a byte-identical blob the next import also stores.
    let evidence_key: String = sqlx::query_scalar(
        "select blob_ref from claude_archive.privacy_deletion_items
         where request_id = $1 and item_kind = 'extracted_artifact' and action = 'erase_blob'",
    )
    .bind(purging.request)
    .fetch_one(purging.wide.pool())
    .await
    .expect("the deleted export has parser evidence to erase");
    let evidence_digest = evidence_key
        .strip_prefix("sha256/")
        .expect("an inventory key is a content address")
        .to_owned();
    let gate = EraseGate::for_digest(evidence_digest.clone());
    let eraser = purging.resume(&gate);
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.reached())
        .await
        .expect("the eraser reaches the evidence blob and holds its digest lock");

    // The same archive arrives for another account: its raw bytes exist, and so does the evidence
    // until the eraser, which has already decided to remove it, gets there.
    receive(
        &purging.db,
        &purging.store,
        purging.other_account,
        &purging.bytes,
    )
    .await;
    let users = PlatformUsers::new(&[(Uuid::parse_str(USER).expect("user UUID"), purging.account)]);
    let pass = worker(&purging.db, &purging.store, users);
    let import = tokio::spawn(async move { pass.process_pending_once().await });
    let blocked = is_blocked(&import).await;
    gate.release();
    eraser.await.expect("the eraser task ends");
    let passes = import.await.expect("the import task ends");

    let key = format!("sha256/{evidence_digest}");
    let rows: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.extracted_artifacts x
         join claude_archive.exports e on e.export_id = x.export_id
         where e.account_ref = $1 and x.blob_ref = $2",
    )
    .bind(purging.other_account)
    .bind(&key)
    .fetch_one(purging.wide.pool())
    .await
    .expect("the evidence rows are countable");
    let evidence_present = purging.store.locate(&evidence_digest).is_ok();
    purging.finish().await;
    assert!(
        blocked,
        "the import waits for the digest lock the eraser holds"
    );
    assert_eq!(
        passes.expect("the import survives the erasure"),
        1,
        "the importer finished its pass"
    );
    assert_eq!(rows, 1, "the import registered its evidence");
    assert!(
        evidence_present,
        "the importer stored the evidence again after the eraser removed it, so the row resolves"
    );
}
