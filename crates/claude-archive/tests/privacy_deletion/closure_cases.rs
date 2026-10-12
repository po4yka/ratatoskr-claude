//! The dependent closure, rows-first ordering and contract tombstones of privacy deletion
//! (XR-021 CONTRACTS.md S07).

use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiArchiveTombstoneSubject};
use ratatoskr_claude_archive::privacy_deletion::handled_references;
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::TenantRef;
use sqlx::PgPool;

use super::*;

const TENANT: &str = "user:00000000-0000-7000-8000-000000000b01";
const ACCOUNT: &str = "b1000000-0000-0000-0000-0000000000b1";
const EXPORT: &str = "b2000000-0000-0000-0000-0000000000b1";
const OTHER_EXPORT: &str = "b2000000-0000-0000-0000-0000000000b2";
const ARCHIVE: &str = "b2100000-0000-0000-0000-0000000000b1";
const CONVERSATION: &str = "b4000000-0000-0000-0000-0000000000b1";
const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";

pub(super) struct Stored {
    pub(super) reference: BlobRef,
    pub(super) key: String,
}

pub(super) fn store_bytes(store: &BlobStore, media_type: &str, bytes: &[u8]) -> Stored {
    let reference = store
        .store(
            MediaType::parse(media_type).expect("fixture media type is canonical"),
            bytes,
        )
        .expect("fixture bytes are stored");
    let key = format!("sha256/{}", reference.digest_hex);
    Stored { reference, key }
}

pub(super) fn resolved(blobs: &[&Stored]) -> Vec<ResolvedDeletionBlob> {
    blobs
        .iter()
        .map(|blob| ResolvedDeletionBlob {
            key: blob.key.clone(),
            reference: blob.reference.clone(),
        })
        .collect()
}

async fn execute(sql: &str, pool: &PgPool) {
    sqlx::raw_sql(sql)
        .execute(pool)
        .await
        .expect("the fixture SQL runs");
}

/// An account, one raw export holding `raw`, and one observed conversation with a Knowledge link.
pub(super) async fn seed_linked_conversation(pool: &PgPool, raw: &Stored) {
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, 'closure-owner')",
    )
    .bind(Uuid::parse_str(ACCOUNT).expect("account UUID"))
    .execute(pool)
    .await
    .expect("the account seeds");
    sqlx::query(
        "insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
          received_at)
         values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
    )
    .bind(Uuid::parse_str(EXPORT).expect("export UUID"))
    .bind(Uuid::parse_str(ARCHIVE).expect("archive UUID"))
    .bind(Uuid::parse_str(ACCOUNT).expect("account UUID"))
    .bind(&raw.reference.digest_hex)
    .bind(&raw.key)
    .bind(i64::try_from(raw.reference.length_bytes).expect("length fits"))
    .execute(pool)
    .await
    .expect("the export seeds");
    execute(
        "insert into claude_archive.conversations
          (conversation_id, account_id, external_conversation_id, upstream_state)
         values ('b4000000-0000-0000-0000-0000000000b1', 'b1000000-0000-0000-0000-0000000000b1',
                 'linked-conversation', 'present');
         insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('b2000000-0000-0000-0000-0000000000b1', 'conversation',
                 'b4000000-0000-0000-0000-0000000000b1');
         insert into claude_archive.knowledge_analysis_links
          (completion_event_id, ai_archive_id, subject_kind, subject_id, content_digest_hex,
           completed_at)
         values ('b5700000-0000-0000-0000-0000000000b1', 'b2100000-0000-0000-0000-0000000000b1',
                 'conversation', 'b4000000-0000-0000-0000-0000000000b1', repeat('44', 32), now());
         insert into claude_archive.announced_subjects
          (subject_kind, subject_id, ai_archive_id, owner_ref, account_id)
         values ('archive', 'b2100000-0000-0000-0000-0000000000b1',
                 'b2100000-0000-0000-0000-0000000000b1',
                 'user:00000000-0000-7000-8000-000000000b01', 'b1000000-0000-0000-0000-0000000000b1'),
                ('conversation', 'b4000000-0000-0000-0000-0000000000b1',
                 'b2100000-0000-0000-0000-0000000000b1',
                 'user:00000000-0000-7000-8000-000000000b01', 'b1000000-0000-0000-0000-0000000000b1');",
        pool,
    )
    .await;
}

pub(super) async fn plan_conversation_deletion(pool: &PgPool, request: Uuid, key: &str) {
    plan_conversation_deletion_correlated(pool, request, key, &format!("privacy-delete:{request}")).await;
}

pub(super) async fn plan_conversation_deletion_correlated(
    pool: &PgPool,
    request: Uuid,
    key: &str,
    correlation_id: &str,
) {
    PrivacyDeletionPlanner::new(pool.clone())
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account UUID"),
            request_id: request,
            request_key: key.to_owned(),
            correlation_id: correlation_id.to_owned(),
            conversation_id: Uuid::parse_str(CONVERSATION).expect("conversation UUID"),
        })
        .await
        .expect("the conversation deletion is planned");
}

pub(super) async fn tombstone_envelopes(pool: &PgPool) -> Vec<EventEnvelope> {
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type = $1 order by occurred_at, event_id",
    )
    .bind(TOMBSTONED)
    .fetch_all(pool)
    .await
    .expect("the tombstone rows are readable");
    rows.into_iter()
        .map(|row| serde_json::from_value(row).expect("every stored row is a complete envelope"))
        .collect()
}

#[tokio::test]
async fn tombstone_outbox_row_is_a_complete_event_envelope() {
    let root = temp_root("closure-envelope");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"closure raw archive");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_linked_conversation(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b1").expect("request UUID");
    plan_conversation_deletion(db.database.pool(), request, "envelope").await;

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let envelopes = tombstone_envelopes(db.database.pool()).await;
    let tombstones: Vec<AiArchiveTombstone> = envelopes
        .iter()
        .map(|envelope| {
            envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("the payload is an AiArchiveTombstone")
        })
        .collect();
    let evidence_exists = tombstones.iter().all(|tombstone| {
        let evidence = &tombstone.evidence_ref;
        let reference = BlobRef {
            owner_service: evidence.owner_service.to_string(),
            algorithm: ratatoskr_claude_archive::DigestAlgorithm::Sha256,
            digest_hex: evidence.digest.hex.to_string(),
            media_type: MediaType::parse(evidence.media_type.as_str()).expect("media type"),
            length_bytes: evidence.length_bytes,
        };
        store.verify(&reference).is_ok()
    });
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    let owner = TenantRef::parse(TENANT).expect("the tenant is a user reference");
    assert_eq!(envelopes.len(), 2, "one archive tombstone and one conversation tombstone");
    assert!(
        tombstones.iter().all(|tombstone| tombstone.reason.as_str() == "user_requested"
            && tombstone.owner == owner
            && tombstone.ai_archive_id.0.to_string() == ARCHIVE
            && tombstone.provider.as_str() == "claude"),
        "every tombstone is a user-requested Claude fact owned by the Platform user"
    );
    assert!(
        envelopes.iter().all(|envelope| envelope.producer.as_str() == "ratatoskr-claude"
            && envelope.tenant_id == Some(owner)
            && envelope.event_type.to_wire() == TOMBSTONED),
        "the envelope names its producer, tenant and event type"
    );
    assert!(evidence_exists, "the evidence blob of every tombstone exists in the blob store");
    assert!(
        envelopes.iter().all(|envelope| envelope.correlation_id.to_string()
            == format!("privacy-delete:{request}")),
        "the envelope carries the correlation id the deletion request was recorded under"
    );
    assert!(
        tombstones
            .iter()
            .any(|tombstone| tombstone.subject == AiArchiveTombstoneSubject::Archive),
        "the deleted archive is tombstoned as a whole"
    );
    assert!(
        tombstones.iter().any(|tombstone| matches!(
            &tombstone.subject,
            AiArchiveTombstoneSubject::Conversation { ai_conversation_id }
                if ai_conversation_id.0.to_string() == CONVERSATION
        )),
        "the Knowledge-linked conversation is tombstoned"
    );
}

#[tokio::test]
async fn privacy_tombstone_does_not_abort_when_provider_tombstone_exists() {
    let root = temp_root("closure-conflict");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"closure conflict archive");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_linked_conversation(db.database.pool(), &raw).await;
    let owner = TenantRef::parse(TENANT).expect("the tenant is a user reference");
    let provider: AiArchiveTombstone = serde_json::from_value(serde_json::json!({
        "ai_archive_id": ARCHIVE,
        "provider": "claude",
        "owner": TENANT,
        "subject": {"subject_kind": "conversation", "ai_conversation_id": CONVERSATION},
        "reason": "provider_deletion_event",
        "evidence_ref": {
            "owner_service": "ratatoskr-claude",
            "digest": {"algorithm": "sha256", "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
            "media_type": "application/json",
            "length_bytes": 2
        },
        "observed_at": "2026-08-27T06:00:00Z"
    }))
    .expect("the provider tombstone decodes");
    assert_eq!(provider.owner, owner);
    ratatoskr_claude_archive::ArchiveOutbox::new(&db.database)
        .enqueue(
            &ratatoskr_claude_archive::ArchiveEventFact::Tombstoned(provider),
            "2026-08-27T06:00:01Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the provider tombstone is enqueued first");
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b2").expect("request UUID");
    plan_conversation_deletion(db.database.pool(), request, "conflict").await;

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let state: String = sqlx::query_scalar(
        "select state from claude_archive.privacy_deletion_requests where request_id = $1",
    )
    .bind(request)
    .fetch_one(db.database.pool())
    .await
    .expect("the request row is readable");
    let reasons: Vec<String> = tombstone_envelopes(db.database.pool())
        .await
        .iter()
        .map(|envelope| {
            envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("a tombstone payload")
                .reason
                .to_string()
        })
        .collect();
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("a provider tombstone for the same aggregate does not abort the deletion");
    assert_eq!(state, "completed", "the deletion still completes");
    assert!(
        reasons.contains(&"provider_deletion_event".to_owned())
            && reasons.contains(&"user_requested".to_owned()),
        "the earlier provider tombstone stays and the archive tombstone is still written: {reasons:?}"
    );
}

/// The owner of a tombstone is the one the subject was announced under (XR-021 CONTRACTS.md
/// R5-06), so the reference the request is recorded under never decides it and a free-form one is
/// planned like any other.
#[tokio::test]
async fn a_free_form_tenant_reference_is_planned_and_tombstones_go_to_the_ledger_owner() {
    let root = temp_root("closure-tenant-ref");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"closure tenant archive");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_linked_conversation(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b3").expect("UUID");
    let planned = PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: "tenant:not-a-user".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account UUID"),
            request_id: request,
            request_key: "free-form".to_owned(),
            correlation_id: "privacy-delete:free-form".to_owned(),
            export_id: Uuid::parse_str(EXPORT).expect("export UUID"),
        })
        .await;
    let recorded: Vec<String> =
        sqlx::query_scalar("select tenant_ref from claude_archive.privacy_deletion_requests")
            .fetch_all(db.database.pool())
            .await
            .expect("the request is readable");
    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let owners: Vec<String> = tombstone_envelopes(db.database.pool())
        .await
        .iter()
        .map(|envelope| {
            envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("the payload is a tombstone")
                .owner
                .to_string()
        })
        .collect();
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    planned.expect("a free-form tenant reference is planned");
    outcome.expect("and executed");
    assert_eq!(recorded, vec!["tenant:not-a-user".to_owned()]);
    assert_eq!(
        owners,
        vec![TENANT.to_owned(), TENANT.to_owned()],
        "the archive and the conversation are tombstoned to the owner the ledger holds"
    );
}

/// An account with an export (raw blob), run, operation import and reparse run; one project with a
/// source and asset; a message with two parts (one stored blob), a relation and an asset; plus a
/// project and message that a second export also observed.
struct DependentFixture {
    raw: Stored,
    other_raw: Stored,
    source_blob: Stored,
    part_blob: Stored,
    shared_blob: Stored,
}

async fn seed_dependents(pool: &PgPool, store: &BlobStore) -> DependentFixture {
    let fixture = DependentFixture {
        raw: store_bytes(store, "application/zip", b"dependent raw archive"),
        other_raw: store_bytes(store, "application/zip", b"second dependent raw archive"),
        source_blob: store_bytes(store, "text/plain", b"exclusive project source bytes"),
        part_blob: store_bytes(store, "image/png", b"exclusive content part bytes"),
        shared_blob: store_bytes(store, "text/plain", b"bytes a retained source also uses"),
    };
    seed_account_and_exports(pool, &fixture).await;
    seed_records_and_runs(pool).await;
    seed_blob_carrying_rows(pool, &fixture).await;
    seed_relations_and_observations(pool).await;
    fixture
}

/// The account and its two raw exports.
async fn seed_account_and_exports(pool: &PgPool, fixture: &DependentFixture) {
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, 'dependent-owner')",
    )
    .bind(Uuid::parse_str(ACCOUNT).expect("account UUID"))
    .execute(pool)
    .await
    .expect("the account seeds");
    for (export, hash, key, length) in [
        (EXPORT, &fixture.raw.reference.digest_hex, &fixture.raw.key, fixture.raw.reference.length_bytes),
        (
            OTHER_EXPORT,
            &fixture.other_raw.reference.digest_hex,
            &fixture.other_raw.key,
            fixture.other_raw.reference.length_bytes,
        ),
    ] {
        sqlx::query(
            "insert into claude_archive.exports
             (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
              received_at)
             values ($1, gen_random_uuid(), $2, 'consumer_export', decode($3, 'hex'), $4, $5, now())",
        )
        .bind(Uuid::parse_str(export).expect("export UUID"))
        .bind(Uuid::parse_str(ACCOUNT).expect("account UUID"))
        .bind(hash)
        .bind(key)
        .bind(i64::try_from(length).expect("length fits"))
        .execute(pool)
        .await
        .expect("the export seeds");
    }
}

/// Projects, the conversation and its messages, and the import and reparse runs of the first
/// export.
async fn seed_records_and_runs(pool: &PgPool) {
    execute(
        "insert into claude_archive.projects (project_id, account_id, external_project_id, upstream_state)
         values ('b3000000-0000-0000-0000-0000000000b1', 'b1000000-0000-0000-0000-0000000000b1', 'removed-project', 'present'),
                ('b3000000-0000-0000-0000-0000000000b2', 'b1000000-0000-0000-0000-0000000000b1', 'retained-project', 'present');
         insert into claude_archive.conversations
          (conversation_id, account_id, external_conversation_id, upstream_state)
         values ('b4000000-0000-0000-0000-0000000000b1', 'b1000000-0000-0000-0000-0000000000b1', 'dependent-conversation', 'present');
         insert into claude_archive.messages
          (message_id, conversation_id, external_message_id, parent_message_id, role)
         values ('b4100000-0000-0000-0000-0000000000b1', 'b4000000-0000-0000-0000-0000000000b1', 'removed-message', null, 'user'),
                ('b4100000-0000-0000-0000-0000000000b2', 'b4000000-0000-0000-0000-0000000000b1', 'reply-message',
                 'b4100000-0000-0000-0000-0000000000b1', 'assistant'),
                ('b4100000-0000-0000-0000-0000000000b3', 'b4000000-0000-0000-0000-0000000000b1', 'retained-message', null, 'user');
         insert into claude_archive.import_runs (run_id, export_id, state)
         values ('b2200000-0000-0000-0000-0000000000b1', 'b2000000-0000-0000-0000-0000000000b1', 'completed');
         insert into claude_archive.platform_operation_imports (operation_id, export_id, import_run_id)
         values ('b2300000-0000-0000-0000-0000000000b1', 'b2000000-0000-0000-0000-0000000000b1', 'b2200000-0000-0000-0000-0000000000b1');
         insert into claude_archive.reparse_runs
          (reparse_run_id, tenant_ref, export_id, parser_name, parser_version, raw_fingerprint,
           registry_fingerprint, projection_fingerprint, plan_fingerprint, state, report, correlation_id)
         values ('b2400000-0000-0000-0000-0000000000b1', 'user:00000000-0000-7000-8000-000000000b01',
                 'b2000000-0000-0000-0000-0000000000b1', 'p', '1', decode('01','hex'), decode('02','hex'),
                 decode('03','hex'), decode('04','hex'), 'applied', '{}', 'reparse:b1');",
        pool,
    )
    .await;
}

/// The rows that carry blobs: project sources, content parts and a message asset.
async fn seed_blob_carrying_rows(pool: &PgPool, fixture: &DependentFixture) {
    sqlx::query(
        "insert into claude_archive.project_sources
          (source_id, project_id, external_source_id, source_kind, blob_ref, locally_backed_up)
         values ('b5100000-0000-0000-0000-0000000000b1', 'b3000000-0000-0000-0000-0000000000b1',
                 'removed-source', 'file', $1, true),
                ('b5100000-0000-0000-0000-0000000000b2', 'b3000000-0000-0000-0000-0000000000b2',
                 'retained-source', 'file', $2, true)",
    )
    .bind(&fixture.source_blob.key)
    .bind(&fixture.shared_blob.key)
    .execute(pool)
    .await
    .expect("the project sources seed");
    sqlx::query(
        "insert into claude_archive.content_parts
          (content_part_id, message_id, part_index, part_kind, body, blob_ref, mime_type)
         values ('b4300000-0000-0000-0000-0000000000b1', 'b4100000-0000-0000-0000-0000000000b1', 0, 'text', 'first part', null, null),
                ('b4300000-0000-0000-0000-0000000000b2', 'b4100000-0000-0000-0000-0000000000b1', 1, 'image', null, $1, 'image/png'),
                ('b4300000-0000-0000-0000-0000000000b3', 'b4100000-0000-0000-0000-0000000000b3', 0, 'text', 'retained part', null, null)",
    )
    .bind(&fixture.part_blob.key)
    .execute(pool)
    .await
    .expect("the content parts seed");
    sqlx::query(
        "insert into claude_archive.assets (asset_id, asset_kind, blob_ref, content_hash, message_id)
         values ('b5300000-0000-0000-0000-0000000000b1', 'upload', $1, decode($2, 'hex'),
                 'b4100000-0000-0000-0000-0000000000b1')",
    )
    .bind(&fixture.part_blob.key)
    .bind(&fixture.part_blob.reference.digest_hex)
    .execute(pool)
    .await
    .expect("the message asset seeds");
}

/// The message relation and which export observed which project and message.
async fn seed_relations_and_observations(pool: &PgPool) {
    execute(
        "insert into claude_archive.message_relations (relation_id, parent_message_id, child_message_id, relation_kind)
         values ('b4200000-0000-0000-0000-0000000000b1', 'b4100000-0000-0000-0000-0000000000b1',
                 'b4100000-0000-0000-0000-0000000000b2', 'branch_continuation');
         insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('b2000000-0000-0000-0000-0000000000b1', 'project', 'b3000000-0000-0000-0000-0000000000b1'),
                ('b2000000-0000-0000-0000-0000000000b1', 'project', 'b3000000-0000-0000-0000-0000000000b2'),
                ('b2000000-0000-0000-0000-0000000000b2', 'project', 'b3000000-0000-0000-0000-0000000000b2'),
                ('b2000000-0000-0000-0000-0000000000b1', 'message', 'b4100000-0000-0000-0000-0000000000b1'),
                ('b2000000-0000-0000-0000-0000000000b1', 'message', 'b4100000-0000-0000-0000-0000000000b3'),
                ('b2000000-0000-0000-0000-0000000000b2', 'message', 'b4100000-0000-0000-0000-0000000000b3');",
        pool,
    )
    .await;
}

async fn dependents_remaining(pool: &PgPool) -> Vec<(String, i64)> {
    sqlx::query_as(
        "select 'content_parts', count(*) from claude_archive.content_parts
         union all select 'project_sources', count(*) from claude_archive.project_sources
         union all select 'assets', count(*) from claude_archive.assets
         union all select 'message_relations', count(*) from claude_archive.message_relations
         union all select 'messages', count(*) from claude_archive.messages
         union all select 'projects', count(*) from claude_archive.projects
         union all select 'import_runs', count(*) from claude_archive.import_runs
         union all select 'platform_operation_imports', count(*) from claude_archive.platform_operation_imports
         union all select 'reparse_runs', count(*) from claude_archive.reparse_runs",
    )
    .fetch_all(pool)
    .await
    .expect("the dependent counts are readable")
}

#[tokio::test]
async fn raw_export_scope_removes_content_parts_and_project_sources() {
    let root = temp_root("closure-raw-export");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let db = TestDatabase::create().await.expect("a disposable database");
    let fixture = seed_dependents(db.database.pool(), &store).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b5").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account UUID"),
            request_id: request,
            request_key: "raw-export-dependents".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: Uuid::parse_str(EXPORT).expect("export UUID"),
        })
        .await
        .expect("the raw export deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(
            request,
            &store,
            &resolved(&[&fixture.raw, &fixture.source_blob, &fixture.part_blob]),
            None,
        )
        .await;
    let remaining = dependents_remaining(db.database.pool()).await;
    let retained_children: (i64, i64) = sqlx::query_as(
        "select (select count(*) from claude_archive.project_sources
                 where source_id = 'b5100000-0000-0000-0000-0000000000b2'),
                (select count(*) from claude_archive.content_parts
                 where content_part_id = 'b4300000-0000-0000-0000-0000000000b3')",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("retained children are readable");
    let erased = (
        store.verify(&fixture.raw.reference).is_err(),
        store.verify(&fixture.source_blob.reference).is_err(),
        store.verify(&fixture.part_blob.reference).is_err(),
        store.verify(&fixture.shared_blob.reference).is_ok(),
    );
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the dependents no longer abort the deletion on a foreign key");
    assert_eq!(
        remaining,
        vec![
            ("content_parts".to_owned(), 1),
            ("project_sources".to_owned(), 1),
            ("assets".to_owned(), 0),
            ("message_relations".to_owned(), 0),
            ("messages".to_owned(), 1),
            ("projects".to_owned(), 1),
            ("import_runs".to_owned(), 0),
            ("platform_operation_imports".to_owned(), 0),
            ("reparse_runs".to_owned(), 0),
        ],
        "the removed parents lose every dependent and the observed-elsewhere parents keep theirs"
    );
    assert_eq!(retained_children, (1, 1), "children of retained parents are untouched");
    assert_eq!(
        erased,
        (true, true, true, true),
        "the raw, source and content-part bytes are erased and the shared bytes survive"
    );
}

#[tokio::test]
async fn conversation_scope_removes_message_dependents() {
    let root = temp_root("closure-conversation");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let db = TestDatabase::create().await.expect("a disposable database");
    let fixture = seed_dependents(db.database.pool(), &store).await;
    execute(
        "insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('b2000000-0000-0000-0000-0000000000b1', 'conversation', 'b4000000-0000-0000-0000-0000000000b1');
         insert into claude_archive.artifacts
          (artifact_id, account_id, conversation_id, external_artifact_id, upstream_state)
         values ('b5000000-0000-0000-0000-0000000000b1', 'b1000000-0000-0000-0000-0000000000b1',
                 'b4000000-0000-0000-0000-0000000000b1', 'conversation-artifact', 'present');
         insert into claude_archive.artifact_versions (version_id, artifact_id, external_version_id, version_index)
         values ('b5200000-0000-0000-0000-0000000000b1', 'b5000000-0000-0000-0000-0000000000b1', 'v1', 0);
         insert into claude_archive.external_references (reference_id, artifact_id, provider_reference)
         values ('b5400000-0000-0000-0000-0000000000b1', 'b5000000-0000-0000-0000-0000000000b1', 'opaque');
         insert into claude_archive.backup_status_audits
          (audit_id, reference_id, external_entity_id, previous_status, new_status, evidence_kind, observed_at)
         values ('b5500000-0000-0000-0000-0000000000b1', 'b5400000-0000-0000-0000-0000000000b1', 'artifact',
                 'reference_only', 'locally_backed_up', 'verified', now());",
        db.database.pool(),
    )
    .await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b6").expect("request UUID");
    plan_conversation_deletion(db.database.pool(), request, "conversation-dependents").await;

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(
            request,
            &store,
            &resolved(&[&fixture.raw, &fixture.source_blob, &fixture.part_blob]),
            None,
        )
        .await;
    let remaining: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "select (select count(*) from claude_archive.messages
                 where conversation_id = 'b4000000-0000-0000-0000-0000000000b1'),
                (select count(*) from claude_archive.content_parts),
                (select count(*) from claude_archive.artifacts),
                (select count(*) from claude_archive.artifact_versions),
                (select count(*) from claude_archive.external_references),
                (select count(*) from claude_archive.backup_status_audits)",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("the dependent counts are readable");
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("conversation dependents no longer abort the deletion");
    assert_eq!(
        remaining,
        (0, 0, 0, 0, 0, 0),
        "messages, parts, artifacts, versions, references and audits of the conversation are gone"
    );
}

#[tokio::test]
async fn tenant_scope_deletes_reparse_runs_and_platform_imports() {
    let root = temp_root("closure-tenant");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let db = TestDatabase::create().await.expect("a disposable database");
    let fixture = seed_dependents(db.database.pool(), &store).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b7").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account UUID"),
            request_id: request,
            request_key: "tenant-dependents".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
        })
        .await
        .expect("the tenant deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(
            request,
            &store,
            &resolved(&[
                &fixture.raw,
                &fixture.other_raw,
                &fixture.source_blob,
                &fixture.part_blob,
                &fixture.shared_blob,
            ]),
            None,
        )
        .await;
    let remaining = dependents_remaining(db.database.pool()).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the tenant deletion completes with operation imports and reparse runs");
    assert!(
        remaining.iter().all(|(_, count)| *count == 0),
        "nothing of the tenant remains: {remaining:?}"
    );
}

#[tokio::test]
async fn every_foreign_key_into_deletion_roots_is_handled() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let roots = [
        "exports",
        "import_runs",
        "accounts",
        "projects",
        "conversations",
        "messages",
        "artifacts",
        "project_sources",
        "external_references",
    ];
    let references: Vec<(String, String, String, String)> = sqlx::query_as(
        "select child.relname::text, att.attname::text, parent.relname::text, con.confdeltype::text
         from pg_constraint con
         join pg_class child on child.oid = con.conrelid
         join pg_class parent on parent.oid = con.confrelid
         join pg_attribute att on att.attrelid = con.conrelid and att.attnum = any (con.conkey)
         where con.contype = 'f'
           and con.connamespace = 'claude_archive'::regnamespace
           and parent.relname = any ($1)
         order by child.relname, att.attname",
    )
    .bind(roots.as_slice())
    .fetch_all(db.database.pool())
    .await
    .expect("the foreign key catalog is readable");
    db.cleanup().await.expect("cleanup succeeds");

    let handled = handled_references();
    let unhandled: Vec<String> = references
        .iter()
        .filter(|(table, column, _, action)| {
            action != "c" && !handled.contains(&(table.as_str(), column.as_str()))
        })
        .map(|(table, column, parent, _)| format!("{table}.{column} -> {parent}"))
        .collect();
    let stale: Vec<String> = handled
        .iter()
        .filter(|(table, column)| {
            !references
                .iter()
                .any(|(child, attribute, _, _)| child == table && attribute == column)
        })
        .map(|(table, column)| format!("{table}.{column}"))
        .collect();
    assert!(
        references.len() >= 20,
        "the census must see the real foreign keys, saw {}",
        references.len()
    );
    assert!(
        stale.is_empty(),
        "HANDLED_REFERENCES names foreign keys that no longer exist: {stale:?}"
    );
    assert!(
        unhandled.is_empty(),
        "foreign keys into deletion roots that are neither cascading nor handled: {unhandled:?}"
    );
}
