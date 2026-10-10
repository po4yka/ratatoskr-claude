//! A deletion tombstones every removed conversation and project, linked to Knowledge or not
//! (XR-021 CONTRACTS.md S07). The facts of an import announce every project and conversation, so
//! the deletion that removes one must say so downstream whether or not Knowledge ever analysed it.

use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiArchiveTombstoneSubject};

use super::closure_cases::{resolved, seed_linked_conversation, store_bytes, tombstone_envelopes};
use super::*;

const TENANT: &str = "user:00000000-0000-7000-8000-000000000b01";
const ACCOUNT: &str = "b1000000-0000-0000-0000-0000000000b1";
const EXPORT: &str = "b2000000-0000-0000-0000-0000000000b1";
const OTHER_EXPORT: &str = "b2000000-0000-0000-0000-0000000000b2";
const OTHER_ARCHIVE: &str = "b2100000-0000-0000-0000-0000000000b2";
const CONVERSATION: &str = "b4000000-0000-0000-0000-0000000000b1";
const PROJECT: &str = "b3000000-0000-0000-0000-0000000000b1";

/// The seeded linked conversation plus one project, both observed by the first export.
async fn seed_unlinked(pool: &sqlx::PgPool, raw: &closure_cases::Stored) {
    seed_linked_conversation(pool, raw).await;
    sqlx::raw_sql(&format!(
        "delete from claude_archive.knowledge_analysis_links;
         insert into claude_archive.projects
          (project_id, account_id, external_project_id, upstream_state, local_backup_status)
         values ('{PROJECT}', '{ACCOUNT}', 'unlinked-project', 'present', 'reference_only');
         insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('{EXPORT}', 'project', '{PROJECT}');"
    ))
    .execute(pool)
    .await
    .expect("the unlinked project seeds");
}

/// The tombstoned subjects in a stable spelling.
async fn tombstoned(pool: &sqlx::PgPool) -> Vec<String> {
    let mut subjects: Vec<String> = tombstone_envelopes(pool)
        .await
        .iter()
        .map(|envelope| {
            let tombstone = envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("the payload is an AiArchiveTombstone");
            match tombstone.subject {
                AiArchiveTombstoneSubject::Archive => "archive".to_owned(),
                AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
                    format!("conversation:{}", ai_conversation_id.0)
                }
                AiArchiveTombstoneSubject::Project { ai_project_id } => {
                    format!("project:{}", ai_project_id.0)
                }
                AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id,
                } => format!("artifact:{external_artifact_id}"),
            }
        })
        .collect();
    subjects.sort();
    subjects
}

fn account() -> Uuid {
    Uuid::parse_str(ACCOUNT).expect("account UUID")
}

#[tokio::test]
async fn a_conversation_with_no_knowledge_link_is_tombstoned() {
    let root = temp_root("tombstone-unlinked-conversation");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"tombstone unlinked conversation");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_unlinked(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000c1").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: account(),
            request_id: request,
            request_key: "unlinked-conversation".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            conversation_id: Uuid::parse_str(CONVERSATION).expect("conversation UUID"),
        })
        .await
        .expect("the conversation deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let subjects = tombstoned(db.database.pool()).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert_eq!(
        subjects,
        vec![
            "archive".to_owned(),
            format!("conversation:{CONVERSATION}"),
            format!("project:{PROJECT}"),
        ],
        "the archive, the conversation and the project the same export observed are tombstoned \
         although Knowledge never linked any of them"
    );
}

#[tokio::test]
async fn a_deleted_project_is_tombstoned() {
    let root = temp_root("tombstone-project");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"tombstone project");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_unlinked(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000c2").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: account(),
            request_id: request,
            request_key: "project".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: Uuid::parse_str(EXPORT).expect("export UUID"),
        })
        .await
        .expect("the export deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let subjects = tombstoned(db.database.pool()).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert_eq!(
        subjects,
        vec![
            "archive".to_owned(),
            format!("conversation:{CONVERSATION}"),
            format!("project:{PROJECT}"),
        ],
        "the archive, its conversation and its project are each tombstoned"
    );
}

#[tokio::test]
async fn a_tenant_deletion_tombstones_every_unlinked_conversation_and_project() {
    let root = temp_root("tombstone-tenant");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"tombstone tenant");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_unlinked(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000c3").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: account(),
            request_id: request,
            request_key: "tenant".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
        })
        .await
        .expect("the tenant deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let subjects = tombstoned(db.database.pool()).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert_eq!(
        subjects,
        vec![
            "archive".to_owned(),
            format!("conversation:{CONVERSATION}"),
            format!("project:{PROJECT}"),
        ],
        "a tenant-scope deletion tombstones the same subjects"
    );
}

/// The edge `design.md` names: a conversation another export also observed survives the deletion
/// of one export, so it is not tombstoned by itself, while the deleted archive is.
///
/// The archive tombstone is what the deleted export owes Knowledge. Whether Knowledge then drops
/// the retained conversation, which was announced under the deleted archive id, is Knowledge's
/// behaviour and is recorded as an open decision for the owner; this test pins what the archive
/// side emits so a change of that behaviour is deliberate.
#[tokio::test]
async fn a_conversation_retained_by_another_export_is_not_tombstoned_by_itself() {
    let root = temp_root("tombstone-retained");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"tombstone retained first");
    let other = store_bytes(&store, "application/zip", b"tombstone retained second");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_unlinked(db.database.pool(), &raw).await;
    sqlx::query(
        "insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
          received_at)
         values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
    )
    .bind(Uuid::parse_str(OTHER_EXPORT).expect("export UUID"))
    .bind(Uuid::parse_str(OTHER_ARCHIVE).expect("archive UUID"))
    .bind(account())
    .bind(&other.reference.digest_hex)
    .bind(&other.key)
    .bind(i64::try_from(other.reference.length_bytes).expect("length fits"))
    .execute(db.database.pool())
    .await
    .expect("the second export seeds");
    sqlx::raw_sql(&format!(
        "insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('{OTHER_EXPORT}', 'conversation', '{CONVERSATION}');"
    ))
    .execute(db.database.pool())
    .await
    .expect("the second export observes the conversation");
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000c4").expect("request UUID");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: TENANT.to_owned(),
            account_id: account(),
            request_id: request,
            request_key: "retained".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: Uuid::parse_str(EXPORT).expect("export UUID"),
        })
        .await
        .expect("the export deletion is planned");

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let subjects = tombstoned(db.database.pool()).await;
    let conversation_rows: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.conversations where conversation_id = $1",
    )
    .bind(Uuid::parse_str(CONVERSATION).expect("conversation UUID"))
    .fetch_one(db.database.pool())
    .await
    .expect("the conversation count is readable");
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert_eq!(conversation_rows, 1, "the retained conversation survives");
    assert_eq!(
        subjects,
        vec!["archive".to_owned(), format!("project:{PROJECT}")],
        "the deleted archive and the project only it observed are tombstoned; the retained \
         conversation is not"
    );
}
