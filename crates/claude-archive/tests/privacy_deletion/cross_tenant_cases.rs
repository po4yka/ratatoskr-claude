//! Two accounts that hold an Artifact with the same provider id stay isolated by a deletion
//! (XR-021 CONTRACTS.md S07). The provider id of an Artifact recurs across tenants, so its
//! Knowledge analysis link, its inbox row and its Artifact tombstone belong to the archive of the
//! account that holds it, never to the id alone.

use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiArchiveTombstoneSubject};

use super::closure_cases::{resolved, store_bytes, tombstone_envelopes};
use super::*;

const ARTIFACT_PROVIDER_ID: &str = "react-component";
const TENANT_A: &str = "user:00000000-0000-7000-8000-0000000000c1";
const TENANT_B: &str = "user:00000000-0000-7000-8000-0000000000c2";
const ACCOUNT_A: &str = "c1000000-0000-0000-0000-0000000000a1";
const ACCOUNT_B: &str = "c1000000-0000-0000-0000-0000000000b1";
const EXPORT_A: &str = "c2000000-0000-0000-0000-0000000000a1";
const EXPORT_B: &str = "c2000000-0000-0000-0000-0000000000b1";
const ARCHIVE_A: &str = "c2100000-0000-0000-0000-0000000000a1";
const ARCHIVE_B: &str = "c2100000-0000-0000-0000-0000000000b1";
const PROJECT_A: &str = "c3000000-0000-0000-0000-0000000000a1";
const PROJECT_B: &str = "c3000000-0000-0000-0000-0000000000b1";
const ARTIFACT_A: &str = "c5000000-0000-0000-0000-0000000000a1";
const ARTIFACT_B: &str = "c5000000-0000-0000-0000-0000000000b1";
const COMPLETION_A: &str = "c5700000-0000-0000-0000-0000000000a1";
const COMPLETION_B: &str = "c5700000-0000-0000-0000-0000000000b1";

/// Seeds one account whose single export observes a project holding the shared-id Artifact, with
/// a Knowledge link and its inbox row for that Artifact.
async fn seed_account(
    pool: &sqlx::PgPool,
    raw: &closure_cases::Stored,
    letter: &str,
    ids: [&str; 7],
) {
    let [
        account,
        export,
        archive,
        project,
        artifact,
        completion,
        digest,
    ] = ids;
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(Uuid::parse_str(account).expect("account UUID"))
    .bind(format!("cross-tenant-{letter}"))
    .execute(pool)
    .await
    .expect("the account seeds");
    sqlx::query(
        "insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
          received_at)
         values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
    )
    .bind(Uuid::parse_str(export).expect("export UUID"))
    .bind(Uuid::parse_str(archive).expect("archive UUID"))
    .bind(Uuid::parse_str(account).expect("account UUID"))
    .bind(&raw.reference.digest_hex)
    .bind(&raw.key)
    .bind(i64::try_from(raw.reference.length_bytes).expect("length fits"))
    .execute(pool)
    .await
    .expect("the export seeds");
    sqlx::raw_sql(&format!(
        "insert into claude_archive.projects
          (project_id, account_id, external_project_id, upstream_state, local_backup_status)
         values ('{project}', '{account}', 'project-{letter}', 'present', 'reference_only');
         insert into claude_archive.artifacts
          (artifact_id, account_id, project_id, external_artifact_id, upstream_state,
           local_backup_status)
         values ('{artifact}', '{account}', '{project}', '{ARTIFACT_PROVIDER_ID}', 'present',
                 'reference_only');
         insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ('{export}', 'project', '{project}'), ('{export}', 'artifact', '{artifact}');
         insert into claude_archive.knowledge_analysis_links
          (completion_event_id, ai_archive_id, subject_kind, subject_id, content_digest_hex,
           completed_at)
         values ('{completion}', '{archive}', 'artifact', '{ARTIFACT_PROVIDER_ID}',
                 repeat('{digest}', 32), now());
         insert into claude_archive.inbox_events
          (consumer_name, event_id, consumed_at, handler_outcome)
         values ('knowledge-analysis-completed', '{completion}', now(), 'processed');"
    ))
    .execute(pool)
    .await
    .expect("the project, Artifact and analysis link seed");
}

async fn plan_export(
    pool: &sqlx::PgPool,
    tenant: &str,
    account: &str,
    export: &str,
    request: Uuid,
) -> DeletionInventory {
    PrivacyDeletionPlanner::new(pool.clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: tenant.to_owned(),
            account_id: Uuid::parse_str(account).expect("account UUID"),
            request_id: request,
            request_key: format!("cross-tenant-{request}"),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: Uuid::parse_str(export).expect("export UUID"),
        })
        .await
        .expect("the export deletion is planned")
}

/// Every tombstone as `(archive, subject)`, in a stable order.
async fn tombstone_pairs(pool: &sqlx::PgPool) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = tombstone_envelopes(pool)
        .await
        .iter()
        .map(|envelope| {
            let tombstone = envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("the payload is an AiArchiveTombstone");
            let subject = match tombstone.subject {
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
            };
            (tombstone.ai_archive_id.0.to_string(), subject)
        })
        .collect();
    pairs.sort();
    pairs
}

async fn count(pool: &sqlx::PgPool, sql: &str, id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(Uuid::parse_str(id).expect("fixture UUID"))
        .fetch_one(pool)
        .await
        .expect("the count is readable")
}

/// Account A and account B, each with its own export, project, Artifact and analysis link.
async fn seed_both_accounts(
    pool: &sqlx::PgPool,
    raw_a: &closure_cases::Stored,
    raw_b: &closure_cases::Stored,
) {
    seed_account(
        pool,
        raw_a,
        "a",
        [
            ACCOUNT_A,
            EXPORT_A,
            ARCHIVE_A,
            PROJECT_A,
            ARTIFACT_A,
            COMPLETION_A,
            "aa",
        ],
    )
    .await;
    seed_account(
        pool,
        raw_b,
        "b",
        [
            ACCOUNT_B,
            EXPORT_B,
            ARCHIVE_B,
            PROJECT_B,
            ARTIFACT_B,
            COMPLETION_B,
            "bb",
        ],
    )
    .await;
}

/// How many of account B's link, inbox row and Artifact rows exist.
async fn rows_of_b(pool: &sqlx::PgPool) -> (i64, i64, i64) {
    (
        count(
            pool,
            "select count(*) from claude_archive.knowledge_analysis_links
             where completion_event_id = $1",
            COMPLETION_B,
        )
        .await,
        count(
            pool,
            "select count(*) from claude_archive.inbox_events where event_id = $1",
            COMPLETION_B,
        )
        .await,
        count(
            pool,
            "select count(*) from claude_archive.artifacts where artifact_id = $1",
            ARTIFACT_B,
        )
        .await,
    )
}

#[tokio::test]
async fn deleting_one_accounts_artifact_leaves_another_accounts_same_id_untouched() {
    let root = temp_root("cross-tenant-artifact");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw_a = store_bytes(&store, "application/zip", b"cross tenant raw archive a");
    let raw_b = store_bytes(&store, "application/zip", b"cross tenant raw archive b");
    let db = TestDatabase::create().await.expect("a disposable database");
    let pool = db.database.pool();
    seed_both_accounts(pool, &raw_a, &raw_b).await;

    let request_a = Uuid::parse_str("c6000000-0000-0000-0000-0000000000a1").expect("request UUID");
    let inventory = plan_export(pool, TENANT_A, ACCOUNT_A, EXPORT_A, request_a).await;
    let planned_links: Vec<&str> = inventory
        .items
        .iter()
        .filter(|item| {
            matches!(
                item.kind,
                DeletionItemKind::AnalysisLink | DeletionItemKind::Inbox
            )
        })
        .map(|item| item.subject_id.as_str())
        .collect();
    let outcome = PrivacyDeletionExecutor::new(pool.clone())
        .finalize_with_blob_store(request_a, &store, &resolved(&[&raw_a]), None)
        .await;
    let after_a = tombstone_pairs(pool).await;
    let b_rows = rows_of_b(pool).await;

    let request_b = Uuid::parse_str("c6000000-0000-0000-0000-0000000000b1").expect("request UUID");
    plan_export(pool, TENANT_B, ACCOUNT_B, EXPORT_B, request_b).await;
    let outcome_b = PrivacyDeletionExecutor::new(pool.clone())
        .finalize_with_blob_store(request_b, &store, &resolved(&[&raw_b]), None)
        .await;
    let after_b = tombstone_pairs(pool).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("account A's deletion completes");
    outcome_b.expect("account B's deletion completes");
    assert_eq!(
        planned_links,
        vec![COMPLETION_A, COMPLETION_A],
        "only account A's analysis link and its inbox row are planned"
    );
    assert_eq!(
        b_rows,
        (1, 1, 1),
        "account B's link, inbox row and Artifact stay"
    );
    let artifact = format!("artifact:{ARTIFACT_PROVIDER_ID}");
    assert_eq!(
        after_a
            .iter()
            .filter(|(_, subject)| *subject == artifact)
            .collect::<Vec<_>>(),
        vec![&(ARCHIVE_A.to_owned(), artifact.clone())],
        "account A's deletion tombstones the Artifact under account A's archive only"
    );
    assert_eq!(
        after_b
            .iter()
            .filter(|(archive, subject)| archive == ARCHIVE_B && *subject == artifact)
            .count(),
        1,
        "account B's own deletion later tombstones its Artifact under account B's archive"
    );
}

const CONVERSATION_ONE: &str = "c4000000-0000-0000-0000-0000000000d1";
const CONVERSATION_TWO: &str = "c4000000-0000-0000-0000-0000000000d2";
const ARTIFACT_ONE: &str = "c5000000-0000-0000-0000-0000000000d1";
const ARTIFACT_TWO: &str = "c5000000-0000-0000-0000-0000000000d2";
const EXPORT_ONE: &str = "c2000000-0000-0000-0000-0000000000d1";
const EXPORT_TWO: &str = "c2000000-0000-0000-0000-0000000000d2";

/// An unpublished `artifact.added` row for the Artifact of `conversation`, shaped as the fact
/// projection stores it: the scope refs sit in the payload, the aggregate carries only the id.
fn artifact_fact_sql(event: &str, conversation: &str, digest: &str) -> String {
    format!(
        "insert into claude_archive.outbox_events
          (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
           correlation_id, tenant_ref, occurred_at)
         values ('{event}', 'ai_archive.artifact.added.v1', 'artifact',
                 'artifact:{ARTIFACT_PROVIDER_ID}',
                 '{{\"payload\":{{\"artifact\":{{\"conversation_ref\":\"ai_conversation:{conversation}\",
                   \"project_ref\":null}}}}}}',
                 decode(repeat('{digest}', 32), 'hex'), 'seed-correlation', '{TENANT_A}', now())"
    )
}

/// Two conversations of one account, each holding an Artifact with the same provider id, each
/// observed by its own export, each with an unpublished fact.
async fn seed_same_id_artifacts(
    pool: &sqlx::PgPool,
    raw_one: &closure_cases::Stored,
    raw_two: &closure_cases::Stored,
) {
    sqlx::query("insert into claude_archive.accounts (account_id, external_account_id) values ($1, 'same-id-owner')")
        .bind(Uuid::parse_str(ACCOUNT_A).expect("account UUID"))
        .execute(pool)
        .await
        .expect("the account seeds");
    for (export, archive, raw) in [
        (EXPORT_ONE, "c2100000-0000-0000-0000-0000000000d1", raw_one),
        (EXPORT_TWO, "c2100000-0000-0000-0000-0000000000d2", raw_two),
    ] {
        sqlx::query(
            "insert into claude_archive.exports
             (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
              received_at)
             values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
        )
        .bind(Uuid::parse_str(export).expect("export UUID"))
        .bind(Uuid::parse_str(archive).expect("archive UUID"))
        .bind(Uuid::parse_str(ACCOUNT_A).expect("account UUID"))
        .bind(&raw.reference.digest_hex)
        .bind(&raw.key)
        .bind(i64::try_from(raw.reference.length_bytes).expect("length fits"))
        .execute(pool)
        .await
        .expect("the export seeds");
    }
    let mut statements: Vec<String> = Vec::new();
    for (export, conversation, artifact) in [
        (EXPORT_ONE, CONVERSATION_ONE, ARTIFACT_ONE),
        (EXPORT_TWO, CONVERSATION_TWO, ARTIFACT_TWO),
    ] {
        statements.push(format!(
            "insert into claude_archive.conversations
              (conversation_id, account_id, external_conversation_id, upstream_state)
             values ('{conversation}', '{ACCOUNT_A}', 'conversation-{conversation}', 'present');
             insert into claude_archive.artifacts
              (artifact_id, account_id, conversation_id, external_artifact_id, upstream_state,
               local_backup_status)
             values ('{artifact}', '{ACCOUNT_A}', '{conversation}', '{ARTIFACT_PROVIDER_ID}',
                     'present', 'reference_only');
             insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
             values ('{export}', 'conversation', '{conversation}'),
                    ('{export}', 'artifact', '{artifact}');"
        ));
    }
    statements.push(artifact_fact_sql(
        "c5800000-0000-0000-0000-0000000000d1",
        CONVERSATION_ONE,
        "d1",
    ));
    statements.push(artifact_fact_sql(
        "c5800000-0000-0000-0000-0000000000d2",
        CONVERSATION_TWO,
        "d2",
    ));
    sqlx::raw_sql(&statements.join(";\n"))
        .execute(pool)
        .await
        .expect("the conversations, Artifacts and facts seed");
}

/// Two Artifacts of one account share a provider id across conversations. Deleting one
/// conversation removes the unpublished fact of its own Artifact and leaves the other's, which
/// would otherwise never be announced.
#[tokio::test]
async fn deleting_a_conversation_keeps_the_unpublished_fact_of_another_conversations_artifact() {
    let root = temp_root("same-id-artifact-fact");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw_one = store_bytes(&store, "application/zip", b"same id artifact raw one");
    let raw_two = store_bytes(&store, "application/zip", b"same id artifact raw two");
    let db = TestDatabase::create().await.expect("a disposable database");
    let pool = db.database.pool();
    seed_same_id_artifacts(pool, &raw_one, &raw_two).await;

    let request = Uuid::parse_str("c6000000-0000-0000-0000-0000000000d1").expect("request UUID");
    PrivacyDeletionPlanner::new(pool.clone())
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: TENANT_A.to_owned(),
            account_id: Uuid::parse_str(ACCOUNT_A).expect("account UUID"),
            request_id: request,
            request_key: "same-id-artifact".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            conversation_id: Uuid::parse_str(CONVERSATION_ONE).expect("conversation UUID"),
        })
        .await
        .expect("the conversation deletion is planned");
    let outcome = PrivacyDeletionExecutor::new(pool.clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw_one]), None)
        .await;
    let remaining: Vec<Uuid> = sqlx::query_scalar(
        "select event_id from claude_archive.outbox_events
         where aggregate_type = 'artifact' order by event_id",
    )
    .fetch_all(pool)
    .await
    .expect("the artifact facts are readable");
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert_eq!(
        remaining,
        vec![Uuid::parse_str("c5800000-0000-0000-0000-0000000000d2").expect("event UUID")],
        "only the fact of the deleted conversation's Artifact is removed"
    );
}
