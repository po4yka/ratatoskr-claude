//! The owner of every announced subject is recorded with the fact that announces it (XR-021
//! CONTRACTS.md R5-06 rule 1).
//!
//! Every test drives the real receipt, import and deletion path against a real `PostgreSQL`.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod synthetic_import;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionExecutor, PrivacyDeletionPlanner, RawExportDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{BlobStore, MediaType, PlatformUsers};
use sqlx::PgPool;
use synthetic_import::{account, receive, versions, worker, zip};
use uuid::Uuid;

const USER_A: &str = "018f0000-0000-7000-8000-0000000000a1";

type Ledger = Vec<(String, String, String, Option<Uuid>, Uuid)>;

fn artifacts() -> serde_json::Value {
    serde_json::json!([{
        "id": "react-component", "type": "text/markdown", "project_id": "project-garden",
        "versions": versions()
    }])
}

fn users(user: &str, account: Uuid) -> PlatformUsers {
    PlatformUsers::new(&[(Uuid::parse_str(user).expect("user UUID"), account)])
}

/// `(subject_kind, subject_id, owner_ref, account_id, ai_archive_id)` of every ledger row.
async fn ledger(pool: &PgPool) -> Ledger {
    sqlx::query_as(
        "select subject_kind, subject_id, owner_ref, account_id, ai_archive_id
         from claude_archive.announced_subjects order by subject_kind, subject_id, owner_ref",
    )
    .fetch_all(pool)
    .await
    .expect("the ledger is readable")
}

/// `(aggregate_type, local id, tenant)` of every fact the outbox holds, tombstones excluded.
async fn announced_facts(pool: &PgPool) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "select distinct aggregate_type, substr(aggregate_id, position(':' in aggregate_id) + 1),
                tenant_ref
         from claude_archive.outbox_events
         where event_type like 'ai_archive.%' and event_type <> 'ai_archive.subject.tombstoned.v1'
         order by 1, 2, 3",
    )
    .fetch_all(pool)
    .await
    .expect("the facts are readable")
}

#[tokio::test]
async fn an_import_records_the_owner_of_every_announced_subject() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let account = account(&db).await;
    let root = temp_root("announced-import");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, users(USER_A, account));
    receive(&db, &store, account, &zip("first", artifacts(), false)).await;
    assert_eq!(worker.process_pending_once().await.expect("imports"), 1);
    let pool = db.database.pool();

    let rows = ledger(pool).await;
    let facts = announced_facts(pool).await;
    let archive: Uuid = sqlx::query_scalar("select ai_archive_id from claude_archive.exports")
        .fetch_one(pool)
        .await
        .expect("the export exists");
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let kinds: std::collections::BTreeSet<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(
        kinds,
        ["archive", "artifact", "conversation", "project"].into(),
        "the import announced an archive, projects, a conversation and an Artifact, and the ledger \
         names each kind"
    );
    let keyed: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| (row.0.clone(), row.1.clone(), row.2.clone()))
        .collect();
    assert_eq!(
        keyed, facts,
        "the ledger holds exactly one row per subject and owner a fact was enqueued for"
    );
    for (_, subject_id, owner, account_id, ai_archive_id) in &rows {
        assert_eq!(owner, &format!("user:{USER_A}"), "{subject_id} owner");
        assert_eq!(*account_id, Some(account), "{subject_id} account");
        assert_eq!(*ai_archive_id, archive, "{subject_id} archive");
    }
}

#[tokio::test]
async fn an_unmapped_account_emits_no_fact_and_no_row() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let account = account(&db).await;
    let root = temp_root("announced-unmapped");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let worker = worker(&db, &store, PlatformUsers::default());
    receive(&db, &store, account, &zip("first", artifacts(), false)).await;
    assert_eq!(worker.process_pending_once().await.expect("imports"), 1);
    let pool = db.database.pool();
    let rows = ledger(pool).await;
    let facts = announced_facts(pool).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert!(facts.is_empty(), "an unmapped account announces no fact");
    assert!(rows.is_empty(), "and therefore records no owner");
}

#[tokio::test]
async fn the_ledger_has_no_foreign_key() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let foreign_keys: i64 = sqlx::query_scalar(
        "select count(*) from pg_constraint
         where contype = 'f'
           and (conrelid = 'claude_archive.announced_subjects'::regclass
                or confrelid = 'claude_archive.announced_subjects'::regclass)",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("the catalog is readable");
    db.cleanup().await.expect("cleanup succeeds");
    assert_eq!(
        foreign_keys, 0,
        "no export or account deletion may cascade the ledger"
    );
}

#[tokio::test]
async fn a_re_announced_survivor_is_recorded_under_the_retained_archives_owner() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let account = account(&db).await;
    let root = temp_root("announced-survivor");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let first = worker(&db, &store, users(USER_A, account));
    for note in ["first", "second"] {
        receive(&db, &store, account, &zip(note, artifacts(), false)).await;
        assert_eq!(first.process_pending_once().await.expect("imports"), 1);
    }
    let pool = db.database.pool();
    let exports: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "select export_id, ai_archive_id from claude_archive.exports order by received_at",
    )
    .fetch_all(pool)
    .await
    .expect("the exports are readable");
    let [(deleted, _), (_, retained_archive)] =
        <[(Uuid, Uuid); 2]>::try_from(exports).expect("two exports were received");

    let request = Uuid::now_v7();
    PrivacyDeletionPlanner::new(pool.clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: format!("user:{USER_A}"),
            account_id: account,
            request_id: request,
            request_key: "announced-survivor".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: deleted,
        })
        .await
        .expect("the export deletion is planned");
    let evidence = store
        .store(
            MediaType::parse("application/json").expect("media type"),
            b"{}",
        )
        .expect("the evidence blob is stored");
    PrivacyDeletionExecutor::new(pool.clone())
        .finalize(request, &evidence, None)
        .await
        .expect("the deletion completes");
    let rows = ledger(pool).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    let conversation = rows
        .iter()
        .find(|row| row.0 == "conversation")
        .expect("the surviving conversation keeps a ledger row");
    assert_eq!(conversation.2, format!("user:{USER_A}"));
    assert_eq!(
        conversation.4, retained_archive,
        "the survivor was announced again under the retained archive, so the row names it"
    );
}
