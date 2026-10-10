//! The commit points of privacy deletion: the envelope correlation of a tombstone, and a crash
//! between the row transaction and the byte erasure (XR-021 CONTRACTS.md S07).

use sqlx::PgPool;

use super::closure_cases::{
    plan_conversation_deletion, plan_conversation_deletion_correlated, resolved,
    seed_linked_conversation, store_bytes, tombstone_envelopes,
};
use super::*;

/// A request whose stored correlation id is not a namespaced reference cannot be an envelope
/// correlation id; the deletion must still complete, so the envelope names the request itself.
#[tokio::test]
async fn a_correlation_id_that_is_not_a_reference_falls_back_to_the_request_reference() {
    let root = temp_root("closure-correlation");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"closure correlation archive");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_linked_conversation(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b9").expect("request UUID");
    plan_conversation_deletion_correlated(
        db.database.pool(),
        request,
        "correlation",
        "not a namespaced reference",
    )
    .await;

    let outcome = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let envelopes = tombstone_envelopes(db.database.pool()).await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    outcome.expect("the deletion completes");
    assert!(
        !envelopes.is_empty()
            && envelopes.iter().all(|envelope| envelope.correlation_id.to_string()
                == format!("privacy_request:{request}")),
        "the envelope correlates to the deletion request: {envelopes:?}"
    );
}

/// Every blob key held by any surviving row that the store no longer holds, as `table.column`.
async fn dangling_blob_references(pool: &PgPool, store: &BlobStore) -> Vec<String> {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "select table_name::text, column_name::text from information_schema.columns
         where table_schema = 'claude_archive' and column_name like '%blob_ref'
         order by 1, 2",
    )
    .fetch_all(pool)
    .await
    .expect("the blob reference columns are listed");
    assert!(
        columns.len() >= 8,
        "the census must see the real blob reference columns, saw {columns:?}"
    );
    let mut dangling = Vec::new();
    for (table, column) in columns {
        let keys: Vec<String> = sqlx::query_scalar(&format!(
            "select distinct {column} from claude_archive.{table} where {column} is not null"
        ))
        .fetch_all(pool)
        .await
        .expect("the blob references are readable");
        for key in keys {
            let held = key
                .strip_prefix("sha256/")
                .is_some_and(|hex| store.locate(hex).is_ok());
            if !held {
                dangling.push(format!("{table}.{column}={key}"));
            }
        }
    }
    dangling
}

#[tokio::test]
async fn rows_are_removed_before_bytes_and_a_crash_between_resumes() {
    let root = temp_root("closure-crash");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store_bytes(&store, "application/zip", b"closure crash archive");
    let db = TestDatabase::create().await.expect("a disposable database");
    seed_linked_conversation(db.database.pool(), &raw).await;
    let request = Uuid::parse_str("b6000000-0000-0000-0000-0000000000b8").expect("request UUID");
    plan_conversation_deletion(db.database.pool(), request, "crash").await;
    let executor = PrivacyDeletionExecutor::new(db.database.pool().clone());

    let crashed = executor
        .finalize_with_blob_store(
            request,
            &store,
            &resolved(&[&raw]),
            Some(FinalizationFault::AfterRowsCommitted),
        )
        .await;
    let after_rows: (i64, i64, i64, String) = sqlx::query_as(
        "select (select count(*) from claude_archive.exports),
                (select count(*) from claude_archive.privacy_deletion_items where request_id = $1),
                (select count(*) from claude_archive.privacy_deletion_audits),
                (select state from claude_archive.privacy_deletion_requests where request_id = $1)",
    )
    .bind(request)
    .fetch_one(db.database.pool())
    .await
    .expect("the post-crash state is readable");
    let raw_still_present = store.verify(&raw.reference).is_ok();
    let dangling_after_rows = dangling_blob_references(db.database.pool(), &store).await;

    let resumed = executor
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    let after_resume: (i64, i64, String) = sqlx::query_as(
        "select (select count(*) from claude_archive.privacy_deletion_audits),
                (select count(*) from claude_archive.privacy_deletion_items where request_id = $1),
                (select state from claude_archive.privacy_deletion_requests where request_id = $1)",
    )
    .bind(request)
    .fetch_one(db.database.pool())
    .await
    .expect("the post-resume state is readable");
    let raw_erased = store.verify(&raw.reference).is_err();
    let dangling_after_resume = dangling_blob_references(db.database.pool(), &store).await;
    let replayed = executor
        .finalize_with_blob_store(request, &store, &resolved(&[&raw]), None)
        .await;
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    assert!(
        matches!(
            crashed,
            Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterRowsCommitted
            ))
        ),
        "the fault fires after the row transaction committed: {crashed:?}"
    );
    assert_eq!(after_rows.0, 0, "the rows are gone");
    assert!(after_rows.1 > 0, "the items that carry the blob references are kept");
    assert_eq!(after_rows.2, 0, "no audit exists before the bytes are erased");
    assert_eq!(after_rows.3, "purging", "the request waits in the purging state");
    assert!(raw_still_present, "the raw blob is still present after the crash");
    let completed = resumed.expect("the rerun finishes the byte phase and completes");
    assert_eq!(after_resume, (1, 0, "completed".to_owned()), "audit written, items dropped");
    assert!(raw_erased, "the rerun erases the raw blob");
    assert!(
        dangling_after_rows.is_empty() && dangling_after_resume.is_empty(),
        "no surviving row references a missing blob at either commit point: \
         {dangling_after_rows:?} {dangling_after_resume:?}"
    );
    assert_eq!(
        replayed.expect("a completed request replays its report"),
        completed,
        "the third run returns the stored report"
    );
}
