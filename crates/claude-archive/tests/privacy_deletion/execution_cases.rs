use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the fault and every coupled database effect form one atomicity proof"
)]
async fn deletion_finalization_is_atomic_with_audit_and_tombstones() {
    const ACCOUNT: &str = "91000000-0000-0000-0000-000000000001";
    const CONVERSATION: &str = "93000000-0000-0000-0000-000000000001";
    const REQUEST: &str = "94000000-0000-0000-0000-000000000001";

    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r"
        insert into claude_archive.accounts (account_id, external_account_id)
        values ('91000000-0000-0000-0000-000000000001', 'finalization-owner');

        insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, received_at)
        values
         ('92000000-0000-0000-0000-000000000001',
          '92100000-0000-0000-0000-000000000001',
          '91000000-0000-0000-0000-000000000001', 'consumer_export',
          decode(repeat('91', 32), 'hex'), 'owned/sha256/finalization-raw', 14,
          '2026-03-01T00:00:00Z');

        insert into claude_archive.conversations
         (conversation_id, account_id, external_conversation_id, upstream_state)
        values ('93000000-0000-0000-0000-000000000001',
                '91000000-0000-0000-0000-000000000001',
                'finalization-target', 'present');

        insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id)
        values ('92000000-0000-0000-0000-000000000001', 'conversation',
                '93000000-0000-0000-0000-000000000001');

        insert into claude_archive.knowledge_analysis_links
         (completion_event_id, ai_archive_id, subject_kind, subject_id,
          content_digest_hex, completed_at)
        values ('95000000-0000-0000-0000-000000000001',
                '92100000-0000-0000-0000-000000000001', 'conversation',
                '93000000-0000-0000-0000-000000000001', repeat('95', 32),
                '2026-03-02T00:00:00Z');
        ",
    )
    .execute(db.database.pool())
    .await
    .expect("owned conversation, raw provenance, and Knowledge linkage seed");

    let request_id = Uuid::parse_str(REQUEST).expect("request fixture UUID is valid");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: "tenant:finalization-owner".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account fixture UUID is valid"),
            request_id,
            request_key: "atomic-finalization".to_owned(),
            correlation_id: "privacy-red-6-1".to_owned(),
            conversation_id: Uuid::parse_str(CONVERSATION)
                .expect("conversation fixture UUID is valid"),
        })
        .await
        .expect("conversation deletion inventory is durably planned");

    // Finalization consumes a persisted propagation item; planning delivery
    // expansion is independent of this transactionality contract.
    sqlx::query(
        "insert into claude_archive.privacy_deletion_items
         (request_id, item_index, item_kind, subject_id, action)
         select $1, coalesce(max(item_index), -1) + 1, 'downstream_tombstone',
                $2, 'emit_tombstone'
         from claude_archive.privacy_deletion_items where request_id = $1",
    )
    .bind(request_id)
    .bind(CONVERSATION)
    .execute(db.database.pool())
    .await
    .expect("the planned Knowledge-removal subject is persisted");

    let result = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize(request_id, Some(FinalizationFault::AfterFirstRemoval))
        .await;
    let (normalized_rows, provenance_rows, terminal_audits, user_requested_tombstones, state) =
        sqlx::query_as::<_, (i64, i64, i64, i64, String)>(
            "select
               (select count(*) from claude_archive.conversations
                 where conversation_id = $1),
               (select count(*) from claude_archive.export_observations
                 where subject_kind = 'conversation' and subject_id = $1),
               (select count(*) from claude_archive.privacy_deletion_audits
                 where request_id = $2),
               (select count(*) from claude_archive.outbox_events
                 where event_type = 'ai_archive.subject.tombstoned.v1'
                   and envelope::text like '%user_requested%'),
               (select state from claude_archive.privacy_deletion_requests
                 where request_id = $2)",
        )
        .bind(Uuid::parse_str(CONVERSATION).expect("conversation fixture UUID is valid"))
        .bind(request_id)
        .fetch_one(db.database.pool())
        .await
        .expect("all finalization effects remain observable together");
    let actual = AtomicFinalizationObservation {
        fault_was_injected: matches!(
            result,
            Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterFirstRemoval
            ))
        ),
        normalized_rows,
        provenance_rows,
        terminal_audits,
        user_requested_tombstones,
        request_state: state,
    };
    let expected = AtomicFinalizationObservation {
        fault_was_injected: true,
        normalized_rows: 1,
        provenance_rows: 1,
        terminal_audits: 0,
        user_requested_tombstones: 0,
        request_state: "planned".to_owned(),
    };

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    assert_eq!(
        actual, expected,
        "a finalization fault must roll back normalized removal, provenance removal, audit, tombstone, and terminal state together"
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayEffects {
    audits: i64,
    tombstones: i64,
    inventory_items: i64,
    conversations: i64,
    provenance_edges: i64,
    raw_exports: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct ReplayObservation {
    first_report_matches_durable: bool,
    replayed_report: Option<DeletionCompletionReport>,
    effects_after_first: ReplayEffects,
    effects_after_replay: ReplayEffects,
}

async fn replay_effects(
    pool: &sqlx::PgPool,
    request_id: Uuid,
) -> Result<ReplayEffects, sqlx::Error> {
    let counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
        "select
           (select count(*) from claude_archive.privacy_deletion_audits
             where request_id = $1),
           (select count(*) from claude_archive.outbox_events
             where event_type = 'ai_archive.subject.tombstoned.v1'),
           (select count(*) from claude_archive.privacy_deletion_items
             where request_id = $1),
           (select count(*) from claude_archive.conversations
             where conversation_id = 'a3000000-0000-0000-0000-000000000001'),
           (select count(*) from claude_archive.export_observations
             where subject_kind = 'conversation'
               and subject_id = 'a3000000-0000-0000-0000-000000000001'),
           (select count(*) from claude_archive.exports
             where export_id = 'a2000000-0000-0000-0000-000000000001')",
    )
    .bind(request_id)
    .fetch_one(pool)
    .await?;
    Ok(ReplayEffects {
        audits: counts.0,
        tombstones: counts.1,
        inventory_items: counts.2,
        conversations: counts.3,
        provenance_edges: counts.4,
        raw_exports: counts.5,
    })
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "first completion and replay effects are one idempotency proof"
)]
async fn completed_deletion_replay_returns_original_report() {
    const ACCOUNT: &str = "a1000000-0000-0000-0000-000000000001";
    const CONVERSATION: &str = "a3000000-0000-0000-0000-000000000001";
    const REQUEST: &str = "a4000000-0000-0000-0000-000000000001";

    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r"
        insert into claude_archive.accounts (account_id, external_account_id)
        values ('a1000000-0000-0000-0000-000000000001', 'replay-owner');

        insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, received_at)
        values
         ('a2000000-0000-0000-0000-000000000001',
          'a2100000-0000-0000-0000-000000000001',
          'a1000000-0000-0000-0000-000000000001', 'consumer_export',
          decode(repeat('a1', 32), 'hex'), 'owned/sha256/replay-raw', 10,
          '2026-04-01T00:00:00Z');

        insert into claude_archive.conversations
         (conversation_id, account_id, external_conversation_id, upstream_state)
        values ('a3000000-0000-0000-0000-000000000001',
                'a1000000-0000-0000-0000-000000000001', 'replay-target', 'present');

        insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id)
        values ('a2000000-0000-0000-0000-000000000001', 'conversation',
                'a3000000-0000-0000-0000-000000000001');
        ",
    )
    .execute(db.database.pool())
    .await
    .expect("minimal replay deletion fixture seeds");

    let request_id = Uuid::parse_str(REQUEST).expect("request fixture UUID is valid");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: "tenant:replay-owner".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account fixture UUID is valid"),
            request_id,
            request_key: "completed-replay".to_owned(),
            correlation_id: "privacy-red-6-3".to_owned(),
            conversation_id: Uuid::parse_str(CONVERSATION)
                .expect("conversation fixture UUID is valid"),
        })
        .await
        .expect("replay deletion inventory is durably planned");
    sqlx::query(
        "insert into claude_archive.privacy_deletion_items
         (request_id, item_index, item_kind, subject_id, action)
         select $1, coalesce(max(item_index), -1) + 1, 'downstream_tombstone',
                $2, 'emit_tombstone'
         from claude_archive.privacy_deletion_items where request_id = $1",
    )
    .bind(request_id)
    .bind(CONVERSATION)
    .execute(db.database.pool())
    .await
    .expect("the replay-safe downstream subject is persisted");
    let executor = PrivacyDeletionExecutor::new(db.database.pool().clone());
    let first_report = executor
        .finalize(request_id, None)
        .await
        .expect("the first execution completes");
    let durable_report: serde_json::Value = sqlx::query_scalar(
        "select completion_report from claude_archive.privacy_deletion_requests
         where request_id = $1",
    )
    .bind(request_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the durable completion report is available");
    let effects_after_first = replay_effects(db.database.pool(), request_id)
        .await
        .expect("first execution effects remain queryable");

    let replayed_report = executor.finalize(request_id, None).await.ok();
    let effects_after_replay = replay_effects(db.database.pool(), request_id)
        .await
        .expect("replay effects remain queryable");
    let actual = ReplayObservation {
        first_report_matches_durable: serde_json::to_value(&first_report)
            .expect("the returned report is serializable")
            == durable_report,
        replayed_report,
        effects_after_first,
        effects_after_replay,
    };
    let expected_effects = ReplayEffects {
        audits: 1,
        tombstones: 1,
        inventory_items: 0,
        conversations: 0,
        provenance_edges: 0,
        raw_exports: 0,
    };
    let expected = ReplayObservation {
        first_report_matches_durable: true,
        replayed_report: Some(first_report),
        effects_after_first: expected_effects.clone(),
        effects_after_replay: expected_effects,
    };

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    assert_eq!(
        actual, expected,
        "completed replay must return the original durable report without duplicating effects"
    );
}
