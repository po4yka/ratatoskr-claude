//! The state machine behind the tombstone acknowledgement (XR-021 CONTRACTS.md R5-11 rule 4):
//! the pump publishes a published, unacknowledged tombstone again with a doubling delay, and
//! Knowledge's acknowledgement ends the resend.
//!
//! The pump runs against the real `JetStream` of `CLAUDE_ARCHIVE_TEST_NATS_URL` and the rows live
//! in a real `PostgreSQL`; the resend delays are read back from the rows, so no test sleeps.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod synthetic_import;

use std::time::Duration;

use ratatoskr_ai_archive_contracts::AiArchiveTombstone;
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::deletion_ack::{Acknowledgement, acknowledge};
use ratatoskr_claude_archive::privacy_deletion::{
    DeletionItemKind, PrivacyDeletionExecutor, PrivacyDeletionPlanner,
    RawExportDeletionPlanRequest, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::{TestDatabase, nats_url};
use ratatoskr_claude_archive::{ArchiveEventFact, ArchiveOutbox, OutboxPublisher};
use ratatoskr_claude_archive::{BlobStore, MediaType, PlatformUsers};
use ratatoskr_operation_contracts::KnowledgeDeletionAcknowledged;
use sqlx::PgPool;
use uuid::Uuid;

const AFTER: Duration = Duration::from_secs(30);
const CAP: Duration = Duration::from_secs(100);
const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const OTHER_OWNER: &str = "user:018f0000-0000-7000-8000-000000000006";

fn tombstone(owner: &str, archive: Uuid) -> AiArchiveTombstone {
    serde_json::from_value(serde_json::json!({
        "ai_archive_id": archive,
        "provider": "claude",
        "owner": owner,
        "subject": {"subject_kind": "archive"},
        "reason": "user_requested",
        "evidence_ref": {
            "owner_service": "ratatoskr-claude",
            "digest": {"algorithm": "sha256",
                       "hex": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
            "media_type": "application/json", "length_bytes": 512
        },
        "observed_at": "2026-08-27T06:00:00Z"
    }))
    .expect("the tombstone fixture decodes")
}

fn acknowledgement(event_id: Uuid, owner: &str, outcome: &str) -> KnowledgeDeletionAcknowledged {
    let mut value = serde_json::json!({
        "acknowledged_event_id": event_id,
        "family": "ai_archive",
        "owner": owner,
        "subject": format!("ai_archive:{}", Uuid::now_v7()),
        "outcome": outcome,
        "acknowledged_at": "2026-08-27T07:00:00Z"
    });
    if outcome == "refused"
        && let Some(object) = value.as_object_mut()
    {
        object.insert("refusal".to_owned(), "foreign_subject".into());
    }
    serde_json::from_value(value).expect("the acknowledgement decodes")
}

async fn connect() -> async_nats::jetstream::Context {
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test broker accepts a connection");
    let jetstream = async_nats::jetstream::new(client);
    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_events".to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the events stream exists");
    jetstream
}

fn publisher(pool: &PgPool) -> OutboxPublisher {
    OutboxPublisher::new(pool.clone()).with_tombstone_resend(AFTER, CAP)
}

async fn enqueue_tombstone(db: &TestDatabase, owner: &str) -> Uuid {
    ArchiveOutbox::new(&db.database)
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone(owner, Uuid::now_v7())),
            "2026-08-27T06:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the tombstone is enqueued")
        .envelope
        .event_id
        .0
}

/// `(published, acknowledged, outcome, resend_count, seconds until the next resend)`.
type State = (bool, bool, Option<String>, i32, Option<f64>);

async fn state(pool: &PgPool, event_id: Uuid) -> State {
    sqlx::query_as(
        "select published_at is not null, acknowledged_at is not null, ack_outcome, resend_count,
                extract(epoch from next_resend_at - now())::float8
         from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("the row is readable")
}

async fn make_due(pool: &PgPool, event_id: Uuid) {
    sqlx::query(
        "update claude_archive.outbox_events set next_resend_at = now() - interval '1 second'
         where event_id = $1",
    )
    .bind(event_id)
    .execute(pool)
    .await
    .expect("the row is made due");
}

#[tokio::test]
async fn a_published_tombstone_is_scheduled_for_resend_after_the_interval() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let event_id = enqueue_tombstone(&db, OWNER).await;
    let before = state(db.database.pool(), event_id).await;
    let published = publisher(db.database.pool())
        .publish_pending_with(&connect().await)
        .await;
    let after = state(db.database.pool(), event_id).await;
    let gap: Option<f64> = sqlx::query_scalar(
        "select extract(epoch from next_resend_at - published_at)::float8
         from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the schedule is readable");
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        before,
        (false, false, None, 0, None),
        "nothing is scheduled before the publish"
    );
    assert!(matches!(published, Ok(1)), "{published:?}");
    assert!(after.0 && !after.1, "published and not acknowledged");
    assert!(
        gap.is_some_and(|gap| (gap - AFTER.as_secs_f64()).abs() < 0.01),
        "the first resend is due one interval after the publish: {gap:?}"
    );
}

#[tokio::test]
async fn a_due_tombstone_is_published_again_with_a_doubling_delay_up_to_the_cap() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let pool = db.database.pool();
    let event_id = enqueue_tombstone(&db, OWNER).await;
    let jetstream = connect().await;
    let publisher = publisher(pool);
    publisher
        .publish_pending_with(&jetstream)
        .await
        .expect("the first publish succeeds");
    let first_published: String = sqlx::query_scalar(
        "select published_at::text from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("the publish time is readable");

    let not_due = publisher.publish_pending_with(&jetstream).await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        make_due(pool, event_id).await;
        let pass = publisher.publish_pending_with(&jetstream).await;
        assert!(
            matches!(pass, Ok(1)),
            "a due resend counts as one publish: {pass:?}"
        );
        seen.push(state(pool, event_id).await);
    }
    let still_published: String = sqlx::query_scalar(
        "select published_at::text from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("the publish time is readable");
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(not_due, Ok(0)),
        "a row that is not due is left alone: {not_due:?}"
    );
    assert_eq!(
        still_published, first_published,
        "a resend keeps the original publish time"
    );
    let counts: Vec<i32> = seen.iter().map(|state| state.3).collect();
    assert_eq!(counts, vec![1, 2, 3], "each resend counts once");
    let delays: Vec<f64> = seen
        .iter()
        .map(|state| state.4.expect("a resend is scheduled"))
        .collect();
    for (delay, expected) in delays.iter().zip([30.0, 60.0, 100.0]) {
        assert!(
            (*delay - expected).abs() < 1.5,
            "the delay doubles from one interval and stops at the cap: {delays:?}"
        );
    }
}

#[tokio::test]
async fn a_resend_never_stops_by_count_however_many_times_the_row_was_resent() {
    // XR-021 CONTRACTS.md R5-11 rule 4: the resend stops only at the acknowledgement. The doubling
    // wait is clamped before it is multiplied, so a row resent for months keeps being rescheduled
    // at the cap instead of overflowing the interval arithmetic.
    for resent in [33_i32, 34, 40, 1000, i32::MAX - 1] {
        let db = TestDatabase::create().await.expect("a disposable database");
        let pool = db.database.pool();
        let event_id = enqueue_tombstone(&db, OWNER).await;
        let jetstream = connect().await;
        let publisher = publisher(pool);
        publisher
            .publish_pending_with(&jetstream)
            .await
            .expect("the first publish succeeds");
        sqlx::query(
            "update claude_archive.outbox_events set resend_count = $2 where event_id = $1",
        )
        .bind(event_id)
        .bind(resent)
        .execute(pool)
        .await
        .expect("the resend count is set");
        make_due(pool, event_id).await;
        let pass = publisher.publish_pending_with(&jetstream).await;
        let row = state(pool, event_id).await;
        let next_pass = publisher.publish_pending_with(&jetstream).await;
        db.cleanup().await.expect("cleanup succeeds");

        assert!(
            matches!(pass, Ok(1)),
            "{resent}: a resend that was repeated {resent} times still succeeds: {pass:?}"
        );
        assert_eq!(row.3, resent + 1, "{resent}: the resend is counted");
        assert!(
            row.4
                .is_some_and(|delay| (delay - CAP.as_secs_f64()).abs() < 1.5),
            "{resent}: the next resend is scheduled at exactly the cap: {:?}",
            row.4
        );
        assert!(
            matches!(next_pass, Ok(0)),
            "{resent}: the row is no longer due, so the pump has nothing left to refuse: {next_pass:?}"
        );
    }
}

#[tokio::test]
async fn an_acknowledgement_ends_the_resend_and_records_the_outcome() {
    for outcome in ["applied", "refused"] {
        let db = TestDatabase::create().await.expect("a disposable database");
        let pool = db.database.pool();
        let event_id = enqueue_tombstone(&db, OWNER).await;
        let jetstream = connect().await;
        let publisher = publisher(pool);
        publisher
            .publish_pending_with(&jetstream)
            .await
            .expect("the first publish succeeds");
        let recorded = acknowledge(pool, &acknowledgement(event_id, OWNER, outcome)).await;
        let duplicate = acknowledge(pool, &acknowledgement(event_id, OWNER, "applied")).await;
        make_due(pool, event_id).await;
        let pass = publisher.publish_pending_with(&jetstream).await;
        let row = state(pool, event_id).await;
        db.cleanup().await.expect("cleanup succeeds");

        assert!(
            matches!(recorded, Ok(Acknowledgement::Recorded)),
            "{outcome}: {recorded:?}"
        );
        assert!(
            matches!(duplicate, Ok(Acknowledgement::AlreadyRecorded)),
            "{outcome}: a duplicate changes nothing: {duplicate:?}"
        );
        assert!(
            matches!(pass, Ok(0)),
            "{outcome}: an acknowledged row is not sent: {pass:?}"
        );
        assert_eq!(
            (row.1, row.2.as_deref(), row.3),
            (true, Some(outcome), 0),
            "{outcome}: the outcome is kept and nothing was resent"
        );
    }
}

#[tokio::test]
async fn an_acknowledgement_for_another_owner_or_an_unknown_id_changes_nothing() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let pool = db.database.pool();
    let event_id = enqueue_tombstone(&db, OWNER).await;
    publisher(pool)
        .publish_pending_with(&connect().await)
        .await
        .expect("the publish succeeds");
    let other_owner = acknowledge(pool, &acknowledgement(event_id, OTHER_OWNER, "applied")).await;
    let unknown = acknowledge(pool, &acknowledgement(Uuid::now_v7(), OWNER, "applied")).await;
    let row = state(pool, event_id).await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(other_owner, Ok(Acknowledgement::Unknown)),
        "{other_owner:?}"
    );
    assert!(
        matches!(unknown, Ok(Acknowledgement::Unknown)),
        "{unknown:?}"
    );
    assert_eq!(
        (row.1, row.2),
        (false, None),
        "the row is still unacknowledged"
    );
}

/// The outbox rows a tenant-scope deletion of `account` would remove, as event ids.
async fn tenant_removal_set(pool: &PgPool, account: Uuid, key: &str) -> Vec<Uuid> {
    let request = Uuid::now_v7();
    let inventory = PrivacyDeletionPlanner::new(pool.clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: account.to_string(),
            account_id: account,
            request_id: request,
            request_key: key.to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
        })
        .await
        .expect("the tenant deletion is planned");
    inventory
        .items
        .iter()
        .filter(|item| item.kind == DeletionItemKind::Outbox)
        .filter_map(|item| Uuid::parse_str(&item.subject_id).ok())
        .collect()
}

#[tokio::test]
async fn only_an_acknowledged_tombstone_is_removed_by_a_later_deletion() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let pool = db.database.pool();
    let account = synthetic_import::account(&db).await;
    let root = temp_root("tombstone-ack-removal");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let user = Uuid::parse_str("018f0000-0000-7000-8000-0000000000a1").expect("user UUID");
    let worker = synthetic_import::worker(&db, &store, PlatformUsers::new(&[(user, account)]));
    for note in ["first", "second"] {
        let artifacts = serde_json::json!([{
            "id": "react-component", "type": "text/markdown", "project_id": "project-garden",
            "versions": synthetic_import::versions()
        }]);
        let archive = synthetic_import::zip(note, artifacts, false);
        synthetic_import::receive(&db, &store, account, &archive).await;
        assert_eq!(worker.process_pending_once().await.expect("imports"), 1);
    }
    let newest: Uuid = sqlx::query_scalar(
        "select export_id from claude_archive.exports order by received_at desc limit 1",
    )
    .fetch_one(pool)
    .await
    .expect("the newest export exists");
    let request = Uuid::now_v7();
    PrivacyDeletionPlanner::new(pool.clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: account.to_string(),
            account_id: account,
            request_id: request,
            request_key: "first-deletion".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id: newest,
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
    let tombstone: Uuid = sqlx::query_scalar(
        "select event_id from claude_archive.outbox_events
         where event_type = 'ai_archive.subject.tombstoned.v1' order by event_id limit 1",
    )
    .fetch_one(pool)
    .await
    .expect("the deletion wrote a tombstone");
    publisher(pool)
        .publish_pending_with(&connect().await)
        .await
        .expect("the tombstones are published");

    let while_unacknowledged = tenant_removal_set(pool, account, "while-unacknowledged").await;
    let owner = format!("user:{user}");
    acknowledge(pool, &acknowledgement(tombstone, &owner, "applied"))
        .await
        .expect("the acknowledgement is recorded");
    let after_acknowledgement = tenant_removal_set(pool, account, "after-acknowledgement").await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        !while_unacknowledged.contains(&tombstone),
        "a published tombstone nobody acknowledged survives a later deletion"
    );
    assert!(
        after_acknowledgement.contains(&tombstone),
        "an acknowledged tombstone is removed like any published row"
    );
}
