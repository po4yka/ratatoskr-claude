//! The tombstone acknowledgement loop against a real broker under the deployed ACL (XR-021
//! CONTRACTS.md R5-11 rule 4): the first publish is not acknowledged, the same bytes are published
//! again with the same `Nats-Msg-Id`, and Knowledge's acknowledgement stops the resend.
//!
//! The broker has two users: a setup user, which provisions the stream and the durable the way
//! Edge does and plays Knowledge, and the reviewed Claude stanza of `deploy/nats/identity.conf`,
//! which publishes the tombstone and consumes the acknowledgements. The stream's duplicate window
//! is one second, so a republished message becomes a second stream copy a test can count.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt as _;
use ratatoskr_ai_archive_contracts::AiArchiveTombstone;
use ratatoskr_claude_archive::deletion_ack::record_backlog;
use ratatoskr_claude_archive::test_support::{Broker, TestDatabase};
use ratatoskr_claude_archive::{
    ArchiveEventFact, ArchiveOutbox, OutboxPublisher, connect_event_bus,
};
use ratatoskr_claude_archive_service::RuntimeState;
use ratatoskr_claude_archive_service::deletion_acks::{
    AcksSummary, DURABLE, FILTER_SUBJECT, attach, consume_deletion_acks,
};
use sqlx::PgPool;
use uuid::Uuid;

const OWNER: &str = "user:018f0000-0000-7000-8000-000000000005";
const TOMBSTONED: &str = "evt.ai_archive.subject.tombstoned.v1";
const AFTER: Duration = Duration::from_secs(2);
const CAP: Duration = Duration::from_secs(4);

/// The Prometheus handle of this test binary, installed once because a recorder is process-wide.
fn metrics() -> &'static metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
        std::sync::OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("the first recorder of this process installs")
    })
}

/// The value of an unlabelled series in the exposition, or zero when it is absent.
fn series(name: &str) -> f64 {
    metrics()
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn tombstone() -> AiArchiveTombstone {
    serde_json::from_value(serde_json::json!({
        "ai_archive_id": Uuid::now_v7(),
        "provider": "claude",
        "owner": OWNER,
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

/// Knowledge's envelope for `acknowledged`, as the Knowledge publisher builds it.
fn acknowledgement_envelope(acknowledged: Uuid, outcome: &str) -> Vec<u8> {
    let mut payload = serde_json::json!({
        "acknowledged_event_id": acknowledged,
        "family": "ai_archive",
        "owner": OWNER,
        "subject": format!("ai_archive:{}", Uuid::now_v7()),
        "outcome": outcome,
        "acknowledged_at": "2026-08-27T07:00:00Z"
    });
    if outcome == "refused"
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("refusal".to_owned(), "foreign_subject".into());
    }
    serde_json::to_vec(&serde_json::json!({
        "event_id": Uuid::now_v7(),
        "event_type": "knowledge.deletion.acknowledged.v1",
        "occurred_at": "2026-08-27T07:00:00Z",
        "producer": "ratatoskr-knowledge",
        "aggregate_id": format!("ai_archive:{}", Uuid::now_v7()),
        "correlation_id": format!("privacy_request:{}", Uuid::now_v7()),
        "causation_id": format!("event:{acknowledged}"),
        "tenant_id": OWNER,
        "schema_version": 1,
        "payload": payload
    }))
    .expect("the envelope serializes")
}

struct Rig {
    broker: Broker,
    db: TestDatabase,
    knowledge: jetstream::Context,
    publisher: OutboxPublisher,
}

impl Rig {
    async fn open() -> Self {
        // The recorder is process-wide and only sees what is recorded after it is installed.
        metrics();
        let broker = Broker::authorized().expect("a broker with only the Claude stanza starts");
        broker
            .provision_durable(DURABLE, FILTER_SUBJECT, Duration::from_secs(1))
            .await
            .expect("the stream and the durable are provisioned as Edge does");
        let db = TestDatabase::create().await.expect("a disposable database");
        let knowledge = jetstream::new(
            broker
                .admin_client()
                .await
                .expect("the setup user connects"),
        );
        let publisher =
            OutboxPublisher::new(db.database.pool().clone()).with_tombstone_resend(AFTER, CAP);
        Self {
            broker,
            db,
            knowledge,
            publisher,
        }
    }

    fn pool(&self) -> &PgPool {
        self.db.database.pool()
    }

    async fn enqueue(&self) -> Uuid {
        ArchiveOutbox::new(&self.db.database)
            .enqueue(
                &ArchiveEventFact::Tombstoned(tombstone()),
                "2026-08-27T06:00:00Z".parse().expect("a canonical instant"),
            )
            .await
            .expect("the tombstone is enqueued")
            .envelope
            .event_id
            .0
    }

    async fn pass(&self) -> usize {
        self.publisher
            .publish_pending_once(&self.broker.url(), &self.broker.service_seed)
            .await
            .expect("the pump publishes through the deployed ACL")
    }

    /// The `Nats-Msg-Id` of every copy of the tombstone subject the stream holds, oldest first.
    async fn copies(&self) -> Vec<String> {
        let stream = self
            .knowledge
            .get_stream("ratatoskr_events")
            .await
            .expect("the stream exists");
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: TOMBSTONED.to_owned(),
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .expect("an ephemeral consumer attaches");
        let mut batch = consumer
            .fetch()
            .max_messages(100)
            .expires(Duration::from_millis(1_500))
            .messages()
            .await
            .expect("the batch opens");
        let mut found = Vec::new();
        while let Some(message) = batch.next().await {
            let message = message.expect("the delivery is readable");
            found.push(
                message
                    .headers
                    .as_ref()
                    .and_then(|headers| headers.get("Nats-Msg-Id"))
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            );
        }
        found
    }

    /// Starts the consumer under the Claude identity and returns a way to stop it.
    async fn consume(
        &self,
    ) -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<AcksSummary>,
    ) {
        let client = connect_event_bus(&self.broker.url(), &self.broker.service_seed)
            .await
            .expect("the Claude identity connects");
        let consumer = attach(&jetstream::new(client))
            .await
            .expect("the Edge-provisioned durable is found and matches its spec");
        let pool = self.pool().clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            consume_deletion_acks(&consumer, &pool, async {
                let _ignored = stopped.await;
            })
            .await
            .expect("the consumer runs until it is stopped")
        });
        (stop, task)
    }

    async fn knowledge_says(&self, body: Vec<u8>) {
        self.knowledge
            .publish("evt.knowledge.deletion.acknowledged.v1", body.into())
            .await
            .expect("Knowledge publishes")
            .await
            .expect("the broker acknowledges Knowledge");
    }

    async fn wait_acknowledged(&self, event_id: Uuid) -> Option<String> {
        for _ in 0..100 {
            let outcome: Option<String> = sqlx::query_scalar(
                "select ack_outcome from claude_archive.outbox_events where event_id = $1",
            )
            .bind(event_id)
            .fetch_one(self.pool())
            .await
            .expect("the row is readable");
            if outcome.is_some() {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    async fn make_due(&self, event_id: Uuid) {
        sqlx::query(
            "update claude_archive.outbox_events set next_resend_at = now() - interval '1 second'
             where event_id = $1",
        )
        .bind(event_id)
        .execute(self.pool())
        .await
        .expect("the row is made due");
    }
}

#[tokio::test]
async fn an_unacknowledged_tombstone_is_published_again_until_knowledge_acknowledges_it() {
    let rig = Rig::open().await;
    let event_id = rig.enqueue().await;

    assert_eq!(rig.pass().await, 1, "the first publish");
    let after_first = rig.copies().await;
    let backlog_unacknowledged = record_backlog(rig.pool())
        .await
        .expect("the backlog is readable");
    let gauge_unacknowledged = series("claude_archive_unacknowledged_tombstones");

    // Nothing acknowledges it, so it is due again; the duplicate window has passed by then.
    tokio::time::sleep(AFTER + Duration::from_millis(200)).await;
    rig.make_due(event_id).await;
    assert_eq!(rig.pass().await, 1, "the resend");
    let after_resend = rig.copies().await;

    let (stop, consumer) = rig.consume().await;
    rig.knowledge_says(acknowledgement_envelope(event_id, "applied"))
        .await;
    let outcome = rig.wait_acknowledged(event_id).await;
    rig.make_due(event_id).await;
    let after_acknowledgement = rig.pass().await;
    let after_stop = rig.copies().await;
    let backlog_after = record_backlog(rig.pool())
        .await
        .expect("the backlog is readable");
    let gauge_after = series("claude_archive_unacknowledged_tombstones");
    drop(stop);
    let summary = consumer.await.expect("the consumer task ends");
    let resent: i32 = sqlx::query_scalar(
        "select resend_count from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event_id)
    .fetch_one(rig.pool())
    .await
    .expect("the row is readable");
    // The transcript of the loop, visible with `--nocapture`: one tombstone, the stream copies
    // (by Nats-Msg-Id) after each step, and what the row recorded.
    eprintln!(
        "tombstone {event_id}\n  1. published, no acknowledgement: stream copies {after_first:?}, \
         unacknowledged gauge {gauge_unacknowledged}\n  2. due again, republished unchanged: \
         stream copies {after_resend:?}\n  3. Knowledge acknowledges (applied): row records \
         {outcome:?}, resend_count {resent}\n  4. due again after the acknowledgement: published \
         {after_acknowledgement}, stream copies {after_stop:?}, unacknowledged gauge {gauge_after}"
    );
    rig.db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        after_first,
        vec![event_id.to_string()],
        "one copy after the first publish"
    );
    assert_eq!(
        after_resend,
        vec![event_id.to_string(), event_id.to_string()],
        "the republished bytes carry the same Nats-Msg-Id"
    );
    assert_eq!(
        outcome.as_deref(),
        Some("applied"),
        "Knowledge's acknowledgement is recorded"
    );
    assert_eq!(
        after_acknowledgement, 0,
        "an acknowledged tombstone is not sent again"
    );
    assert_eq!(after_stop.len(), 2, "no third copy reached the stream");
    assert_eq!(resent, 1);
    assert_eq!(summary.recorded, 1);
    assert_eq!(backlog_unacknowledged.unacknowledged, 1);
    assert!(
        (gauge_unacknowledged - 1.0).abs() < f64::EPSILON,
        "the gauge counts the backlog"
    );
    assert_eq!(backlog_after.unacknowledged, 0);
    assert!(gauge_after.abs() < f64::EPSILON, "and returns to zero");
}

#[tokio::test]
async fn a_refused_acknowledgement_stops_the_resend_and_is_counted() {
    let rig = Rig::open().await;
    let event_id = rig.enqueue().await;
    assert_eq!(rig.pass().await, 1);
    let before = series("claude_archive_tombstone_refused_total");

    let (stop, consumer) = rig.consume().await;
    rig.knowledge_says(acknowledgement_envelope(event_id, "refused"))
        .await;
    let outcome = rig.wait_acknowledged(event_id).await;
    rig.make_due(event_id).await;
    let resent = rig.pass().await;
    let after = series("claude_archive_tombstone_refused_total");
    drop(stop);
    consumer.await.expect("the consumer task ends");
    rig.db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(outcome.as_deref(), Some("refused"));
    assert_eq!(resent, 0, "a refused tombstone is not sent again");
    assert!(
        (after - before - 1.0).abs() < f64::EPSILON,
        "claude_archive_tombstone_refused_total rose by one: {before} -> {after}"
    );
}

#[tokio::test]
async fn acknowledgements_for_no_row_and_garbage_are_consumed_without_effect() {
    let rig = Rig::open().await;
    let event_id = rig.enqueue().await;
    assert_eq!(rig.pass().await, 1);

    let (stop, consumer) = rig.consume().await;
    rig.knowledge_says(acknowledgement_envelope(Uuid::now_v7(), "applied"))
        .await;
    rig.knowledge_says(b"{ not an envelope".to_vec()).await;
    // A wrong producer cannot be attributed, so it is terminated too.
    let mut forged: serde_json::Value =
        serde_json::from_slice(&acknowledgement_envelope(event_id, "applied"))
            .expect("the envelope is JSON");
    forged["producer"] = "ratatoskr-chatgpt".into();
    rig.knowledge_says(serde_json::to_vec(&forged).expect("serializes"))
        .await;
    // The last message is real; once it is recorded the earlier ones were all handled.
    rig.knowledge_says(acknowledgement_envelope(event_id, "applied"))
        .await;
    let outcome = rig.wait_acknowledged(event_id).await;
    drop(stop);
    let summary = consumer.await.expect("the consumer task ends");
    let mut stream = rig
        .knowledge
        .get_stream("ratatoskr_events")
        .await
        .expect("the stream exists");
    let last_sequence = stream
        .info()
        .await
        .expect("stream info")
        .state
        .last_sequence;
    let info = stream
        .get_consumer::<jetstream::consumer::pull::Config>(DURABLE)
        .await
        .expect("the durable exists")
        .cached_info()
        .clone();
    rig.db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(outcome.as_deref(), Some("applied"));
    assert_eq!(summary.recorded, 1);
    assert_eq!(
        summary.ignored, 1,
        "the acknowledgement for no row is ignored"
    );
    assert_eq!(
        summary.terminated, 2,
        "the garbage and the forged producer are terminated"
    );
    assert_eq!(info.num_pending, 0, "everything was consumed");
    assert_eq!(
        info.ack_floor.stream_sequence, last_sequence,
        "and every delivery was settled"
    );
}

#[tokio::test]
async fn readiness_stays_green_while_knowledge_never_answers() {
    let rig = Rig::open().await;
    let event_id = rig.enqueue().await;
    let runtime = RuntimeState::new();
    runtime.mark_startup_complete();
    runtime.set_deletion_ack_consumer_ready(true);
    for _ in 0..3 {
        assert_eq!(
            rig.pass().await,
            1,
            "published, then republished, never acknowledged"
        );
        rig.make_due(event_id).await;
    }
    // Counted in SQL: the gauges are process-wide and another test of this binary asserts them.
    let unacknowledged: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'ai_archive.subject.tombstoned.v1' and acknowledged_at is null",
    )
    .fetch_one(rig.pool())
    .await
    .expect("the backlog is readable");
    rig.db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(unacknowledged, 1);
    assert!(
        runtime.is_ready(),
        "a Knowledge outage never makes this service unready"
    );
}
