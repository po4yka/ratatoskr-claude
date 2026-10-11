//! The generic ack-gated pump: every archive event type reaches a real `JetStream` on the subject
//! its contract type names, an unknown type is a hard error, and the pending queue is ordered.
//!
//! The pump tests need `CLAUDE_ARCHIVE_TEST_NATS_URL` to name a JetStream-enabled broker
//! (`nats-server -js`); a missing broker fails them, it never skips them.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::time::Duration;

use async_nats::jetstream::consumer::{DeliverPolicy, pull};
use futures_util::StreamExt as _;
use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiConversationAdded};
use ratatoskr_claude_archive::test_support::{TestDatabase, nats_url};
use ratatoskr_claude_archive::{ArchiveEventFact, ArchiveOutbox, OutboxError, OutboxPublisher};
use ratatoskr_event_envelope::EventEnvelope;
use uuid::Uuid;

const EVENTS_STREAM: &str = "ratatoskr_events";

const CONVERSATION_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "conversation":{"ai_conversation_id":"018f0000-0000-7000-8000-000000000403","provider":"claude","external_conversation_id":"conversation-001","owner":"user:018f0000-0000-7000-8000-000000000005","messages":[],"content_digest":{"algorithm":"sha256","hex":"37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

const TOMBSTONE: &str = r#"{
  "ai_archive_id":"018f0000-0000-7000-8000-000000000402",
  "provider":"claude",
  "owner":"user:018f0000-0000-7000-8000-000000000005",
  "subject":{"subject_kind":"archive"},
  "reason":"user_requested",
  "evidence_ref":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/json","length_bytes":512},
  "observed_at":"2026-08-27T06:00:00Z"
}"#;

async fn events_stream(
    jetstream: &async_nats::jetstream::Context,
) -> async_nats::jetstream::stream::Stream {
    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: EVENTS_STREAM.to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the events stream exists")
}

/// Every envelope on `subject` whose `Nats-Msg-Id` equals `event_id`.
async fn published_on(
    stream: &async_nats::jetstream::stream::Stream,
    subject: &str,
    event_id: Uuid,
) -> Vec<EventEnvelope> {
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: subject.to_owned(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("an ephemeral consumer attaches");
    let mut batch = consumer
        .fetch()
        .max_messages(1000)
        .expires(Duration::from_secs(3))
        .messages()
        .await
        .expect("the batch opens");
    let mut found = Vec::new();
    while let Some(message) = batch.next().await {
        let message = message.expect("the delivery is readable");
        let id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Msg-Id"))
            .map(ToString::to_string);
        if id.as_deref() == Some(event_id.to_string().as_str()) {
            found.push(
                serde_json::from_slice(&message.payload).expect("the body is an event envelope"),
            );
        }
    }
    found
}

/// The stream sequence at which each of `event_ids` was stored on `subjects`, by `Nats-Msg-Id`.
async fn stream_sequences(
    stream: &async_nats::jetstream::stream::Stream,
    event_ids: &[Uuid],
) -> Vec<Option<u64>> {
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: "evt.ai_archive.>".to_owned(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .expect("an ephemeral consumer attaches");
    let mut batch = consumer
        .fetch()
        .max_messages(1000)
        .expires(Duration::from_secs(3))
        .messages()
        .await
        .expect("the batch opens");
    let mut found = vec![None; event_ids.len()];
    while let Some(message) = batch.next().await {
        let message = message.expect("the delivery is readable");
        let id = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Msg-Id"))
            .map(ToString::to_string);
        let sequence = message
            .info()
            .expect("the delivery carries stream info")
            .stream_sequence;
        for (slot, event_id) in found.iter_mut().zip(event_ids) {
            if id.as_deref() == Some(event_id.to_string().as_str()) {
                *slot = Some(sequence);
            }
        }
    }
    found
}

async fn connect() -> async_nats::jetstream::Context {
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test broker accepts a connection");
    async_nats::jetstream::new(client)
}

#[tokio::test]
async fn conversation_added_envelope_is_published() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let added: AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED).expect("the conversation fixture decodes");
    let event = ArchiveOutbox::new(&db.database)
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T08:00:01Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued");
    let jetstream = connect().await;
    let stream = events_stream(&jetstream).await;

    let published = OutboxPublisher::new(db.database.pool().clone())
        .publish_pending_with(&jetstream)
        .await
        .expect("the pump publishes the pending fact");

    let on_the_bus = published_on(
        &stream,
        "evt.ai_archive.conversation.added.v1",
        event.envelope.event_id.0,
    )
    .await;
    let marked: bool = sqlx::query_scalar(
        "select published_at is not null from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event.envelope.event_id.0)
    .fetch_one(db.database.pool())
    .await
    .expect("the row is readable");
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(published, 1, "exactly the one pending fact is published");
    assert_eq!(
        on_the_bus,
        vec![event.envelope],
        "the stored envelope appears unchanged on the conversation-added subject"
    );
    assert!(
        marked,
        "the row is marked published only after the acknowledgement"
    );
}

#[tokio::test]
async fn an_event_type_outside_the_closed_list_is_a_hard_error() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    sqlx::query(
        "alter table claude_archive.outbox_events drop constraint outbox_events_event_type_check",
    )
    .execute(db.database.pool())
    .await
    .expect("the closed-list constraint exists and can be dropped for this proof");
    sqlx::query(
        "insert into claude_archive.outbox_events
         (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
          correlation_id, occurred_at)
         values ($1, 'claude.unknown.fact.v1', 'archive', 'x', '{}', decode(repeat('ab', 32), 'hex'),
                 'corr', now())",
    )
    .bind(Uuid::now_v7())
    .execute(db.database.pool())
    .await
    .expect("the unknown row inserts once the constraint is gone");
    let jetstream = connect().await;

    let result = OutboxPublisher::new(db.database.pool().clone())
        .publish_pending_with(&jetstream)
        .await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(result, Err(OutboxError::UnknownEventType)),
        "an unknown event type stops the worker instead of being skipped: {result:?}"
    );
}

#[tokio::test]
async fn the_pending_queue_orders_by_occurred_at_and_drops_published_rows() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let outbox = ArchiveOutbox::new(&db.database);
    let tombstone: AiArchiveTombstone =
        serde_json::from_str(TOMBSTONE).expect("the tombstone fixture decodes");
    let added: AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED).expect("the conversation fixture decodes");
    let later = outbox
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone),
            "2026-08-27T09:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the tombstone is enqueued");
    let earlier = outbox
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T08:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued");
    let publisher = OutboxPublisher::new(db.database.pool().clone());

    let both = publisher
        .pending_event_ids()
        .await
        .expect("the pending queue is readable");
    sqlx::query("update claude_archive.outbox_events set published_at = now() where event_id = $1")
        .bind(earlier.envelope.event_id.0)
        .execute(db.database.pool())
        .await
        .expect("the earlier row is marked published");
    let remaining = publisher
        .pending_event_ids()
        .await
        .expect("the pending queue is readable");
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(
        both,
        vec![earlier.envelope.event_id.0, later.envelope.event_id.0],
        "the queue holds the tombstone beside the fact, ordered by occurred_at then event_id"
    );
    assert_eq!(
        remaining,
        vec![later.envelope.event_id.0],
        "a published row leaves the queue and the unpublished tombstone stays"
    );
}

/// The ordering a consumer relies on is the pump's, not a copy of its query: a tombstone that
/// happened after a conversation fact reaches the bus after it, whatever order they were stored.
#[tokio::test]
async fn the_pump_publishes_different_event_types_in_occurred_at_order() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let outbox = ArchiveOutbox::new(&db.database);
    let tombstone: AiArchiveTombstone =
        serde_json::from_str(TOMBSTONE).expect("the tombstone fixture decodes");
    let added: AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED).expect("the conversation fixture decodes");
    let later = outbox
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone),
            "2026-08-27T09:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the tombstone is enqueued first");
    let earlier = outbox
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T08:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued second");
    let jetstream = connect().await;
    let stream = events_stream(&jetstream).await;

    let published = OutboxPublisher::new(db.database.pool().clone())
        .publish_pending_with(&jetstream)
        .await
        .expect("the pump publishes both rows");
    let sequences = stream_sequences(
        &stream,
        &[earlier.envelope.event_id.0, later.envelope.event_id.0],
    )
    .await;
    db.cleanup().await.expect("cleanup succeeds");

    assert_eq!(published, 2);
    let [Some(first), Some(second)] = sequences[..] else {
        panic!("both events must reach the bus: {sequences:?}");
    };
    assert!(
        first < second,
        "the earlier fact is stored before the later tombstone: {first} then {second}"
    );
}

#[tokio::test]
async fn the_schema_refuses_an_event_type_outside_the_closed_list() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");

    let inserted = sqlx::query(
        "insert into claude_archive.outbox_events
         (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
          correlation_id, occurred_at)
         values ($1, 'claude.unknown.fact.v1', 'archive', 'x', '{}', decode(repeat('ab', 32), 'hex'),
                 'corr', now())",
    )
    .bind(Uuid::now_v7())
    .execute(db.database.pool())
    .await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        inserted.is_err(),
        "a row the pump could not publish must be impossible to insert"
    );
}

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

/// The row's `undeliverable_at` as JSON text, read without naming the column so the same query
/// answers before and after the column exists.
async fn undeliverable_at(db: &TestDatabase, event_id: Uuid) -> Option<String> {
    sqlx::query_scalar(
        "select to_jsonb(o) ->> 'undeliverable_at' from claude_archive.outbox_events o
         where o.event_id = $1",
    )
    .bind(event_id)
    .fetch_one(db.database.pool())
    .await
    .expect("the row is readable")
}

/// A body larger than the connected server's `max_payload` can never be published, so retrying it
/// forever would wedge the pump and keep readiness false (XR-021 CONTRACTS.md R2-04 rule 2). The
/// row is marked undeliverable, counted, skipped, and the rows behind it are published in the same
/// pass, which succeeds. The broker's own limit is read, never assumed.
#[tokio::test]
async fn an_undeliverable_row_does_not_wedge_the_pump() {
    let recorder = metrics();
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let outbox = ArchiveOutbox::new(&db.database);
    let tombstone: AiArchiveTombstone =
        serde_json::from_str(TOMBSTONE).expect("the tombstone fixture decodes");
    let added: AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED).expect("the conversation fixture decodes");
    let oversized = outbox
        .enqueue(
            &ArchiveEventFact::Tombstoned(tombstone),
            "2026-08-27T08:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the tombstone is enqueued");
    let deliverable = outbox
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T09:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued");
    let jetstream = connect().await;
    let limit = jetstream.client().server_info().max_payload;
    sqlx::query(
        "update claude_archive.outbox_events
         set envelope = jsonb_set(envelope, '{payload,padding}', to_jsonb(repeat('x', $2)))
         where event_id = $1",
    )
    .bind(oversized.envelope.event_id.0)
    .bind(i32::try_from(limit + 2048).expect("the broker limit fits"))
    .execute(db.database.pool())
    .await
    .expect("the head row is padded past the broker payload limit");
    let stream = events_stream(&jetstream).await;
    let publisher = OutboxPublisher::new(db.database.pool().clone());

    let first_pass = publisher.publish_pending_with(&jetstream).await;
    assert!(
        matches!(first_pass, Ok(1)),
        "an undeliverable row is not a failed pass: the pass publishes the row behind it and \
         succeeds, so readiness stays true: {first_pass:?}"
    );
    let marked = undeliverable_at(&db, oversized.envelope.event_id.0).await;
    let (was_published, attempts, last_error): (bool, i32, Option<String>) = sqlx::query_as(
        "select published_at is not null, attempt_count, last_error
         from claude_archive.outbox_events where event_id = $1",
    )
    .bind(oversized.envelope.event_id.0)
    .fetch_one(db.database.pool())
    .await
    .expect("the oversized row is readable");
    let on_the_bus = published_on(
        &stream,
        "evt.ai_archive.conversation.added.v1",
        deliverable.envelope.event_id.0,
    )
    .await;
    let second_pass = publisher.publish_pending_with(&jetstream).await;
    let pending = publisher
        .pending_event_ids()
        .await
        .expect("the pending queue is readable");
    let exposition = recorder.render();
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        marked.is_some(),
        "the oversized row is marked undeliverable"
    );
    assert!(!was_published, "an undeliverable row is never published");
    assert_eq!(attempts, 0, "no publish was attempted for it");
    assert_eq!(
        last_error.as_deref(),
        Some("payload_too_large"),
        "the row records the safe class"
    );
    assert_eq!(
        on_the_bus,
        vec![deliverable.envelope],
        "the deliverable envelope reached the bus unchanged"
    );
    assert!(
        matches!(second_pass, Ok(0)),
        "the next pass offers nothing: {second_pass:?}"
    );
    assert!(
        pending.is_empty(),
        "an undeliverable row leaves the pending queue: {pending:?}"
    );
    assert!(
        exposition.lines().any(|line| line
            .starts_with("claude_archive_outbox_undeliverable_total{")
            && line.contains("class=\"payload_too_large\"")),
        "the counter claude_archive_outbox_undeliverable_total{{class}} is exported: {exposition}"
    );
}

/// A refusal that says nothing about the row (an unreachable broker, an acknowledgement that never
/// arrives) is transient: it counts the attempt, records a safe class and backs off, and never
/// marks the row undeliverable. It passes before the undeliverable column exists, and guards the
/// classifier against ever treating an outage as a property of the row.
#[tokio::test]
async fn transient_refusals_never_make_a_row_undeliverable() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let added: AiConversationAdded =
        serde_json::from_str(CONVERSATION_ADDED).expect("the conversation fixture decodes");
    let event = ArchiveOutbox::new(&db.database)
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T09:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued");
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test broker accepts a connection");
    let impatient = async_nats::jetstream::new(client.clone());
    // A drained client has left the broker: every publish through it fails, which is what the
    // pump sees during an outage.
    client.drain().await.expect("the client drains");
    let publisher = OutboxPublisher::new(db.database.pool().clone());

    let first_pass = publisher.publish_pending_with(&impatient).await;
    let (was_published, attempts, last_error, backed_off): (bool, i32, Option<String>, bool) =
        sqlx::query_as(
            "select published_at is not null, attempt_count, last_error, next_attempt_at > now()
             from claude_archive.outbox_events where event_id = $1",
        )
        .bind(event.envelope.event_id.0)
        .fetch_one(db.database.pool())
        .await
        .expect("the refused row is readable");
    let marked = undeliverable_at(&db, event.envelope.event_id.0).await;
    let second_pass = publisher.publish_pending_with(&impatient).await;
    sqlx::query(
        "update claude_archive.outbox_events set next_attempt_at = now() where event_id = $1",
    )
    .bind(event.envelope.event_id.0)
    .execute(db.database.pool())
    .await
    .expect("the backoff is released");
    let recovered = publisher.publish_pending_with(&connect().await).await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        first_pass
            .as_ref()
            .is_err_and(|error| matches!(error, OutboxError::Broker(_)) && !error.is_fatal()),
        "a refused pass reports a non-fatal broker error: {first_pass:?}"
    );
    assert!(!was_published, "a refused row stays unpublished");
    assert_eq!(attempts, 1, "the refused attempt is counted");
    assert!(
        matches!(last_error.as_deref(), Some("publish_failed" | "ack_failed")),
        "the refusal records a transient class, got {last_error:?}"
    );
    assert!(
        backed_off,
        "the refused row waits until a future attempt time"
    );
    assert_eq!(marked, None, "a transient refusal never marks the row");
    assert!(
        matches!(second_pass, Ok(0)),
        "the backed-off row is not offered again before its time: {second_pass:?}"
    );
    assert!(
        matches!(recovered, Ok(1)),
        "once the broker answers, the same row is published: {recovered:?}"
    );
}
