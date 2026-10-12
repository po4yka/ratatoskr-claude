//! The pump records the stream sequence of the broker acknowledgement of every row it publishes
//! (XR-021 CONTRACTS.md R5-12 rule 2), so a deletion can ask Edge to erase exactly that message.
//!
//! The pump runs against the real `JetStream` of `CLAUDE_ARCHIVE_TEST_NATS_URL`; a missing broker
//! fails the tests, it never skips them.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::time::Duration;

use async_nats::jetstream::consumer::{DeliverPolicy, pull};
use futures_util::StreamExt as _;
use ratatoskr_ai_archive_contracts::AiConversationAdded;
use ratatoskr_claude_archive::test_support::{TestDatabase, nats_url};
use ratatoskr_claude_archive::{ArchiveEventFact, ArchiveOutbox, OutboxPublisher};
use uuid::Uuid;

const CONVERSATION_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "conversation":{"ai_conversation_id":"018f0000-0000-7000-8000-000000000403","provider":"claude","external_conversation_id":"conversation-001","owner":"user:018f0000-0000-7000-8000-000000000005","messages":[],"content_digest":{"algorithm":"sha256","hex":"37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

async fn events_stream() -> (
    async_nats::jetstream::Context,
    async_nats::jetstream::stream::Stream,
) {
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test broker accepts a connection");
    let jetstream = async_nats::jetstream::new(client);
    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_events".to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the events stream exists");
    (jetstream, stream)
}

/// The stream sequence at which the message with this `Nats-Msg-Id` is stored.
async fn stored_at(stream: &async_nats::jetstream::stream::Stream, event_id: Uuid) -> Vec<u64> {
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
                message
                    .info()
                    .expect("the delivery carries stream info")
                    .stream_sequence,
            );
        }
    }
    found
}

async fn recorded(db: &TestDatabase, event_id: Uuid) -> Option<i64> {
    sqlx::query_scalar(
        "select to_jsonb(o) -> 'published_stream_seq' from claude_archive.outbox_events o
         where o.event_id = $1",
    )
    .bind(event_id)
    .fetch_one(db.database.pool())
    .await
    .map(|value: serde_json::Value| value.as_i64())
    .expect("the row is readable")
}

async fn enqueue_fact(db: &TestDatabase) -> Uuid {
    // A conversation id unique to this call, so the shared broker never saw the message id.
    let added: AiConversationAdded = serde_json::from_str(&CONVERSATION_ADDED.replace(
        "conversation-001",
        &format!("conversation-{}", Uuid::now_v7()),
    ))
    .expect("the conversation fixture decodes");
    ArchiveOutbox::new(&db.database)
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T09:00:00Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued")
        .envelope
        .event_id
        .0
}

#[tokio::test]
async fn the_pump_records_the_sequence_of_the_stream_message() {
    let (jetstream, stream) = events_stream().await;
    let db = TestDatabase::create().await.expect("a disposable database");
    let event_id = enqueue_fact(&db).await;
    let before = recorded(&db, event_id).await;
    let pass = OutboxPublisher::new(db.database.pool().clone())
        .publish_pending_with(&jetstream)
        .await;
    let after = recorded(&db, event_id).await;
    let stored = stored_at(&stream, event_id).await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(matches!(pass, Ok(1)), "{pass:?}");
    assert_eq!(before, None, "nothing is recorded before the publish");
    let [sequence] = stored.as_slice() else {
        panic!("the stream holds exactly one copy: {stored:?}");
    };
    assert_eq!(
        after,
        Some(i64::try_from(*sequence).expect("a sequence fits")),
        "published_stream_seq is the sequence of the stream message"
    );
}

#[tokio::test]
async fn a_duplicate_acknowledgement_keeps_the_original_sequence() {
    let (jetstream, stream) = events_stream().await;
    let db = TestDatabase::create().await.expect("a disposable database");
    let event_id = enqueue_fact(&db).await;
    let publisher = OutboxPublisher::new(db.database.pool().clone());
    publisher
        .publish_pending_with(&jetstream)
        .await
        .expect("the first publish succeeds");
    let first = recorded(&db, event_id).await;
    // The pump died after the broker stored the message and before it marked the row: the next
    // pass publishes the same message id again and the broker answers with a duplicate.
    sqlx::query(
        "update claude_archive.outbox_events
         set published_at = null, published_stream_seq = null where event_id = $1",
    )
    .bind(event_id)
    .execute(db.database.pool())
    .await
    .expect("the crash is simulated");
    // Other tests publish into the same stream meanwhile, so the new sequence would differ.
    let pass = publisher.publish_pending_with(&jetstream).await;
    let second = recorded(&db, event_id).await;
    let stored = stored_at(&stream, event_id).await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(matches!(pass, Ok(1)), "{pass:?}");
    assert_eq!(
        stored.len(),
        1,
        "the broker collapsed the repeated message id"
    );
    assert!(first.is_some(), "the first publish recorded a sequence");
    assert_eq!(
        second, first,
        "the duplicate acknowledgement carries the original sequence"
    );
}
