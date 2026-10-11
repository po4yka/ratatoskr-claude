//! The pump completes `JetStream` requests under the one ACL that is deployed (XR-021
//! CONTRACTS.md R3-01).
//!
//! The broker here has no user but the stanza of `deploy/nats/identity.conf`, which is a byte-equal
//! copy of the `CLAUDE` stanza of Platform's `ratatoskr.conf`: that identity may subscribe to
//! `_INBOX_claude.>` and to nothing else. A connection without that inbox prefix never receives
//! the reply to a `JetStream` publish, so the pass fails. The tests need `nats-server` on `PATH`;
//! a missing binary fails them, it never skips them.

mod support;

use ratatoskr_ai_archive_contracts::AiConversationAdded;
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{ArchiveEventFact, ArchiveOutbox, OutboxPublisher};
use support::Broker;

const CONVERSATION_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "conversation":{"ai_conversation_id":"018f0000-0000-7000-8000-000000000403","provider":"claude","external_conversation_id":"conversation-001","owner":"user:018f0000-0000-7000-8000-000000000005","messages":[],"content_digest":{"algorithm":"sha256","hex":"37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

#[tokio::test]
async fn the_pump_completes_jetstream_requests_under_its_inbox_prefix() {
    let broker = Broker::authorized().expect("a broker with only the Claude stanza starts");
    broker
        .provision_events_stream()
        .await
        .expect("the events stream is provisioned as Edge does");
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

    let pass = OutboxPublisher::new(db.database.pool().clone())
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    let marked: bool = sqlx::query_scalar(
        "select published_at is not null from claude_archive.outbox_events where event_id = $1",
    )
    .bind(event.envelope.event_id.0)
    .fetch_one(db.database.pool())
    .await
    .expect("the row is readable");
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(pass, Ok(1)),
        "the pump publishes through the deployed ACL and receives the PubAck: {pass:?}"
    );
    assert!(marked, "the acknowledged row is marked published");
}
