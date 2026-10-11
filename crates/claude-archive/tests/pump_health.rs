//! What the publisher reports about its broker, and when it lets go of a connection (XR-021
//! CONTRACTS.md R3-07 rule 1).
//!
//! A pass over an empty outbox makes no network call, so it can say nothing new about the broker:
//! the verdict is that of the last pass that did talk to it. A connection that failed a pass is
//! dropped, so the next pass reads the nkey seed file again and a rotated seed takes effect
//! without a restart. The tests need `nats-server` on `PATH`; a missing binary fails them.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::time::Duration;

use ratatoskr_ai_archive_contracts::AiConversationAdded;
use ratatoskr_claude_archive::test_support::{Broker, TestDatabase};
use ratatoskr_claude_archive::{ArchiveEventFact, ArchiveOutbox, OutboxPublisher};

const CONVERSATION_ADDED: &str = r#"{
  "import_provenance":{"ai_archive_id":"018f0000-0000-7000-8000-000000000401","provider":"claude","owner":"user:018f0000-0000-7000-8000-000000000005","source_export":{"owner_service":"ratatoskr-claude","digest":{"algorithm":"sha256","hex":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"media_type":"application/zip","length_bytes":1048576},"imported_at":"2026-08-27T08:00:00Z","parser_name":"claude_export","parser_version":"2026.08.1"},
  "conversation":{"ai_conversation_id":"018f0000-0000-7000-8000-000000000403","provider":"claude","external_conversation_id":"conversation-001","owner":"user:018f0000-0000-7000-8000-000000000005","messages":[],"content_digest":{"algorithm":"sha256","hex":"37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570"},"parser_name":"claude_export","parser_version":"2026.08.1"}
}"#;

/// The first retry delay of a refused row is two seconds.
const FIRST_BACKOFF: Duration = Duration::from_millis(2_500);

async fn enqueue_conversation(db: &TestDatabase, number: u32) {
    let added: AiConversationAdded = serde_json::from_str(
        &CONVERSATION_ADDED.replace("conversation-001", &format!("conversation-{number}")),
    )
    .expect("the conversation fixture decodes");
    ArchiveOutbox::new(&db.database)
        .enqueue(
            &ArchiveEventFact::ConversationAdded(added),
            "2026-08-27T08:00:01Z".parse().expect("a canonical instant"),
        )
        .await
        .expect("the conversation fact is enqueued");
}

/// A publisher with nothing to publish has no failure to report, and a missing seed file only
/// matters once a row needs the broker.
#[tokio::test]
async fn an_idle_publisher_is_healthy_and_a_failed_connect_is_not() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let publisher = OutboxPublisher::new(db.database.pool().clone());
    let missing = std::path::Path::new("/nonexistent/claude-seed.nkey");
    let idle = publisher
        .publish_pending_once("nats://127.0.0.1:1", missing)
        .await;
    let idle_health = publisher.is_healthy();
    enqueue_conversation(&db, 1).await;
    let busy = publisher
        .publish_pending_once("nats://127.0.0.1:1", missing)
        .await;
    let busy_health = publisher.is_healthy();
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(idle, Ok(0)),
        "an empty pass needs no seed: {idle:?}"
    );
    assert!(idle_health, "nothing has failed yet");
    assert!(busy.is_err(), "a pending row needs the seed: {busy:?}");
    assert!(!busy_health, "a pass that could not connect is a failure");
}

/// The backoff window of a refused row yields passes that return `Ok(0)` without any network
/// call; they must not flip a failing publisher back to healthy.
#[tokio::test]
async fn an_empty_pass_during_a_backoff_keeps_the_failure_until_a_publish_succeeds() {
    let mut broker = Broker::open().expect("a private broker starts");
    broker
        .provision_events_stream()
        .await
        .expect("the events stream is provisioned");
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let publisher = OutboxPublisher::new(db.database.pool().clone());
    enqueue_conversation(&db, 1).await;
    let first = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    let first_health = publisher.is_healthy();
    broker.stop();
    enqueue_conversation(&db, 2).await;
    let refused = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    let refused_health = publisher.is_healthy();
    let backing_off = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    let backing_off_health = publisher.is_healthy();
    broker.restart().expect("the broker starts again");
    tokio::time::sleep(FIRST_BACKOFF).await;
    let recovered = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    let recovered_health = publisher.is_healthy();
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(first, Ok(1)),
        "the first pass publishes: {first:?}"
    );
    assert!(first_health, "an acknowledged publish is healthy");
    assert!(
        refused.is_err(),
        "a pass over a stopped broker fails: {refused:?}"
    );
    assert!(!refused_health, "the failed pass is reported");
    assert!(
        matches!(backing_off, Ok(0)),
        "the refused row waits out its delay: {backing_off:?}"
    );
    assert!(
        !backing_off_health,
        "a pass that never spoke to the broker cannot clear the failure"
    );
    assert!(
        matches!(recovered, Ok(1)),
        "the broker is back and the row is published: {recovered:?}"
    );
    assert!(
        recovered_health,
        "an acknowledged publish clears the failure"
    );
}

/// A rotated seed file is read again once a pass has failed. Without that, the held client keeps
/// reconnecting with the credentials it was created with and the old identity never stops
/// working.
#[tokio::test]
async fn a_rotated_seed_is_used_after_a_failed_pass() {
    let mut broker = Broker::authorized().expect("a broker with only the Claude stanza starts");
    broker
        .provision_events_stream()
        .await
        .expect("the events stream is provisioned");
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let publisher = OutboxPublisher::new(db.database.pool().clone());
    let original = std::fs::read(&broker.service_seed).expect("the seed file is readable");
    enqueue_conversation(&db, 1).await;
    let first = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    broker.stop();
    enqueue_conversation(&db, 2).await;
    let refused = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;

    broker
        .rotate_service_seed()
        .expect("the seed file is replaced by a key the broker does not know");
    broker.restart().expect("the broker starts again");
    tokio::time::sleep(FIRST_BACKOFF).await;
    let rotated = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    std::fs::write(&broker.service_seed, original).expect("the original seed is put back");
    let restored = publisher
        .publish_pending_once(&broker.url(), &broker.service_seed)
        .await;
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(first, Ok(1)),
        "the first pass publishes: {first:?}"
    );
    assert!(
        refused.is_err(),
        "a pass over a stopped broker fails: {refused:?}"
    );
    assert!(
        rotated.is_err(),
        "the new seed is read, and the broker does not know it: {rotated:?}"
    );
    assert!(
        matches!(restored, Ok(1)),
        "the original seed is read again and the row is published: {restored:?}"
    );
}
