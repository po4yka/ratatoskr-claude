//! A fact row in flight is serialized with its deletion, and every published copy of a deleted
//! fact is named by an erasure command (XR-021 CONTRACTS.md R6-03).
//!
//! Every case drives the real receipt, import, pump and deletion path against a real `PostgreSQL`
//! and a private `JetStream` whose two streams are shaped like `ratatoskr_events` and
//! `ratatoskr_commands`; a stand-in for the Edge handler executes the commands. The pump is held at
//! a named point by a [`PublishProbe`], so each race is replayed exactly instead of hoped for.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod cumulative;
mod erasure_support;
mod survivor_series;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use erasure_support::{Command, commands, edge_erases, named, plan_tenant};
use ratatoskr_claude_archive::PublishProbe;
use ratatoskr_claude_archive::test_support::Broker;
use ratatoskr_claude_archive::{OutboxError, OutboxPublisher, PlatformUsers, PublishPause};
use ratatoskr_event_envelope::EventEnvelope;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use survivor_series::{Cumulative, export, import_three};
use uuid::Uuid;

const OTHER_USER: &str = "018f0000-0000-7000-8000-0000000000d2";
const FACT_TYPES: &str =
    "event_type like 'ai_archive.%' and event_type <> 'ai_archive.subject.tombstoned.v1'";

/// The deleted owner (`USER`), a second owner whose facts must survive, a private broker with the
/// two streams of the deployment, and a pump whose pool is its own.
struct Rig {
    broker: Broker,
    imported: Cumulative,
    pump_pool: PgPool,
    probe: Arc<PublishProbe>,
    publisher: OutboxPublisher,
    jetstream: async_nats::jetstream::Context,
    events: async_nats::jetstream::stream::Stream,
    commands: async_nats::jetstream::stream::Stream,
}

/// One message of the events stream that is still stored.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stored {
    sequence: u64,
    subject: String,
    message_id: Option<Uuid>,
    owner: Option<String>,
}

impl Rig {
    async fn open(label: &str, duplicate_window: Duration) -> Self {
        let broker = Broker::open().expect("a private broker starts");
        broker
            .provision_streams(duplicate_window)
            .await
            .expect("the two streams are provisioned");
        let client = async_nats::connect(broker.url())
            .await
            .expect("the private broker accepts a connection");
        let jetstream = async_nats::jetstream::new(client);
        let events = jetstream
            .get_stream("ratatoskr_events")
            .await
            .expect("the events stream exists");
        let commands = jetstream
            .get_stream("ratatoskr_commands")
            .await
            .expect("the commands stream exists");
        let imported = import_three(label).await;
        let other_account = cumulative::add_account(&imported.series.db).await;
        let other = cumulative::worker_over(
            &imported.series.db,
            &imported.series.store,
            PlatformUsers::new(&[(
                Uuid::parse_str(OTHER_USER).expect("user UUID"),
                other_account,
            )]),
        );
        imported.series.receive_as(other_account, &export(1)).await;
        assert_eq!(other.process_pending_once().await.expect("imports"), 1);
        let pool = imported.series.pool();
        // The operation reports are not under test: publish them out of the way, and offer the
        // deleted owner's facts before the other owner's.
        sqlx::query(
            "update claude_archive.outbox_events set published_at = now()
             where event_type = 'platform.operation.reported.v1'",
        )
        .execute(pool)
        .await
        .expect("the reports are set aside");
        sqlx::query(
            "update claude_archive.outbox_events set occurred_at = occurred_at - interval '1 day'
             where tenant_ref = $1",
        )
        .bind(Cumulative::tenant_ref())
        .execute(pool)
        .await
        .expect("the deleted owner's facts are offered first");
        let pump_pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .expect("the pump's own pool connects");
        let probe = Arc::new(PublishProbe::new());
        let publisher =
            OutboxPublisher::new(pump_pool.clone()).with_publish_probe(Arc::clone(&probe));
        Self {
            broker,
            imported,
            pump_pool,
            probe,
            publisher,
            jetstream,
            events,
            commands,
        }
    }

    fn pool(&self) -> &PgPool {
        self.imported.series.pool()
    }

    /// Publishes everything that is pending, as the pump of the service does.
    async fn pump_all(&self) {
        while self
            .publisher
            .publish_pending_with(&self.jetstream)
            .await
            .expect("the pending rows are published")
            > 0
        {}
    }

    /// The fact messages of the events stream, whoever owns them.
    async fn facts_on_stream(&self) -> Vec<Stored> {
        let mut events = self.events.clone();
        let state = events
            .info()
            .await
            .expect("the stream info is readable")
            .state
            .clone();
        let mut stored = Vec::new();
        for sequence in state.first_sequence..=state.last_sequence {
            let Ok(message) = events.get_raw_message(sequence).await else {
                continue;
            };
            let subject = message.subject.to_string();
            if !subject.starts_with("evt.ai_archive.")
                || subject == "evt.ai_archive.subject.tombstoned.v1"
            {
                continue;
            }
            let envelope = EventEnvelope::from_json(&message.payload).expect("a fact envelope");
            stored.push(Stored {
                sequence,
                subject,
                message_id: message
                    .headers
                    .get("Nats-Msg-Id")
                    .and_then(|id| Uuid::parse_str(id.as_ref()).ok()),
                owner: envelope.tenant_id.map(|owner| owner.to_string()),
            });
        }
        stored
    }

    async fn on_stream_of(&self, owner: &str) -> Vec<Stored> {
        self.facts_on_stream()
            .await
            .into_iter()
            .filter(|message| message.owner.as_deref() == Some(owner))
            .collect()
    }

    /// The first fact row the pump offers, which belongs to the deleted owner.
    async fn first_fact_row(&self) -> Uuid {
        sqlx::query_scalar(&format!(
            "select event_id from claude_archive.outbox_events
             where published_at is null and {FACT_TYPES} order by occurred_at, event_id limit 1"
        ))
        .fetch_one(self.pool())
        .await
        .expect("a fact row is pending")
    }

    async fn recorded(&self, event_id: Uuid) -> Vec<i64> {
        sqlx::query_scalar(
            "select published_stream_seqs from claude_archive.outbox_events where event_id = $1",
        )
        .bind(event_id)
        .fetch_one(self.pool())
        .await
        .expect("the row is readable")
    }

    /// Executes every command the outbox holds, as Edge would.
    async fn edge_executes(&self) -> usize {
        let ids: BTreeSet<Uuid> = commands(self.pool())
            .await
            .iter()
            .map(|command| command.id)
            .collect();
        edge_erases(&self.commands, &self.events, &ids).await
    }

    async fn finish(self) {
        self.pump_pool.close().await;
        self.imported.finish().await;
    }
}

fn owner() -> String {
    Cumulative::tenant_ref()
}

fn other_owner() -> String {
    format!("user:{OTHER_USER}")
}

/// The commands of `owner`, whatever they are named for.
fn commands_for<'a>(all: &'a [Command], owner: &str) -> Vec<&'a Command> {
    all.iter()
        .filter(|command| command.owner == owner)
        .collect()
}

/// R6-03 rule 7 (a): the candidates are chosen, a deletion of the newest export commits, the pump
/// resumes. The rows the deletion removed are skipped, nothing of them is published and the stream
/// holds no message of the deleted facts.
#[tokio::test]
async fn a_row_deleted_before_the_pump_locks_it_is_never_published() {
    let rig = Rig::open("in-flight-before-lock", Duration::from_mins(2)).await;
    let fact_rows = || async {
        sqlx::query_scalar::<_, Uuid>(&format!(
            "select event_id from claude_archive.outbox_events
             where tenant_ref = $1 and {FACT_TYPES}"
        ))
        .bind(owner())
        .fetch_all(rig.pool())
        .await
        .expect("the owner's fact rows are readable")
    };
    let before = fact_rows().await;
    rig.probe.pause_at(PublishPause::AfterCandidates);
    let pass = tokio::spawn({
        let publisher = rig.publisher.clone();
        let jetstream = rig.jetstream.clone();
        async move { publisher.publish_pending_with(&jetstream).await }
    });
    rig.probe.reached(PublishPause::AfterCandidates).await;
    rig.imported.delete_export(rig.imported.exports[2]).await;
    let after = fact_rows().await;
    rig.probe.release(PublishPause::AfterCandidates);
    let result = pass.await.expect("the pass ends");
    rig.pump_all().await;
    let deleted: Vec<Uuid> = before
        .iter()
        .filter(|id| !after.contains(id))
        .copied()
        .collect();
    let published_deleted: Vec<Stored> = rig
        .facts_on_stream()
        .await
        .into_iter()
        .filter(|message| message.message_id.is_some_and(|id| deleted.contains(&id)))
        .collect();
    let on_stream = rig.on_stream_of(&owner()).await;
    let issued = commands(rig.pool()).await;
    rig.finish().await;

    assert!(
        !deleted.is_empty(),
        "the deletion removed fact rows the pump had chosen"
    );
    assert_eq!(
        published_deleted,
        Vec::new(),
        "the pump published fact rows after their deletion committed"
    );
    assert!(
        matches!(result, Ok(published) if published > 0),
        "the surviving candidates were still published: {result:?}"
    );
    assert!(
        !on_stream.is_empty(),
        "the facts that survived the deletion are on the stream"
    );
    assert!(
        commands_for(&issued, &owner()).is_empty(),
        "nothing of the deleted rows was published, so nothing is erased: {issued:?}"
    );
}

/// R6-03 rule 7 (c): the pump is held after the acknowledgement and before its commit, a deletion
/// starts. It waits for the pump, then names the acknowledged sequence, and after the stand-in for
/// Edge executes the command the stream message is gone and another owner's fact is untouched.
#[tokio::test]
async fn a_deletion_inside_the_acknowledgement_window_waits_and_names_the_sequence() {
    let rig = Rig::open("in-flight-after-ack", Duration::from_mins(2)).await;
    rig.probe.pause_at(PublishPause::AfterPubAck);
    let pass = tokio::spawn({
        let publisher = rig.publisher.clone();
        let jetstream = rig.jetstream.clone();
        async move { publisher.publish_pending_with(&jetstream).await }
    });
    rig.probe.reached(PublishPause::AfterPubAck).await;
    let in_flight = rig.on_stream_of(&owner()).await;
    let [acknowledged] = in_flight.as_slice() else {
        panic!("exactly one copy is on the stream at the pause: {in_flight:?}");
    };
    let acknowledged = acknowledged.sequence;

    let mut deletion = Box::pin(async {
        let request = plan_tenant(&rig.imported).await;
        rig.imported.execute(request).await;
    });
    let waited = tokio::time::timeout(Duration::from_millis(500), &mut deletion).await;
    assert!(
        waited.is_err(),
        "the deletion completed while the pump held the row after its acknowledgement"
    );
    rig.probe.release(PublishPause::AfterPubAck);
    deletion.await;
    let result = pass.await.expect("the pass ends");
    let other_before = rig.on_stream_of(&other_owner()).await;
    rig.pump_all().await;
    let issued = commands(rig.pool()).await;
    let erased = rig.edge_executes().await;
    let left = rig.on_stream_of(&owner()).await;
    let other_after = rig.on_stream_of(&other_owner()).await;
    rig.finish().await;

    assert!(result.is_ok(), "{result:?}");
    let named_for_owner = named(&issued);
    assert!(
        named_for_owner
            .get(&owner())
            .is_some_and(|sequences| sequences.contains(&acknowledged)),
        "a command names the acknowledged sequence {acknowledged}: {issued:?}"
    );
    assert!(erased > 0, "the stand-in for Edge erased messages");
    assert_eq!(left, Vec::new(), "no message of the deleted owner is left");
    assert!(!other_before.is_empty(), "the other owner published facts");
    assert_eq!(
        other_after, other_before,
        "the other owner's messages are untouched"
    );
}

/// R6-03 rule 7 (b): a commit failure after the acknowledgement, a duplicate window that passes,
/// and a second publication leave two stream copies. The row records both, and one deletion names
/// both.
#[tokio::test]
async fn a_republish_after_the_duplicate_window_records_both_sequences() {
    let rig = Rig::open("in-flight-republish", Duration::from_secs(1)).await;
    let first = rig.first_fact_row().await;
    rig.probe.fail_commit_once();
    let failed = rig.publisher.publish_pending_with(&rig.jetstream).await;
    let after_failure = rig.recorded(first).await;
    let first_copy = rig.facts_on_stream().await;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    rig.pump_all().await;
    let recorded = rig.recorded(first).await;
    let copies: Vec<u64> = rig
        .facts_on_stream()
        .await
        .into_iter()
        .filter(|message| message.message_id == Some(first))
        .map(|message| message.sequence)
        .collect();

    let request = plan_tenant(&rig.imported).await;
    rig.imported.execute(request).await;
    rig.pump_all().await;
    let issued = commands(rig.pool()).await;
    rig.edge_executes().await;
    let left = rig.on_stream_of(&owner()).await;
    let other = rig.on_stream_of(&other_owner()).await;
    rig.finish().await;

    assert!(
        matches!(failed, Err(OutboxError::Database(_))),
        "the pass reports the failed commit: {failed:?}"
    );
    assert_eq!(
        after_failure,
        Vec::<i64>::new(),
        "the failed commit recorded nothing"
    );
    assert_eq!(first_copy.len(), 1, "the broker holds the first copy");
    assert_eq!(
        copies.len(),
        2,
        "the republish is a second copy: {copies:?}"
    );
    let both: Vec<i64> = copies
        .iter()
        .map(|sequence| i64::try_from(*sequence).expect("a sequence fits"))
        .collect();
    assert_eq!(recorded, both, "the row records both sequences, ascending");
    let for_owner = commands_for(&issued, &owner());
    assert!(
        for_owner
            .iter()
            .any(|command| copies.iter().all(|copy| command.sequences.contains(copy))),
        "one command names both copies: {issued:?}"
    );
    assert_eq!(left, Vec::new(), "neither copy is left on the stream");
    assert!(!other.is_empty(), "the other owner's facts are untouched");
}

/// R6-03 rule 4: the copy whose commit failed is carried, and when its row is deleted before the
/// retry the pump itself asks for its erasure, once, in the owner's name.
#[tokio::test]
async fn a_carried_copy_whose_row_was_deleted_is_erased_by_the_pump() {
    let rig = Rig::open("in-flight-carried", Duration::from_secs(1)).await;
    let orphan = rig.first_fact_row().await;
    rig.probe.fail_commit_once();
    let failed = rig.publisher.publish_pending_with(&rig.jetstream).await;
    let carried = rig.on_stream_of(&owner()).await;
    let request = plan_tenant(&rig.imported).await;
    rig.imported.execute(request).await;
    let by_deletion = commands(rig.pool()).await;
    // The service's entry point: the carried copy is checked before the pass looks at the queue.
    let second_pass = rig
        .publisher
        .publish_pending_once(&rig.broker.url(), &rig.broker.service_seed)
        .await;
    let after_second_pass = commands(rig.pool()).await;
    rig.pump_all().await;
    let after_third_pass = commands(rig.pool()).await;
    let erased = rig.edge_executes().await;
    let left = rig.on_stream_of(&owner()).await;
    let other = rig.on_stream_of(&other_owner()).await;
    rig.finish().await;

    assert!(failed.is_err(), "the commit failed: {failed:?}");
    assert!(
        second_pass.is_ok(),
        "the second pass publishes: {second_pass:?}"
    );
    let [carried] = carried.as_slice() else {
        panic!("the broker holds the one carried copy: {carried:?}");
    };
    assert!(
        by_deletion.is_empty(),
        "the deletion saw no recorded sequence: {by_deletion:?}"
    );
    let reference = format!("privacy_request:{orphan}");
    let pump_made: Vec<&Command> = after_second_pass
        .iter()
        .filter(|command| command.request_ref == reference)
        .collect();
    let [command] = pump_made.as_slice() else {
        panic!("the second pass enqueued one command for the orphan: {after_second_pass:?}");
    };
    assert_eq!(command.owner, owner());
    assert_eq!(command.producer, "ratatoskr-claude");
    assert_eq!(command.sequences, vec![carried.sequence]);
    assert_eq!(
        after_third_pass.len(),
        after_second_pass.len(),
        "a third pass enqueues nothing more: the carried entry is gone"
    );
    assert!(erased > 0, "the stand-in for Edge executed the command");
    assert_eq!(left, Vec::new(), "the carried copy is gone from the stream");
    assert!(!other.is_empty(), "the other owner's facts are untouched");
}
