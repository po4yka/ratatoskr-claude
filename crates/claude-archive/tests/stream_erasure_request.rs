//! A deletion that removes a published fact row from the outbox asks Edge to erase that message
//! from the events stream (XR-021 CONTRACTS.md R5-12 rules 2 and 3).
//!
//! Every case drives the real receipt, import and deletion path against a real `PostgreSQL`; the
//! last one also drives the pump and a stand-in for Edge against a real `JetStream`. Rows are
//! marked published with a made-up stream sequence where the broker is not the subject of the test,
//! so each command can be compared with the sequences of the rows that left.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod cumulative;
mod survivor_series;

use std::time::Duration;

use std::collections::{BTreeMap, BTreeSet};

use async_nats::jetstream::consumer::{DeliverPolicy, pull};
use futures_util::StreamExt as _;
use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionPlanner, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::nats_url;
use ratatoskr_claude_archive::{OutboxPublisher, PlatformUsers};
use ratatoskr_event_envelope::{CommandEnvelope, EventEnvelope};
use ratatoskr_operation_contracts::PlatformStreamErasureRequested;
use sqlx::PgPool;
use survivor_series::{Cumulative, export, import_series, import_three};
use uuid::Uuid;

const COMMAND: &str = "platform.stream_erasure.requested.v1";
const FACT_TYPES: &str =
    "event_type like 'ai_archive.%' and event_type <> 'ai_archive.subject.tombstoned.v1'";
const OTHER_USER: &str = "018f0000-0000-7000-8000-0000000000d2";

/// `(event_id, event_type, aggregate_id, tenant_ref, published_stream_seqs)` of one fact row.
type FactRow = (Uuid, String, String, Option<String>, Vec<i64>);

/// One fact row as the outbox holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fact {
    event_type: String,
    aggregate: String,
    owner: Option<String>,
    sequences: Vec<u64>,
}

/// One erasure command as the outbox holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Command {
    owner: String,
    producer: String,
    request_ref: String,
    correlation: String,
    sequences: Vec<u64>,
    id: Uuid,
}

/// Marks every fact row published, at a sequence of its own, and returns what each row holds.
async fn publish_all(pool: &PgPool) -> BTreeMap<Uuid, Fact> {
    sqlx::query(&format!(
        "update claude_archive.outbox_events o
         set published_at = now(), published_stream_seqs = array[1000 + x.n]
         from (select event_id, row_number() over (order by occurred_at, event_id) as n
               from claude_archive.outbox_events where {FACT_TYPES}) x
         where o.event_id = x.event_id"
    ))
    .execute(pool)
    .await
    .expect("the facts are marked published");
    facts(pool).await
}

async fn facts(pool: &PgPool) -> BTreeMap<Uuid, Fact> {
    let rows: Vec<FactRow> = sqlx::query_as(&format!(
        "select event_id, event_type, aggregate_id, tenant_ref, published_stream_seqs
         from claude_archive.outbox_events where {FACT_TYPES}"
    ))
    .fetch_all(pool)
    .await
    .expect("the facts are readable");
    rows.into_iter()
        .map(|(id, event_type, aggregate, owner, sequences)| {
            (
                id,
                Fact {
                    event_type,
                    aggregate,
                    owner,
                    sequences: sequences
                        .into_iter()
                        .map(|sequence| u64::try_from(sequence).expect("a sequence is positive"))
                        .collect(),
                },
            )
        })
        .collect()
}

async fn commands(pool: &PgPool) -> Vec<Command> {
    let rows: Vec<(Uuid, serde_json::Value, String)> = sqlx::query_as(
        "select event_id, envelope, aggregate_type from claude_archive.outbox_events
         where event_type = $1 order by occurred_at, event_id",
    )
    .bind(COMMAND)
    .fetch_all(pool)
    .await
    .expect("the commands are readable");
    rows.into_iter()
        .map(|(id, raw, aggregate_type)| {
            assert_eq!(aggregate_type, "privacy_request");
            let envelope: CommandEnvelope =
                serde_json::from_value(raw).expect("a complete command envelope");
            assert_eq!(envelope.command_id.0, id, "the envelope carries the row id");
            assert_eq!(envelope.command_type.to_wire(), COMMAND);
            let payload = envelope
                .payload_as::<PlatformStreamErasureRequested>()
                .expect("a valid erasure request");
            assert_eq!(
                envelope.tenant_id,
                Some(payload.owner),
                "the envelope tenant is the payload owner"
            );
            assert_eq!(
                envelope.aggregate_id, payload.request_ref,
                "the aggregate is the deletion request"
            );
            Command {
                owner: payload.owner.to_string(),
                producer: envelope.producer.as_str().to_owned(),
                request_ref: payload.request_ref.to_string(),
                correlation: envelope.correlation_id.to_string(),
                sequences: payload.stream_sequences,
                id,
            }
        })
        .collect()
}

/// Every sequence a set of commands names, per owner.
fn named(commands: &[Command]) -> BTreeMap<String, BTreeSet<u64>> {
    commands
        .iter()
        .fold(BTreeMap::new(), |mut grouped, command| {
            grouped
                .entry(command.owner.clone())
                .or_default()
                .extend(command.sequences.iter().copied());
            grouped
        })
}

fn sequence_of(
    facts: &BTreeMap<Uuid, Fact>,
    event_type: &str,
    aggregate_kind: &str,
) -> Option<u64> {
    facts
        .values()
        .find(|fact| fact.event_type == event_type && fact.aggregate.starts_with(aggregate_kind))
        .and_then(|fact| fact.sequences.first().copied())
}

/// What every command must satisfy whatever removed the rows.
fn assert_well_formed(commands: &[Command]) {
    for command in commands {
        assert_eq!(command.producer, "ratatoskr-claude");
        assert!(
            command.request_ref.starts_with("privacy_request:"),
            "{}",
            command.request_ref
        );
        assert!(
            !command.sequences.is_empty() && command.sequences.len() <= 500,
            "one to 500 sequences per command"
        );
        assert!(
            command
                .sequences
                .windows(2)
                .all(|pair| matches!(pair, [earlier, later] if earlier < later)),
            "ascending: {:?}",
            command.sequences
        );
    }
}

#[tokio::test]
async fn the_row_removal_asks_for_the_erasure_of_every_published_fact_it_deletes() {
    let imported = import_three("erasure-tenant").await;
    let pool = imported.series.pool();
    let published = publish_all(pool).await;
    let owner = format!("user:{}", cumulative::USER);
    // A tombstone of an earlier request that Knowledge acknowledged is removed with the tenant,
    // and its stream copy is not Edge's to erase.
    sqlx::query(
        "insert into claude_archive.outbox_events
          (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
           correlation_id, tenant_ref, occurred_at, published_at, published_stream_seqs,
           acknowledged_at, ack_outcome)
         values ($1, 'ai_archive.subject.tombstoned.v1', 'tombstone', 'ai_archive:x', '{}',
                 decode(repeat('7a', 32), 'hex'), 'c', $2, now(), now(), array[90000], now(), 'applied')",
    )
    .bind(Uuid::now_v7())
    .bind(&owner)
    .execute(pool)
    .await
    .expect("the acknowledged tombstone seeds");
    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    let left = facts(pool).await;
    let issued = commands(pool).await;
    let tombstone_rows: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'ai_archive.subject.tombstoned.v1' and published_stream_seqs = array[90000]::bigint[]",
    )
    .fetch_one(pool)
    .await
    .expect("the tombstone count is readable");
    imported.finish().await;

    assert!(
        left.is_empty(),
        "the tenant deletion removed every fact row"
    );
    assert_well_formed(&issued);
    let expected: BTreeSet<u64> = published
        .values()
        .flat_map(|fact| fact.sequences.iter().copied())
        .collect();
    assert_eq!(
        named(&issued).get(&owner),
        Some(&expected),
        "the commands name exactly the published facts that left, for their owner"
    );
    assert_eq!(named(&issued).len(), 1, "and for no other owner");
    assert_eq!(
        tombstone_rows, 0,
        "the acknowledged tombstone was removed too"
    );
    assert!(
        issued
            .iter()
            .all(|command| !command.sequences.contains(&90_000)),
        "and its sequence is not listed"
    );
    assert!(
        issued
            .iter()
            .all(|command| command.request_ref == format!("privacy_request:{request}")),
        "every command names the request: {issued:?}"
    );
    assert!(
        issued
            .iter()
            .all(|command| command.correlation == format!("privacy-delete:{request}")),
        "and carries its correlation id"
    );
}

async fn plan_tenant(imported: &Cumulative) -> Uuid {
    let request = Uuid::now_v7();
    PrivacyDeletionPlanner::new(imported.series.pool().clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: imported.series.account.to_string(),
            account_id: imported.series.account,
            request_id: request,
            request_key: format!("erasure-{request}"),
            correlation_id: format!("privacy-delete:{request}"),
        })
        .await
        .expect("the tenant deletion is planned");
    request
}

#[tokio::test]
async fn the_facts_of_a_deleted_archive_under_a_kept_subject_are_erased_too() {
    // Deleting the first export tombstones its archive, and the garden project, the planting
    // conversation and the Artifact were announced only under it. Each is announced again under a
    // retained archive, and the fact under the deleted one leaves the outbox.
    let imported = import_three("erasure-purge").await;
    let pool = imported.series.pool();
    let published = publish_all(pool).await;
    imported.delete_export(imported.exports[0]).await;
    let left = facts(pool).await;
    let issued = commands(pool).await;
    imported.finish().await;

    assert_well_formed(&issued);
    let gone: BTreeSet<u64> = published
        .iter()
        .filter(|(id, _)| !left.contains_key(id))
        .flat_map(|(_, fact)| fact.sequences.iter().copied())
        .collect();
    let owner = format!("user:{}", cumulative::USER);
    assert_eq!(
        named(&issued).get(&owner),
        Some(&gone),
        "the commands name exactly the published facts that left"
    );
    let garden_project = sequence_of(&published, "ai_archive.project.added.v1", "ai_project:")
        .expect("the garden project was announced and published");
    assert!(
        issued
            .iter()
            .any(|command| command.sequences.contains(&garden_project)),
        "an earlier project fact that carried the deleted export's provenance is erased"
    );
}

#[tokio::test]
async fn the_earlier_facts_of_a_rebuilt_conversation_are_erased() {
    // The newest export retitles the conversation and does not carry the part the second export
    // appended. Deleting the second export rebuilds the conversation, so every earlier fact of it,
    // the one under the first export included, carried or may carry text that is gone.
    let mut third = export(1);
    third["conversations"][0]["title"] = serde_json::json!("A new title");
    let imported = import_series("erasure-stale", [export(1), export(2), third]).await;
    let pool = imported.series.pool();
    let published = publish_all(pool).await;
    let first_fact = sequence_of(
        &published,
        "ai_archive.conversation.added.v1",
        "ai_conversation:",
    )
    .expect("the conversation was announced and published");
    imported.delete_export(imported.exports[1]).await;
    let issued = commands(pool).await;
    imported.finish().await;

    assert_well_formed(&issued);
    assert!(
        issued
            .iter()
            .any(|command| command.sequences.contains(&first_fact)),
        "the first export's conversation fact is erased: {issued:?}"
    );
}

#[tokio::test]
async fn a_fact_replaced_by_an_identical_one_is_erased() {
    // Deleting the newest export restores the garden project to the version the second export
    // announced, so the survivor fact is exactly the second export's fact. The stored row is
    // replaced by a new event, and the message it had published is erased.
    let imported = import_three("erasure-replace").await;
    let pool = imported.series.pool();
    let published = publish_all(pool).await;
    let second_exports_project = published
        .iter()
        .filter(|(_, fact)| fact.event_type == "ai_archive.project.updated.v1")
        .filter(|(_, fact)| fact.aggregate.starts_with("ai_project:"))
        .min_by_key(|(_, fact)| fact.sequences.first().copied())
        .map(|(id, fact)| (*id, fact.sequences.first().copied()))
        .expect("an updated project fact exists");
    imported.delete_export(imported.exports[2]).await;
    let left = facts(pool).await;
    let issued = commands(pool).await;
    imported.finish().await;

    assert_well_formed(&issued);
    let replaced = second_exports_project.1.expect("it was published");
    assert!(
        !left.contains_key(&second_exports_project.0),
        "the stored fact was replaced by a new event"
    );
    assert!(
        issued
            .iter()
            .any(|command| command.sequences.contains(&replaced)),
        "and the message it published is erased: {issued:?}"
    );
}

#[tokio::test]
async fn an_unpublished_fact_needs_no_command() {
    let imported = import_three("erasure-unpublished").await;
    let pool = imported.series.pool();
    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    let issued = commands(pool).await;
    imported.finish().await;

    assert!(
        issued.is_empty(),
        "nothing was published, so nothing is erased: {issued:?}"
    );
}

#[tokio::test]
async fn each_owner_gets_its_own_command_and_a_command_names_at_most_500_sequences() {
    let imported = import_three("erasure-owners").await;
    let pool = imported.series.pool();
    // A second owner announced the account's later archive after the mapping changed.
    let later = cumulative::worker_over(
        &imported.series.db,
        &imported.series.store,
        PlatformUsers::new(&[(
            Uuid::parse_str(OTHER_USER).expect("user UUID"),
            imported.series.account,
        )]),
    );
    // Another archive, so it is not a duplicate of the third export.
    let mut again = export(3);
    again
        .as_object_mut()
        .expect("the export is an object")
        .insert(
            "exported_by".to_owned(),
            serde_json::json!("the later user"),
        );
    imported.series.receive(&again).await;
    assert_eq!(later.process_pending_once().await.expect("imports"), 1);
    sqlx::query(
        "insert into claude_archive.outbox_events
          (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
           correlation_id, tenant_ref, occurred_at)
         select gen_random_uuid(), 'ai_archive.conversation.added.v1', 'conversation',
                'ai_conversation:' || gen_random_uuid(), '{}', sha256(n::text::bytea), 'c',
                $1, now() + n * interval '1 millisecond'
         from generate_series(1, 1100) n",
    )
    .bind(format!("user:{}", cumulative::USER))
    .execute(pool)
    .await
    .expect("the bulk facts seed");
    let published = publish_all(pool).await;
    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    let issued = commands(pool).await;
    imported.finish().await;

    assert_well_formed(&issued);
    let by_owner = named(&issued);
    let expected = |owner: &str| -> BTreeSet<u64> {
        published
            .values()
            .filter(|fact| fact.owner.as_deref() == Some(owner))
            .flat_map(|fact| fact.sequences.iter().copied())
            .collect()
    };
    let first = format!("user:{}", cumulative::USER);
    let second = format!("user:{OTHER_USER}");
    assert!(
        !expected(&first).is_empty() && !expected(&second).is_empty(),
        "both owners published facts"
    );
    assert_eq!(by_owner.len(), 2, "one set of commands per owner");
    assert!(
        by_owner.get(&first) == Some(&expected(&first))
            && by_owner.get(&second) == Some(&expected(&second)),
        "each owner's commands name exactly that owner's published facts, and no other owner's"
    );
    let first_commands: Vec<&Command> = issued
        .iter()
        .filter(|command| command.owner == first)
        .collect();
    assert!(
        first_commands.len() >= 3,
        "more than 1100 sequences need at least three commands of at most 500: {}",
        first_commands.len()
    );
}

#[tokio::test]
async fn the_deletion_does_not_wait_for_edge() {
    // The erasure is eventual: the request completes with the commands still unpublished.
    let imported = import_three("erasure-eventual").await;
    let pool = imported.series.pool();
    publish_all(pool).await;
    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    let state: String = sqlx::query_scalar(
        "select state from claude_archive.privacy_deletion_requests where request_id = $1",
    )
    .bind(request)
    .fetch_one(pool)
    .await
    .expect("the request is readable");
    let unpublished: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = $1 and published_at is null",
    )
    .bind(COMMAND)
    .fetch_one(pool)
    .await
    .expect("the commands are countable");
    imported.finish().await;

    assert_eq!(state, "completed");
    assert!(unpublished > 0, "the commands wait for the pump");
}

#[tokio::test]
async fn a_later_deletion_keeps_an_erasure_request_nobody_published_yet() {
    // XR-021 CONTRACTS.md R5-12 rule 3: the erasure is eventual. A request the pump has not
    // published (the broker is down, or the pump has not run) is the only record of the sequences
    // it names, so a later tenant-scope deletion must neither remove it nor lose its sequences.
    let imported = import_three("erasure-pending").await;
    let pool = imported.series.pool();
    let published = publish_all(pool).await;
    imported.delete_export(imported.exports[0]).await;
    let first = commands(pool).await;
    assert!(
        !first.is_empty() && first.iter().all(|command| !command.sequences.is_empty()),
        "the first deletion asked for the erasure of the facts it removed: {first:?}"
    );
    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    let after = commands(pool).await;
    imported.finish().await;

    assert_well_formed(&after);
    let owner = format!("user:{}", cumulative::USER);
    let every_published: BTreeSet<u64> = published
        .values()
        .flat_map(|fact| fact.sequences.iter().copied())
        .collect();
    assert_eq!(
        named(&after).get(&owner),
        Some(&every_published),
        "every published fact is still requested after the later deletion: {after:?}"
    );
    for command in &first {
        assert!(
            after.iter().any(|kept| kept.id == command.id),
            "the unpublished request {} survived the later deletion",
            command.id
        );
    }
}

/// The content-bearing fact subjects Edge may erase from (XR-021 CONTRACTS.md R5-12 rule 4).
const ERASABLE_SUBJECTS: [&str; 7] = [
    "evt.ai_archive.archive.imported.v1",
    "evt.ai_archive.conversation.added.v1",
    "evt.ai_archive.conversation.updated.v1",
    "evt.ai_archive.project.added.v1",
    "evt.ai_archive.project.updated.v1",
    "evt.ai_archive.artifact.added.v1",
    "evt.ai_archive.artifact.updated.v1",
];

/// A stand-in for the Edge handler: it reads each named message, erases it only when its subject,
/// producer and tenant are the command's, and reports how many it erased.
async fn edge_erases(
    commands: &async_nats::jetstream::stream::Stream,
    events: &async_nats::jetstream::stream::Stream,
    ids: &BTreeSet<Uuid>,
) -> usize {
    let consumer = commands
        .create_consumer(pull::Config {
            filter_subject: "cmd.platform.stream_erasure.requested.v1".to_owned(),
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
    let mut erased = 0;
    while let Some(message) = batch.next().await {
        let message = message.expect("the delivery is readable");
        let named = message
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Msg-Id"))
            .and_then(|id| Uuid::parse_str(id.as_ref()).ok());
        if !named.is_some_and(|id| ids.contains(&id)) {
            continue;
        }
        let command = CommandEnvelope::from_json(&message.payload).expect("a command envelope");
        let request = command
            .payload_as::<PlatformStreamErasureRequested>()
            .expect("a valid erasure request");
        for sequence in request.stream_sequences {
            let Ok(stored) = events.get_raw_message(sequence).await else {
                continue;
            };
            let fact = EventEnvelope::from_json(&stored.payload).expect("a fact envelope");
            if ERASABLE_SUBJECTS.contains(&stored.subject.as_str())
                && fact.producer == command.producer
                && fact.tenant_id == Some(request.owner)
            {
                events
                    .delete_message(sequence)
                    .await
                    .expect("the message is erased");
                erased += 1;
            }
        }
    }
    erased
}

/// The two streams of the deployment on the test broker.
struct Streams {
    jetstream: async_nats::jetstream::Context,
    events: async_nats::jetstream::stream::Stream,
    commands: async_nats::jetstream::stream::Stream,
}

async fn streams() -> Streams {
    let client = async_nats::connect(nats_url())
        .await
        .expect("the test broker accepts a connection");
    let jetstream = async_nats::jetstream::new(client);
    let events = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_events".to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the events stream exists");
    let commands = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_commands".to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..Default::default()
        })
        .await
        .expect("the commands stream exists");
    Streams {
        jetstream,
        events,
        commands,
    }
}

/// Publishes every pending row, as the pump of the service does.
async fn pump_all(pool: &PgPool, jetstream: &async_nats::jetstream::Context) {
    let pump = OutboxPublisher::new(pool.clone());
    while pump
        .publish_pending_with(jetstream)
        .await
        .expect("the pending rows are published")
        > 0
    {}
}

/// The stream sequences of the published facts of `owner`.
fn sequences_of(published: &BTreeMap<Uuid, Fact>, owner: &str) -> Vec<u64> {
    published
        .values()
        .filter(|fact| fact.owner.as_deref() == Some(owner))
        .flat_map(|fact| fact.sequences.iter().copied())
        .collect()
}

#[tokio::test]
async fn a_consumer_standing_in_for_edge_erases_the_named_messages_and_no_others() {
    let streams = streams().await;
    // Two owners, each with its own account, announced through the real import path.
    let imported = import_three("erasure-broker").await;
    let pool = imported.series.pool();
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
    pump_all(pool, &streams.jetstream).await;
    let published = facts(pool).await;
    let erase = sequences_of(&published, &format!("user:{}", cumulative::USER));
    let keep = sequences_of(&published, &format!("user:{OTHER_USER}"));

    let request = plan_tenant(&imported).await;
    imported.execute(request).await;
    pump_all(pool, &streams.jetstream).await;
    let ids: BTreeSet<Uuid> = commands(pool)
        .await
        .iter()
        .map(|command| command.id)
        .collect();
    let published_commands: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = $1 and published_at is not null",
    )
    .bind(COMMAND)
    .fetch_one(pool)
    .await
    .expect("the commands are countable");
    let erased = edge_erases(&streams.commands, &streams.events, &ids).await;
    let mut gone = Vec::new();
    for sequence in &erase {
        gone.push(streams.events.get_raw_message(*sequence).await.is_err());
    }
    let mut kept = Vec::new();
    for sequence in &keep {
        kept.push(streams.events.get_raw_message(*sequence).await.is_ok());
    }
    imported.finish().await;

    assert!(
        !erase.is_empty() && !keep.is_empty(),
        "both owners published facts"
    );
    assert!(!ids.is_empty(), "the deletion enqueued commands");
    assert_eq!(
        usize::try_from(published_commands).expect("a count fits"),
        ids.len(),
        "the pump published every command to cmd.platform.stream_erasure.requested.v1"
    );
    assert_eq!(
        erased,
        erase.len(),
        "Edge erased exactly the named messages"
    );
    assert!(
        gone.iter().all(|gone| *gone),
        "the deleted owner's fact messages are gone"
    );
    assert!(
        kept.iter().all(|kept| *kept),
        "the other owner's messages are untouched"
    );
}
