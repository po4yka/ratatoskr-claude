//! What the stream-erasure tests share: the commands a deletion enqueued, the deletion itself,
//! and a stand-in for the Edge handler that executes them against a real `JetStream`.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_nats::jetstream::consumer::{DeliverPolicy, pull};
use futures_util::StreamExt as _;
use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionPlanner, TenantDeletionPlanRequest,
};
use ratatoskr_event_envelope::{CommandEnvelope, EventEnvelope};
use ratatoskr_operation_contracts::PlatformStreamErasureRequested;
use sqlx::PgPool;
use uuid::Uuid;

use crate::survivor_series::Cumulative;

pub(crate) const COMMAND: &str = "platform.stream_erasure.requested.v1";

/// One erasure command as the outbox holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Command {
    pub(crate) owner: String,
    pub(crate) producer: String,
    pub(crate) request_ref: String,
    pub(crate) correlation: String,
    pub(crate) sequences: Vec<u64>,
    pub(crate) id: Uuid,
}

pub(crate) async fn commands(pool: &PgPool) -> Vec<Command> {
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
pub(crate) fn named(commands: &[Command]) -> BTreeMap<String, BTreeSet<u64>> {
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

pub(crate) async fn plan_tenant(imported: &Cumulative) -> Uuid {
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
pub(crate) async fn edge_erases(
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
