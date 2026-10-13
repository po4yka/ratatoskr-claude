//! Durable publication of archive events: operation reports and `ai_archive.*` facts.
//!
//! One pump serves every event type the outbox may hold. It publishes the stored envelope
//! unchanged with `Nats-Msg-Id` set to the row's event id, and marks a row published only after
//! the broker acknowledged it. A refused row backs off and never starves the rows behind it; an
//! event type outside the closed list is a hard error (XR-021 CONTRACTS.md S02 and S07).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::Row as _;
use uuid::Uuid;

mod publish;

/// The only command type this service publishes: the request that Edge erase messages it published
/// from the events stream (XR-021 CONTRACTS.md R5-12). Its outbox row stores a command envelope.
pub(crate) const STREAM_ERASURE_TYPE: &str = "platform.stream_erasure.requested.v1";
/// The Platform-owned subject for the per-provider operation report ingress.
const OPERATION_REPORTED_SUBJECT: &str = "evt.ai-archive.claude.operation.reported.v1";
/// The inbox prefix of every connection this service opens. The deployed ACL lets the identity
/// subscribe to `_INBOX_claude.>` and to nothing else (XR-021 CONTRACTS.md R3-01), so a reply on
/// the shared `_INBOX.>` namespace would never reach it.
const INBOX_PREFIX: &str = "_INBOX_claude";
/// One pass never monopolises the broker or database queue.
const BATCH_SIZE: i64 = 32;
/// A background retry must not wait forever for an unavailable broker.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// How long [`OutboxPublisher::close`] waits for the client to report that it is closed.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Room the broker needs beside the body for the subject and headers of one message.
const HEADER_HEADROOM_BYTES: usize = 1024;
/// The safe error class of a row whose body no publish can ever fit.
const PAYLOAD_TOO_LARGE: &str = "payload_too_large";
/// Counts rows marked undeliverable, labelled by their safe class.
const UNDELIVERABLE_METRIC: &str = "claude_archive_outbox_undeliverable_total";
/// Counts acknowledged publishes whose mark did not affect exactly one row (XR-021 CONTRACTS.md
/// R6-03 rule 2).
const MARK_LOST_METRIC: &str = "claude_archive_outbox_mark_lost_total";

/// The subject an outbox event type is published on, or `None` outside the closed list.
///
/// The schema `CHECK` on `outbox_events.event_type` lists exactly these types, so a row the pump
/// cannot place is impossible to insert.
pub(crate) fn subject_for_event_type(event_type: &str) -> Option<&'static str> {
    Some(match event_type {
        "platform.operation.reported.v1" => OPERATION_REPORTED_SUBJECT,
        "ai_archive.archive.imported.v1" => "evt.ai_archive.archive.imported.v1",
        "ai_archive.conversation.added.v1" => "evt.ai_archive.conversation.added.v1",
        "ai_archive.conversation.updated.v1" => "evt.ai_archive.conversation.updated.v1",
        "ai_archive.project.added.v1" => "evt.ai_archive.project.added.v1",
        "ai_archive.project.updated.v1" => "evt.ai_archive.project.updated.v1",
        "ai_archive.artifact.added.v1" => "evt.ai_archive.artifact.added.v1",
        "ai_archive.artifact.updated.v1" => "evt.ai_archive.artifact.updated.v1",
        "ai_archive.subject.tombstoned.v1" => "evt.ai_archive.subject.tombstoned.v1",
        // A command publishes to `cmd.` plus its type (XR-021 CONTRACTS.md S02 rule 2).
        STREAM_ERASURE_TYPE => "cmd.platform.stream_erasure.requested.v1",
        _ => return None,
    })
}

/// The options of the one broker connection of this service: nkey authentication, the service's
/// own inbox prefix, and a callback that raises `closed` when the client has shut down for good.
///
/// async-nats repairs a lost connection by itself and never gives up reconnecting with the
/// default `max_reconnects`, so `Event::Closed` (the client was drained or every handle dropped)
/// is the one state it cannot recover from.
fn connect_options(seed: String, closed: Arc<AtomicBool>) -> async_nats::ConnectOptions {
    async_nats::ConnectOptions::with_nkey(seed)
        .custom_inbox_prefix(INBOX_PREFIX)
        .event_callback(move |event| {
            let closed = Arc::clone(&closed);
            async move {
                if matches!(event, async_nats::Event::Closed) {
                    closed.store(true, Ordering::Release);
                }
            }
        })
}

/// The published tombstones nobody acknowledged whose resend is due (XR-021 CONTRACTS.md R5-11).
macro_rules! resend_predicate {
    () => {
        "event_type = 'ai_archive.subject.tombstoned.v1' and published_at is not null
         and acknowledged_at is null and undeliverable_at is null and next_resend_at <= now()
         and (next_attempt_at is null or next_attempt_at <= now())"
    };
}

/// Opens the one authenticated connection a consumer of this service uses: nkey authentication
/// from the seed file and the service's own inbox prefix, so the deployed ACL lets its replies
/// through.
///
/// # Errors
///
/// [`OutboxError::Broker`] when the seed cannot be read or the broker refuses the connection, and
/// [`OutboxError::BrokerTimeout`] when it does not answer in time.
pub async fn connect_event_bus(
    endpoint: &str,
    nkey_seed_path: &std::path::Path,
) -> Result<async_nats::Client, OutboxError> {
    let seed = tokio::fs::read_to_string(nkey_seed_path)
        .await
        .map_err(OutboxError::broker)?;
    let options = connect_options(seed.trim().to_owned(), Arc::new(AtomicBool::new(false)));
    tokio::time::timeout(CONNECT_TIMEOUT, options.connect(endpoint))
        .await
        .map_err(|_| OutboxError::BrokerTimeout)?
        .map_err(OutboxError::broker)
}

/// The unpublished rows or the due resends exist, by the same predicates as [`PENDING_SQL`] and
/// [`RESEND_SQL`].
const HAS_PENDING_SQL: &str = concat!(
    "select exists (select 1 from claude_archive.outbox_events
     where published_at is null and undeliverable_at is null
       and (next_attempt_at is null or next_attempt_at <= now()))
     or exists (select 1 from claude_archive.outbox_events where ",
    resend_predicate!(),
    ")"
);

/// The broker connection a publisher keeps for its lifetime.
#[derive(Debug)]
struct HeldConnection {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
    closed: Arc<AtomicBool>,
}

/// The unpublished rows a pass would offer next, oldest first.
const PENDING_SQL: &str = "select event_id, event_type, envelope from claude_archive.outbox_events
     where published_at is null and undeliverable_at is null
       and (next_attempt_at is null or next_attempt_at <= now())
     order by occurred_at, event_id limit $1";

/// The tombstones whose resend is due, oldest due first.
const RESEND_SQL: &str = concat!(
    "select event_id, event_type, envelope from claude_archive.outbox_events where ",
    resend_predicate!(),
    " order by next_resend_at, event_id limit $1"
);

/// The local outbox publisher for every archive event.
///
/// Clones share one lazily created broker connection (XR-021 CONTRACTS.md R3-07 rule 1). A
/// connection that failed a pass is dropped, so the next pass reads the nkey seed file again and
/// a rotated seed takes effect without a restart.
#[derive(Debug, Clone)]
pub struct OutboxPublisher {
    pool: sqlx::PgPool,
    resend: crate::deletion_ack::ResendPolicy,
    connection: Arc<tokio::sync::Mutex<Option<HeldConnection>>>,
    failing: Arc<AtomicBool>,
}

impl OutboxPublisher {
    /// Uses the archive service's established database pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            resend: crate::deletion_ack::ResendPolicy::default(),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
            failing: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Sets how long a published tombstone waits for Knowledge's acknowledgement before the pump
    /// publishes it again, and the longest wait between two resends (XR-021 CONTRACTS.md R5-11).
    #[must_use]
    pub const fn with_tombstone_resend(mut self, after: Duration, cap: Duration) -> Self {
        self.resend = crate::deletion_ack::ResendPolicy { after, cap };
        self
    }

    /// Exports the tombstone backlog as the two gauges of the operator listener (XR-021
    /// CONTRACTS.md R5-11). It reads the table, never the broker.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the outbox cannot be read.
    pub async fn record_backlog(&self) -> Result<crate::deletion_ack::Backlog, OutboxError> {
        crate::deletion_ack::record_backlog(&self.pool)
            .await
            .map_err(OutboxError::Database)
    }

    /// Whether the last pass that needed the broker succeeded.
    ///
    /// A publisher that has not needed the broker yet is healthy: a pass over an empty outbox
    /// makes no network call and so learns nothing, and it neither sets nor clears a failure.
    /// The failure stays until a later pass publishes through the broker, including every pass
    /// that falls inside the retry delay of a refused row.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.failing.load(Ordering::Acquire)
    }

    /// Drains the held broker connection, if any, and waits until the client reports itself
    /// closed. The next pass then replaces it with a new connection.
    pub async fn close(&self) {
        let held = self.connection.lock().await;
        let Some(open) = held.as_ref() else {
            return;
        };
        if open.client.drain().await.is_err() {
            return;
        }
        let closed = async {
            while !open.closed.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(CLOSE_TIMEOUT, closed).await.is_err() {
            tracing::warn!("the archive event broker connection did not report closed in time");
        }
    }

    /// The unpublished events a pass would publish next, in publication order.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Database`] when the queue cannot be read.
    pub async fn pending_event_ids(&self) -> Result<Vec<Uuid>, OutboxError> {
        let rows = sqlx::query(PENDING_SQL)
            .bind(BATCH_SIZE)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
        rows.iter()
            .map(|row| row.try_get("event_id").map_err(OutboxError::Database))
            .collect()
    }

    /// Makes one bounded attempt to publish pending events.
    ///
    /// A pass with nothing to publish returns `Ok(0)` before it touches the network. Otherwise
    /// it publishes through the publisher's one connection, opened on first need with the
    /// `endpoint` and the nkey seed read from `nkey_seed_path`. It is opened again when the
    /// client reports it closed and after any pass that failed, which reads the seed file again.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError`] when the broker cannot connect, when a publish is not
    /// acknowledged, when the durable queue cannot be read or marked, or when a stored event is
    /// outside the closed publication list.
    pub async fn publish_pending_once(
        &self,
        endpoint: &str,
        nkey_seed_path: &std::path::Path,
    ) -> Result<usize, OutboxError> {
        let pending: bool = sqlx::query_scalar(HAS_PENDING_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
        if !pending {
            return Ok(0);
        }
        let result = async {
            let jetstream = self.jetstream(endpoint, nkey_seed_path).await?;
            self.publish_pending_with(&jetstream).await
        }
        .await;
        match &result {
            Ok(_) => self.failing.store(false, Ordering::Release),
            Err(OutboxError::Broker(_) | OutboxError::BrokerTimeout) => {
                self.failing.store(true, Ordering::Release);
                *self.connection.lock().await = None;
            }
            // The broker holds a copy that no row records: the pass failed even though the broker
            // answered, and the failure stays until a later pass publishes cleanly.
            Err(OutboxError::MarkLost) => self.failing.store(true, Ordering::Release),
            Err(_) => {}
        }
        result
    }

    /// The `JetStream` context of the held connection, which is created or replaced when absent
    /// or closed.
    async fn jetstream(
        &self,
        endpoint: &str,
        nkey_seed_path: &std::path::Path,
    ) -> Result<async_nats::jetstream::Context, OutboxError> {
        let mut held = self.connection.lock().await;
        if let Some(open) = held
            .as_ref()
            .filter(|open| !open.closed.load(Ordering::Acquire))
        {
            return Ok(open.jetstream.clone());
        }
        let seed = tokio::fs::read_to_string(nkey_seed_path)
            .await
            .map_err(OutboxError::broker)?;
        let closed = Arc::new(AtomicBool::new(false));
        let options = connect_options(seed.trim().to_owned(), Arc::clone(&closed));
        let client = tokio::time::timeout(CONNECT_TIMEOUT, options.connect(endpoint))
            .await
            .map_err(|_| OutboxError::BrokerTimeout)?
            .map_err(OutboxError::broker)?;
        let jetstream = async_nats::jetstream::new(client.clone());
        *held = Some(HeldConnection {
            client,
            jetstream: jetstream.clone(),
            closed,
        });
        Ok(jetstream)
    }
}

/// Failure while delivering a pending archive event.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// The broker could not be reached or did not acknowledge a message.
    #[error("the archive event broker was unavailable")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The broker did not answer before the finite connection bound.
    #[error("the archive event broker connection timed out")]
    BrokerTimeout,
    /// A stored event type is outside the closed publication list.
    #[error("the outbox holds an event type outside the closed publication list")]
    UnknownEventType,
    /// A stored document is not the envelope of the event it is filed under.
    #[error("the outbox holds a document that is not the envelope of its declared event")]
    EnvelopeMismatch,
    /// The durable queue could not be read or acknowledged as published.
    #[error("the archive event outbox database operation failed")]
    Database(#[source] sqlx::Error),
    /// A durable payload could not be encoded for the bus.
    #[error("the archive event payload could not be encoded")]
    Encode(#[source] serde_json::Error),
    /// The mark of an acknowledged row did not affect exactly one row.
    #[error("the outbox row of an acknowledged publish was not marked")]
    MarkLost,
}

impl OutboxError {
    fn broker(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Broker(Box::new(error))
    }

    fn broker_refused() -> Self {
        Self::Broker(
            "a publish was not acknowledged; check the NATS server log for a Publish Violation"
                .into(),
        )
    }

    /// Whether retrying can never help: the row itself is wrong, so the worker must stop and the
    /// service must exit instead of reporting ready while it cannot publish.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::UnknownEventType | Self::EnvelopeMismatch | Self::Encode(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::subject_for_event_type;

    const FACT_TYPES: [&str; 8] = [
        "ai_archive.archive.imported.v1",
        "ai_archive.conversation.added.v1",
        "ai_archive.conversation.updated.v1",
        "ai_archive.project.added.v1",
        "ai_archive.project.updated.v1",
        "ai_archive.artifact.added.v1",
        "ai_archive.artifact.updated.v1",
        "ai_archive.subject.tombstoned.v1",
    ];

    #[test]
    fn the_operation_report_keeps_its_per_provider_ingress_subject() {
        assert_eq!(
            subject_for_event_type("platform.operation.reported.v1"),
            Some("evt.ai-archive.claude.operation.reported.v1"),
            "Platform consumes the operation report on the legacy per-provider subject"
        );
    }

    #[test]
    fn each_fact_type_publishes_to_evt_plus_its_type_name() {
        for event_type in FACT_TYPES {
            assert_eq!(
                subject_for_event_type(event_type).map(str::to_owned),
                Some(format!("evt.{event_type}")),
                "{event_type} publishes on the class prefix plus the contract type name"
            );
        }
    }

    #[test]
    fn the_stream_erasure_command_publishes_to_cmd_plus_its_type_name() {
        assert_eq!(
            subject_for_event_type("platform.stream_erasure.requested.v1"),
            Some("cmd.platform.stream_erasure.requested.v1"),
            "S02 rule 2: a command publishes to `cmd.` plus its command type"
        );
    }

    #[test]
    fn an_event_type_outside_the_closed_list_has_no_subject() {
        for event_type in [
            "claude.subject.reparsed.v1",
            "ai_archive.conversation.deleted.v1",
            "evt.ai_archive.conversation.added.v1",
            "",
        ] {
            assert_eq!(
                subject_for_event_type(event_type),
                None,
                "{event_type:?} is not publishable"
            );
        }
    }
}
