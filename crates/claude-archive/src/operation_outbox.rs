//! Durable publication of archive events: operation reports and `ai_archive.*` facts.
//!
//! One pump serves every event type the outbox may hold. It publishes the stored envelope
//! unchanged with `Nats-Msg-Id` set to the row's event id, and marks a row published only after
//! the broker acknowledged it. A refused row backs off and never starves the rows behind it; an
//! event type outside the closed list is a hard error (XR-021 CONTRACTS.md S02 and S07).

use std::time::Duration;

use ratatoskr_event_envelope::EventEnvelope;
use sqlx::Row as _;
use uuid::Uuid;

/// The Platform-owned subject for the per-provider operation report ingress.
const OPERATION_REPORTED_SUBJECT: &str = "evt.ai-archive.claude.operation.reported.v1";
/// One pass never monopolises the broker or database queue.
const BATCH_SIZE: i64 = 32;
/// A background retry must not wait forever for an unavailable broker.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

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
        _ => return None,
    })
}

/// The unpublished rows a pass would offer next, oldest first.
const PENDING_SQL: &str = "select event_id, event_type, envelope from claude_archive.outbox_events
     where published_at is null and (next_attempt_at is null or next_attempt_at <= now())
     order by occurred_at, event_id limit $1";

/// The local outbox publisher for every archive event.
#[derive(Debug, Clone)]
pub struct OutboxPublisher {
    pool: sqlx::PgPool,
}

impl OutboxPublisher {
    /// Uses the archive service's established database pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
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
        let seed = tokio::fs::read_to_string(nkey_seed_path)
            .await
            .map_err(OutboxError::broker)?;
        let options = async_nats::ConnectOptions::with_nkey(seed.trim().to_owned());
        let client = tokio::time::timeout(CONNECT_TIMEOUT, options.connect(endpoint))
            .await
            .map_err(|_| OutboxError::BrokerTimeout)?
            .map_err(OutboxError::broker)?;
        self.publish_pending_with(&async_nats::jetstream::new(client))
            .await
    }

    /// Makes one bounded attempt to publish pending events through an open `JetStream` context.
    ///
    /// A refused row stays unpublished, counts the attempt, records a safe error class and backs
    /// off, and the rows behind it are still offered in the same pass.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::UnknownEventType`] or [`OutboxError::EnvelopeMismatch`] for a row
    /// the pump must never skip, [`OutboxError::Broker`] after a pass in which any publish was
    /// refused, and [`OutboxError::Database`] when the queue cannot be read or marked.
    pub async fn publish_pending_with(
        &self,
        jetstream: &async_nats::jetstream::Context,
    ) -> Result<usize, OutboxError> {
        let rows = sqlx::query(PENDING_SQL)
            .bind(BATCH_SIZE)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::Database)?;

        let mut published = 0;
        let mut refused = 0_usize;
        for row in rows {
            let event_id: Uuid = row.try_get("event_id").map_err(OutboxError::Database)?;
            let event_type: String = row.try_get("event_type").map_err(OutboxError::Database)?;
            let subject =
                subject_for_event_type(&event_type).ok_or(OutboxError::UnknownEventType)?;
            let envelope: EventEnvelope =
                serde_json::from_value(row.try_get("envelope").map_err(OutboxError::Database)?)
                    .map_err(OutboxError::Encode)?;
            if envelope.event_type.to_wire() != event_type || envelope.event_id.0 != event_id {
                return Err(OutboxError::EnvelopeMismatch);
            }
            let body = serde_json::to_vec(&envelope).map_err(OutboxError::Encode)?;
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", event_id.to_string());
            let acknowledged = match jetstream
                .publish_with_headers(subject, headers, body.into())
                .await
            {
                Ok(acknowledgement) => acknowledgement.await.map(|_| ()).map_err(|_| "ack_failed"),
                Err(_) => Err("publish_failed"),
            };
            match acknowledged {
                Ok(()) => {
                    self.mark_published(event_id).await?;
                    published += 1;
                }
                Err(class) => {
                    self.record_refusal(event_id, class).await?;
                    refused += 1;
                }
            }
        }
        if refused > 0 {
            return Err(OutboxError::broker_refused());
        }
        Ok(published)
    }

    async fn mark_published(&self, event_id: Uuid) -> Result<(), OutboxError> {
        sqlx::query(
            "update claude_archive.outbox_events set published_at = now(), last_error = null
             where event_id = $1 and published_at is null",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(OutboxError::Database)?;
        Ok(())
    }

    /// Counts the refused attempt and delays the row, doubling from two seconds up to five
    /// minutes, so a failing head row cannot starve the rows behind it.
    async fn record_refusal(&self, event_id: Uuid, class: &str) -> Result<(), OutboxError> {
        sqlx::query(
            "update claude_archive.outbox_events
             set attempt_count = attempt_count + 1, last_error = $2,
                 next_attempt_at = now()
                     + least(300, 2 * power(2, least(attempt_count, 7))) * interval '1 second'
             where event_id = $1 and published_at is null",
        )
        .bind(event_id)
        .bind(class)
        .execute(&self.pool)
        .await
        .map_err(OutboxError::Database)?;
        Ok(())
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
