//! Durable publication of Platform operation reports.

use std::time::Duration;

use ratatoskr_event_envelope::EventEnvelope;
use sqlx::Row as _;

/// The Platform-owned event topic for producer progress facts.
const OPERATION_REPORTED_SUBJECT: &str = "evt.platform.operation.reported.v1";
/// One pass never monopolises the broker or database queue.
const BATCH_SIZE: i64 = 32;
/// A background retry must not wait forever for an unavailable broker.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The local outbox publisher for terminal Platform operation reports.
#[derive(Debug, Clone)]
pub struct OperationReportOutbox {
    pool: sqlx::PgPool,
}

impl OperationReportOutbox {
    /// Uses the archive service's established database pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Makes one bounded attempt to publish pending terminal reports.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError`] when the broker cannot connect or acknowledge
    /// a report, or when the durable queue cannot be read or marked.
    pub async fn publish_pending_once(&self, endpoint: &str) -> Result<usize, OutboxError> {
        let client = tokio::time::timeout(CONNECT_TIMEOUT, async_nats::connect(endpoint))
            .await
            .map_err(|_| OutboxError::BrokerTimeout)?
            .map_err(OutboxError::broker)?;
        let jetstream = async_nats::jetstream::new(client);
        let rows = sqlx::query(
            "select event_id, envelope from claude_archive.outbox_events
             where event_type = 'platform.operation.reported.v1' and published_at is null
             order by occurred_at, event_id limit $1",
        )
        .bind(BATCH_SIZE)
        .fetch_all(&self.pool)
        .await
        .map_err(OutboxError::Database)?;

        let mut published = 0;
        for row in rows {
            let event_id: uuid::Uuid = row.try_get("event_id").map_err(OutboxError::Database)?;
            let envelope: EventEnvelope =
                serde_json::from_value(row.try_get("envelope").map_err(OutboxError::Database)?)
                    .map_err(OutboxError::Encode)?;
            let body = serde_json::to_vec(&envelope.payload).map_err(OutboxError::Encode)?;
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", event_id.to_string());
            let acknowledgement = jetstream
                .publish_with_headers(OPERATION_REPORTED_SUBJECT, headers, body.into())
                .await
                .map_err(OutboxError::broker)?;
            acknowledgement.await.map_err(OutboxError::broker)?;
            sqlx::query(
                "update claude_archive.outbox_events set published_at = now()
                 where event_id = $1 and published_at is null",
            )
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
            published += 1;
        }
        Ok(published)
    }
}

/// Failure while delivering a pending terminal report.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// The broker could not be reached or did not acknowledge a message.
    #[error("the operation report broker was unavailable")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The broker did not answer before the finite connection bound.
    #[error("the operation report broker connection timed out")]
    BrokerTimeout,
    /// The durable queue could not be read or acknowledged as published.
    #[error("the operation report outbox database operation failed")]
    Database(#[source] sqlx::Error),
    /// A durable payload could not be encoded for the bus.
    #[error("the operation report payload could not be encoded")]
    Encode(#[source] serde_json::Error),
}

impl OutboxError {
    fn broker(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Broker(Box::new(error))
    }
}
