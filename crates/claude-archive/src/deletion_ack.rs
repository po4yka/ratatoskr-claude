//! Re-sending tombstones until Knowledge acknowledges them (XR-021 CONTRACTS.md R5-11 rule 4).
//!
//! The events stream drops its oldest message at seven days or 1 GiB, and a tombstone is marked
//! published at the broker acknowledgement. Knowledge therefore answers every applied or refused
//! `ai_archive.subject.tombstoned.v1` with `knowledge.deletion.acknowledged.v1`, and the pump
//! publishes the stored tombstone again, unchanged, until that answer arrives.

use std::time::Duration;

use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_operation_contracts::{
    DeletionAckFamily, DeletionAckOutcome, KnowledgeDeletionAcknowledged,
};
use uuid::Uuid;

/// How long a tombstone waits for its acknowledgement, and the longest wait between two resends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResendPolicy {
    /// The wait after a publish, and the first wait between resends.
    pub after: Duration,
    /// The wait never grows beyond this.
    pub cap: Duration,
}

impl Default for ResendPolicy {
    fn default() -> Self {
        Self {
            after: Duration::from_mins(10),
            cap: Duration::from_hours(6),
        }
    }
}

/// What recording an acknowledgement did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acknowledgement {
    /// The row was unacknowledged and now records the outcome.
    Recorded,
    /// The row already recorded an acknowledgement, which is kept.
    AlreadyRecorded,
    /// No tombstone of this service has that event id and owner.
    Unknown,
}

/// Why a message is not an acknowledgement this service can attribute.
///
/// The consumer terminates such a message: it can never become valid, and it names no row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Rejection {
    /// The bytes are not an event envelope.
    #[error("the message is not an event envelope")]
    Undecodable,
    /// The envelope is another event type.
    #[error("the envelope is not a deletion acknowledgement")]
    WrongType,
    /// The envelope was not produced by Knowledge.
    #[error("the envelope was not produced by Knowledge")]
    WrongProducer,
    /// The envelope has no tenant, or one that is not the payload's owner.
    #[error("the envelope tenant is missing or is not the payload owner")]
    TenantMismatch,
    /// The payload is not a valid acknowledgement.
    #[error("the payload is not a valid deletion acknowledgement")]
    InvalidPayload,
}

/// The envelope type of an acknowledgement.
const EVENT_TYPE: &str = "knowledge.deletion.acknowledged.v1";
/// The only producer of acknowledgements.
const PRODUCER: &str = "ratatoskr-knowledge";

/// Decodes and validates one message of the acknowledgement subject.
///
/// # Errors
///
/// [`Rejection`] names the first rule the message breaks; no variant carries content.
pub fn decode(bytes: &[u8]) -> Result<KnowledgeDeletionAcknowledged, Rejection> {
    let envelope = EventEnvelope::from_json(bytes).map_err(|_| Rejection::Undecodable)?;
    if envelope.event_type.to_wire() != EVENT_TYPE {
        return Err(Rejection::WrongType);
    }
    if envelope.producer.as_str() != PRODUCER {
        return Err(Rejection::WrongProducer);
    }
    let acknowledgement = envelope
        .payload_as::<KnowledgeDeletionAcknowledged>()
        .map_err(|_| Rejection::InvalidPayload)?;
    if envelope.tenant_id != Some(acknowledgement.owner) {
        return Err(Rejection::TenantMismatch);
    }
    Ok(acknowledgement)
}

/// Counts refused acknowledgements: the data stays in Knowledge and an operator must look.
const REFUSED_METRIC: &str = "claude_archive_tombstone_refused_total";
/// The tombstones nobody acknowledged yet.
const UNACKNOWLEDGED_METRIC: &str = "claude_archive_unacknowledged_tombstones";
/// The age of the oldest tombstone nobody acknowledged.
const OLDEST_METRIC: &str = "claude_archive_oldest_unacknowledged_tombstone_age_seconds";

/// Records Knowledge's acknowledgement on the tombstone row it names.
///
/// The row is the tombstone whose event id and owner the acknowledgement names. Both outcomes end
/// the resend; a `refused` one is also counted and logged at ERROR, because the data stays in
/// Knowledge. A second acknowledgement keeps the first outcome, and one that names no row of this
/// service changes nothing.
///
/// # Errors
///
/// Returns the database error when the row cannot be read or updated.
pub async fn acknowledge(
    pool: &sqlx::PgPool,
    acknowledgement: &KnowledgeDeletionAcknowledged,
) -> Result<Acknowledgement, sqlx::Error> {
    if acknowledgement.family != DeletionAckFamily::AiArchive {
        return Ok(Acknowledgement::Unknown);
    }
    let outcome = match acknowledgement.outcome {
        DeletionAckOutcome::Applied => "applied",
        _ => "refused",
    };
    let event_id = acknowledgement.acknowledged_event_id.0;
    let owner = acknowledgement.owner.to_string();
    let recorded: Option<Uuid> = sqlx::query_scalar(
        "update claude_archive.outbox_events
         set acknowledged_at = now(), ack_outcome = $3
         where event_id = $1 and tenant_ref = $2
           and event_type = 'ai_archive.subject.tombstoned.v1' and acknowledged_at is null
         returning event_id",
    )
    .bind(event_id)
    .bind(&owner)
    .bind(outcome)
    .fetch_optional(pool)
    .await?;
    if recorded.is_some() {
        if outcome == "refused" {
            metrics::counter!(REFUSED_METRIC).increment(1);
            tracing::error!(
                class = "tombstone_refused",
                "Knowledge refused a tombstone; the data stays in Knowledge and needs an operator"
            );
        }
        return Ok(Acknowledgement::Recorded);
    }
    let known: bool = sqlx::query_scalar(
        "select exists (select 1 from claude_archive.outbox_events
         where event_id = $1 and tenant_ref = $2
           and event_type = 'ai_archive.subject.tombstoned.v1')",
    )
    .bind(event_id)
    .bind(owner)
    .fetch_one(pool)
    .await?;
    Ok(if known {
        Acknowledgement::AlreadyRecorded
    } else {
        Acknowledgement::Unknown
    })
}

/// The tombstones nobody acknowledged: how many, and how long the oldest has been waiting.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backlog {
    /// Rows with no acknowledgement, published or not.
    pub unacknowledged: i64,
    /// Seconds since the oldest of them was recorded; zero when there is none.
    pub oldest_age_seconds: f64,
}

/// Reads the backlog and exports it as the two gauges of the operator listener.
///
/// Readiness is not tied to it: a Knowledge outage must not make this service unready.
///
/// # Errors
///
/// Returns the database error when the outbox cannot be read.
pub async fn record_backlog(pool: &sqlx::PgPool) -> Result<Backlog, sqlx::Error> {
    let (unacknowledged, oldest_age_seconds): (i64, f64) = sqlx::query_as(
        "select count(*), coalesce(extract(epoch from now() - min(occurred_at)), 0)::float8
         from claude_archive.outbox_events
         where event_type = 'ai_archive.subject.tombstoned.v1' and acknowledged_at is null
           and undeliverable_at is null",
    )
    .fetch_one(pool)
    .await?;
    #[expect(
        clippy::cast_precision_loss,
        reason = "a gauge of a row count, far below the exact range of f64"
    )]
    metrics::gauge!(UNACKNOWLEDGED_METRIC).set(unacknowledged as f64);
    metrics::gauge!(OLDEST_METRIC).set(oldest_age_seconds.max(0.0));
    Ok(Backlog {
        unacknowledged,
        oldest_age_seconds: oldest_age_seconds.max(0.0),
    })
}
