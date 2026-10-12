//! Consumption of Knowledge's deletion acknowledgements (XR-021 CONTRACTS.md R5-11 rule 4).
//!
//! Knowledge answers every applied or refused `ai_archive.subject.tombstoned.v1` with
//! `knowledge.deletion.acknowledged.v1`, and the pump publishes the tombstone again until that
//! answer arrives. The durable `ratatoskr_claude_deletion_acks` is provisioned by Edge on the
//! events stream; this consumer only verifies it, and an acknowledgement that names no tombstone of
//! this service is acknowledged to the broker and ignored.

use std::future::Future;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::MessagesErrorKind;
use futures_util::StreamExt as _;
use ratatoskr_claude_archive::deletion_ack::{self, Acknowledgement};

/// The Edge-provisioned durable of this consumer (XR-021 CONTRACTS.md R5-11 rule 3).
pub const DURABLE: &str = "ratatoskr_claude_deletion_acks";

/// The stream the durable belongs to.
pub const STREAM: &str = "ratatoskr_events";

/// The only subject the durable is filtered to.
pub const FILTER_SUBJECT: &str = "evt.knowledge.deletion.acknowledged.v1";

/// The acknowledgement wait Edge provisions for the durable.
const ACK_WAIT: Duration = Duration::from_secs(30);

/// Totals from one consumer session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AcksSummary {
    /// Acknowledgements that ended the resend of one of this service's tombstones.
    pub recorded: u64,
    /// Acknowledgements for no tombstone of this service, or repeating one already recorded.
    pub ignored: u64,
    /// Messages that could never be valid and were terminated.
    pub terminated: u64,
    /// Messages left for redelivery after a database failure.
    pub retried: u64,
}

/// Why the consumer could not start or had to stop.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AcksError {
    /// The broker refused the durable lookup, or the durable does not match its spec.
    #[error("the deletion acknowledgement consumer cannot use JetStream: {0}")]
    Bus(String),
    /// The message stream ended without a shutdown request.
    #[error("the deletion acknowledgement consumer stopped without a shutdown request")]
    Stopped,
}

/// The attached durable.
pub type AckConsumer = jetstream::consumer::Consumer<jetstream::consumer::pull::Config>;

/// Opens the Edge-provisioned durable and verifies its filter, acknowledgement policy and wait.
///
/// # Errors
///
/// [`AcksError::Bus`] when the durable is absent or differs from its spec. This client never
/// creates a consumer: that could widen a compromised identity's filter.
pub async fn attach(context: &jetstream::Context) -> Result<AckConsumer, AcksError> {
    let consumer = context
        .get_consumer_from_stream(DURABLE, STREAM)
        .await
        .map_err(|error| AcksError::Bus(error.to_string()))?;
    let config = &consumer.cached_info().config;
    if config.durable_name.as_deref() != Some(DURABLE)
        || config.filter_subject != FILTER_SUBJECT
        || config.ack_policy != jetstream::consumer::AckPolicy::Explicit
        || config.ack_wait != ACK_WAIT
        || config.deliver_subject.is_some()
    {
        return Err(AcksError::Bus(
            "the Edge-provisioned deletion acknowledgement durable is mismatched".to_owned(),
        ));
    }
    Ok(consumer)
}

/// How long a transiently failed acknowledgement waits before redelivery.
const NAK_DELAY: Duration = Duration::from_secs(2);

/// Pause after a recoverable stream error, so a broken connection cannot spin the loop.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

/// Runs the pull loop until `shutdown` resolves.
///
/// An acknowledgement is acknowledged to the broker after its outcome is committed, or when it
/// names no tombstone of this service. A message that can never be valid is terminated, and a
/// database failure is redelivered after a short delay (XR-021 CONTRACTS.md S02 rule 7).
///
/// # Errors
///
/// [`AcksError`] when the durable was deleted or the message stream ended without a shutdown
/// request.
pub async fn consume_deletion_acks(
    consumer: &AckConsumer,
    pool: &sqlx::PgPool,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<AcksSummary, AcksError> {
    let mut messages = consumer
        .messages()
        .await
        .map_err(|error| AcksError::Bus(error.to_string()))?;
    let mut summary = AcksSummary::default();
    tokio::pin!(shutdown);
    loop {
        let next = tokio::select! {
            biased;
            () = &mut shutdown => return Ok(summary),
            next = messages.next() => next,
        };
        let message = match next {
            None => return Err(AcksError::Stopped),
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                if matches!(
                    error.kind(),
                    MessagesErrorKind::ConsumerDeleted | MessagesErrorKind::PushBasedConsumer
                ) {
                    return Err(AcksError::Bus(error.to_string()));
                }
                tracing::warn!("the deletion acknowledgement stream reported a recoverable error");
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(summary),
                    () = tokio::time::sleep(RETRY_PAUSE) => {}
                }
                continue;
            }
        };
        let disposition = match deletion_ack::decode(&message.payload) {
            Err(rejection) => {
                tracing::warn!(class = %rejection, "terminating a deletion acknowledgement");
                summary.terminated += 1;
                AckKind::Term
            }
            Ok(acknowledgement) => match deletion_ack::acknowledge(pool, &acknowledgement).await {
                Ok(Acknowledgement::Recorded) => {
                    summary.recorded += 1;
                    AckKind::Ack
                }
                Ok(Acknowledgement::AlreadyRecorded | Acknowledgement::Unknown) => {
                    summary.ignored += 1;
                    AckKind::Ack
                }
                Err(_) => {
                    summary.retried += 1;
                    tracing::error!("leaving a deletion acknowledgement for redelivery");
                    AckKind::Nak(Some(NAK_DELAY))
                }
            },
        };
        if message.ack_with(disposition).await.is_err() {
            tracing::warn!("the deletion acknowledgement could not be settled with the broker");
        }
    }
}
