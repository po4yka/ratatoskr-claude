//! The publication pass: which rows a pass offers, how each is published, and how it is marked.
//!
//! The first publication of a row is one short transaction that locks the row, so a deletion that
//! meets the lock waits for the pump and a row deleted before the lock is never published (XR-021
//! CONTRACTS.md R6-03 rule 2). The resend of a tombstone that was already published takes no lock.

use sqlx::Row as _;
use uuid::Uuid;

use ratatoskr_event_envelope::{CommandEnvelope, EventEnvelope};

use super::{
    BATCH_SIZE, HEADER_HEADROOM_BYTES, MARK_LOST_METRIC, OutboxError, OutboxPublisher,
    PAYLOAD_TOO_LARGE, PENDING_SQL, PublishPause, RESEND_SQL, STREAM_ERASURE_TYPE,
    UNDELIVERABLE_METRIC, subject_for_event_type,
};

/// Locks the row of a first publication, or finds nothing: the row was deleted, is held by a
/// deletion, or was published. The pump never waits on a lock, so it cannot take part in a
/// deadlock; the deletion waits for the pump instead.
const LOCK_SQL: &str = "select tenant_ref from claude_archive.outbox_events
     where event_id = $1 and published_at is null and undeliverable_at is null
     for update skip locked";

/// What one first publication came to.
enum FirstPublication {
    /// The broker acknowledged the row and the mark committed.
    Published,
    /// The row was gone or held by a deletion, and nothing was published.
    Skipped,
    /// The broker did not acknowledge the row; the refusal was recorded.
    Refused,
}

impl OutboxPublisher {
    /// Makes one bounded attempt to publish pending events through an open `JetStream` context.
    ///
    /// A refused row stays unpublished, counts the attempt, records a safe error class and backs
    /// off, and the rows behind it are still offered in the same pass. A pass first asks Edge to
    /// erase any copy it carried from a failed commit whose row has since been deleted.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::UnknownEventType`] or [`OutboxError::EnvelopeMismatch`] for a row
    /// the pump must never skip, [`OutboxError::Broker`] after a pass in which any publish was
    /// refused, [`OutboxError::MarkLost`] when an acknowledged row could not be marked, and
    /// [`OutboxError::Database`] when the queue cannot be read or marked.
    pub async fn publish_pending_with(
        &self,
        jetstream: &async_nats::jetstream::Context,
    ) -> Result<usize, OutboxError> {
        self.erase_orphaned_copies().await?;
        let pending = sqlx::query(PENDING_SQL)
            .bind(BATCH_SIZE)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
        let resends = sqlx::query(RESEND_SQL)
            .bind(BATCH_SIZE)
            .fetch_all(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
        if let Some(probe) = &self.probe {
            probe.park(PublishPause::AfterCandidates).await;
        }
        let rows = pending
            .into_iter()
            .map(|row| (row, false))
            .chain(resends.into_iter().map(|row| (row, true)));

        // The server limit is read from the connection, never assumed: a body over it is refused
        // on every attempt, so it is a property of the row, not of the broker's mood.
        let largest_body = jetstream
            .client()
            .server_info()
            .max_payload
            .saturating_sub(HEADER_HEADROOM_BYTES);
        let mut published = 0;
        let mut refused = 0_usize;
        for (row, resend) in rows {
            let event_id: Uuid = row.try_get("event_id").map_err(OutboxError::Database)?;
            let event_type: String = row.try_get("event_type").map_err(OutboxError::Database)?;
            let subject =
                subject_for_event_type(&event_type).ok_or(OutboxError::UnknownEventType)?;
            let body = stored_body(
                &event_type,
                event_id,
                row.try_get("envelope").map_err(OutboxError::Database)?,
            )?;
            if body.len() > largest_body {
                if resend {
                    // It fitted when it was first published, so the broker limit shrank. Treat it
                    // as a refusal: the resend backs off instead of coming back every pass.
                    self.record_refusal(&self.pool, event_id, PAYLOAD_TOO_LARGE)
                        .await?;
                    refused += 1;
                } else {
                    self.mark_undeliverable(event_id).await?;
                }
                continue;
            }
            let outcome = if resend {
                self.resend_published(jetstream, event_id, subject, body)
                    .await?
            } else {
                self.publish_first(jetstream, event_id, &event_type, subject, body)
                    .await?
            };
            match outcome {
                FirstPublication::Published => published += 1,
                FirstPublication::Skipped => {}
                FirstPublication::Refused => refused += 1,
            }
        }
        if refused > 0 {
            return Err(OutboxError::broker_refused());
        }
        Ok(published)
    }

    /// Publishes a tombstone again: it is already published, no sequence is recorded for a resend
    /// and no deletion removes it before Knowledge acknowledged it, so the row takes no lock.
    async fn resend_published(
        &self,
        jetstream: &async_nats::jetstream::Context,
        event_id: Uuid,
        subject: &'static str,
        body: Vec<u8>,
    ) -> Result<FirstPublication, OutboxError> {
        match publish_one(jetstream, event_id, subject, body).await {
            Ok(_) => {
                self.mark_resent(event_id).await?;
                Ok(FirstPublication::Published)
            }
            Err(class) => {
                self.record_refusal(&self.pool, event_id, class).await?;
                Ok(FirstPublication::Refused)
            }
        }
    }

    /// The first publication of one row, as one short transaction (R6-03 rule 2): lock the row,
    /// publish, await the acknowledgement, mark, commit.
    ///
    /// One pool connection is held while the publish is in flight, for at most
    /// [`super::PUBLISH_REQUEST_TIMEOUT`]. A pass is sequential, so the pump never holds more than
    /// one for this, and the pool of the service is sized for that beside its other workers.
    async fn publish_first(
        &self,
        jetstream: &async_nats::jetstream::Context,
        event_id: Uuid,
        event_type: &str,
        subject: &'static str,
        body: Vec<u8>,
    ) -> Result<FirstPublication, OutboxError> {
        let mut transaction = self.pool.begin().await.map_err(OutboxError::Database)?;
        let locked: Option<Option<String>> = sqlx::query_scalar(LOCK_SQL)
            .bind(event_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(OutboxError::Database)?;
        let Some(owner) = locked else {
            return Ok(FirstPublication::Skipped);
        };
        let sequence = match publish_one(jetstream, event_id, subject, body).await {
            Ok(sequence) => sequence,
            Err(class) => {
                self.record_refusal(&mut *transaction, event_id, class)
                    .await?;
                transaction.commit().await.map_err(OutboxError::Database)?;
                return Ok(FirstPublication::Refused);
            }
        };
        if let Some(probe) = &self.probe {
            probe.park(PublishPause::AfterPubAck).await;
        }
        let sequences = self.carried.with_new(event_id, sequence);
        let marked = self
            .mark_and_commit(transaction, event_id, &sequences)
            .await;
        match marked {
            Ok(()) => {
                self.carried.forget(event_id);
                Ok(FirstPublication::Published)
            }
            Err(error) => {
                self.carried
                    .keep(event_id, event_type, owner.as_deref(), &sequences);
                Err(error)
            }
        }
    }

    /// Marks the locked row and commits, or fails the commit on request of the test seam.
    async fn mark_and_commit(
        &self,
        mut transaction: sqlx::Transaction<'_, sqlx::Postgres>,
        event_id: Uuid,
        sequences: &[i64],
    ) -> Result<(), OutboxError> {
        if self
            .probe
            .as_ref()
            .is_some_and(|probe| probe.take_commit_fault())
        {
            return Err(OutboxError::Database(sqlx::Error::Io(
                std::io::Error::other("injected commit failure"),
            )));
        }
        self.mark_published(&mut transaction, event_id, sequences)
            .await?;
        transaction.commit().await.map_err(OutboxError::Database)
    }

    /// Retires a row whose body is larger than the broker accepts: no retry can ever succeed, so
    /// the pass skips it and goes on, and it never counts as a refused publish. Only the safe
    /// class is logged and exported.
    async fn mark_undeliverable(&self, event_id: Uuid) -> Result<(), OutboxError> {
        sqlx::query(
            "update claude_archive.outbox_events
             set undeliverable_at = now(), last_error = $2
             where event_id = $1 and published_at is null and undeliverable_at is null",
        )
        .bind(event_id)
        .bind(PAYLOAD_TOO_LARGE)
        .execute(&self.pool)
        .await
        .map_err(OutboxError::Database)?;
        metrics::counter!(UNDELIVERABLE_METRIC, "class" => PAYLOAD_TOO_LARGE).increment(1);
        tracing::warn!(
            class = PAYLOAD_TOO_LARGE,
            "an archive event is larger than the broker accepts and was marked undeliverable"
        );
        Ok(())
    }

    /// Marks a locked row published after the broker acknowledged it, and records every stream
    /// sequence it holds: the ones already on the row, the ones carried from a commit that failed
    /// and the new one, ascending and without duplicates (a duplicate acknowledgement carries the
    /// original sequence and adds nothing). A tombstone is also scheduled for its first resend,
    /// one interval later.
    ///
    /// The times are `clock_timestamp()`, not `now()`: the transaction has been open since the row
    /// was locked, and `now()` would be the instant the publish began.
    ///
    /// The update must affect exactly one row. Anything else means the broker holds a copy that no
    /// row records, so it is counted and returned, never ignored (R6-03 rule 2).
    async fn mark_published(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event_id: Uuid,
        sequences: &[i64],
    ) -> Result<(), OutboxError> {
        let marked = sqlx::query(
            "update claude_archive.outbox_events
             set published_at = clock_timestamp(), last_error = null,
                 published_stream_seqs = (select coalesce(array_agg(distinct s order by s), '{}')
                                          from unnest(published_stream_seqs || $3::bigint[]) s),
                 next_resend_at = case when event_type = 'ai_archive.subject.tombstoned.v1'
                                       then clock_timestamp() + make_interval(secs => $2) end
             where event_id = $1 and published_at is null",
        )
        .bind(event_id)
        .bind(self.resend.after.as_secs_f64())
        .bind(sequences)
        .execute(&mut **transaction)
        .await
        .map_err(OutboxError::Database)?;
        if marked.rows_affected() == 1 {
            return Ok(());
        }
        metrics::counter!(MARK_LOST_METRIC).increment(1);
        tracing::error!("the mark of an acknowledged archive event did not affect exactly one row");
        Err(OutboxError::MarkLost)
    }

    /// Counts a resend and schedules the next one, doubling the wait up to the cap.
    ///
    /// The wait is computed in seconds as a float and clamped to the cap before it becomes an
    /// interval, and the exponent is bounded: a row resent for months has a doubling far past the
    /// range of a `PostgreSQL` interval, and the resend never stops by count (R5-11 rule 4).
    async fn mark_resent(&self, event_id: Uuid) -> Result<(), OutboxError> {
        sqlx::query(
            "update claude_archive.outbox_events
             set resend_count = resend_count + 1, last_error = null,
                 next_resend_at = now() + make_interval(
                     secs => least($3::float8,
                                   $2::float8 * power(2::float8, least(resend_count, 60))))
             where event_id = $1 and acknowledged_at is null",
        )
        .bind(event_id)
        .bind(self.resend.after.as_secs_f64())
        .bind(self.resend.cap.as_secs_f64())
        .execute(&self.pool)
        .await
        .map_err(OutboxError::Database)?;
        Ok(())
    }

    /// Counts the refused attempt and delays the row, doubling from two seconds up to five
    /// minutes, so a failing head row cannot starve the rows behind it. A first publication
    /// records it inside the transaction that holds the row, because a second connection would
    /// wait on that lock forever. The delay starts at `clock_timestamp()`: `now()` is the start of
    /// that transaction, which may be a whole publish timeout ago.
    async fn record_refusal<'e>(
        &self,
        executor: impl sqlx::PgExecutor<'e>,
        event_id: Uuid,
        class: &str,
    ) -> Result<(), OutboxError> {
        sqlx::query(
            "update claude_archive.outbox_events
             set attempt_count = attempt_count + 1, last_error = $2,
                 next_attempt_at = clock_timestamp()
                     + least(300, 2 * power(2, least(attempt_count, 7))) * interval '1 second'
             where event_id = $1",
        )
        .bind(event_id)
        .bind(class)
        .execute(executor)
        .await
        .map_err(OutboxError::Database)?;
        Ok(())
    }
}

/// Publishes one stored body with `Nats-Msg-Id` set to the row's event id and returns the stream
/// sequence of the acknowledgement, or the safe class of the failure.
async fn publish_one(
    jetstream: &async_nats::jetstream::Context,
    event_id: Uuid,
    subject: &'static str,
    body: Vec<u8>,
) -> Result<i64, &'static str> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", event_id.to_string());
    let acknowledgement = jetstream
        .publish_with_headers(subject, headers, body.into())
        .await
        .map_err(|_| "publish_failed")?;
    let acknowledged = acknowledgement.await.map_err(|_| "ack_failed")?;
    i64::try_from(acknowledged.sequence).map_err(|_| "ack_failed")
}

/// The bytes a stored row is published as: its complete envelope, which must be the envelope of
/// the type and id the row is filed under. A fact is an event envelope; the stream erasure request
/// is a command envelope.
fn stored_body(
    event_type: &str,
    event_id: Uuid,
    stored: serde_json::Value,
) -> Result<Vec<u8>, OutboxError> {
    if event_type == STREAM_ERASURE_TYPE {
        let envelope: CommandEnvelope =
            serde_json::from_value(stored).map_err(OutboxError::Encode)?;
        if envelope.command_type.to_wire() != event_type || envelope.command_id.0 != event_id {
            return Err(OutboxError::EnvelopeMismatch);
        }
        return serde_json::to_vec(&envelope).map_err(OutboxError::Encode);
    }
    let envelope: EventEnvelope = serde_json::from_value(stored).map_err(OutboxError::Encode)?;
    if envelope.event_type.to_wire() != event_type || envelope.event_id.0 != event_id {
        return Err(OutboxError::EnvelopeMismatch);
    }
    serde_json::to_vec(&envelope).map_err(OutboxError::Encode)
}

#[cfg(test)]
mod mark_tests {
    use super::super::{OutboxError, OutboxPublisher};
    use crate::test_support::TestDatabase;
    use uuid::Uuid;

    async fn insert_row(db: &TestDatabase, published: bool) -> Uuid {
        let event_id = Uuid::now_v7();
        sqlx::query(
            "insert into claude_archive.outbox_events
              (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
               correlation_id, occurred_at, published_at)
             values ($1, 'ai_archive.conversation.added.v1', 'conversation', 'ai_conversation:x',
                     '{}', sha256($1::text::bytea), 'c', now(), case when $2 then now() end)",
        )
        .bind(event_id)
        .bind(published)
        .execute(db.database.pool())
        .await
        .expect("the row seeds");
        event_id
    }

    /// The Prometheus handle of this test binary, installed once because a recorder is
    /// process-wide.
    fn metrics() -> &'static metrics_exporter_prometheus::PrometheusHandle {
        static HANDLE: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
            std::sync::OnceLock::new();
        HANDLE.get_or_init(|| {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .install_recorder()
                .expect("the first recorder of this process installs")
        })
    }

    /// XR-021 CONTRACTS.md R6-03 rule 2: a mark that does not affect exactly one row is a counted
    /// error, never a silent success, because the broker already holds a copy nobody recorded.
    #[tokio::test]
    async fn a_mark_that_finds_no_unpublished_row_is_a_counted_error() {
        let recorder = metrics();
        let db = TestDatabase::create().await.expect("a disposable database");
        let publisher = OutboxPublisher::new(db.database.pool().clone());
        let mut transaction = db
            .database
            .pool()
            .begin()
            .await
            .expect("a transaction opens");
        let deleted = publisher
            .mark_published(&mut transaction, Uuid::now_v7(), &[7])
            .await;
        let already = insert_row(&db, true).await;
        let twice = publisher
            .mark_published(&mut transaction, already, &[8])
            .await;
        transaction.rollback().await.expect("the transaction ends");
        let exposition = recorder.render();
        db.cleanup().await.expect("cleanup succeeds");

        assert!(
            matches!(deleted, Err(OutboxError::MarkLost)),
            "a row that no longer exists: {deleted:?}"
        );
        assert!(
            matches!(twice, Err(OutboxError::MarkLost)),
            "a row that is already published: {twice:?}"
        );
        assert!(
            exposition
                .lines()
                .any(|line| line == "claude_archive_outbox_mark_lost_total 2"),
            "the counter claude_archive_outbox_mark_lost_total counted both: {exposition}"
        );
    }
}
