//! The stream copies whose commit failed (XR-021 CONTRACTS.md R6-03 rule 4).
//!
//! When the broker acknowledged a fact and the transaction that records its sequence did not
//! commit, the stream holds a copy that no row names. The pump keeps that sequence in memory,
//! merges it into the next successful mark of the row, and, if the row was deleted before the
//! retry, asks Edge to erase it itself, because no deletion knew about it.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use ratatoskr_identifiers::TenantRef;
use ratatoskr_operation_contracts::MAX_STREAM_SEQUENCES;
use uuid::Uuid;

use super::{OutboxError, OutboxPublisher};
use crate::stream_erasure::{ErasureRequest, enqueue_command, is_content_bearing};

/// The sequences carried for one row and the owner whose fact they are.
#[derive(Debug)]
struct Carried {
    owner: String,
    sequences: BTreeSet<i64>,
}

/// The carried copies by outbox row, shared by the clones of one publisher. The lock is never held
/// across an await.
#[derive(Debug, Clone, Default)]
pub(super) struct CarriedCopies(Arc<Mutex<HashMap<Uuid, Carried>>>);

impl CarriedCopies {
    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<Uuid, Carried>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The sequences to record for `event_id`: those carried and the new one, ascending.
    pub(super) fn with_new(&self, event_id: Uuid, sequence: i64) -> Vec<i64> {
        let mut merged: BTreeSet<i64> = self
            .entries()
            .get(&event_id)
            .map(|carried| carried.sequences.clone())
            .unwrap_or_default();
        merged.insert(sequence);
        merged.into_iter().collect()
    }

    /// Remembers `sequences` for a row whose mark did not commit. Only a content-bearing fact of a
    /// known owner is carried: nothing else is ever erased from the stream.
    pub(super) fn keep(
        &self,
        event_id: Uuid,
        event_type: &str,
        owner: Option<&str>,
        sequences: &[i64],
    ) {
        let Some(owner) = owner.filter(|_| is_content_bearing(event_type)) else {
            return;
        };
        self.entries().insert(
            event_id,
            Carried {
                owner: owner.to_owned(),
                sequences: sequences.iter().copied().collect(),
            },
        );
    }

    pub(super) fn forget(&self, event_id: Uuid) {
        self.entries().remove(&event_id);
    }

    fn snapshot(&self) -> Vec<(Uuid, String, Vec<i64>)> {
        self.entries()
            .iter()
            .map(|(id, carried)| {
                (
                    *id,
                    carried.owner.clone(),
                    carried.sequences.iter().copied().collect(),
                )
            })
            .collect()
    }
}

impl OutboxPublisher {
    /// Checks every carried copy against the table at the start of a pass.
    ///
    /// A row that is still unpublished keeps its entry for the next mark, and a row that is
    /// published no longer needs one. A row that no longer exists was deleted by a request that
    /// recorded no sequence for this copy, so the pump enqueues the erasure command itself, in the
    /// owner's name, and drops the entry. The command names `privacy_request:<the row's event id>`,
    /// which is no real deletion request and is documented as such.
    pub(super) async fn erase_orphaned_copies(&self) -> Result<(), OutboxError> {
        for (event_id, owner, sequences) in self.carried.snapshot() {
            let published: Option<bool> = sqlx::query_scalar(
                "select published_at is not null from claude_archive.outbox_events
                 where event_id = $1",
            )
            .bind(event_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(OutboxError::Database)?;
            match published {
                Some(false) => {}
                Some(true) => self.carried.forget(event_id),
                None => {
                    self.enqueue_orphan_erasure(event_id, &owner, &sequences)
                        .await?;
                    self.carried.forget(event_id);
                }
            }
        }
        Ok(())
    }

    async fn enqueue_orphan_erasure(
        &self,
        event_id: Uuid,
        owner: &str,
        sequences: &[i64],
    ) -> Result<(), OutboxError> {
        let request = ErasureRequest::new(event_id, &format!("privacy_request:{event_id}"))
            .map_err(OutboxError::Erasure)?;
        let owner = TenantRef::parse(owner)
            .map_err(|_| OutboxError::Erasure(crate::ArchiveEventError::Identifier))?;
        let ascending: Vec<u64> = sequences
            .iter()
            .filter_map(|sequence| u64::try_from(*sequence).ok())
            .collect();
        let mut transaction = self.pool.begin().await.map_err(OutboxError::Database)?;
        for chunk in ascending.chunks(MAX_STREAM_SEQUENCES) {
            enqueue_command(&mut transaction, &request, owner, chunk)
                .await
                .map_err(OutboxError::Erasure)?;
        }
        transaction.commit().await.map_err(OutboxError::Database)
    }
}
