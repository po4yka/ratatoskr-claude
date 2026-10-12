//! Asking Edge to erase the stream copy of the facts a deletion removes (XR-021 CONTRACTS.md
//! R5-12 rules 2 and 3).
//!
//! A conversation, project or Artifact fact carries message text on the wire and stays in
//! `ratatoskr_events` for up to seven days after this service removed its own outbox row, and no
//! producer may remove a stream message. Edge is the only identity that may, on request. The pump
//! records the stream sequence of every row it publishes; every code path that deletes a published
//! fact row from the outbox goes through [`drop_published_fact_rows`], which reads those sequences
//! before the delete and enqueues the request in the same transaction. The request is published
//! like any other row and Edge erases eventually: a deletion does not wait for it.

use std::collections::{BTreeMap, BTreeSet};

use ratatoskr_event_envelope::CommandEnvelope;
use ratatoskr_identifiers::{CommandId, EntityRef, Extensions, TenantRef, WireTimestamp};
use ratatoskr_operation_contracts::{MAX_STREAM_SEQUENCES, PlatformStreamErasureRequested};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::ArchiveEventError;
use crate::operation_outbox::STREAM_ERASURE_TYPE;

/// The content-bearing fact types whose stream copy may be erased. Never the tombstone, which
/// carries no content and is what tells Knowledge to delete, and never an operation report.
const CONTENT_BEARING: [&str; 7] = [
    "ai_archive.archive.imported.v1",
    "ai_archive.conversation.added.v1",
    "ai_archive.conversation.updated.v1",
    "ai_archive.project.added.v1",
    "ai_archive.project.updated.v1",
    "ai_archive.artifact.added.v1",
    "ai_archive.artifact.updated.v1",
];

/// The deletion request on whose behalf published fact rows are removed.
#[derive(Debug, Clone)]
pub(crate) struct ErasureRequest {
    request_ref: EntityRef,
    correlation: EntityRef,
}

impl ErasureRequest {
    /// The request `request_id`, whose commands carry `correlation_id` when it is a reference and
    /// the request reference otherwise.
    ///
    /// # Errors
    ///
    /// [`ArchiveEventError::Identifier`] when the request reference cannot be composed.
    pub(crate) fn new(request_id: Uuid, correlation_id: &str) -> Result<Self, ArchiveEventError> {
        let request_ref = EntityRef::parse(&format!("privacy_request:{request_id}"))
            .map_err(|_| ArchiveEventError::Identifier)?;
        let correlation = EntityRef::parse(correlation_id).unwrap_or_else(|_| request_ref.clone());
        Ok(Self {
            request_ref,
            correlation,
        })
    }
}

/// Deletes these outbox rows, and asks Edge to erase the stream copy of every published
/// content-bearing fact among them.
///
/// The sequences are read before the rows are deleted and grouped by the owner the row's envelope
/// names; each owner gets ascending commands of at most 500 sequences, enqueued in the same
/// transaction. A row that was never published has no sequence and needs nothing. Returns how many
/// rows were deleted.
///
/// # Errors
///
/// [`ArchiveEventError`] when a command cannot be formed or stored, or a statement fails.
pub(crate) async fn drop_published_fact_rows(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &ErasureRequest,
    event_ids: &[Uuid],
) -> Result<u64, ArchiveEventError> {
    if event_ids.is_empty() {
        return Ok(0);
    }
    let rows: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
        "select event_type, tenant_ref, published_stream_seq
         from claude_archive.outbox_events where event_id = any($1)",
    )
    .bind(event_ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut by_owner: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    for (event_type, owner, sequence) in rows {
        let (true, Some(owner), Some(sequence)) = (
            CONTENT_BEARING.contains(&event_type.as_str()),
            owner,
            sequence.and_then(|sequence| u64::try_from(sequence).ok()),
        ) else {
            continue;
        };
        if sequence > 0 {
            by_owner.entry(owner).or_default().insert(sequence);
        }
    }
    for (owner, sequences) in by_owner {
        let owner = TenantRef::parse(&owner).map_err(|_| ArchiveEventError::Identifier)?;
        let sequences: Vec<u64> = sequences.into_iter().collect();
        for chunk in sequences.chunks(MAX_STREAM_SEQUENCES) {
            enqueue_command(transaction, request, owner, chunk).await?;
        }
    }
    let removed = sqlx::query("delete from claude_archive.outbox_events where event_id = any($1)")
        .bind(event_ids)
        .execute(&mut **transaction)
        .await?;
    Ok(removed.rows_affected())
}

async fn enqueue_command(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &ErasureRequest,
    owner: TenantRef,
    sequences: &[u64],
) -> Result<(), ArchiveEventError> {
    let command_id = CommandId::new_v7();
    let issued_at = WireTimestamp::now();
    let mut envelope: CommandEnvelope = serde_json::from_value(serde_json::json!({
        "command_id": command_id,
        "command_type": STREAM_ERASURE_TYPE,
        "issued_at": issued_at,
        "producer": "ratatoskr-claude",
        "aggregate_id": request.request_ref,
        "correlation_id": request.correlation,
        "tenant_id": owner,
        "schema_version": 1,
        "payload": {}
    }))?;
    envelope.set_payload(&PlatformStreamErasureRequested {
        request_ref: request.request_ref.clone(),
        owner,
        stream_sequences: sequences.to_vec(),
        extensions: Extensions::new(),
    })?;
    let digest = Sha256::digest(serde_json::to_vec(&envelope.payload)?).to_vec();
    sqlx::query(
        "insert into claude_archive.outbox_events
             (event_id, event_type, aggregate_type, aggregate_id, envelope,
              payload_digest, correlation_id, tenant_ref, occurred_at)
         values ($1, $2, 'privacy_request', $3, $4, $5, $6, $7, $8::timestamptz)
         on conflict (event_type, payload_digest) do nothing",
    )
    .bind(command_id.0)
    .bind(STREAM_ERASURE_TYPE)
    .bind(request.request_ref.to_string())
    .bind(serde_json::to_value(&envelope)?)
    .bind(digest)
    .bind(request.correlation.to_string())
    .bind(owner.to_string())
    .bind(issued_at.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
