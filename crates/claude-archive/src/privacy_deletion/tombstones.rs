//! Contract tombstones for user-requested deletion.
//!
//! One `ai_archive.subject.tombstoned.v1` envelope per deleted archive, plus one per
//! Knowledge-linked conversation or Artifact, owned by the Platform user and pointing at a
//! content-free evidence blob (XR-021 CONTRACTS.md S07).

use ratatoskr_ai_archive_contracts::{
    AiArchiveTombstone, AiArchiveTombstoneReason, AiArchiveTombstoneSubject, AiProvider,
};
use ratatoskr_identifiers::{
    AiArchiveId, AiConversationId, EntityLocalId, EntityRef, Extensions, TenantRef, WireTimestamp,
};

use super::*;
use crate::{ArchiveEventError, ArchiveOutbox};

/// One subject that needs a downstream tombstone, read before its rows are deleted.
pub(super) struct TombstoneSubject {
    ai_archive_id: Uuid,
    subject: AiArchiveTombstoneSubject,
}

/// Rejects a plan that must tombstone but whose tenant reference is not a Platform user.
///
/// The owner of a tombstone is the Platform tenant `user:<uuid>`, which Knowledge indexes under.
/// A free-form reference cannot name it, so the request is refused before anything persists.
pub(super) fn require_platform_owner(
    tenant_ref: &str,
    items: &[DeletionInventoryItem],
) -> Result<(), PrivacyDeletionError> {
    let needs_owner = items.iter().any(|item| {
        matches!(
            item.kind,
            DeletionItemKind::RawArchive | DeletionItemKind::DownstreamTombstone
        )
    });
    if needs_owner && TenantRef::parse(tenant_ref).is_err() {
        return Err(PrivacyDeletionError::Conflict);
    }
    Ok(())
}

/// Reads the tombstone subjects of a request while their rows and links still exist.
pub(super) async fn collect_subjects(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<Vec<TombstoneSubject>, sqlx::Error> {
    let archives: Vec<Uuid> = sqlx::query_scalar(
        "select distinct e.ai_archive_id from claude_archive.exports e
         join claude_archive.privacy_deletion_items i
           on i.subject_id = e.export_id::text and i.item_kind = 'raw_archive'
         where i.request_id = $1 order by e.ai_archive_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let linked: Vec<(Uuid, String, String)> = sqlx::query_as(
        "select distinct l.ai_archive_id, l.subject_kind, l.subject_id
         from claude_archive.knowledge_analysis_links l
         join claude_archive.privacy_deletion_items i
           on i.subject_id = l.subject_id and i.item_kind = 'downstream_tombstone'
          and i.action = 'emit_tombstone'
         where i.request_id = $1 order by 1, 2, 3",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut subjects: Vec<TombstoneSubject> = archives
        .into_iter()
        .map(|ai_archive_id| TombstoneSubject {
            ai_archive_id,
            subject: AiArchiveTombstoneSubject::Archive,
        })
        .collect();
    for (ai_archive_id, kind, subject_id) in linked {
        let subject = match kind.as_str() {
            "conversation" => Uuid::parse_str(&subject_id).ok().map(|id| {
                AiArchiveTombstoneSubject::Conversation {
                    ai_conversation_id: AiConversationId(id),
                }
            }),
            "artifact" => EntityLocalId::parse(&subject_id).ok().map(|external| {
                AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id: external,
                }
            }),
            _ => None,
        };
        if let Some(subject) = subject {
            subjects.push(TombstoneSubject {
                ai_archive_id,
                subject,
            });
        }
    }
    Ok(subjects)
}

/// Inserts one tombstone envelope per subject, in the row-removal transaction.
///
/// A tombstone whose aggregate already has one (a provider tombstone, or an earlier request for
/// the same subject) is skipped instead of aborting the deletion: the insert names no conflict
/// target, so neither the payload key nor `outbox_privacy_tombstone_once` can fail it.
pub(super) async fn enqueue_tombstones(
    transaction: &mut Transaction<'_, Postgres>,
    request: &super::rows::DurableDeletionRequest,
    request_id: Uuid,
    subjects: Vec<TombstoneSubject>,
    evidence: &crate::BlobRef,
    observed_at: WireTimestamp,
) -> Result<usize, PrivacyDeletionExecutionError> {
    if subjects.is_empty() {
        return Ok(0);
    }
    let owner = TenantRef::parse(&request.tenant_ref)
        .map_err(|_| PrivacyDeletionExecutionError::InvalidTenant)?;
    let evidence_ref = crate::events::contract_blob_ref(evidence)?;
    let correlation = EntityRef::parse(&request.correlation_id)
        .or_else(|_| EntityRef::parse(&format!("privacy_request:{request_id}")))
        .map_err(|_| ArchiveEventError::Identifier)?;
    let provider = AiProvider::parse("claude").map_err(|_| ArchiveEventError::Identifier)?;
    let reason = AiArchiveTombstoneReason::parse("user_requested")
        .map_err(|_| ArchiveEventError::Identifier)?;
    let mut inserted = 0;
    for subject in subjects {
        let tombstone = AiArchiveTombstone {
            ai_archive_id: AiArchiveId(subject.ai_archive_id),
            provider: provider.clone(),
            owner,
            subject: subject.subject,
            reason: reason.clone(),
            evidence_ref: evidence_ref.clone(),
            observed_at,
            parser: None,
            extensions: Extensions::new(),
        };
        if ArchiveOutbox::enqueue_tombstone_in(
            transaction,
            &tombstone,
            correlation.clone(),
            observed_at,
        )
        .await?
        .is_some()
        {
            inserted += 1;
        }
    }
    Ok(inserted)
}
