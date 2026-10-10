//! Contract tombstones for user-requested deletion.
//!
//! One `ai_archive.subject.tombstoned.v1` envelope per deleted archive, per deleted conversation
//! and project, and per Knowledge-linked Artifact, owned by the Platform user and pointing at a
//! content-free evidence blob (XR-021 CONTRACTS.md S07).

use ratatoskr_ai_archive_contracts::{
    AiArchiveTombstone, AiArchiveTombstoneReason, AiArchiveTombstoneSubject, AiProvider,
};
use ratatoskr_identifiers::{
    AiArchiveId, AiConversationId, AiProjectId, EntityLocalId, EntityRef, Extensions, TenantRef,
    WireTimestamp,
};

use super::*;
use crate::contract_projection::{Deletion, Survivors, reannounce};
use crate::{ArchiveEventError, ArchiveOutbox};

/// One subject that needs a downstream tombstone, read before its rows are deleted.
pub(super) struct TombstoneSubject {
    ai_archive_id: Uuid,
    subject: AiArchiveTombstoneSubject,
}

/// The archives a set of tombstone subjects deletes.
pub(super) fn deleted_archives(subjects: &[TombstoneSubject]) -> Vec<Uuid> {
    subjects
        .iter()
        .filter(|subject| matches!(subject.subject, AiArchiveTombstoneSubject::Archive))
        .map(|subject| subject.ai_archive_id)
        .collect()
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

/// Reads the tombstone subjects of a request while their rows and observations still exist.
///
/// The subjects are the deleted raw archives, every conversation and project the request removes,
/// and the Knowledge-linked Artifacts (XR-021 CONTRACTS.md S07). A conversation or project is
/// tombstoned whether or not Knowledge analysed it: its facts announced it, so its removal must be
/// announced too. It is tombstoned under the earliest deleted archive that observed it, which is
/// the archive its first fact carried.
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
    let removed: Vec<(String, String, Option<Uuid>)> = sqlx::query_as(
        "select r.item_kind, r.subject_id,
                (select e.ai_archive_id
                 from claude_archive.export_observations o
                 join claude_archive.exports e on e.export_id = o.export_id
                 join claude_archive.privacy_deletion_items x
                   on x.request_id = r.request_id and x.item_kind = 'raw_archive'
                  and x.subject_id = e.export_id::text
                 where o.subject_kind = r.item_kind and o.subject_id::text = r.subject_id
                 order by e.export_id limit 1)
         from claude_archive.privacy_deletion_items r
         join claude_archive.privacy_deletion_items t
           on t.request_id = r.request_id and t.item_kind = 'downstream_tombstone'
          and t.action = 'emit_tombstone' and t.subject_id = r.subject_id
         where r.request_id = $1 and r.item_kind in ('conversation', 'project')
           and r.action = 'remove_record'
         group by r.request_id, r.item_kind, r.subject_id
         order by r.item_kind, r.subject_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let linked: Vec<(Uuid, String)> = sqlx::query_as(
        "select distinct l.ai_archive_id, l.subject_id
         from claude_archive.knowledge_analysis_links l
         join claude_archive.privacy_deletion_items i
           on i.subject_id = l.subject_id and i.item_kind = 'downstream_tombstone'
          and i.action = 'emit_tombstone'
         where i.request_id = $1 and l.subject_kind = 'artifact'
           and l.ai_archive_id = any($2) order by 1, 2",
    )
    .bind(request_id)
    .bind(&archives)
    .fetch_all(&mut **transaction)
    .await?;
    let first_archive = archives.first().copied();
    let mut subjects: Vec<TombstoneSubject> = archives
        .into_iter()
        .map(|ai_archive_id| TombstoneSubject {
            ai_archive_id,
            subject: AiArchiveTombstoneSubject::Archive,
        })
        .collect();
    for (kind, subject_id, observed_in) in removed {
        let Some(ai_archive_id) = observed_in.or(first_archive) else {
            continue;
        };
        let Ok(id) = Uuid::parse_str(&subject_id) else {
            continue;
        };
        let subject = match kind.as_str() {
            "conversation" => AiArchiveTombstoneSubject::Conversation {
                ai_conversation_id: AiConversationId(id),
            },
            _ => AiArchiveTombstoneSubject::Project {
                ai_project_id: AiProjectId(id),
            },
        };
        subjects.push(TombstoneSubject {
            ai_archive_id,
            subject,
        });
    }
    for (ai_archive_id, subject_id) in linked {
        if let Ok(external) = EntityLocalId::parse(&subject_id) {
            subjects.push(TombstoneSubject {
                ai_archive_id,
                subject: AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id: external,
                },
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

/// Announces again, after the archive tombstones, every project, conversation and Artifact that
/// stays because a retained export also observed it.
///
/// Their facts were announced under an archive this request tombstones, and Knowledge drops
/// everything announced under a tombstoned archive. The survivors are the request's
/// `retain_evidenced` items; the facts are enqueued in the row-removal transaction, so the
/// tombstone and the repair commit together (XR-021 CONTRACTS.md S07).
pub(super) async fn reannounce_survivors(
    transaction: &mut Transaction<'_, Postgres>,
    request: &super::rows::DurableDeletionRequest,
    request_id: Uuid,
    archives: &[Uuid],
    tombstoned_at: WireTimestamp,
) -> Result<usize, PrivacyDeletionExecutionError> {
    let Ok(owner) = TenantRef::parse(&request.tenant_ref) else {
        return Ok(0);
    };
    if archives.is_empty() {
        return Ok(0);
    }
    let retained: Vec<(String, String)> = sqlx::query_as(
        "select item_kind, subject_id from claude_archive.privacy_deletion_items
         where request_id = $1 and action = 'retain_evidenced'
           and item_kind in ('project', 'conversation', 'artifact')
         order by item_kind, subject_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut survivors = Survivors::default();
    for (kind, subject_id) in retained {
        let Ok(id) = Uuid::parse_str(&subject_id) else {
            continue;
        };
        match kind.as_str() {
            "project" => survivors.projects.push(id),
            "conversation" => survivors.conversations.push(id),
            _ => survivors.artifacts.push(id),
        }
    }
    let deletion = Deletion {
        owner,
        archives,
        tombstoned_at,
    };
    Ok(reannounce(transaction, &deletion, &survivors).await?)
}
