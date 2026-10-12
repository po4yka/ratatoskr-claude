//! Contract tombstones for user-requested deletion.
//!
//! One `ai_archive.subject.tombstoned.v1` envelope per deleted archive, per deleted conversation
//! and project, and per Knowledge-linked Artifact, for every Platform user the subject was
//! announced to, pointing at a content-free evidence blob (XR-021 CONTRACTS.md S07 and R5-06). The
//! owners come from the ledger of announcements; no part of a deletion reads the configured
//! account mapping.

use ratatoskr_ai_archive_contracts::{
    AiArchiveTombstone, AiArchiveTombstoneReason, AiArchiveTombstoneSubject, AiProvider,
};
use ratatoskr_identifiers::{
    AiArchiveId, AiConversationId, AiProjectId, EntityLocalId, EntityRef, Extensions, TenantRef,
    WireTimestamp,
};

use std::collections::HashMap;

use super::*;
use crate::announced_subjects;
use crate::contract_projection::{Deletion, Survivors, reannounce};
use crate::stream_erasure::{ErasureRequest, drop_published_fact_rows};
use crate::{ArchiveEventError, ArchiveOutbox};

/// One subject that needs a downstream tombstone for one owner, read before its rows are deleted.
pub(super) struct TombstoneSubject {
    ai_archive_id: Uuid,
    subject: AiArchiveTombstoneSubject,
    owner: TenantRef,
}

impl TombstoneSubject {
    /// The ledger key of the subject, which a deletion removes with its tombstones.
    fn ledger_key(&self) -> (String, String) {
        match &self.subject {
            AiArchiveTombstoneSubject::Archive => {
                ("archive".to_owned(), self.ai_archive_id.to_string())
            }
            AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
                ("conversation".to_owned(), ai_conversation_id.0.to_string())
            }
            AiArchiveTombstoneSubject::Project { ai_project_id } => {
                ("project".to_owned(), ai_project_id.0.to_string())
            }
            AiArchiveTombstoneSubject::Artifact {
                external_artifact_id,
            } => ("artifact".to_owned(), external_artifact_id.to_string()),
        }
    }
}

/// The ledger keys of the subjects a deletion tombstones.
pub(super) fn ledger_keys(subjects: &[TombstoneSubject]) -> Vec<(String, String)> {
    let mut keys: Vec<(String, String)> =
        subjects.iter().map(TombstoneSubject::ledger_key).collect();
    keys.sort();
    keys.dedup();
    keys
}

/// What a request tombstones: the subjects, and every archive it deletes whether or not that
/// archive was ever announced.
pub(super) struct Tombstones {
    pub(super) subjects: Vec<TombstoneSubject>,
    pub(super) archives: Vec<Uuid>,
}

/// Reads the tombstone subjects of a request while their rows and observations still exist.
///
/// The subjects are the deleted raw archives, every conversation and project the request removes,
/// and the Knowledge-linked Artifacts (XR-021 CONTRACTS.md S07). A conversation or project is
/// tombstoned whether or not Knowledge analysed it: its facts announced it, so its removal must be
/// announced too. Each subject yields one entry per owner the ledger holds for it among the
/// request's accounts (R5-06 rule 2), and a subject the ledger does not know was never announced
/// and yields none. For one owner the subject is tombstoned under the earliest deleted archive
/// that observed it and was announced to that owner, else the earliest deleted archive that
/// observed it.
pub(super) async fn collect_subjects(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    accounts: &[Uuid],
) -> Result<Tombstones, sqlx::Error> {
    let archives: Vec<Uuid> = sqlx::query_scalar(
        "select distinct e.ai_archive_id from claude_archive.exports e
         join claude_archive.privacy_deletion_items i
           on i.subject_id = e.export_id::text and i.item_kind = 'raw_archive'
         where i.request_id = $1 order by e.ai_archive_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut archive_owners: HashMap<Uuid, Vec<TenantRef>> = HashMap::new();
    let mut subjects = Vec::new();
    for ai_archive_id in &archives {
        let owners = announced_subjects::owners_of_subject(
            transaction,
            "archive",
            &ai_archive_id.to_string(),
            accounts,
        )
        .await?;
        for owner in &owners {
            subjects.push(TombstoneSubject {
                ai_archive_id: *ai_archive_id,
                subject: AiArchiveTombstoneSubject::Archive,
                owner: *owner,
            });
        }
        archive_owners.insert(*ai_archive_id, owners);
    }
    let first_archive = archives.first().copied();
    for (kind, subject_id, observers) in removed_subjects(transaction, request_id).await? {
        let Ok(id) = Uuid::parse_str(&subject_id) else {
            continue;
        };
        let (ledger_kind, subject) = match kind.as_str() {
            "conversation" => (
                "conversation",
                AiArchiveTombstoneSubject::Conversation {
                    ai_conversation_id: AiConversationId(id),
                },
            ),
            _ => (
                "project",
                AiArchiveTombstoneSubject::Project {
                    ai_project_id: AiProjectId(id),
                },
            ),
        };
        for owner in
            announced_subjects::owners_of_subject(transaction, ledger_kind, &subject_id, accounts)
                .await?
        {
            let announced_to_owner = observers.iter().copied().find(|archive| {
                archive_owners
                    .get(archive)
                    .is_some_and(|owners| owners.contains(&owner))
            });
            let Some(ai_archive_id) = announced_to_owner
                .or_else(|| observers.first().copied())
                .or(first_archive)
            else {
                continue;
            };
            subjects.push(TombstoneSubject {
                ai_archive_id,
                subject: subject.clone(),
                owner,
            });
        }
    }
    for (ai_archive_id, subject_id) in linked_artifacts(transaction, request_id, &archives).await? {
        let Ok(external) = EntityLocalId::parse(&subject_id) else {
            continue;
        };
        for owner in
            announced_subjects::owners_of_subject(transaction, "artifact", &subject_id, accounts)
                .await?
        {
            subjects.push(TombstoneSubject {
                ai_archive_id,
                subject: AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id: external.clone(),
                },
                owner,
            });
        }
    }
    Ok(Tombstones { subjects, archives })
}

/// Every conversation and project the request removes, with the archives it deletes that
/// observed them, oldest export first.
async fn removed_subjects(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<Vec<(String, String, Vec<Uuid>)>, sqlx::Error> {
    sqlx::query_as(
        "select r.item_kind, r.subject_id,
                array(select e.ai_archive_id
                      from claude_archive.export_observations o
                      join claude_archive.exports e on e.export_id = o.export_id
                      join claude_archive.privacy_deletion_items x
                        on x.request_id = r.request_id and x.item_kind = 'raw_archive'
                       and x.subject_id = e.export_id::text
                      where o.subject_kind = r.item_kind and o.subject_id::text = r.subject_id
                      order by e.export_id)
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
    .await
}

/// The Knowledge-linked Artifacts of the request, with the archive their link names.
async fn linked_artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    archives: &[Uuid],
) -> Result<Vec<(Uuid, String)>, sqlx::Error> {
    sqlx::query_as(
        "select distinct l.ai_archive_id, l.subject_id
         from claude_archive.knowledge_analysis_links l
         join claude_archive.privacy_deletion_items i
           on i.subject_id = l.subject_id and i.item_kind = 'downstream_tombstone'
          and i.action = 'emit_tombstone'
         where i.request_id = $1 and l.subject_kind = 'artifact'
           and l.ai_archive_id = any($2) order by 1, 2",
    )
    .bind(request_id)
    .bind(archives)
    .fetch_all(&mut **transaction)
    .await
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
            owner: subject.owner,
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
/// tombstone and the repair commit together (XR-021 CONTRACTS.md S07). Each survivor is announced
/// to the owner of the retained archive it is announced under (R5-06 rule 3).
pub(super) async fn reannounce_survivors(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    archives: &[Uuid],
    tombstoned_at: WireTimestamp,
    erasure: &ErasureRequest,
) -> Result<usize, PrivacyDeletionExecutionError> {
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
        archives,
        tombstoned_at,
        erasure,
    };
    Ok(reannounce(transaction, &deletion, &survivors).await?)
}

/// Removes the facts of the deleted archives that stay in the outbox under a subject the deletion
/// keeps.
///
/// A retained project, conversation or Artifact keeps its earlier facts, and the ones announced
/// under a deleted archive carry the deleted export's content: they leave with it, after the
/// survivors were announced again from the restored rows (XR-021 CONTRACTS.md R4-03). Tombstones
/// stay, and the facts of subjects the request removes already left with their rows.
pub(super) async fn purge_facts_of_deleted_archives(
    transaction: &mut Transaction<'_, Postgres>,
    erasure: &ErasureRequest,
    archives: &[Uuid],
) -> Result<u64, PrivacyDeletionExecutionError> {
    if archives.is_empty() {
        return Ok(0);
    }
    let archives: Vec<String> = archives.iter().map(Uuid::to_string).collect();
    let event_ids: Vec<Uuid> = sqlx::query_scalar(
        "select event_id from claude_archive.outbox_events
         where event_type like 'ai_archive.%'
           and event_type <> 'ai_archive.subject.tombstoned.v1'
           and (envelope #>> '{payload,import_provenance,ai_archive_id}') = any($1)",
    )
    .bind(&archives)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(drop_published_fact_rows(transaction, erasure, &event_ids).await?)
}
