//! Facts for the subjects a deletion keeps (XR-021 CONTRACTS.md S07).
//!
//! A project, conversation or Artifact is announced under the archive of the export that first saw
//! it, and a later export that sees the same record announces nothing. When a deletion tombstones
//! that archive, Knowledge drops everything announced under it, including what a retained export
//! still holds. Each such survivor is announced again as an update whose provenance names the
//! earliest retained export that presented the content the survivor holds now, stamped after the
//! tombstone so the pump publishes it afterwards.

use std::collections::BTreeMap;

use super::loaders::{
    ARCHIVE_IMPORTED, ARTIFACT_ADDED, ARTIFACT_UPDATED, ArtifactScope, CONVERSATION_ADDED,
    CONVERSATION_UPDATED, PROJECT_ADDED, PROJECT_UPDATED, TIMESTAMP_TEXT, latest_envelope,
    load_artifacts, load_conversations, load_projects,
};
use super::*;
use crate::{DigestAlgorithm, MediaType};

/// The rows a deletion keeps because an export that stays observed them.
#[derive(Debug, Default)]
pub(crate) struct Survivors {
    pub(crate) projects: Vec<Uuid>,
    pub(crate) conversations: Vec<Uuid>,
    pub(crate) artifacts: Vec<Uuid>,
}

impl Survivors {
    pub(crate) fn is_empty(&self) -> bool {
        self.projects.is_empty() && self.conversations.is_empty() && self.artifacts.is_empty()
    }
}

/// The deletion whose survivors are announced again.
pub(crate) struct Deletion<'a> {
    pub(crate) owner: TenantRef,
    /// The archives the deletion tombstones.
    pub(crate) archives: &'a [Uuid],
    /// The instant of the archive tombstones; every fact is stamped after it.
    pub(crate) tombstoned_at: WireTimestamp,
}

/// What a fact needs to name the retained export it is announced under.
struct RetainedImport {
    archive_id: Uuid,
    source_export: ratatoskr_identifiers::BlobRef,
    imported_at: WireTimestamp,
}

impl RetainedImport {
    async fn load(
        transaction: &mut Transaction<'_, Postgres>,
        export_id: Uuid,
        deletion: &Deletion<'_>,
    ) -> Result<Self, ReparseError> {
        let (archive_id, hash, byte_size): (Uuid, String, i64) = sqlx::query_as(
            "select ai_archive_id, encode(archive_hash, 'hex'), byte_size
             from claude_archive.exports where export_id = $1",
        )
        .bind(export_id)
        .fetch_one(&mut **transaction)
        .await?;
        let raw = BlobRef {
            owner_service: crate::blob_store::OWNER_SERVICE.to_owned(),
            algorithm: DigestAlgorithm::Sha256,
            digest_hex: hash,
            media_type: MediaType::parse("application/zip")?,
            length_bytes: u64::try_from(byte_size).map_err(|_| ReparseError::Conflict)?,
        };
        // The instant the import head announced, so the fact agrees with the head it points at.
        let announced: Option<String> = sqlx::query_scalar(
            "select envelope #>> '{payload,imported_at}' from claude_archive.outbox_events
             where tenant_ref = $1 and aggregate_type = 'archive' and aggregate_id = $2
               and event_type = $3
             order by occurred_at, event_id limit 1",
        )
        .bind(deletion.owner.to_string())
        .bind(AiArchiveId(archive_id).as_entity_ref().to_string())
        .bind(ARCHIVE_IMPORTED)
        .fetch_optional(&mut **transaction)
        .await?
        .flatten();
        Ok(Self {
            archive_id,
            source_export: contract_blob_ref(&raw)?,
            imported_at: announced
                .and_then(|value| WireTimestamp::parse(&value).ok())
                .unwrap_or(deletion.tombstoned_at),
        })
    }

    fn provenance(
        &self,
        owner: TenantRef,
        provider: &AiProvider,
        stamp: (ParserName, ParserVersion),
    ) -> AiArchiveProvenance {
        AiArchiveProvenance {
            ai_archive_id: AiArchiveId(self.archive_id),
            provider: provider.clone(),
            owner,
            source_export: self.source_export.clone(),
            imported_at: self.imported_at,
            parser_name: stamp.0,
            parser_version: stamp.1,
            extensions: Extensions::new(),
        }
    }
}

/// The provenance a fact was announced with, which is all the latest fact is read for here.
#[derive(serde::Deserialize)]
struct Announced {
    import_provenance: AiArchiveProvenance,
}

/// The provenance and conversation a fact was announced with.
#[derive(serde::Deserialize)]
struct AnnouncedConversation {
    import_provenance: AiArchiveProvenance,
    conversation: AiConversation,
}

/// Whether the latest fact of this subject names one of the archives being tombstoned.
async fn announced_under_deleted(
    transaction: &mut Transaction<'_, Postgres>,
    deletion: &Deletion<'_>,
    aggregate_type: &str,
    aggregate_id: &str,
    event_types: [&str; 2],
    scope: Option<&ArtifactScope>,
) -> Result<bool, ReparseError> {
    let latest = latest_envelope(
        transaction,
        deletion.owner,
        aggregate_type,
        aggregate_id,
        event_types,
        scope,
    )
    .await?;
    Ok(latest
        .and_then(|envelope| {
            serde_json::from_value::<Announced>(serde_json::Value::Object(envelope.payload)).ok()
        })
        .is_some_and(|fact| {
            deletion
                .archives
                .contains(&fact.import_provenance.ai_archive_id.0)
        }))
}

/// The earliest retained export that presented the content each subject holds now, by
/// `received_at` and then export id.
///
/// A project or conversation row holds one version, and an export presented it when its
/// observation names the digest of the row. A subject whose observations carry no digest (an
/// Artifact) falls back to the earliest retained export that observed it, and so does a record
/// no retained export presented, which a restored row never is.
async fn earliest_retained(
    transaction: &mut Transaction<'_, Postgres>,
    kind: &str,
    ids: &[Uuid],
) -> Result<BTreeMap<Uuid, Vec<Uuid>>, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "select (array_agg(o.export_id order by (o.content_digest is not distinct from cur.digest) desc,
                                                e.received_at, e.export_id))[1], o.subject_id
         from claude_archive.export_observations o
         join claude_archive.exports e on e.export_id = o.export_id
         left join (select project_id as id, content_digest as digest
                    from claude_archive.projects
                    union all
                    select conversation_id, content_digest
                    from claude_archive.conversations) cur on cur.id = o.subject_id
         where o.subject_kind = $1 and o.subject_id = any($2)
         group by o.subject_id order by 1, 2",
    )
    .bind(kind)
    .bind(ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut by_export: BTreeMap<Uuid, Vec<Uuid>> = BTreeMap::new();
    for (export_id, subject_id) in rows {
        by_export.entry(export_id).or_default().push(subject_id);
    }
    Ok(by_export)
}

/// Announces again every survivor whose latest fact names a deleted archive, and returns how many.
///
/// Runs in the row-removal transaction, after the rows are gone, so a survivor is read as the rows
/// now stand. Projects come first, then conversations and Artifacts, the order an import uses.
pub(crate) async fn reannounce(
    transaction: &mut Transaction<'_, Postgres>,
    deletion: &Deletion<'_>,
    survivors: &Survivors,
) -> Result<usize, ReparseError> {
    if survivors.is_empty() || deletion.archives.is_empty() {
        return Ok(0);
    }
    let provider = claude()?;
    let mut facts = projects(transaction, deletion, &provider, &survivors.projects).await?;
    let (conversation_facts, stale) =
        conversations(transaction, deletion, &provider, &survivors.conversations).await?;
    facts.extend(conversation_facts);
    facts.extend(artifacts(transaction, deletion, &provider, &survivors.artifacts).await?);
    drop_stale_conversation_facts(transaction, deletion.owner, &stale).await?;
    for (ordinal, fact) in facts.iter().enumerate() {
        let at = after(transaction, deletion.tombstoned_at, ordinal).await?;
        ArchiveOutbox::enqueue_again_in(transaction, fact, at).await?;
    }
    Ok(facts.len())
}

async fn projects(
    transaction: &mut Transaction<'_, Postgres>,
    deletion: &Deletion<'_>,
    provider: &AiProvider,
    ids: &[Uuid],
) -> Result<Vec<ArchiveEventFact>, ReparseError> {
    let mut facts = Vec::new();
    let mut omitted = Omitted::default();
    for (export_id, ids) in earliest_retained(transaction, "project", ids).await? {
        let import = RetainedImport::load(transaction, export_id, deletion).await?;
        let records =
            load_projects(transaction, export_id, provider, &mut omitted, Some(&ids)).await?;
        for record in records {
            let aggregate = record.project.ai_project_id.as_entity_ref().to_string();
            let event_types = [PROJECT_ADDED, PROJECT_UPDATED];
            if announced_under_deleted(
                transaction,
                deletion,
                "project",
                &aggregate,
                event_types,
                None,
            )
            .await?
            {
                let project = record.project;
                facts.push(ArchiveEventFact::ProjectUpdated(AiProjectUpdated {
                    import_provenance: import.provenance(deletion.owner, provider, record.stamp),
                    content_digest: digest(&project)?,
                    project,
                    extensions: Extensions::new(),
                }));
            }
        }
    }
    Ok(facts)
}

/// The conversation facts to announce again, and the conversations whose earlier facts carry
/// content the restored row no longer has.
///
/// A conversation is announced again when its latest fact names a deleted archive, and also when
/// that fact, under a retained archive, differs from the restored conversation: the content parts
/// are an append-only history, so a fact built after the deleted export was imported can carry a
/// part that only the deleted export wrote. The earlier facts are removed from this service's outbox;
/// Knowledge replaces its own head only when the new fact's `observed_at` is later, which a fact
/// under a retained archive's original `imported_at` is not (an open cross-repository gap).
async fn conversations(
    transaction: &mut Transaction<'_, Postgres>,
    deletion: &Deletion<'_>,
    provider: &AiProvider,
    ids: &[Uuid],
) -> Result<(Vec<ArchiveEventFact>, Vec<String>), ReparseError> {
    let mut facts = Vec::new();
    let mut stale = Vec::new();
    let mut omitted = Omitted::default();
    for (export_id, ids) in earliest_retained(transaction, "conversation", ids).await? {
        let import = RetainedImport::load(transaction, export_id, deletion).await?;
        let records = load_conversations(
            transaction,
            export_id,
            deletion.owner,
            provider,
            &mut omitted,
            Some(&ids),
        )
        .await?;
        for conversation in records {
            let aggregate = conversation.ai_conversation_id.as_entity_ref().to_string();
            let latest = latest_envelope(
                transaction,
                deletion.owner,
                "conversation",
                &aggregate,
                [CONVERSATION_ADDED, CONVERSATION_UPDATED],
                None,
            )
            .await?
            .and_then(|envelope| {
                serde_json::from_value::<AnnouncedConversation>(serde_json::Value::Object(
                    envelope.payload,
                ))
                .ok()
            });
            let under_deleted = latest.as_ref().is_some_and(|fact| {
                deletion
                    .archives
                    .contains(&fact.import_provenance.ai_archive_id.0)
            });
            let outdated = latest
                .as_ref()
                .is_some_and(|fact| fact.conversation != conversation);
            if !(under_deleted || outdated) {
                continue;
            }
            if outdated {
                stale.push(aggregate);
            }
            let stamp = (
                conversation.parser_name.clone(),
                conversation.parser_version.clone(),
            );
            facts.push(ArchiveEventFact::ConversationUpdated(
                AiConversationUpdated {
                    import_provenance: import.provenance(deletion.owner, provider, stamp),
                    conversation,
                    extensions: Extensions::new(),
                },
            ));
        }
    }
    Ok((facts, stale))
}

/// Removes every earlier conversation fact of the aggregates whose latest fact no longer says what
/// the restored conversation says; the fact announced again replaces them.
async fn drop_stale_conversation_facts(
    transaction: &mut Transaction<'_, Postgres>,
    owner: TenantRef,
    aggregates: &[String],
) -> Result<(), ReparseError> {
    if aggregates.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "delete from claude_archive.outbox_events
         where tenant_ref = $1 and aggregate_type = 'conversation' and aggregate_id = any($2)
           and event_type = any($3)",
    )
    .bind(owner.to_string())
    .bind(aggregates)
    .bind([CONVERSATION_ADDED, CONVERSATION_UPDATED].as_slice())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    deletion: &Deletion<'_>,
    provider: &AiProvider,
    ids: &[Uuid],
) -> Result<Vec<ArchiveEventFact>, ReparseError> {
    let mut facts = Vec::new();
    let mut omitted = Omitted::default();
    for (export_id, ids) in earliest_retained(transaction, "artifact", ids).await? {
        let import = RetainedImport::load(transaction, export_id, deletion).await?;
        let records = load_artifacts(
            transaction,
            export_id,
            deletion.owner,
            provider,
            &mut omitted,
            Some(&ids),
        )
        .await?;
        for record in records {
            let identity = format!("artifact:{}", record.artifact.external_artifact_id);
            let aggregate = EntityRef::parse(&identity)
                .map_err(|_| ArchiveEventError::Identifier)?
                .to_string();
            let scope = ArtifactScope::of(&record.artifact);
            let event_types = [ARTIFACT_ADDED, ARTIFACT_UPDATED];
            if announced_under_deleted(
                transaction,
                deletion,
                "artifact",
                &aggregate,
                event_types,
                Some(&scope),
            )
            .await?
            {
                facts.push(ArchiveEventFact::ArtifactUpdated(AiArtifactUpdated {
                    import_provenance: import.provenance(
                        deletion.owner,
                        provider,
                        (record.parser_name, record.parser_version),
                    ),
                    artifact: record.artifact,
                    extensions: Extensions::new(),
                }));
            }
        }
    }
    Ok(facts)
}

/// An instant a millisecond after `tombstoned_at`, plus `ordinal` microseconds so the facts of one
/// deletion keep their order under the pump's `occurred_at, event_id` ordering.
async fn after(
    transaction: &mut Transaction<'_, Postgres>,
    tombstoned_at: WireTimestamp,
    ordinal: usize,
) -> Result<WireTimestamp, ReparseError> {
    let rendered: String = sqlx::query_scalar(&format!(
        "select to_char(($1::timestamptz + interval '1 millisecond'
                         + $2::int * interval '1 microsecond') at time zone 'UTC',
                        {TIMESTAMP_TEXT})"
    ))
    .bind(tombstoned_at.to_string())
    .bind(i32::try_from(ordinal).unwrap_or(i32::MAX))
    .fetch_one(&mut **transaction)
    .await?;
    instant(Some(rendered)).ok_or(ReparseError::Conflict)
}
