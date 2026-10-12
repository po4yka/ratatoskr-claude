//! The dependent closure of a deletion scope.
//!
//! The observed-subject inventory names the projects, conversations, messages and Artifacts a
//! raw export evidenced, but not what hangs off them: sources, parts, relations, assets,
//! versions, references, analysis links and unpublished facts. Deleting a parent without these
//! violates a foreign key, so the closure adds every dependent of a removed parent, with the
//! parent's action, and the blobs they carry as separate erase or retain items.

use std::collections::{BTreeSet, HashSet};

use super::*;

type ItemKey = (DeletionItemKind, String, DeletionAction);

struct Closure<'a> {
    items: &'a mut Vec<DeletionInventoryItem>,
    seen: BTreeSet<ItemKey>,
    /// Child records with a blob, so sharing can be decided once the whole closure is known.
    blobs: Vec<(DeletionItemKind, String, String)>,
}

impl Closure<'_> {
    fn push(
        &mut self,
        kind: DeletionItemKind,
        subject_id: String,
        action: DeletionAction,
        blob_ref: Option<String>,
    ) {
        if self.seen.insert((kind, subject_id.clone(), action)) {
            self.items.push(DeletionInventoryItem {
                kind,
                subject_id,
                action,
                blob_ref,
            });
        }
    }

    fn remove(&mut self, kind: DeletionItemKind, id: Uuid, blob_ref: Option<String>) {
        self.push(kind, id.to_string(), DeletionAction::RemoveRecord, blob_ref);
    }

    /// The ids of records of `kind` that the closure currently removes.
    fn removed(&self, kind: DeletionItemKind) -> Vec<Uuid> {
        self.items
            .iter()
            .filter(|item| item.kind == kind && item.action == DeletionAction::RemoveRecord)
            .filter_map(|item| Uuid::parse_str(&item.subject_id).ok())
            .collect()
    }
}

/// Adds every dependent of a removed parent to `items`, deduplicated against what is there.
///
/// Children inherit the removal of their parent, including a child another retained export also
/// observed: the parent row cannot go while a child still references it. A message under a
/// removed message is a child, whatever conversation it belongs to.
pub(super) async fn add_dependent_closure(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
    items: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    let seen = items
        .iter()
        .map(|item| (item.kind, item.subject_id.clone(), item.action))
        .collect();
    let mut closure = Closure {
        items,
        seen,
        blobs: Vec::new(),
    };
    add_messages(transaction, &mut closure).await?;
    add_project_children(transaction, &mut closure).await?;
    add_message_children(transaction, &mut closure).await?;
    add_artifact_children(transaction, &mut closure).await?;
    add_blob_decisions(transaction, &mut closure).await?;
    add_downstream(transaction, tenant_ref, &mut closure).await
}

async fn ids(
    transaction: &mut Transaction<'_, Postgres>,
    query: &str,
    parents: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar(query)
        .bind(parents)
        .fetch_all(&mut **transaction)
        .await
}

async fn id_blobs(
    transaction: &mut Transaction<'_, Postgres>,
    query: &str,
    parents: &[Uuid],
) -> Result<Vec<(Uuid, Option<String>)>, sqlx::Error> {
    if parents.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_as(query)
        .bind(parents)
        .fetch_all(&mut **transaction)
        .await
}

/// Messages of removed conversations, then every descendant of a removed message.
async fn add_messages(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    let conversations = closure.removed(DeletionItemKind::Conversation);
    for id in ids(
        transaction,
        "select message_id from claude_archive.messages
         where conversation_id = any($1) order by message_id",
        &conversations,
    )
    .await?
    {
        closure.remove(DeletionItemKind::Message, id, None);
    }
    let messages = closure.removed(DeletionItemKind::Message);
    for id in ids(
        transaction,
        "with recursive descendants(message_id) as (
             select message_id from claude_archive.messages where parent_message_id = any($1)
             union
             select m.message_id from claude_archive.messages m
             join descendants d on m.parent_message_id = d.message_id
         ) select message_id from descendants order by message_id",
        &messages,
    )
    .await?
    {
        closure.remove(DeletionItemKind::Message, id, None);
    }
    Ok(())
}

async fn add_project_children(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    let projects = closure.removed(DeletionItemKind::Project);
    for id in ids(
        transaction,
        "select source_id from claude_archive.project_sources
         where project_id = any($1) order by source_id",
        &projects,
    )
    .await?
    {
        closure.remove(DeletionItemKind::ProjectSource, id, None);
    }
    // Every removed source, whether its project goes or stays, hands its blob to the decision.
    let sources = closure.removed(DeletionItemKind::ProjectSource);
    for (id, blob) in id_blobs(
        transaction,
        "select source_id, blob_ref from claude_archive.project_sources
         where source_id = any($1) and blob_ref is not null order by source_id",
        &sources,
    )
    .await?
    {
        if let Some(blob) = blob {
            closure
                .blobs
                .push((DeletionItemKind::ProjectSource, id.to_string(), blob));
        }
    }
    for id in ids(
        transaction,
        "select artifact_id from claude_archive.artifacts
         where project_id = any($1) order by artifact_id",
        &projects,
    )
    .await?
    {
        closure.remove(DeletionItemKind::Artifact, id, None);
    }
    for id in ids(
        transaction,
        "select reference_id from claude_archive.external_references
         where project_id = any($1) order by reference_id",
        &projects,
    )
    .await?
    {
        closure.remove(DeletionItemKind::ExternalReference, id, None);
    }
    let conversations = closure.removed(DeletionItemKind::Conversation);
    for id in ids(
        transaction,
        "select artifact_id from claude_archive.artifacts
         where conversation_id = any($1) order by artifact_id",
        &conversations,
    )
    .await?
    {
        closure.remove(DeletionItemKind::Artifact, id, None);
    }
    for id in ids(
        transaction,
        "select reference_id from claude_archive.external_references
         where conversation_id = any($1) order by reference_id",
        &conversations,
    )
    .await?
    {
        closure.remove(DeletionItemKind::ExternalReference, id, None);
    }
    Ok(())
}

async fn add_message_children(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    let messages = closure.removed(DeletionItemKind::Message);
    let parts: Vec<(Uuid, String, Option<String>)> = if messages.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as(
            "select content_part_id, part_kind, blob_ref from claude_archive.content_parts
             where message_id = any($1) order by content_part_id",
        )
        .bind(&messages)
        .fetch_all(&mut **transaction)
        .await?
    };
    for (id, kind, blob) in parts {
        closure.remove(DeletionItemKind::ContentPart, id, None);
        if kind == "unknown" {
            closure.remove(DeletionItemKind::UnknownRecord, id, None);
        }
        if let Some(blob) = blob {
            closure
                .blobs
                .push((DeletionItemKind::ContentPart, id.to_string(), blob));
        }
    }
    for id in ids(
        transaction,
        "select relation_id from claude_archive.message_relations
         where parent_message_id = any($1) or child_message_id = any($1) order by relation_id",
        &messages,
    )
    .await?
    {
        closure.remove(DeletionItemKind::MessageRelation, id, None);
    }
    for id in ids(
        transaction,
        "select artifact_id from claude_archive.artifacts
         where message_id = any($1) order by artifact_id",
        &messages,
    )
    .await?
    {
        closure.remove(DeletionItemKind::Artifact, id, None);
    }
    let sources = closure.removed(DeletionItemKind::ProjectSource);
    let mut assets = id_blobs(
        transaction,
        "select asset_id, blob_ref from claude_archive.assets
         where message_id = any($1) order by asset_id",
        &messages,
    )
    .await?;
    assets.extend(
        id_blobs(
            transaction,
            "select asset_id, blob_ref from claude_archive.assets
             where source_id = any($1) order by asset_id",
            &sources,
        )
        .await?,
    );
    for (id, blob) in assets {
        closure.remove(DeletionItemKind::Asset, id, blob.clone());
        if let Some(blob) = blob {
            closure
                .blobs
                .push((DeletionItemKind::Asset, id.to_string(), blob));
        }
    }
    Ok(())
}

async fn add_artifact_children(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    let artifacts = closure.removed(DeletionItemKind::Artifact);
    for (id, blob) in id_blobs(
        transaction,
        "select version_id, blob_ref from claude_archive.artifact_versions
         where artifact_id = any($1) order by version_id",
        &artifacts,
    )
    .await?
    {
        closure.remove(DeletionItemKind::ArtifactVersion, id, None);
        if let Some(blob) = blob {
            closure
                .blobs
                .push((DeletionItemKind::ArtifactVersion, id.to_string(), blob));
        }
    }
    for id in ids(
        transaction,
        "select reference_id from claude_archive.external_references
         where artifact_id = any($1) order by reference_id",
        &artifacts,
    )
    .await?
    {
        closure.remove(DeletionItemKind::ExternalReference, id, None);
    }
    Ok(())
}

/// Erases a child's blob unless a record the deletion keeps still references it.
async fn add_blob_decisions(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    if closure.blobs.is_empty() {
        return Ok(());
    }
    let keys: Vec<String> = closure
        .blobs
        .iter()
        .map(|(_, _, blob)| blob.clone())
        .collect();
    let references: Vec<(String, String, String)> = sqlx::query_as(
        "select blob_ref, kind, id from (
             select blob_ref, 'project_source' as kind, source_id::text as id
               from claude_archive.project_sources where blob_ref = any($1)
             union all select blob_ref, 'content_part', content_part_id::text
               from claude_archive.content_parts where blob_ref = any($1)
             union all select blob_ref, 'artifact_version', version_id::text
               from claude_archive.artifact_versions where blob_ref = any($1)
             union all select blob_ref, 'asset', asset_id::text
               from claude_archive.assets where blob_ref = any($1)
             union all select blob_ref, 'raw_archive', export_id::text
               from claude_archive.exports where blob_ref = any($1)
             union all select blob_ref, 'extracted_artifact', extracted_artifact_id::text
               from claude_archive.extracted_artifacts where blob_ref = any($1)
         ) references_to_blobs order by blob_ref, kind, id",
    )
    .bind(&keys)
    .fetch_all(&mut **transaction)
    .await?;
    let removed: HashSet<(&str, String)> = closure
        .items
        .iter()
        .filter(|item| {
            matches!(
                item.action,
                DeletionAction::RemoveRecord | DeletionAction::EraseBlob
            )
        })
        .map(|item| (item.kind.as_str(), item.subject_id.clone()))
        .collect();
    let shared: HashSet<&str> = references
        .iter()
        .filter(|(_, kind, id)| !removed.contains(&(kind.as_str(), id.clone())))
        .map(|(blob, _, _)| blob.as_str())
        .collect();
    let decisions: Vec<(DeletionItemKind, String, String, bool)> = closure
        .blobs
        .iter()
        .map(|(kind, id, blob)| {
            (
                *kind,
                id.clone(),
                blob.clone(),
                shared.contains(blob.as_str()),
            )
        })
        .collect();
    for (kind, id, blob, is_shared) in decisions {
        if is_shared {
            closure.push(
                DeletionItemKind::SharedBlob,
                blob.clone(),
                DeletionAction::RetainShared,
                Some(blob),
            );
        } else {
            closure.push(kind, id, DeletionAction::EraseBlob, Some(blob));
        }
    }
    Ok(())
}

/// Analysis links, inbox rows, unpublished-or-published facts and tombstone subjects of the
/// removed conversations and Artifacts.
async fn add_downstream(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
    closure: &mut Closure<'_>,
) -> Result<(), sqlx::Error> {
    let conversations = closure.removed(DeletionItemKind::Conversation);
    let artifacts = closure.removed(DeletionItemKind::Artifact);
    let projects = closure.removed(DeletionItemKind::Project);
    let artifact_ids: Vec<(Uuid, String)> = if artifacts.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as(
            "select artifact_id, external_artifact_id from claude_archive.artifacts
             where artifact_id = any($1)",
        )
        .bind(&artifacts)
        .fetch_all(&mut **transaction)
        .await?
    };
    let mut subjects: Vec<(&'static str, String)> = conversations
        .iter()
        .map(|id| ("conversation", id.to_string()))
        .collect();
    subjects.extend(
        artifact_ids
            .iter()
            .map(|(_, external)| ("artifact", external.clone())),
    );
    let kinds: Vec<&str> = subjects.iter().map(|(kind, _)| *kind).collect();
    let subject_ids: Vec<String> = subjects.iter().map(|(_, id)| id.clone()).collect();
    // A provider Artifact id recurs across tenants, so a link is the request's only when it sits
    // under an archive of an export this deletion removes.
    let archives = scope_archives(transaction, closure).await?;
    let linked: Vec<(Uuid, String)> = sqlx::query_as(
        "select l.completion_event_id, l.subject_id
         from claude_archive.knowledge_analysis_links l
         join unnest($1::text[], $2::text[]) as s(kind, id)
           on s.kind = l.subject_kind and s.id = l.subject_id
         where l.ai_archive_id = any($3)
         order by l.completion_event_id",
    )
    .bind(&kinds)
    .bind(&subject_ids)
    .bind(&archives)
    .fetch_all(&mut **transaction)
    .await?;
    let completion_ids: Vec<Uuid> = linked.iter().map(|(id, _)| *id).collect();
    for id in &completion_ids {
        closure.remove(DeletionItemKind::AnalysisLink, *id, None);
    }
    let inbox: Vec<Uuid> = ids(
        transaction,
        "select event_id from claude_archive.inbox_events
         where event_id = any($1) order by event_id",
        &completion_ids,
    )
    .await?;
    for id in inbox {
        closure.remove(DeletionItemKind::Inbox, id, None);
    }
    // Every removed conversation and project is tombstoned, linked to Knowledge or not.
    for id in conversations.iter().chain(projects.iter()) {
        closure.push(
            DeletionItemKind::DownstreamTombstone,
            id.to_string(),
            DeletionAction::EmitTombstone,
            None,
        );
    }
    let mut seen_subjects = BTreeSet::new();
    for (_, subject_id) in linked {
        if seen_subjects.insert(subject_id.clone()) {
            closure.push(
                DeletionItemKind::DownstreamTombstone,
                subject_id,
                DeletionAction::EmitTombstone,
                None,
            );
        }
    }
    let mut aggregates: Vec<String> = conversations
        .iter()
        .map(|id| format!("ai_conversation:{id}"))
        .chain(projects.iter().map(|id| format!("ai_project:{id}")))
        .collect();
    aggregates.extend(archive_aggregates(&archives));
    let outbox: Vec<Uuid> = sqlx::query_scalar(
        "select event_id from claude_archive.outbox_events
         where tenant_ref = $1 and aggregate_id = any($2)
           and event_type <> 'ai_archive.subject.tombstoned.v1'
         order by event_id",
    )
    .bind(tenant_ref)
    .bind(&aggregates)
    .fetch_all(&mut **transaction)
    .await?;
    for id in outbox
        .into_iter()
        .chain(artifact_facts(transaction, tenant_ref, &artifacts).await?)
    {
        closure.remove(DeletionItemKind::Outbox, id, None);
    }
    Ok(())
}

/// The facts of the removed Artifacts, matched by aggregate and by the scope the fact carries.
///
/// A provider id such as `react-component` is minted per conversation or project, so two retained
/// and removed Artifacts of one tenant can share the aggregate `artifact:<id>`; only the facts
/// whose `conversation_ref` and `project_ref` are those of a removed Artifact leave the outbox.
async fn artifact_facts(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
    artifacts: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    if artifacts.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar(
        "select distinct o.event_id
         from claude_archive.artifacts a
         left join claude_archive.messages m on m.message_id = a.message_id
         join claude_archive.outbox_events o
           on o.tenant_ref = $1 and o.aggregate_type = 'artifact'
          and o.aggregate_id = 'artifact:' || a.external_artifact_id
          and o.event_type <> 'ai_archive.subject.tombstoned.v1'
          and (o.envelope #>> '{payload,artifact,conversation_ref}') is not distinct from
              ('ai_conversation:' || coalesce(a.conversation_id, m.conversation_id)::text)
          and (o.envelope #>> '{payload,artifact,project_ref}') is not distinct from
              ('ai_project:' || a.project_id::text)
         where a.artifact_id = any($2)
         order by o.event_id",
    )
    .bind(tenant_ref)
    .bind(artifacts)
    .fetch_all(&mut **transaction)
    .await
}

/// The archives of the exports the scope deletes.
async fn scope_archives(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &Closure<'_>,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let exports: Vec<Uuid> = closure
        .items
        .iter()
        .filter(|item| item.kind == DeletionItemKind::RawArchive)
        .filter_map(|item| Uuid::parse_str(&item.subject_id).ok())
        .collect();
    ids(
        transaction,
        "select distinct ai_archive_id from claude_archive.exports
         where export_id = any($1) order by ai_archive_id",
        &exports,
    )
    .await
}

/// The archive aggregates of those archives. Their import heads name a `source_export` whose
/// bytes the deletion erases, so the heads leave the outbox with them.
fn archive_aggregates(archives: &[Uuid]) -> Vec<String> {
    archives
        .iter()
        .map(|id| {
            ratatoskr_identifiers::AiArchiveId(*id)
                .as_entity_ref()
                .to_string()
        })
        .collect()
}
