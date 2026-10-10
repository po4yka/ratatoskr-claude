//! Loaders that read the persisted import rows back as contract records, and the decision
//! whether each record is a new fact, a changed fact or nothing.

use super::*;

pub(super) const CONVERSATION_ADDED: &str = "ai_archive.conversation.added.v1";
pub(super) const CONVERSATION_UPDATED: &str = "ai_archive.conversation.updated.v1";
pub(super) const PROJECT_ADDED: &str = "ai_archive.project.added.v1";
pub(super) const PROJECT_UPDATED: &str = "ai_archive.project.updated.v1";
pub(super) const ARTIFACT_ADDED: &str = "ai_archive.artifact.added.v1";
pub(super) const ARTIFACT_UPDATED: &str = "ai_archive.artifact.updated.v1";
pub(super) const ARCHIVE_IMPORTED: &str = "ai_archive.archive.imported.v1";
const ROLE_WARNING_CODE: &str = "ai_archive.message_role_unrepresentable";

type MessageRow = (
    Uuid,
    Option<String>,
    Option<Uuid>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);
type ConversationRow = (
    Uuid,
    String,
    Option<String>,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    Option<String>,
);
type ProjectRow = (
    Uuid,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);
type ArtifactRow = (
    Uuid,
    String,
    Option<String>,
    Option<Uuid>,
    Option<Uuid>,
    Option<String>,
    Option<String>,
);

pub(super) const TIMESTAMP_TEXT: &str = "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'";

pub(super) struct ProjectRecord {
    pub(super) project: AiProject,
    pub(super) stamp: (ParserName, ParserVersion),
}

pub(super) async fn load_projects(
    transaction: &mut Transaction<'_, Postgres>,
    export_id: Uuid,
    provider: &AiProvider,
    omitted: &mut Omitted,
    only: Option<&[Uuid]>,
) -> Result<Vec<ProjectRecord>, ReparseError> {
    let rows: Vec<ProjectRow> = sqlx::query_as(
        "select p.project_id, p.external_project_id, p.title, p.description, p.instructions,
                p.parser_name, p.parser_version
         from claude_archive.projects p
         join claude_archive.export_observations o
           on o.subject_kind = 'project' and o.subject_id = p.project_id
         where o.export_id = $1 and ($2::uuid[] is null or p.project_id = any($2))
         order by p.external_project_id",
    )
    .bind(export_id)
    .bind(only)
    .fetch_all(&mut **transaction)
    .await?;
    let mut records = Vec::new();
    for (project_id, external, title, description, instructions, name, version) in rows {
        let Some(stamp) = stamp(name, version)? else {
            tracing::warn!(
                class = "project_unstamped",
                "a project row has no parser stamp"
            );
            omitted.projects += 1;
            continue;
        };
        let Some(title) = title
            .as_deref()
            .and_then(|value| AiTitle::parse(value).ok())
            .or_else(|| AiTitle::parse(&external).ok())
        else {
            // The contract requires a title, and neither the name nor the provider id can be one.
            // The project is named as a gap; failing the import would be retried forever.
            tracing::warn!(
                class = "project_unnamed",
                "a project has neither a name nor an id the contract can hold"
            );
            omitted.projects += 1;
            continue;
        };
        records.push(ProjectRecord {
            project: AiProject {
                ai_project_id: AiProjectId(project_id),
                provider: provider.clone(),
                external_project_id: EntityLocalId::parse(&external).ok(),
                title,
                description: description
                    .as_deref()
                    .and_then(|value| AiText::parse(value).ok()),
                instructions: instructions
                    .as_deref()
                    .and_then(|value| AiText::parse(value).ok()),
                provider_created_at: None,
                provider_updated_at: None,
                parser_name: stamp.0.clone(),
                parser_version: stamp.1.clone(),
                extensions: Extensions::new(),
            },
            stamp,
        });
    }
    Ok(records)
}

/// The decision for one subject against the latest fact already enqueued for its identity.
enum Decision {
    Unchanged,
    Added,
    Updated,
}

/// The part of an Artifact fact that tells two Artifacts of one provider id apart.
///
/// A provider Artifact id such as `react-component` is minted per conversation or project, so it
/// recurs inside one tenant and the aggregate (`artifact:<id>`) alone cannot say which Artifact an
/// earlier fact described. The scope refs are what the wire carries to separate them.
pub(super) struct ArtifactScope {
    conversation_ref: Option<String>,
    project_ref: Option<String>,
}

impl ArtifactScope {
    pub(super) fn of(artifact: &AiArtifact) -> Self {
        Self {
            conversation_ref: artifact.conversation_ref.as_ref().map(ToString::to_string),
            project_ref: artifact.project_ref.as_ref().map(ToString::to_string),
        }
    }
}

/// The latest fact already enqueued for one identity of one tenant.
///
/// The lookup never crosses tenants: another tenant's facts for the same aggregate id (an Artifact
/// id recurs across tenants) must not decide whether this tenant's subject is new. An Artifact
/// lookup additionally matches the scope, so two Artifacts that share an id are not each other's
/// previous revision.
pub(super) async fn latest_envelope(
    transaction: &mut Transaction<'_, Postgres>,
    owner: TenantRef,
    aggregate_type: &str,
    aggregate_id: &str,
    event_types: [&str; 2],
    scope: Option<&ArtifactScope>,
) -> Result<Option<EventEnvelope>, ReparseError> {
    let stored: Option<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where tenant_ref = $1 and aggregate_type = $2 and aggregate_id = $3
           and event_type = any($4)
           and (not $5
                or ((envelope #>> '{payload,artifact,conversation_ref}') is not distinct from $6
                    and (envelope #>> '{payload,artifact,project_ref}') is not distinct from $7))
         order by occurred_at desc, event_id desc limit 1",
    )
    .bind(owner.to_string())
    .bind(aggregate_type)
    .bind(aggregate_id)
    .bind(event_types.as_slice())
    .bind(scope.is_some())
    .bind(scope.and_then(|scope| scope.conversation_ref.as_deref()))
    .bind(scope.and_then(|scope| scope.project_ref.as_deref()))
    .fetch_optional(&mut **transaction)
    .await?;
    stored
        .map(serde_json::from_value)
        .transpose()
        .map_err(ReparseError::from)
}

/// Whether the latest head enqueued for this archive already says what `head` says about
/// completeness.
pub(super) async fn head_already_announced(
    transaction: &mut Transaction<'_, Postgres>,
    head: &AiArchiveImport,
) -> Result<bool, ReparseError> {
    let aggregate = head.ai_archive_id.as_entity_ref().to_string();
    let latest = latest_envelope(
        transaction,
        head.owner,
        "archive",
        &aggregate,
        [ARCHIVE_IMPORTED, ARCHIVE_IMPORTED],
        None,
    )
    .await?
    .and_then(|envelope| envelope.payload_as::<AiArchiveImport>().ok());
    Ok(latest.is_some_and(|known| known.completeness_report == head.completeness_report))
}

pub(super) async fn project_fact(
    transaction: &mut Transaction<'_, Postgres>,
    provenance: AiArchiveProvenance,
    record: ProjectRecord,
) -> Result<Option<ArchiveEventFact>, ReparseError> {
    let project = record.project;
    let content_digest = digest(&project)?;
    let aggregate = project.ai_project_id.as_entity_ref().to_string();
    let latest = latest_envelope(
        transaction,
        provenance.owner,
        "project",
        &aggregate,
        [PROJECT_ADDED, PROJECT_UPDATED],
        None,
    )
    .await?
    .and_then(|envelope| {
        envelope
            .payload_as::<AiProjectAdded>()
            .map(|added| added.project)
            .or_else(|_| {
                envelope
                    .payload_as::<AiProjectUpdated>()
                    .map(|updated| updated.project)
            })
            .ok()
    });
    Ok(match decide(latest, &project) {
        Decision::Unchanged => None,
        Decision::Added => Some(ArchiveEventFact::ProjectAdded(AiProjectAdded {
            import_provenance: provenance,
            project,
            content_digest,
            extensions: Extensions::new(),
        })),
        Decision::Updated => Some(ArchiveEventFact::ProjectUpdated(AiProjectUpdated {
            import_provenance: provenance,
            project,
            content_digest,
            extensions: Extensions::new(),
        })),
    })
}

fn decide<T: PartialEq>(latest: Option<T>, current: &T) -> Decision {
    match latest {
        None => Decision::Added,
        Some(known) if &known == current => Decision::Unchanged,
        Some(_) => Decision::Updated,
    }
}

pub(super) async fn load_conversations(
    transaction: &mut Transaction<'_, Postgres>,
    export_id: Uuid,
    owner: TenantRef,
    provider: &AiProvider,
    omitted: &mut Omitted,
    only: Option<&[Uuid]>,
) -> Result<Vec<AiConversation>, ReparseError> {
    let rows: Vec<ConversationRow> = sqlx::query_as(&format!(
        "select c.conversation_id, c.external_conversation_id, c.title, c.project_id,
                to_char(c.provider_created_at at time zone 'UTC', {TIMESTAMP_TEXT}),
                c.parser_name, c.parser_version
         from claude_archive.conversations c
         join claude_archive.export_observations o
           on o.subject_kind = 'conversation' and o.subject_id = c.conversation_id
         where o.export_id = $1 and ($2::uuid[] is null or c.conversation_id = any($2))
         order by c.external_conversation_id"
    ))
    .bind(export_id)
    .bind(only)
    .fetch_all(&mut **transaction)
    .await?;
    let mut conversations = Vec::new();
    for (conversation_id, external, title, project_id, created, name, version) in rows {
        let Some(stamp) = stamp(name, version)? else {
            tracing::warn!(
                class = "conversation_unstamped",
                "a conversation row has no parser stamp"
            );
            omitted.conversations += 1;
            continue;
        };
        let Some(messages) = load_messages(transaction, conversation_id).await? else {
            omitted.conversations += 1;
            continue;
        };
        let content_digest = AiConversation::compute_content_digest(&messages.messages)
            .map_err(|_| ArchiveEventError::Identifier)?;
        conversations.push(AiConversation {
            ai_conversation_id: AiConversationId(conversation_id),
            provider: provider.clone(),
            external_conversation_id: EntityLocalId::parse(&external).ok(),
            owner,
            project_ref: project_id.map(|id| AiProjectId(id).as_entity_ref()),
            title: title
                .as_deref()
                .and_then(|value| AiTitle::parse(value).ok()),
            provider_created_at: instant(created),
            provider_updated_at: None,
            messages: messages.messages,
            content_digest,
            parser_name: stamp.0,
            parser_version: stamp.1,
            warnings: messages.warnings,
            extensions: Extensions::new(),
        });
    }
    Ok(conversations)
}

struct Messages {
    messages: Vec<AiMessage>,
    warnings: Vec<WarningEnvelope>,
}

/// The messages of one conversation in presentation order, or `None` when a row has no stamp.
///
/// A message whose provider role is `internal` or unknown has no contract author role: it is left
/// out, the conversation records one warning, and its children are re-parented to the nearest
/// represented ancestor.
async fn load_messages(
    transaction: &mut Transaction<'_, Postgres>,
    conversation_id: Uuid,
) -> Result<Option<Messages>, ReparseError> {
    let rows: Vec<MessageRow> = sqlx::query_as(&format!(
        "select message_id, external_message_id, parent_message_id, role, model,
                to_char(provider_created_at at time zone 'UTC', {TIMESTAMP_TEXT}),
                parser_name, parser_version
         from claude_archive.messages where conversation_id = $1
         order by source_ordinal nulls last, first_seen_at, message_id"
    ))
    .bind(conversation_id)
    .fetch_all(&mut **transaction)
    .await?;
    let message_ids: Vec<Uuid> = rows.iter().map(|row| row.0).collect();
    let part_rows: Vec<(Uuid, String, Option<String>, Option<serde_json::Value>)> = sqlx::query_as(
        "select message_id, part_kind, body, payload from claude_archive.content_parts
             where message_id = any($1) order by message_id, part_index",
    )
    .bind(&message_ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut parts: HashMap<Uuid, Vec<AiContentPart>> = HashMap::new();
    for (message_id, kind, body, payload) in part_rows {
        parts
            .entry(message_id)
            .or_default()
            .push(content_part(&kind, body, payload));
    }
    let parents: HashMap<Uuid, Option<Uuid>> = rows.iter().map(|row| (row.0, row.2)).collect();
    let represented: HashSet<Uuid> = rows
        .iter()
        .filter(|row| author_role(&row.3).is_some())
        .map(|row| row.0)
        .collect();
    let externals: HashMap<Uuid, EntityLocalId> = rows
        .iter()
        .filter_map(|row| {
            let id = row
                .1
                .as_deref()
                .and_then(|value| EntityLocalId::parse(value).ok());
            id.map(|id| (row.0, id))
        })
        .collect();
    let mut messages = Vec::new();
    let mut left_out = false;
    for (message_id, _, parent, role, model, created, name, version) in &rows {
        let Some(author_role) = author_role(role) else {
            left_out = true;
            continue;
        };
        let Some((parser_name, parser_version)) = stamp(name.clone(), version.clone())? else {
            tracing::warn!(
                class = "message_unstamped",
                "a message row has no parser stamp"
            );
            return Ok(None);
        };
        let Some(external) = externals.get(message_id) else {
            tracing::warn!(
                class = "message_unidentified",
                "a message row has no usable provider id"
            );
            return Ok(None);
        };
        let mut ancestor = *parent;
        let mut hops = 0_usize;
        while let Some(candidate) = ancestor {
            if represented.contains(&candidate) || hops > rows.len() {
                break;
            }
            ancestor = parents.get(&candidate).copied().flatten();
            hops += 1;
        }
        messages.push(AiMessage {
            external_message_id: external.clone(),
            author_role,
            parent_message_id: ancestor
                .filter(|candidate| represented.contains(candidate))
                .and_then(|candidate| externals.get(&candidate).cloned()),
            parts: parts.remove(message_id).unwrap_or_default(),
            model: model
                .as_deref()
                .and_then(|value| AiModelName::parse(value).ok()),
            provider_created_at: instant(created.clone()),
            parser_name,
            parser_version,
            extensions: Extensions::new(),
        });
    }
    let warnings = if left_out {
        warning(
            ROLE_WARNING_CODE,
            "A message with an author role the contract cannot represent was left out.",
        )
        .into_iter()
        .collect()
    } else {
        Vec::new()
    };
    Ok(Some(Messages { messages, warnings }))
}

fn author_role(role: &str) -> Option<AiAuthorRole> {
    match role {
        "user" => Some(AiAuthorRole::User),
        "assistant" => Some(AiAuthorRole::Assistant),
        "system" => Some(AiAuthorRole::System),
        "tool" => Some(AiAuthorRole::Tool),
        _ => None,
    }
}

/// One persisted part as a contract part. A part the contract grammar cannot hold travels
/// verbatim as an unknown part instead of being dropped or rewritten.
fn content_part(
    kind: &str,
    body: Option<String>,
    payload: Option<serde_json::Value>,
) -> AiContentPart {
    if let Some(raw) = payload.filter(|_| kind == "unknown") {
        return AiContentPart::Unknown(raw);
    }
    let text = body.unwrap_or_default();
    match (kind, AiText::parse(&text)) {
        ("text", Ok(text)) => AiContentPart::Text { text },
        ("markdown", Ok(markdown)) => AiContentPart::Markdown { markdown },
        _ => AiContentPart::Unknown(serde_json::json!({
            "part_kind": "unrepresentable_part",
            "source_kind": kind,
            "text": text,
        })),
    }
}

pub(super) async fn conversation_fact(
    transaction: &mut Transaction<'_, Postgres>,
    provenance: AiArchiveProvenance,
    conversation: AiConversation,
) -> Result<Option<ArchiveEventFact>, ReparseError> {
    let aggregate = conversation.ai_conversation_id.as_entity_ref().to_string();
    let latest = latest_envelope(
        transaction,
        conversation.owner,
        "conversation",
        &aggregate,
        [CONVERSATION_ADDED, CONVERSATION_UPDATED],
        None,
    )
    .await?
    .and_then(|envelope| {
        envelope
            .payload_as::<AiConversationAdded>()
            .map(|added| added.conversation)
            .or_else(|_| {
                envelope
                    .payload_as::<AiConversationUpdated>()
                    .map(|updated| updated.conversation)
            })
            .ok()
    });
    Ok(match decide(latest, &conversation) {
        Decision::Unchanged => None,
        Decision::Added => Some(ArchiveEventFact::ConversationAdded(AiConversationAdded {
            import_provenance: provenance,
            conversation,
            extensions: Extensions::new(),
        })),
        Decision::Updated => Some(ArchiveEventFact::ConversationUpdated(
            AiConversationUpdated {
                import_provenance: provenance,
                conversation,
                extensions: Extensions::new(),
            },
        )),
    })
}

pub(super) struct ArtifactRecord {
    pub(super) artifact: AiArtifact,
    pub(super) parser_name: ParserName,
    pub(super) parser_version: ParserVersion,
}

pub(super) async fn load_artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    export_id: Uuid,
    owner: TenantRef,
    provider: &AiProvider,
    omitted: &mut Omitted,
    only: Option<&[Uuid]>,
) -> Result<Vec<ArtifactRecord>, ReparseError> {
    let rows: Vec<ArtifactRow> = sqlx::query_as(
        "select a.artifact_id, a.external_artifact_id, a.title,
                coalesce(a.conversation_id, m.conversation_id), a.project_id,
                a.parser_name, a.parser_version
         from claude_archive.artifacts a
         left join claude_archive.messages m on m.message_id = a.message_id
         join claude_archive.export_observations o
           on o.subject_kind = 'artifact' and o.subject_id = a.artifact_id
         where o.export_id = $1 and ($2::uuid[] is null or a.artifact_id = any($2))
         order by a.external_artifact_id",
    )
    .bind(export_id)
    .bind(only)
    .fetch_all(&mut **transaction)
    .await?;
    let mut records = Vec::new();
    for (artifact_id, external, title, conversation_id, project_id, name, version) in rows {
        let Some((parser_name, parser_version)) = stamp(name, version)? else {
            tracing::warn!(
                class = "artifact_unstamped",
                "an Artifact row has no parser stamp"
            );
            omitted.artifacts += 1;
            continue;
        };
        let latest: Option<(String, Option<String>, Option<i64>)> = sqlx::query_as(
            "select blob_ref, media_type, byte_size from claude_archive.artifact_versions
             where artifact_id = $1 and blob_ref is not null
             order by version_index desc limit 1",
        )
        .bind(artifact_id)
        .fetch_optional(&mut **transaction)
        .await?;
        let Some((blob_key, Some(media_type), Some(length))) = latest else {
            // No verified bytes: the contract carries an Artifact by content blob only.
            omitted.artifacts += 1;
            continue;
        };
        let Some(hex) = blob_key.strip_prefix("sha256/") else {
            omitted.artifacts += 1;
            continue;
        };
        let content_digest = ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(hex).map_err(|_| ArchiveEventError::Identifier)?,
        };
        let content_blob = serde_json::from_value(serde_json::json!({
            "owner_service": crate::blob_store::OWNER_SERVICE,
            "digest": {"algorithm": "sha256", "hex": hex},
            "media_type": media_type,
            "length_bytes": length,
        }))
        .map_err(|_| ArchiveEventError::Identifier)?;
        let Ok(external_artifact_id) = EntityLocalId::parse(&external) else {
            omitted.artifacts += 1;
            continue;
        };
        records.push(ArtifactRecord {
            artifact: AiArtifact {
                external_artifact_id,
                provider: provider.clone(),
                owner,
                title: title
                    .as_deref()
                    .and_then(|value| AiTitle::parse(value).ok()),
                artifact_kind: ratatoskr_ai_archive_contracts::AiAssetKind::parse("artifact")
                    .map_err(|_| ArchiveEventError::Identifier)?,
                content_blob,
                content_digest,
                project_ref: project_id.map(|id| AiProjectId(id).as_entity_ref()),
                conversation_ref: conversation_id.map(|id| AiConversationId(id).as_entity_ref()),
                parser_name: parser_name.clone(),
                parser_version: parser_version.clone(),
                extensions: Extensions::new(),
            },
            parser_name,
            parser_version,
        });
    }
    Ok(records)
}

pub(super) async fn artifact_fact(
    transaction: &mut Transaction<'_, Postgres>,
    provenance: AiArchiveProvenance,
    record: ArtifactRecord,
) -> Result<Option<ArchiveEventFact>, ReparseError> {
    let artifact = record.artifact;
    let aggregate = EntityRef::parse(&format!("artifact:{}", artifact.external_artifact_id))
        .map_err(|_| ArchiveEventError::Identifier)?
        .to_string();
    let scope = ArtifactScope::of(&artifact);
    let latest = latest_envelope(
        transaction,
        artifact.owner,
        "artifact",
        &aggregate,
        [ARTIFACT_ADDED, ARTIFACT_UPDATED],
        Some(&scope),
    )
    .await?
    .and_then(|envelope| {
        envelope
            .payload_as::<AiArtifactAdded>()
            .map(|added| added.artifact)
            .or_else(|_| {
                envelope
                    .payload_as::<AiArtifactUpdated>()
                    .map(|updated| updated.artifact)
            })
            .ok()
    });
    Ok(match decide(latest, &artifact) {
        Decision::Unchanged => None,
        Decision::Added => Some(ArchiveEventFact::ArtifactAdded(AiArtifactAdded {
            import_provenance: provenance,
            artifact,
            extensions: Extensions::new(),
        })),
        Decision::Updated => Some(ArchiveEventFact::ArtifactUpdated(AiArtifactUpdated {
            import_provenance: provenance,
            artifact,
            extensions: Extensions::new(),
        })),
    })
}
