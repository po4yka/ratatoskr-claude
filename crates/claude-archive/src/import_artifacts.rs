//! Persistence of the reconciled Artifacts of an initial import, and the registration of every blob
//! the import is about to reference.
//!
//! Artifacts are first-class rows with immutable version chains. Each Artifact and version is
//! recorded as observed by the import's raw export, which is what lets scoped privacy deletion and
//! the fact projection find them.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::artifact::{ArtifactVersionAvailability, ReconciledArtifact};
use crate::initial_import::PreparedInitialImport;
use crate::reparse::{ReparseError, ReparsePlan, decode_hex};
use crate::{BlobRef, BlobStore, KnowledgeFileAvailability};

/// Takes the digest locks of every blob the import is about to reference and checks each is still
/// stored (XR-021 CONTRACTS.md R2-07 and R4-02 rule 3).
///
/// The import holds the parser evidence, the Artifact payloads and the knowledge-file bytes, so a blob an eraser removed
/// since it was stored is stored again from those bytes. Call it after the tenant lock and before
/// the first row that references a blob.
pub(crate) async fn register_blobs(
    tx: &mut Transaction<'_, Postgres>,
    store: &BlobStore,
    prepared: &PreparedInitialImport,
) -> Result<(), ReparseError> {
    let mut references: Vec<&BlobRef> = vec![&prepared.evidence_ref];
    let mut staged: Vec<&[u8]> = vec![prepared.plan.evidence.as_slice()];
    for artifact in &prepared.artifacts {
        for version in &artifact.versions {
            if let ArtifactVersionAvailability::Verified { blob_ref } = &version.availability {
                references.push(blob_ref);
            }
        }
    }
    for outcome in &prepared.knowledge_outcomes {
        match &outcome.availability {
            KnowledgeFileAvailability::Verified { blob_ref } => references.push(blob_ref),
            KnowledgeFileAvailability::Quarantined {
                evidence_blob_ref, ..
            } => references.push(evidence_blob_ref),
            KnowledgeFileAvailability::ReferencedOnly => {}
        }
    }
    for artifact in &prepared.plan.parsed.artifacts {
        staged.extend(
            artifact
                .versions
                .iter()
                .filter_map(|version| version.bytes.as_deref()),
        );
    }
    staged.extend(
        prepared
            .plan
            .parsed
            .project_knowledge_files
            .iter()
            .filter_map(|file| file.bytes.as_deref()),
    );
    crate::blob_protocol::register(tx, store, &references, &staged).await?;
    Ok(())
}

/// The one owner the schema allows an Artifact: a message, a conversation or a project.
#[derive(Debug, Clone, Copy, Default)]
struct Owner {
    message: Option<Uuid>,
    conversation: Option<Uuid>,
    project: Option<Uuid>,
}

/// Persists every reconciled Artifact of `plan`'s export inside the import transaction.
pub(crate) async fn persist_artifacts(
    tx: &mut Transaction<'_, Postgres>,
    plan: &ReparsePlan,
    artifacts: &[ReconciledArtifact],
) -> Result<(), ReparseError> {
    for artifact in artifacts {
        let owner = owner_of(tx, plan.tenant_id, artifact).await?;
        let artifact_id = insert_artifact(tx, plan.tenant_id, artifact, owner).await?;
        observe(tx, plan.export_id, "artifact", artifact_id).await?;
        insert_versions(tx, plan, artifact_id, artifact).await?;
    }
    Ok(())
}

/// The most specific owner the export named: message, else conversation, else project.
async fn owner_of(
    tx: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
    artifact: &ReconciledArtifact,
) -> Result<Owner, sqlx::Error> {
    let project = scope_id(
        tx,
        "select project_id from claude_archive.projects
         where account_id = $1 and external_project_id = $2",
        account_id,
        artifact.project_external_id.as_deref(),
    )
    .await?;
    let conversation = scope_id(
        tx,
        "select conversation_id from claude_archive.conversations
         where account_id = $1 and external_conversation_id = $2",
        account_id,
        artifact.conversation_external_id.as_deref(),
    )
    .await?;
    let message: Option<Uuid> = match (conversation, artifact.message_external_id.as_deref()) {
        (Some(conversation), Some(external)) => {
            sqlx::query_scalar(
                "select message_id from claude_archive.messages
                 where conversation_id = $1 and external_message_id = $2",
            )
            .bind(conversation)
            .bind(external)
            .fetch_optional(&mut **tx)
            .await?
        }
        _ => None,
    };
    Ok(if message.is_some() {
        Owner {
            message,
            ..Owner::default()
        }
    } else if conversation.is_some() {
        Owner {
            conversation,
            ..Owner::default()
        }
    } else {
        Owner {
            project,
            ..Owner::default()
        }
    })
}

/// Inserts the Artifact, or returns the row an earlier export of the same account created.
///
/// The identity of an Artifact is the account, its scope (message, conversation, project or none)
/// and the provider id; the schema's unique indexes make each of those scopes conflict.
async fn insert_artifact(
    tx: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
    artifact: &ReconciledArtifact,
    owner: Owner,
) -> Result<Uuid, sqlx::Error> {
    let backed_up = artifact.versions.iter().any(|version| {
        matches!(
            version.availability,
            ArtifactVersionAvailability::Verified { .. }
        )
    });
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "insert into claude_archive.artifacts
         (artifact_id, account_id, conversation_id, message_id, project_id, external_artifact_id,
          artifact_type, language, title, upstream_state, local_backup_status,
          parser_name, parser_version)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'present', $10, $11, $12)
         on conflict do nothing returning artifact_id",
    )
    .bind(Uuid::now_v7())
    .bind(account_id)
    .bind(owner.conversation)
    .bind(owner.message)
    .bind(owner.project)
    .bind(&artifact.external_id)
    .bind(&artifact.artifact_type)
    .bind(&artifact.language)
    .bind(&artifact.title)
    .bind(if backed_up {
        "locally_backed_up"
    } else {
        "reference_only"
    })
    .bind(&artifact.parser.parser_identifier)
    .bind(&artifact.parser.parser_version)
    .fetch_optional(&mut **tx)
    .await?;
    match inserted {
        Some(id) => Ok(id),
        // Another export of the same account already holds this Artifact: reuse its identity.
        None => {
            sqlx::query_scalar(
                "select artifact_id from claude_archive.artifacts
                 where account_id = $1 and external_artifact_id = $2
                   and conversation_id is not distinct from $3
                   and message_id is not distinct from $4
                   and project_id is not distinct from $5",
            )
            .bind(account_id)
            .bind(&artifact.external_id)
            .bind(owner.conversation)
            .bind(owner.message)
            .bind(owner.project)
            .fetch_one(&mut **tx)
            .await
        }
    }
}

async fn insert_versions(
    tx: &mut Transaction<'_, Postgres>,
    plan: &ReparsePlan,
    artifact_id: Uuid,
    artifact: &ReconciledArtifact,
) -> Result<(), ReparseError> {
    for (index, version) in artifact.versions.iter().enumerate() {
        let blob = match &version.availability {
            ArtifactVersionAvailability::Verified { blob_ref } => Some(blob_ref),
            _ => None,
        };
        let inserted: Option<Uuid> = sqlx::query_scalar(
            "insert into claude_archive.artifact_versions
             (version_id, artifact_id, external_version_id, previous_external_version_id,
              version_index, blob_ref, media_type, content_hash, byte_size, raw_record)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             on conflict (artifact_id, external_version_id) do nothing returning version_id",
        )
        .bind(Uuid::now_v7())
        .bind(artifact_id)
        .bind(&version.external_id)
        .bind(&version.previous_external_id)
        .bind(i64::try_from(index).map_err(|_| ReparseError::Conflict)?)
        .bind(blob.map(|blob| format!("sha256/{}", blob.digest_hex)))
        .bind(&version.media_type)
        .bind(blob.and_then(|blob| decode_hex(&blob.digest_hex)))
        .bind(blob.and_then(|blob| i64::try_from(blob.length_bytes).ok()))
        .bind(&version.raw)
        .fetch_optional(&mut **tx)
        .await?;
        let version_id = match inserted {
            Some(id) => id,
            None => {
                sqlx::query_scalar(
                    "select version_id from claude_archive.artifact_versions
                     where artifact_id = $1 and external_version_id = $2",
                )
                .bind(artifact_id)
                .bind(&version.external_id)
                .fetch_one(&mut **tx)
                .await?
            }
        };
        observe(tx, plan.export_id, "artifact_version", version_id).await?;
    }
    Ok(())
}

async fn scope_id(
    tx: &mut Transaction<'_, Postgres>,
    query: &str,
    account_id: Uuid,
    external: Option<&str>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let Some(external) = external else {
        return Ok(None);
    };
    sqlx::query_scalar(query)
        .bind(account_id)
        .bind(external)
        .fetch_optional(&mut **tx)
        .await
}

async fn observe(
    tx: &mut Transaction<'_, Postgres>,
    export_id: Uuid,
    kind: &str,
    subject_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ($1, $2, $3) on conflict do nothing",
    )
    .bind(export_id)
    .bind(kind)
    .bind(subject_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
