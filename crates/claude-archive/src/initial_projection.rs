//! The projection of the first import of an export: projects, their knowledge sources and the
//! conversation graph, written inside the import transaction.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::reparse::{ReparseError, ReparsePlan, apply_conversations, decode_hex};
use crate::{KnowledgeFileAvailability, ProjectKnowledgeFile, ProjectKnowledgeIngestResult};

type Tx<'a> = Transaction<'a, Postgres>;

/// What the source row records of one knowledge file (XR-021 CONTRACTS.md R4-02 rule 2).
struct SourceBlob {
    blob_ref: Option<String>,
    content_hash: Option<Vec<u8>>,
    byte_size: Option<i64>,
    locally_backed_up: bool,
}

impl SourceBlob {
    /// A verified file is a backup, a quarantined file keeps its evidence blob without being one,
    /// and a reference holds no blob.
    fn of(
        file: &ProjectKnowledgeFile,
        outcome: &ProjectKnowledgeIngestResult,
    ) -> Result<Self, ReparseError> {
        let (blob, backed_up) = match &outcome.availability {
            KnowledgeFileAvailability::Verified { blob_ref } => (Some(blob_ref), true),
            KnowledgeFileAvailability::Quarantined {
                evidence_blob_ref, ..
            } => (Some(evidence_blob_ref), false),
            KnowledgeFileAvailability::ReferencedOnly => (None, false),
        };
        Ok(match blob {
            Some(blob) => Self {
                blob_ref: Some(format!("sha256/{}", blob.digest_hex)),
                content_hash: Some(decode_hex(&blob.digest_hex).ok_or(ReparseError::Conflict)?),
                byte_size: Some(
                    i64::try_from(blob.length_bytes).map_err(|_| ReparseError::Conflict)?,
                ),
                locally_backed_up: backed_up,
            },
            None => Self {
                blob_ref: None,
                content_hash: None,
                byte_size: file
                    .bytes
                    .as_ref()
                    .and_then(|bytes| i64::try_from(bytes.len()).ok()),
                locally_backed_up: false,
            },
        })
    }

    /// Verified over quarantined over reference only.
    const fn rank(&self) -> u8 {
        if self.locally_backed_up {
            2
        } else if self.blob_ref.is_some() {
            1
        } else {
            0
        }
    }
}

/// Writes the projects, the knowledge sources and the conversations of the export being imported.
///
/// `outcomes` holds one ingest outcome per parsed knowledge file, in file order.
pub(crate) async fn persist_initial_projection(
    tx: &mut Tx<'_>,
    plan: &ReparsePlan,
    outcomes: &[ProjectKnowledgeIngestResult],
) -> Result<(), ReparseError> {
    if outcomes.len() != plan.parsed.project_knowledge_files.len() {
        return Err(ReparseError::Conflict);
    }
    for project in &plan.parsed.projects {
        let instructions = plan
            .parsed
            .project_instructions
            .iter()
            .find(|instruction| instruction.project_external_id == project.external_id)
            .map(|instruction| instruction.text.as_str());
        let project_id: Uuid = sqlx::query_scalar("insert into claude_archive.projects (project_id,account_id,external_project_id,title,description,instructions,parser_name,parser_version,upstream_state) values ($1,$2,$3,$4,$5,$6,$7,$8,'present') on conflict (account_id,external_project_id) do update set title=excluded.title,description=excluded.description,instructions=excluded.instructions,parser_name=excluded.parser_name,parser_version=excluded.parser_version,updated_at=now() returning project_id")
            .bind(Uuid::now_v7()).bind(plan.tenant_id).bind(&project.external_id).bind(&project.name)
            .bind(&project.description).bind(instructions)
            .bind(&project.parser.parser_identifier).bind(&project.parser.parser_version)
            .fetch_one(&mut **tx).await?;
        sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'project',$2) on conflict do nothing")
            .bind(plan.export_id).bind(project_id).execute(&mut **tx).await?;
        let files = plan
            .parsed
            .project_knowledge_files
            .iter()
            .zip(outcomes)
            .filter(|(file, _)| file.project_external_id == project.external_id);
        for (file, outcome) in files {
            persist_source(tx, plan.export_id, project_id, file, outcome).await?;
        }
        refresh_backup_status(tx, project_id).await?;
    }
    apply_conversations(tx, plan).await?;
    Ok(())
}

/// Upserts one source row and records the importing export as having observed it.
///
/// A later export that supplies the bytes upgrades a source an earlier export only referenced; an
/// export that only references the file leaves better evidence alone.
async fn persist_source(
    tx: &mut Tx<'_>,
    export_id: Uuid,
    project_id: Uuid,
    file: &ProjectKnowledgeFile,
    outcome: &ProjectKnowledgeIngestResult,
) -> Result<(), ReparseError> {
    let incoming = SourceBlob::of(file, outcome)?;
    let existing: Option<(bool, bool)> = sqlx::query_as(
        "select locally_backed_up, blob_ref is not null from claude_archive.project_sources
         where project_id = $1 and external_source_id = $2",
    )
    .bind(project_id)
    .bind(&file.external_id)
    .fetch_optional(&mut **tx)
    .await?;
    let upgrades = existing.is_none_or(|(backed_up, has_blob)| {
        let held = if backed_up { 2 } else { u8::from(has_blob) };
        incoming.rank() >= held
    });
    let source_id: Uuid = sqlx::query_scalar(
        "insert into claude_archive.project_sources
         (source_id, project_id, external_source_id, source_kind, title, mime_type, byte_size,
          content_hash, blob_ref, locally_backed_up)
         values ($1, $2, $3, 'file', $4, $5, $6, $7, $8, $9)
         on conflict (project_id, external_source_id) do update set
             title = excluded.title, mime_type = excluded.mime_type, last_seen_at = now(),
             byte_size = case when $10 then excluded.byte_size
                              else claude_archive.project_sources.byte_size end,
             content_hash = case when $10 then excluded.content_hash
                                 else claude_archive.project_sources.content_hash end,
             blob_ref = case when $10 then excluded.blob_ref
                             else claude_archive.project_sources.blob_ref end,
             locally_backed_up = case when $10 then excluded.locally_backed_up
                                      else claude_archive.project_sources.locally_backed_up end
         returning source_id",
    )
    .bind(Uuid::now_v7())
    .bind(project_id)
    .bind(&file.external_id)
    .bind(&file.filename)
    .bind(&file.media_type)
    .bind(incoming.byte_size)
    .bind(&incoming.content_hash)
    .bind(&incoming.blob_ref)
    .bind(incoming.locally_backed_up)
    .bind(upgrades)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
         values ($1, 'project_source', $2) on conflict do nothing",
    )
    .bind(export_id)
    .bind(source_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// `locally_backed_up` exactly when the project has at least one source and all are verified
/// (XR-021 CONTRACTS.md R4-02 rule 5).
pub(crate) async fn refresh_backup_status(
    tx: &mut Tx<'_>,
    project_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update claude_archive.projects set local_backup_status = case
             when exists (select 1 from claude_archive.project_sources s
                          where s.project_id = $1)
              and not exists (select 1 from claude_archive.project_sources s
                              where s.project_id = $1 and not s.locally_backed_up)
             then 'locally_backed_up' else 'reference_only' end
         where project_id = $1",
    )
    .bind(project_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
