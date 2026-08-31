//! Verified deterministic replay of preserved raw Claude archives.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    ArchiveError, ArchiveInspector, BlobRef, BlobStore, DigestAlgorithm, Limits, MediaType,
    ParsedExport, ParserCapability, ParserExecutionError, ParserExecutionInput, ParserIdentity,
    ParserRegistry, StoreError,
};

/// Classification of one normalized subject comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReparseChangeKind {
    /// Newly evidenced subject.
    Added,
    /// Existing subject changed.
    Changed,
    /// Existing subject is equivalent.
    Unchanged,
    /// Existing subject was omitted and remains retained.
    ProposedRemoval,
}

/// One deterministic subject comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReparseChange {
    /// Normalized subject class.
    pub subject_kind: String,
    /// Stable provider identity.
    pub subject_id: String,
    /// Comparison result.
    pub kind: ReparseChangeKind,
}

/// Content-free coverage warning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReparseWarning {
    /// Stable warning code.
    pub code: String,
    /// Related provider identity when available.
    pub subject_id: Option<String>,
}

/// Stable report shared by dry-run and apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReparseReport {
    /// Selected archive identity.
    pub archive_id: Uuid,
    /// Exact target parser.
    pub target_parser: String,
    /// Immutable raw digest.
    pub raw_digest: String,
    /// Sorted comparisons.
    pub changes: Vec<ReparseChange>,
    /// Sorted warnings.
    pub warnings: Vec<ReparseWarning>,
    /// Sorted proposed downstream subjects.
    pub event_subjects: Vec<String>,
    /// Conservative completeness class.
    pub completeness: String,
}

/// Immutable comparison plan bound to safety-relevant inputs.
#[derive(Debug, Clone)]
pub struct ReparsePlan {
    /// Operator-visible report.
    pub report: ReparseReport,
    pub(crate) tenant_id: Uuid,
    pub(crate) export_id: Uuid,
    pub(crate) target: ParserIdentity,
    pub(crate) raw: BlobRef,
    pub(crate) registry_fingerprint: Vec<u8>,
    pub(crate) projection_fingerprint: Vec<u8>,
    pub(crate) plan_fingerprint: Vec<u8>,
    pub(crate) evidence: Vec<u8>,
    pub(crate) parsed: ParsedExport,
    pub(crate) current: BTreeMap<String, CurrentConversation>,
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentConversation {
    id: Uuid,
    title: String,
}

type ExportReplayRow = (Uuid, String, Vec<u8>, i64, String, String, String);

/// Reparse failure without private content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReparseError {
    /// Selected input is unavailable, stale, or incompatible.
    #[error("reparse input is unavailable or incompatible")]
    Conflict,
    /// Owned persistence failed.
    #[error("reparse persistence failed")]
    Store(#[from] sqlx::Error),
    /// Raw evidence failed integrity or hostile inspection.
    #[error("reparse raw evidence failed inspection")]
    Intake(#[from] ArchiveError),
    /// Blob storage failed.
    #[error("reparse blob operation failed")]
    Blob(#[from] StoreError),
    /// The exact compiled parser failed.
    #[error("reparse parser execution failed")]
    Parser(#[from] ParserExecutionError),
    /// Stable report encoding failed.
    #[error("reparse report encoding failed")]
    Encode(#[from] serde_json::Error),
}

/// Plans and applies exact parser replay.
#[derive(Debug, Clone)]
pub struct ReparseEngine {
    pool: PgPool,
    blobs: BlobStore,
    registry: Arc<ParserRegistry>,
    limits: Limits,
}

impl ReparseEngine {
    /// Creates a reparse engine.
    #[must_use]
    pub fn new(
        pool: PgPool,
        blobs: BlobStore,
        registry: Arc<ParserRegistry>,
        limits: Limits,
    ) -> Self {
        Self {
            pool,
            blobs,
            registry,
            limits,
        }
    }

    /// Produces a side-effect-free plan from verified raw evidence.
    ///
    /// # Errors
    /// Returns [`ReparseError`] for unavailable or invalid evidence.
    pub async fn plan(
        &self,
        tenant_id: Uuid,
        archive_id: Uuid,
        target: ParserIdentity,
    ) -> Result<ReparsePlan, ReparseError> {
        if privacy_blocked(&self.pool, tenant_id).await? {
            return Err(ReparseError::Conflict);
        }
        let row: Option<ExportReplayRow> = sqlx::query_as(
            "select export_id,blob_ref,archive_hash,byte_size,acquisition,coalesce(detected_schema,''),coalesce(parser_version,'') from claude_archive.exports where ai_archive_id=$1 and (account_ref=$2 or organization_ref=$2)")
            .bind(archive_id).bind(tenant_id).fetch_optional(&self.pool).await?;
        let Some((export_id, blob_key, raw_hash, byte_size, acquisition, schema, current_version)) =
            row
        else {
            return Err(ReparseError::Conflict);
        };
        let detected = crate::DetectedSchema::new(
            crate::AcquisitionMode::parse(&acquisition).ok_or(ReparseError::Conflict)?,
            schema,
        );
        let capabilities = [ParserCapability::Conversations];
        let compatible = self.registry.compatible_versions(&detected, &capabilities);
        let current_index = compatible
            .iter()
            .position(|item| item.version() == current_version)
            .ok_or(ReparseError::Conflict)?;
        let target_index = compatible
            .iter()
            .position(|item| item == &target)
            .ok_or(ReparseError::Conflict)?;
        if target_index <= current_index {
            return Err(ReparseError::Conflict);
        }
        let compiled = self
            .registry
            .find_exact(&target, &detected, &capabilities)
            .ok_or(ReparseError::Conflict)?;
        let digest = encode_hex(&raw_hash);
        if blob_key != format!("sha256/{digest}") || byte_size < 0 {
            return Err(ReparseError::Conflict);
        }
        let raw = BlobRef {
            owner_service: crate::blob_store::OWNER_SERVICE.to_owned(),
            algorithm: DigestAlgorithm::Sha256,
            digest_hex: digest.clone(),
            media_type: MediaType::parse("application/zip")?,
            length_bytes: u64::try_from(byte_size).map_err(|_| ReparseError::Conflict)?,
        };
        self.blobs.verify(&raw)?;
        let inventory = ArchiveInspector::new(&self.limits).inspect(&self.blobs, &raw)?;
        let evidence = read_parser_evidence(&self.blobs.read(&raw)?, &inventory)?;
        let parsed = compiled.execute(ParserExecutionInput {
            detected_schema: &detected,
            evidence: &evidence,
        })?;
        if parsed.parser.parser_identifier != target.identifier()
            || parsed.parser.parser_version != target.version()
        {
            return Err(ReparseError::Conflict);
        }
        let current = current_projection(&self.pool, export_id).await?;
        let registry_fingerprint = digest_json(
            &compatible
                .iter()
                .map(|id| format!("{}@{}", id.identifier(), id.version()))
                .collect::<Vec<_>>(),
        )?;
        let projection_fingerprint = digest_current(&current)?;
        let report = compare(archive_id, &target, &digest, &current, &parsed);
        let plan_fingerprint = digest_json(&report)?;
        Ok(ReparsePlan {
            report,
            tenant_id,
            export_id,
            target,
            raw,
            registry_fingerprint,
            projection_fingerprint,
            plan_fingerprint,
            evidence,
            parsed,
            current,
        })
    }

    /// Applies an unchanged plan atomically, or returns the prior report.
    ///
    /// # Errors
    /// Returns [`ReparseError`] when fingerprints changed or persistence fails.
    pub async fn apply(&self, plan: &ReparsePlan) -> Result<ReparseReport, ReparseError> {
        if let Some(report) = prior_report(&self.pool, plan).await? {
            return Ok(report);
        }
        if privacy_blocked(&self.pool, plan.tenant_id).await?
            || digest_current(&current_projection(&self.pool, plan.export_id).await?)?
                != plan.projection_fingerprint
        {
            return Err(ReparseError::Conflict);
        }
        self.blobs.verify(&plan.raw)?;
        let evidence_ref = self
            .blobs
            .store(MediaType::parse("application/json")?, &plan.evidence)?;
        let report_json = serde_json::to_value(&plan.report)?;
        let state = if plan.report.changes.iter().any(|change| {
            matches!(
                change.kind,
                ReparseChangeKind::Added | ReparseChangeKind::Changed
            )
        }) {
            "applied"
        } else {
            "unchanged"
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query("select pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(plan.tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        if let Some(report) = prior_report_in(&mut tx, plan).await? {
            tx.rollback().await?;
            return Ok(report);
        }
        sqlx::query("insert into claude_archive.extracted_artifacts (extracted_artifact_id,export_id,artifact_index,artifact_kind,blob_ref,content_hash,byte_size) values ($1,$2,0,'entry',$3,$4,$5) on conflict (export_id,artifact_index) do nothing")
            .bind(Uuid::now_v7()).bind(plan.export_id).bind(format!("sha256/{}",evidence_ref.digest_hex))
            .bind(decode_hex(&evidence_ref.digest_hex).ok_or(ReparseError::Conflict)?)
            .bind(i64::try_from(evidence_ref.length_bytes).map_err(|_| ReparseError::Conflict)?).execute(&mut *tx).await?;
        apply_conversations(&mut tx, plan).await?;
        let import_run_id = Uuid::now_v7();
        sqlx::query("insert into claude_archive.import_runs (run_id,export_id,state,warnings,finished_at) values ($1,$2,'completed',$3,now())")
            .bind(import_run_id).bind(plan.export_id).bind(serde_json::to_value(&plan.report.warnings)?).execute(&mut *tx).await?;
        sqlx::query("insert into claude_archive.completeness_reports (report_id,run_id,status,discovered_counts,missing_assets,unknown_variants,warnings) values ($1,$2,$3,$4,0,0,$5)")
            .bind(Uuid::now_v7()).bind(import_run_id).bind(&plan.report.completeness)
            .bind(serde_json::json!({"conversations":plan.parsed.conversations.len()}))
            .bind(serde_json::to_value(&plan.report.warnings)?).execute(&mut *tx).await?;
        sqlx::query("insert into claude_archive.reparse_runs (reparse_run_id,tenant_ref,export_id,parser_name,parser_version,raw_fingerprint,registry_fingerprint,projection_fingerprint,plan_fingerprint,dry_run,state,report,correlation_id,completed_at) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,false,$10,$11,$12,now())")
            .bind(Uuid::now_v7()).bind(plan.tenant_id.to_string()).bind(plan.export_id).bind(plan.target.identifier()).bind(plan.target.version())
            .bind(decode_hex(&plan.raw.digest_hex).ok_or(ReparseError::Conflict)?).bind(&plan.registry_fingerprint)
            .bind(&plan.projection_fingerprint).bind(&plan.plan_fingerprint).bind(state).bind(report_json)
            .bind(format!("reparse:{}",plan.report.archive_id)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(plan.report.clone())
    }

    /// Returns the owned pool for migration orchestration.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn privacy_blocked(pool: &PgPool, tenant_id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("select exists(select 1 from claude_archive.privacy_deletion_requests where tenant_ref=$1 and state<>'completed')")
        .bind(tenant_id.to_string()).fetch_one(pool).await
}

pub(crate) fn read_parser_evidence(
    raw: &[u8],
    inventory: &crate::ArchiveInventory,
) -> Result<Vec<u8>, ReparseError> {
    let selected = inventory
        .entries()
        .iter()
        .find(|entry| {
            std::path::Path::new(entry.path())
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        })
        .ok_or(ReparseError::Conflict)?;
    let mut zip =
        zip::ZipArchive::new(std::io::Cursor::new(raw)).map_err(|_| ArchiveError::InvalidZip)?;
    let mut entry = zip
        .by_name(selected.path())
        .map_err(|_| ArchiveError::InvalidZip)?;
    let mut evidence = Vec::new();
    entry.read_to_end(&mut evidence).map_err(ArchiveError::Io)?;
    Ok(evidence)
}

pub(crate) async fn current_projection(
    pool: &PgPool,
    export_id: Uuid,
) -> Result<BTreeMap<String, CurrentConversation>, sqlx::Error> {
    let rows: Vec<(Uuid,String,Option<String>)> = sqlx::query_as("select c.conversation_id,c.external_conversation_id,c.title from claude_archive.conversations c join claude_archive.export_observations o on o.subject_kind='conversation' and o.subject_id=c.conversation_id where o.export_id=$1 order by c.external_conversation_id")
        .bind(export_id).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|(id, external, title)| {
            (
                external,
                CurrentConversation {
                    id,
                    title: title.unwrap_or_default(),
                },
            )
        })
        .collect())
}

pub(crate) fn compare(
    archive_id: Uuid,
    target: &ParserIdentity,
    raw_digest: &str,
    current: &BTreeMap<String, CurrentConversation>,
    parsed: &ParsedExport,
) -> ReparseReport {
    let parsed_map = parsed
        .conversations
        .iter()
        .map(|item| (item.external_id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let mut changes = Vec::new();
    let mut warnings = Vec::new();
    let mut event_subjects = Vec::new();
    for (id, item) in &parsed_map {
        let kind = match current.get(id) {
            None => ReparseChangeKind::Added,
            Some(old) if old.title == item.title => ReparseChangeKind::Unchanged,
            Some(_) => ReparseChangeKind::Changed,
        };
        if matches!(kind, ReparseChangeKind::Added | ReparseChangeKind::Changed) {
            event_subjects.push(format!("conversation:{id}"));
        }
        changes.push(ReparseChange {
            subject_kind: "conversation".to_owned(),
            subject_id: id.clone(),
            kind,
        });
    }
    for id in current.keys().filter(|id| !parsed_map.contains_key(*id)) {
        changes.push(ReparseChange {
            subject_kind: "conversation".to_owned(),
            subject_id: id.clone(),
            kind: ReparseChangeKind::ProposedRemoval,
        });
        warnings.push(ReparseWarning {
            code: "coverage_omission".to_owned(),
            subject_id: Some(id.clone()),
        });
    }
    changes.sort_by(|a, b| (&a.subject_kind, &a.subject_id).cmp(&(&b.subject_kind, &b.subject_id)));
    warnings.sort_by(|a, b| (&a.code, &a.subject_id).cmp(&(&b.code, &b.subject_id)));
    event_subjects.sort();
    ReparseReport {
        archive_id,
        target_parser: format!("{}@{}", target.identifier(), target.version()),
        raw_digest: raw_digest.to_owned(),
        changes,
        warnings,
        event_subjects,
        completeness: if current.keys().all(|id| parsed_map.contains_key(id)) {
            "conversations_complete"
        } else {
            "structurally_partial"
        }
        .to_owned(),
    }
}

pub(crate) async fn apply_conversations(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &ReparsePlan,
) -> Result<(), ReparseError> {
    for change in &plan.report.changes {
        let Some(parsed) = plan
            .parsed
            .conversations
            .iter()
            .find(|item| item.external_id == change.subject_id)
        else {
            continue;
        };
        let subject_id = if let Some(current) = plan.current.get(&change.subject_id) {
            current.id
        } else {
            let id = Uuid::now_v7();
            sqlx::query("insert into claude_archive.conversations (conversation_id,account_id,external_conversation_id,title,upstream_state) values ($1,$2,$3,$4,'present')").bind(id).bind(plan.tenant_id).bind(&parsed.external_id).bind(&parsed.title).execute(&mut **tx).await?;
            sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'conversation',$2) on conflict do nothing").bind(plan.export_id).bind(id).execute(&mut **tx).await?;
            id
        };
        if change.kind == ReparseChangeKind::Changed {
            sqlx::query("update claude_archive.conversations set title=$1,last_seen_at=now() where conversation_id=$2").bind(&parsed.title).bind(subject_id).execute(&mut **tx).await?;
        }
        if matches!(
            change.kind,
            ReparseChangeKind::Added | ReparseChangeKind::Changed
        ) {
            let revision_index:i64=sqlx::query_scalar("select coalesce(max(revision_index),-1)+1 from claude_archive.revisions where subject_kind='conversation' and subject_id=$1").bind(subject_id).fetch_one(&mut **tx).await?;
            let raw_record = serde_json::json!({"external_id":parsed.external_id,"title":parsed.title,"parser":plan.report.target_parser});
            sqlx::query("insert into claude_archive.revisions (revision_id,subject_kind,subject_id,export_id,revision_index,raw_record) values ($1,'conversation',$2,$3,$4,$5)").bind(Uuid::now_v7()).bind(subject_id).bind(plan.export_id).bind(revision_index).bind(raw_record).execute(&mut **tx).await?;
            let envelope = serde_json::json!({"archive_id":plan.report.archive_id,"subject_kind":"conversation","subject_id":subject_id,"parser":plan.report.target_parser});
            let bytes = serde_json::to_vec(&envelope)?;
            sqlx::query("insert into claude_archive.outbox_events (event_id,event_type,aggregate_type,aggregate_id,envelope,payload_digest,correlation_id,tenant_ref,occurred_at) values ($1,'claude.subject.reparsed.v1','conversation',$2,$3,$4,$5,$6,now()) on conflict (event_type,payload_digest) do nothing").bind(Uuid::now_v7()).bind(plan.export_id.to_string()).bind(envelope).bind(Sha256::digest(bytes).as_slice()).bind(format!("reparse:{}",plan.report.archive_id)).bind(plan.tenant_id.to_string()).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

async fn prior_report(
    pool: &PgPool,
    plan: &ReparsePlan,
) -> Result<Option<ReparseReport>, ReparseError> {
    let value:Option<serde_json::Value>=sqlx::query_scalar("select report from claude_archive.reparse_runs where export_id=$1 and parser_name=$2 and parser_version=$3 and raw_fingerprint=$4 and registry_fingerprint=$5 and projection_fingerprint=$6").bind(plan.export_id).bind(plan.target.identifier()).bind(plan.target.version()).bind(decode_hex(&plan.raw.digest_hex).ok_or(ReparseError::Conflict)?).bind(&plan.registry_fingerprint).bind(&plan.projection_fingerprint).fetch_optional(pool).await?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}
async fn prior_report_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &ReparsePlan,
) -> Result<Option<ReparseReport>, ReparseError> {
    let value:Option<serde_json::Value>=sqlx::query_scalar("select report from claude_archive.reparse_runs where export_id=$1 and parser_name=$2 and parser_version=$3 and raw_fingerprint=$4 and registry_fingerprint=$5 and projection_fingerprint=$6").bind(plan.export_id).bind(plan.target.identifier()).bind(plan.target.version()).bind(decode_hex(&plan.raw.digest_hex).ok_or(ReparseError::Conflict)?).bind(&plan.registry_fingerprint).bind(&plan.projection_fingerprint).fetch_optional(&mut **tx).await?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}
fn digest_current(
    value: &BTreeMap<String, CurrentConversation>,
) -> Result<Vec<u8>, serde_json::Error> {
    digest_json(
        &value
            .iter()
            .map(|(id, item)| (id, &item.title))
            .collect::<Vec<_>>(),
    )
}
fn digest_json(value: &impl Serialize) -> Result<Vec<u8>, serde_json::Error> {
    Ok(Sha256::digest(serde_json::to_vec(value)?).to_vec())
}
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}
pub(crate) fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if value.len() != 64 {
        return None;
    }
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|part| u8::from_str_radix(part, 16).ok())
        })
        .collect()
}
