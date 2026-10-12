//! Verified deterministic replay of preserved raw Claude archives.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::contract_projection::{ImportProjection, ReportView};
use crate::entity_versions::{self, conversation_digest};
use ratatoskr_identifiers::WireTimestamp;
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    ArchiveError, ArchiveInspector, BlobRef, BlobStore, DigestAlgorithm, Limits, MediaType,
    ParsedExport, ParserCapability, ParserExecutionError, ParserExecutionInput, ParserIdentity,
    ParserRegistry, PlatformUsers, StoreError,
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
}

/// A conversation as one export presented it: the shared row and the digest of the version this
/// export observed, which is not necessarily the content the row holds now.
#[derive(Debug, Clone)]
pub(crate) struct CurrentConversation {
    digest: Option<String>,
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
    /// The blob store failed while a knowledge file was ingested. Retryable: nothing references
    /// the blob yet, so the next pass ingests the file again (XR-021 CONTRACTS.md R4-02 rule 1).
    #[error("knowledge file ingest failed")]
    KnowledgeIngest(#[source] StoreError),
    /// The exact compiled parser failed.
    #[error("reparse parser execution failed")]
    Parser(#[from] ParserExecutionError),
    /// Stable report encoding failed.
    #[error("reparse report encoding failed")]
    Encode(#[from] serde_json::Error),
    /// Artifact reconciliation refused the observed evidence.
    #[error("reparse artifact reconciliation failed")]
    Artifact(#[from] crate::ArtifactReconciliationError),
    /// An archive fact could not be built or enqueued.
    #[error("reparse archive fact could not be recorded")]
    Event(#[from] crate::ArchiveEventError),
    /// A blob the rows are about to reference could not be registered. Retryable: an erasure may
    /// have removed it, and the next pass stores it again.
    #[error("reparse blob registration failed")]
    Registration(#[from] crate::BlobRegistrationError),
}

/// Plans and applies exact parser replay.
#[derive(Debug, Clone)]
pub struct ReparseEngine {
    pool: PgPool,
    blobs: BlobStore,
    registry: Arc<ParserRegistry>,
    limits: Limits,
    platform_users: PlatformUsers,
    max_event_bytes: usize,
}

impl ReparseEngine {
    /// Creates a reparse engine.
    #[must_use]
    pub fn new(
        pool: PgPool,
        blobs: BlobStore,
        registry: Arc<ParserRegistry>,
        limits: Limits,
        platform_users: PlatformUsers,
    ) -> Self {
        Self {
            pool,
            blobs,
            registry,
            limits,
            platform_users,
            max_event_bytes: crate::config::DEFAULT_MAX_EVENT_BYTES,
        }
    }

    /// Sets the largest serialized event envelope this engine enqueues. A fact over it is not
    /// enqueued (XR-021 CONTRACTS.md R2-04).
    #[must_use]
    pub const fn with_max_event_bytes(mut self, max_event_bytes: usize) -> Self {
        self.max_event_bytes = max_event_bytes;
        self
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
        if privacy_blocked(&self.pool, tenant_id, &self.platform_users).await? {
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
        if privacy_blocked(&self.pool, plan.tenant_id, &self.platform_users).await?
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
        crate::blob_protocol::register(
            &mut tx,
            &self.blobs,
            &[&evidence_ref],
            &[plan.evidence.as_slice()],
        )
        .await?;
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
        ImportProjection {
            users: &self.platform_users,
            account_id: plan.tenant_id,
            export_id: plan.export_id,
            archive_id: plan.report.archive_id,
            raw: &plan.raw,
            parser: &plan.target,
            report: ReportView::original(&mut tx, plan.export_id).await?,
            head_always: false,
            max_event_bytes: self.max_event_bytes,
        }
        .publish(&mut tx)
        .await?;
        tx.commit().await?;
        Ok(plan.report.clone())
    }

    /// Returns the owned pool for migration orchestration.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Whether an unfinished deletion holds this account. A deletion is keyed by the Platform user
/// reference when its account is mapped and by the account id otherwise, so both are checked.
async fn privacy_blocked(
    pool: &PgPool,
    tenant_id: Uuid,
    users: &PlatformUsers,
) -> Result<bool, sqlx::Error> {
    let mut references = vec![tenant_id.to_string()];
    references.extend(users.tenant_ref_of(tenant_id));
    sqlx::query_scalar("select exists(select 1 from claude_archive.privacy_deletion_requests where tenant_ref=any($1) and state<>'completed')")
        .bind(references).fetch_one(pool).await
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
    let rows: Vec<(String, Option<String>)> = sqlx::query_as("select c.external_conversation_id,o.content_digest from claude_archive.conversations c join claude_archive.export_observations o on o.subject_kind='conversation' and o.subject_id=c.conversation_id where o.export_id=$1 order by c.external_conversation_id")
        .bind(export_id).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|(external, digest)| (external, CurrentConversation { digest }))
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
            Some(old) if old.digest.as_deref() == Some(conversation_digest(item).as_str()) => {
                ReparseChangeKind::Unchanged
            }
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
        if !matches!(
            change.kind,
            ReparseChangeKind::Added | ReparseChangeKind::Changed
        ) {
            continue;
        }
        let Some(parsed) = plan
            .parsed
            .conversations
            .iter()
            .find(|item| item.external_id == change.subject_id)
        else {
            continue;
        };
        let subject_id = write_conversation(tx, plan, parsed).await?;
        persist_messages(tx, plan.export_id, subject_id, parsed).await?;
        let revision_index:i64=sqlx::query_scalar("select coalesce(max(revision_index),-1)+1 from claude_archive.revisions where subject_kind='conversation' and subject_id=$1").bind(subject_id).fetch_one(&mut **tx).await?;
        let raw_record = serde_json::json!({"external_id":parsed.external_id,"title":parsed.title,"parser":plan.report.target_parser});
        sqlx::query("insert into claude_archive.revisions (revision_id,subject_kind,subject_id,export_id,revision_index,raw_record) values ($1,'conversation',$2,$3,$4,$5)").bind(Uuid::now_v7()).bind(subject_id).bind(plan.export_id).bind(revision_index).bind(raw_record).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Writes the shared conversation row with the newest version and records the version this
/// export presented (XR-021 CONTRACTS.md R4-03 rules 1 and 2).
///
/// The parser stamp changes with the content only, so a newer parser that reads identical content
/// leaves the published content digest stable.
async fn write_conversation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &ReparsePlan,
    parsed: &crate::Conversation,
) -> Result<Uuid, ReparseError> {
    let project_id: Option<Uuid> = match parsed.project_external_id.as_deref() {
        Some(external) => sqlx::query_scalar("select project_id from claude_archive.projects where account_id=$1 and external_project_id=$2")
            .bind(plan.tenant_id).bind(external).fetch_optional(&mut **tx).await?,
        None => None,
    };
    let created = parsed
        .created_at
        .as_deref()
        .and_then(|value| WireTimestamp::parse(value).ok())
        .map(|instant| instant.to_string());
    let digest = conversation_digest(parsed);
    let id: Uuid = sqlx::query_scalar("insert into claude_archive.conversations (conversation_id,account_id,external_conversation_id,project_id,title,upstream_state,provider_created_at,parser_name,parser_version,content_digest) values ($1,$2,$3,$4,$5,'present',$6::timestamptz,$7,$8,$9) on conflict (account_id,external_conversation_id) do update set title=excluded.title,project_id=excluded.project_id,provider_created_at=excluded.provider_created_at,parser_name=case when claude_archive.conversations.content_digest is distinct from excluded.content_digest then excluded.parser_name else claude_archive.conversations.parser_name end,parser_version=case when claude_archive.conversations.content_digest is distinct from excluded.content_digest then excluded.parser_version else claude_archive.conversations.parser_version end,content_digest=excluded.content_digest,last_seen_at=now() returning conversation_id")
        .bind(Uuid::now_v7()).bind(plan.tenant_id).bind(&parsed.external_id).bind(project_id).bind(&parsed.title)
        .bind(created).bind(&parsed.parser.parser_identifier).bind(&parsed.parser.parser_version).bind(&digest)
        .fetch_one(&mut **tx).await?;
    entity_versions::present(
        tx,
        plan.export_id,
        entity_versions::CONVERSATION,
        id,
        &digest,
        serde_json::to_value(parsed)?,
    )
    .await?;
    Ok(id)
}

/// Persists the messages and parts of one parsed conversation and links each message to its parent.
///
/// The first import and a reparse both call it, so a conversation announced downstream always has
/// the messages the fact carries. Every write is an upsert keyed by the provider ids.
pub(crate) async fn persist_messages(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    export_id: Uuid,
    conversation_id: Uuid,
    conversation: &crate::Conversation,
) -> Result<(), ReparseError> {
    let mut message_ids = BTreeMap::new();
    for (ordinal, message) in conversation.messages.iter().enumerate() {
        let role = match message.role.as_str() {
            "user" | "assistant" | "system" | "tool" | "internal" => message.role.as_str(),
            _ => "unknown",
        };
        let created = message
            .created_at
            .as_deref()
            .and_then(|value| WireTimestamp::parse(value).ok())
            .map(|instant| instant.to_string());
        let message_id: Uuid = sqlx::query_scalar("insert into claude_archive.messages (message_id,conversation_id,external_message_id,role,model,provider_created_at,source_ordinal,parser_name,parser_version) values ($1,$2,$3,$4,$5,$6::timestamptz,$7,$8,$9) on conflict (conversation_id,external_message_id) do update set role=excluded.role,model=excluded.model,last_seen_at=now() returning message_id")
            .bind(Uuid::now_v7()).bind(conversation_id).bind(&message.external_id).bind(role).bind(&message.model)
            .bind(created).bind(i32::try_from(ordinal).unwrap_or(i32::MAX))
            .bind(&message.parser.parser_identifier).bind(&message.parser.parser_version)
            .fetch_one(&mut **tx).await?;
        message_ids.insert(message.external_id.clone(), message_id);
        sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'message',$2) on conflict do nothing")
            .bind(export_id).bind(message_id).execute(&mut **tx).await?;
        for (index, part) in message.content.iter().enumerate() {
            let (kind, body, payload) = match part {
                crate::ContentPart::Text { text, .. } => ("text", Some(text.as_str()), None),
                crate::ContentPart::Markdown { markdown, .. } => {
                    ("markdown", Some(markdown.as_str()), None)
                }
                crate::ContentPart::Unknown { raw, .. } => ("unknown", None, Some(raw)),
            };
            sqlx::query("insert into claude_archive.content_parts (content_part_id,message_id,part_index,part_kind,body,payload) values ($1,$2,$3,$4,$5,$6) on conflict (message_id,part_index) do nothing")
                .bind(Uuid::now_v7()).bind(message_id).bind(i32::try_from(index).unwrap_or(i32::MAX)).bind(kind).bind(body).bind(payload).execute(&mut **tx).await?;
        }
    }
    for message in &conversation.messages {
        if let (Some(message_id), Some(parent_id)) = (
            message_ids.get(&message.external_id),
            message
                .parent_external_id
                .as_ref()
                .and_then(|parent| message_ids.get(parent)),
        ) {
            sqlx::query(
                "update claude_archive.messages set parent_message_id=$2 where message_id=$1",
            )
            .bind(message_id)
            .bind(parent_id)
            .execute(&mut **tx)
            .await?;
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
            .map(|(id, item)| (id, &item.digest))
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
