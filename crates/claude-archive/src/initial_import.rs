//! Restart-safe initial Claude archive import and terminal operation reporting.

use std::sync::Arc;

use ratatoskr_ai_archive_contracts::{
    AiArchiveCompleteness, AiArchiveOperationSummary, AiProvider,
};
use ratatoskr_error_contracts::{ErrorCode, WarningEnvelope};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_identifiers::{
    AiArchiveId, EntityRef, EventId, Extensions, OperationId, SafeMessage, WireTimestamp,
};
use ratatoskr_operation_contracts::{
    OperationReported, OperationResultKind, OperationResultRef, OperationStatus,
};
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use ratatoskr_ai_archive_contracts::platform_receipt::incomplete_import_warning;

use crate::contract_projection::{GapInput, ImportProjection, Omitted, ReportView, gaps_for};
use crate::import_artifacts::{persist_artifacts, register_blobs};
use crate::initial_projection::persist_initial_projection;
use crate::reparse::{
    ReparseError, ReparsePlan, compare, current_projection, decode_hex, encode_hex,
    read_parser_evidence,
};
use crate::{
    ArchiveInspector, BlobRef, BlobStore, DigestAlgorithm, Limits, MediaType, ParserCapability,
    ParserExecutionInput, ParserIdentity, ParserRegistry, PlatformUsers,
};

/// The warning code of an import that left facts out because they exceed the event limit.
const FACTS_TOO_LARGE_WARNING_CODE: &str = "ai_archive.facts.too_large";
/// The user-facing text of [`FACTS_TOO_LARGE_WARNING_CODE`].
const FACTS_TOO_LARGE_WARNING_MESSAGE: &str =
    "Some conversations are too large to add to the library.";

/// Restart-safe executor for stored raw archives and late operation bindings.
#[derive(Debug, Clone)]
pub struct InitialImportWorker {
    pool: PgPool,
    blobs: BlobStore,
    registry: Arc<ParserRegistry>,
    limits: Limits,
    platform_users: PlatformUsers,
    max_event_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct PreparedInitialImport {
    pub(crate) plan: ReparsePlan,
    pub(crate) evidence_ref: BlobRef,
    completeness: crate::ArchiveCompletenessReport,
    detected: crate::DetectedSchema,
    pub(crate) artifacts: Vec<crate::ReconciledArtifact>,
    /// One ingest outcome per parsed knowledge file, in file order.
    pub(crate) knowledge_outcomes: Vec<crate::ProjectKnowledgeIngestResult>,
}

#[derive(Debug)]
struct InitialImportInput {
    export_id: Uuid,
    archive_id: Uuid,
    account_id: Uuid,
    blob_key: String,
    hash: Vec<u8>,
    byte_size: i64,
    mode: String,
}

impl InitialImportWorker {
    /// Creates a worker over process-owned durable dependencies.
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

    /// Sets the largest serialized event envelope this worker enqueues. A fact over it is not
    /// enqueued and the import is reported as partial (XR-021 CONTRACTS.md R2-04).
    #[must_use]
    pub const fn with_max_event_bytes(mut self, max_event_bytes: usize) -> Self {
        self.max_event_bytes = max_event_bytes;
        self
    }

    /// Processes at most one import or one set of late operation correlations.
    ///
    /// # Errors
    ///
    /// Returns [`ReparseError`] when durable evidence, parsing, or persistence fails.
    pub async fn process_pending_once(&self) -> Result<usize, ReparseError> {
        type PendingRow = (
            Uuid,
            Uuid,
            Uuid,
            Option<Uuid>,
            String,
            Vec<u8>,
            i64,
            String,
            String,
        );
        let row: Option<PendingRow> = sqlx::query_as(
            "select r.run_id,e.export_id,e.ai_archive_id,e.account_ref,e.blob_ref,e.archive_hash,
                    e.byte_size,e.acquisition,r.state
             from claude_archive.import_runs r join claude_archive.exports e on e.export_id=r.export_id
             where r.state in ('received','stored','inspecting','schema_detected','extracting',
                               'staging','validating','reconciling','publishing')
                or (r.state in ('completed','partial','failed') and exists (
                    select 1 from claude_archive.platform_operation_imports p
                    where p.import_run_id=r.run_id and p.reported_at is null))
             order by r.started_at,r.run_id limit 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some((
            run_id,
            export_id,
            archive_id,
            account_id,
            blob_key,
            hash,
            byte_size,
            mode,
            state,
        )) = row
        else {
            return Ok(0);
        };
        if state == "failed" {
            // A later upload of the same bytes bound its operation to this failed run: it ends
            // `failed` now, with the report the first failure sent (CONTRACTS.md R3-04 rule 2).
            report_failed_operations(&self.pool, run_id).await?;
            return Ok(1);
        }
        if matches!(state.as_str(), "completed" | "partial") {
            let summary = load_summary(&self.pool, run_id).await?;
            enqueue_reports(&self.pool, run_id, export_id, archive_id, summary).await?;
            return Ok(1);
        }
        let account_id = account_id.ok_or(ReparseError::Conflict)?;
        let prepared = self
            .prepare_initial_import(InitialImportInput {
                export_id,
                archive_id,
                account_id,
                blob_key,
                hash,
                byte_size,
                mode,
            })
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) if is_permanent_import_error(&error) => {
                mark_permanent_failure(&self.pool, run_id, failure_class(&error)).await?;
                return Ok(1);
            }
            Err(error) => return Err(error),
        };
        self.persist_initial_import(run_id, archive_id, &prepared)
            .await?;
        Ok(1)
    }

    async fn prepare_initial_import(
        &self,
        input: InitialImportInput,
    ) -> Result<PreparedInitialImport, ReparseError> {
        let digest = encode_hex(&input.hash);
        if input.blob_key != format!("sha256/{digest}") || input.byte_size < 0 {
            return Err(ReparseError::Conflict);
        }
        let raw = BlobRef {
            owner_service: crate::blob_store::OWNER_SERVICE.to_owned(),
            algorithm: DigestAlgorithm::Sha256,
            digest_hex: digest,
            media_type: MediaType::parse("application/zip")?,
            length_bytes: u64::try_from(input.byte_size).map_err(|_| ReparseError::Conflict)?,
        };
        self.blobs.verify(&raw)?;
        let inventory = ArchiveInspector::new(&self.limits).inspect(&self.blobs, &raw)?;
        let evidence = read_parser_evidence(&self.blobs.read(&raw)?, &inventory)?;
        let schema = serde_json::from_slice::<serde_json::Value>(&evidence)
            .ok()
            .and_then(|value| {
                value
                    .get("schema")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or(ReparseError::Conflict)?;
        let acquisition =
            crate::AcquisitionMode::parse(&input.mode).ok_or(ReparseError::Conflict)?;
        let detected = crate::DetectedSchema::new(acquisition, schema);
        let capabilities = [
            ParserCapability::Projects,
            ParserCapability::ProjectInstructions,
            ParserCapability::ProjectKnowledgeFiles,
            ParserCapability::Conversations,
            ParserCapability::Messages,
            ParserCapability::ContentParts,
            ParserCapability::Artifacts,
        ];
        let descriptor = self
            .registry
            .select(&detected, &capabilities)
            .map_err(|_| ReparseError::Conflict)?;
        let identity = ParserIdentity::new(descriptor.identifier(), descriptor.version());
        let parser = self
            .registry
            .find_exact(&identity, &detected, &capabilities)
            .ok_or(ReparseError::Conflict)?;
        let parsed = parser.execute(ParserExecutionInput {
            detected_schema: &detected,
            evidence: &evidence,
        })?;
        let artifacts =
            crate::ArtifactReconciler::new(&self.blobs).reconcile(std::slice::from_ref(&parsed))?;
        let ingestor = crate::ProjectKnowledgeIngestor::new(&self.blobs);
        let knowledge_outcomes = parsed
            .project_knowledge_files
            .iter()
            .map(|file| ingestor.ingest(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ReparseError::KnowledgeIngest)?;
        let completeness = crate::ArchiveCompletenessReport::from_ingest(
            input.archive_id.to_string(),
            &parsed,
            &knowledge_outcomes,
        );
        let evidence_ref = self
            .blobs
            .store(MediaType::parse("application/json")?, &evidence)?;
        let current = current_projection(&self.pool, input.export_id).await?;
        let report = compare(
            input.archive_id,
            &identity,
            &raw.digest_hex,
            &current,
            &parsed,
        );
        Ok(PreparedInitialImport {
            plan: ReparsePlan {
                report,
                tenant_id: input.account_id,
                export_id: input.export_id,
                target: identity,
                raw,
                registry_fingerprint: Vec::new(),
                projection_fingerprint: Vec::new(),
                plan_fingerprint: Vec::new(),
                evidence,
                parsed,
                current,
            },
            evidence_ref,
            completeness,
            detected,
            artifacts,
            knowledge_outcomes,
        })
    }

    async fn persist_initial_import(
        &self,
        run_id: Uuid,
        archive_id: Uuid,
        prepared: &PreparedInitialImport,
    ) -> Result<(), ReparseError> {
        let summary = summary_from(&prepared.completeness);
        let warnings = serde_json::to_value(
            prepared
                .completeness
                .warnings
                .iter()
                .map(|warning| &warning.code)
                .collect::<Vec<_>>(),
        )?;
        let counts = serde_json::json!({
            "projects": prepared.completeness.counts.projects,
            "project_instructions": prepared.completeness.counts.project_instructions,
            "knowledge_references": prepared.completeness.counts.knowledge_references,
            "verified_knowledge_files": prepared.completeness.counts.verified_knowledge_files,
            "missing_knowledge_files": prepared.completeness.counts.missing_knowledge_files,
            "quarantined_knowledge_files": prepared.completeness.counts.quarantined_knowledge_files,
            "conversations": prepared.completeness.counts.conversations,
            "messages": prepared.completeness.counts.messages,
            "unknown_variants": prepared.completeness.counts.unknown_variants,
            "locally_backed_up_entities": prepared.completeness.counts.locally_backed_up_entities,
            "reference_only_entities": prepared.completeness.counts.reference_only_entities,
        });
        let status = completeness_status(prepared.completeness.status);
        let terminal_state = if prepared.completeness.status == crate::CompletenessStatus::Complete
        {
            "completed"
        } else {
            "partial"
        };

        let mut tx = self.pool.begin().await?;
        sqlx::query("select pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(prepared.plan.tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let current_state: String = sqlx::query_scalar(
            "select state from claude_archive.import_runs where run_id=$1 for update",
        )
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;
        if matches!(current_state.as_str(), "completed" | "partial") {
            tx.rollback().await?;
            return Ok(());
        }
        register_blobs(&mut tx, &self.blobs, prepared).await?;
        sqlx::query("insert into claude_archive.extracted_artifacts (extracted_artifact_id,export_id,artifact_index,artifact_kind,blob_ref,content_hash,byte_size) values ($1,$2,0,'entry',$3,$4,$5) on conflict (export_id,artifact_index) do nothing")
            .bind(Uuid::now_v7()).bind(prepared.plan.export_id).bind(format!("sha256/{}",prepared.evidence_ref.digest_hex))
            .bind(decode_hex(&prepared.evidence_ref.digest_hex).ok_or(ReparseError::Conflict)?)
            .bind(i64::try_from(prepared.evidence_ref.length_bytes).map_err(|_| ReparseError::Conflict)?).execute(&mut *tx).await?;
        persist_initial_projection(&mut tx, &prepared.plan, &prepared.knowledge_outcomes).await?;
        persist_artifacts(&mut tx, &prepared.plan, &prepared.artifacts).await?;
        sqlx::query("insert into claude_archive.completeness_reports (report_id,run_id,status,discovered_counts,missing_assets,unknown_variants,warnings) values ($1,$2,$3,$4,$5,$6,$7) on conflict (run_id) do nothing")
            .bind(Uuid::now_v7()).bind(run_id).bind(status).bind(&counts)
            .bind(i32::try_from(prepared.completeness.counts.missing_knowledge_files).unwrap_or(i32::MAX))
            .bind(i32::try_from(prepared.completeness.counts.unknown_variants).unwrap_or(i32::MAX))
            .bind(&warnings).execute(&mut *tx).await?;
        sqlx::query("update claude_archive.exports set detected_schema=$2,parser_version=$3 where export_id=$1")
            .bind(prepared.plan.export_id).bind(prepared.detected.identifier()).bind(prepared.plan.target.version()).execute(&mut *tx).await?;
        sqlx::query("update claude_archive.import_runs set state=$2,warnings=$3,finished_at=now() where run_id=$1")
            .bind(run_id).bind(terminal_state).bind(warnings).execute(&mut *tx).await?;
        let omitted = ImportProjection {
            users: &self.platform_users,
            account_id: prepared.plan.tenant_id,
            export_id: prepared.plan.export_id,
            archive_id,
            raw: &prepared.plan.raw,
            parser: &prepared.plan.target,
            report: ReportView {
                status: status.to_owned(),
                counts,
                warnings: prepared
                    .completeness
                    .warnings
                    .iter()
                    .map(|warning| warning.code.clone())
                    .collect(),
            },
            head_always: true,
            max_event_bytes: self.max_event_bytes,
        }
        .publish(&mut tx)
        .await?;
        enqueue_reports_in(
            &mut tx,
            run_id,
            prepared.plan.export_id,
            archive_id,
            summary.with_omitted(omitted),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

const fn is_permanent_import_error(error: &ReparseError) -> bool {
    matches!(
        error,
        ReparseError::Conflict
            | ReparseError::Intake(_)
            | ReparseError::Parser(_)
            | ReparseError::Encode(_)
            | ReparseError::Artifact(
                crate::ArtifactReconciliationError::IdentityConflict { .. }
                    | crate::ArtifactReconciliationError::VersionConflict { .. }
                    | crate::ArtifactReconciliationError::MissingPredecessor { .. }
                    | crate::ArtifactReconciliationError::VersionCycle { .. },
            )
            | ReparseError::Blob(
                crate::StoreError::Collision { .. }
                    | crate::StoreError::Mismatch { .. }
                    | crate::StoreError::Missing { .. }
                    | crate::StoreError::InvalidMediaType
                    | crate::StoreError::InvalidIdentity
                    | crate::StoreError::LimitExceeded { .. }
                    | crate::StoreError::EmptyInput
                    | crate::StoreError::DeclaredIdentityMismatch
            )
    )
}

/// The closed class a permanently failed import is logged under; never a path, name or content.
const fn failure_class(error: &ReparseError) -> &'static str {
    match error {
        ReparseError::Intake(_) => "archive_invalid",
        ReparseError::Parser(_) => "parse_failed",
        ReparseError::Conflict => "reparse_conflict",
        ReparseError::Encode(_) => "encode_failed",
        ReparseError::Artifact(_) => "artifact_conflict",
        ReparseError::Blob(_) => "blob_invalid",
        _ => "import_failed",
    }
}

async fn mark_permanent_failure(
    pool: &PgPool,
    run_id: Uuid,
    class: &'static str,
) -> Result<(), ReparseError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "update claude_archive.import_runs
         set state='failed', warnings='[\"claude.archive.invalid\"]'::jsonb, finished_at=now()
         where run_id=$1 and state not in ('completed','partial','failed')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
    report_failed_operations_in(&mut tx, run_id).await?;
    tx.commit().await?;
    tracing::warn!(class, run_id = %run_id, "an archive import failed permanently");
    Ok(())
}

/// Reports the operations bound to a run that failed earlier and were not reported yet.
async fn report_failed_operations(pool: &PgPool, run_id: Uuid) -> Result<(), ReparseError> {
    let mut tx = pool.begin().await?;
    report_failed_operations_in(&mut tx, run_id).await?;
    tx.commit().await?;
    Ok(())
}

/// Enqueues the `failed` operation report of every operation bound to a failed run that has not
/// been reported yet, and marks each operation reported.
async fn report_failed_operations_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
) -> Result<(), ReparseError> {
    let operations: Vec<Uuid> = sqlx::query_scalar(
        "select operation_id from claude_archive.platform_operation_imports
         where import_run_id=$1 and reported_at is null order by operation_id",
    )
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    for operation_id in operations {
        let report: OperationReported = serde_json::from_value(serde_json::json!({
            "operation_id": operation_id,
            "status": "failed",
            "error": {
                "code": "claude.archive.invalid",
                "message": "The Claude archive could not be imported.",
                "retryable": false,
                "correlation_id": format!("operation:{operation_id}")
            }
        }))?;
        let envelope = operation_report_envelope(&report)?;
        let payload = serde_json::to_vec(&envelope.payload)?;
        sqlx::query("insert into claude_archive.outbox_events (event_id,event_type,aggregate_type,aggregate_id,envelope,payload_digest,correlation_id,tenant_ref,occurred_at) values ($1,$2,'operation',$3,$4,$5,$6,null,$7::timestamptz) on conflict (event_type,payload_digest) do nothing")
            .bind(envelope.event_id.0).bind(envelope.event_type.to_wire()).bind(operation_id.to_string())
            .bind(serde_json::to_value(&envelope)?).bind(Sha256::digest(payload).as_slice())
            .bind(envelope.correlation_id.to_string()).bind(envelope.occurred_at.to_string())
            .execute(&mut **tx).await?;
        sqlx::query("update claude_archive.platform_operation_imports set reported_at=now() where operation_id=$1 and reported_at is null")
            .bind(operation_id).execute(&mut **tx).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ImportSummary {
    completeness: AiArchiveCompleteness,
    conversations: u32,
    messages: u32,
    assets: u32,
    gaps: u32,
    warnings: u32,
    /// Facts the import did not enqueue because they exceed the event limit.
    too_large: bool,
}

impl ImportSummary {
    /// The summary of an import whose projection also lost `omitted` subjects: the operation
    /// report agrees with the archive head about completeness and gaps.
    fn with_omitted(self, omitted: Omitted) -> Self {
        Self {
            completeness: omitted.completeness(self.completeness),
            gaps: self
                .gaps
                .saturating_add(u32::try_from(omitted.gaps().len()).unwrap_or(u32::MAX)),
            too_large: omitted.too_large > 0,
            ..self
        }
    }
}

fn summary_from(report: &crate::ArchiveCompletenessReport) -> ImportSummary {
    ImportSummary {
        completeness: contract_completeness(report.status),
        conversations: u32::try_from(report.counts.conversations).unwrap_or(u32::MAX),
        messages: u32::try_from(report.counts.messages).unwrap_or(u32::MAX),
        assets: u32::try_from(report.counts.verified_knowledge_files).unwrap_or(u32::MAX),
        gaps: u32::try_from(
            gaps_for(GapInput {
                complete: report.status == crate::CompletenessStatus::Complete,
                unknown_variants: report.counts.unknown_variants,
                missing_files: report.counts.missing_knowledge_files,
                quarantined_files: report.counts.quarantined_knowledge_files,
            })
            .len(),
        )
        .unwrap_or(u32::MAX),
        warnings: u32::try_from(report.warnings.len()).unwrap_or(u32::MAX),
        too_large: false,
    }
}

const fn contract_completeness(status: crate::CompletenessStatus) -> AiArchiveCompleteness {
    match status {
        crate::CompletenessStatus::Complete => AiArchiveCompleteness::Complete,
        crate::CompletenessStatus::ConversationsComplete => {
            AiArchiveCompleteness::ConversationsComplete
        }
        crate::CompletenessStatus::StructurallyPartial => {
            AiArchiveCompleteness::StructurallyPartial
        }
        crate::CompletenessStatus::AssetsPartial => AiArchiveCompleteness::AssetsPartial,
        crate::CompletenessStatus::Unknown => AiArchiveCompleteness::Unknown,
        crate::CompletenessStatus::FailedValidation => AiArchiveCompleteness::FailedValidation,
    }
}

const fn completeness_status(status: crate::CompletenessStatus) -> &'static str {
    match status {
        crate::CompletenessStatus::Complete => "complete",
        crate::CompletenessStatus::ConversationsComplete => "conversations_complete",
        crate::CompletenessStatus::StructurallyPartial => "structurally_partial",
        crate::CompletenessStatus::AssetsPartial => "assets_partial",
        crate::CompletenessStatus::Unknown => "unknown",
        crate::CompletenessStatus::FailedValidation => "failed_validation",
    }
}

async fn load_summary(pool: &PgPool, run_id: Uuid) -> Result<ImportSummary, ReparseError> {
    let (status, counts, warnings): (String, serde_json::Value, Option<serde_json::Value>) =
        sqlx::query_as(
            "select status,discovered_counts,warnings from claude_archive.completeness_reports where run_id=$1",
        )
        .bind(run_id)
        .fetch_one(pool)
        .await?;
    let count = |name: &str| {
        counts
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_default()
    };
    let completeness = match status.as_str() {
        "complete" => AiArchiveCompleteness::Complete,
        "conversations_complete" => AiArchiveCompleteness::ConversationsComplete,
        "assets_partial" => AiArchiveCompleteness::AssetsPartial,
        "unknown" => AiArchiveCompleteness::Unknown,
        "failed_validation" => AiArchiveCompleteness::FailedValidation,
        _ => AiArchiveCompleteness::StructurallyPartial,
    };
    let gaps =
        u32::try_from(gaps_for(GapInput::from_counts(&status, &counts)).len()).unwrap_or(u32::MAX);
    Ok(ImportSummary {
        completeness,
        conversations: count("conversations"),
        messages: count("messages"),
        assets: count("verified_knowledge_files"),
        gaps,
        warnings: warnings
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .map_or(0, |values| u32::try_from(values.len()).unwrap_or(u32::MAX)),
        too_large: false,
    })
}

async fn enqueue_reports(
    pool: &PgPool,
    run_id: Uuid,
    export_id: Uuid,
    archive_id: Uuid,
    summary: ImportSummary,
) -> Result<(), ReparseError> {
    let mut tx = pool.begin().await?;
    enqueue_reports_in(&mut tx, run_id, export_id, archive_id, summary).await?;
    tx.commit().await?;
    Ok(())
}

async fn enqueue_reports_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: Uuid,
    _export_id: Uuid,
    archive_id: Uuid,
    summary: ImportSummary,
) -> Result<(), ReparseError> {
    let operations: Vec<Uuid> = sqlx::query_scalar(
        "select operation_id from claude_archive.platform_operation_imports
         where import_run_id=$1 and reported_at is null order by operation_id",
    )
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    for operation_id in operations {
        let report = operation_report(operation_id, archive_id, summary)?;
        let envelope = operation_report_envelope(&report)?;
        let payload = serde_json::to_vec(&envelope.payload)?;
        sqlx::query("insert into claude_archive.outbox_events (event_id,event_type,aggregate_type,aggregate_id,envelope,payload_digest,correlation_id,tenant_ref,occurred_at) values ($1,$2,'operation',$3,$4,$5,$6,null,$7::timestamptz) on conflict (event_type,payload_digest) do nothing")
            .bind(envelope.event_id.0).bind(envelope.event_type.to_wire()).bind(operation_id.to_string())
            .bind(serde_json::to_value(&envelope)?).bind(Sha256::digest(payload).as_slice())
            .bind(envelope.correlation_id.to_string()).bind(envelope.occurred_at.to_string())
            .execute(&mut **tx).await?;
        sqlx::query("update claude_archive.platform_operation_imports set reported_at=now() where operation_id=$1 and reported_at is null")
            .bind(operation_id).execute(&mut **tx).await?;
    }
    Ok(())
}

fn operation_report(
    operation_id: Uuid,
    archive_id: Uuid,
    summary: ImportSummary,
) -> Result<OperationReported, ReparseError> {
    let archive =
        AiArchiveId::parse(&archive_id.to_string()).map_err(|_| ReparseError::Conflict)?;
    let operation =
        OperationId::parse(&operation_id.to_string()).map_err(|_| ReparseError::Conflict)?;
    let provider = AiProvider::parse("claude").map_err(|_| ReparseError::Conflict)?;
    let result_kind =
        OperationResultKind::parse("ai_archive.import").map_err(|_| ReparseError::Conflict)?;
    let status = if summary.completeness == AiArchiveCompleteness::Complete {
        OperationStatus::Succeeded
    } else {
        OperationStatus::PartiallySucceeded
    };
    Ok(OperationReported {
        operation_id: operation,
        status,
        stage: None,
        progress_percent: None,
        results: vec![OperationResultRef {
            result_kind,
            target: EntityRef::from(archive),
            blob: None,
            ai_archive_import_summary: Some(AiArchiveOperationSummary {
                ai_archive_id: archive,
                provider,
                completeness: summary.completeness,
                conversation_count: summary.conversations,
                message_count: summary.messages,
                asset_count: summary.assets,
                gap_count: summary.gaps,
                warning_count: summary.warnings,
            }),
            extensions: Extensions::new(),
        }],
        error: None,
        // A partial success without a diagnostic is invalid (S06 D3): the archive was imported,
        // and the warning says it is not complete. Facts too large to enqueue are named as well,
        // so the user is not told a success whose conversation Knowledge never receives (R2-04).
        warnings: import_warnings(status, summary),
        extensions: Extensions::new(),
    })
}

fn import_warnings(status: OperationStatus, summary: ImportSummary) -> Vec<WarningEnvelope> {
    let mut warnings = Vec::new();
    if status == OperationStatus::PartiallySucceeded {
        warnings.push(incomplete_import_warning());
    }
    if summary.too_large {
        warnings.extend(facts_too_large_warning());
    }
    warnings
}

/// The warning for facts that exceed the event limit (XR-021 CONTRACTS.md R2-04 rule 4).
fn facts_too_large_warning() -> Option<WarningEnvelope> {
    Some(WarningEnvelope {
        code: ErrorCode::parse(FACTS_TOO_LARGE_WARNING_CODE).ok()?,
        message: SafeMessage::parse(FACTS_TOO_LARGE_WARNING_MESSAGE).ok()?,
        field_path: None,
        extensions: Extensions::new(),
    })
}

fn operation_report_envelope(report: &OperationReported) -> Result<EventEnvelope, ReparseError> {
    let event_id = EventId::new_v7();
    let mut envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
        "event_id": event_id,
        "event_type": OperationReported::EVENT_TYPE,
        "occurred_at": WireTimestamp::now(),
        "producer": "ratatoskr-claude",
        "aggregate_id": EntityRef::from(report.operation_id),
        "correlation_id": event_id.as_entity_ref(),
        "schema_version": 1,
        "payload": {}
    }))?;
    envelope
        .set_payload(report)
        .map_err(|_| ReparseError::Conflict)?;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use ratatoskr_ai_archive_contracts::platform_receipt::incomplete_import_warning;

    use super::*;

    fn summary(completeness: AiArchiveCompleteness) -> ImportSummary {
        ImportSummary {
            completeness,
            conversations: 1,
            messages: 2,
            assets: 0,
            gaps: 1,
            warnings: 0,
            too_large: false,
        }
    }

    #[test]
    fn an_incomplete_import_reports_partial_success_with_the_contract_warning() {
        let report = operation_report(
            Uuid::now_v7(),
            Uuid::now_v7(),
            summary(AiArchiveCompleteness::AssetsPartial),
        )
        .expect("the report is built");

        assert_eq!(
            report.status,
            OperationStatus::PartiallySucceeded,
            "a non-complete import is a partial success"
        );
        assert_eq!(
            report.warnings,
            vec![incomplete_import_warning()],
            "the partial success carries exactly the contract warning"
        );
        assert!(
            report.error.is_none(),
            "the partial success carries no error"
        );
        assert!(
            report.validate().is_ok(),
            "a producer must not emit a partial success without a diagnostic"
        );
    }

    #[test]
    fn a_complete_import_reports_success_without_warnings() {
        let report = operation_report(
            Uuid::now_v7(),
            Uuid::now_v7(),
            summary(AiArchiveCompleteness::Complete),
        )
        .expect("the report is built");

        assert_eq!(report.status, OperationStatus::Succeeded);
        assert!(report.warnings.is_empty());
        assert!(report.validate().is_ok());
    }

    #[test]
    fn a_store_failure_while_ingesting_a_knowledge_file_is_never_permanent() {
        // CONTRACTS.md R4-02 rule 1: nothing references the blob yet, so the next pass ingests
        // again, whatever the class of the store error.
        for error in [
            crate::StoreError::Io(std::io::Error::other("the disk is full")),
            crate::StoreError::Unavailable,
            crate::StoreError::Collision {
                digest_hex: "0".repeat(64),
            },
            crate::StoreError::Missing {
                digest_hex: "0".repeat(64),
            },
        ] {
            assert!(
                !is_permanent_import_error(&ReparseError::KnowledgeIngest(error)),
                "a knowledge-file ingest failure is retried, never a permanent import failure"
            );
        }
        assert!(
            is_permanent_import_error(&ReparseError::Blob(crate::StoreError::Collision {
                digest_hex: "0".repeat(64),
            })),
            "the same class of error elsewhere in the import stays permanent"
        );
    }
}
