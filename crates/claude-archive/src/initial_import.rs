//! Restart-safe initial Claude archive import and terminal operation reporting.

use std::collections::BTreeMap;
use std::sync::Arc;

use ratatoskr_ai_archive_contracts::{
    AiArchiveCompleteness, AiArchiveOperationSummary, AiProvider,
};
use ratatoskr_event_envelope::{EventEnvelope, EventPayload};
use ratatoskr_identifiers::{
    AiArchiveId, EntityRef, EventId, Extensions, OperationId, WireTimestamp,
};
use ratatoskr_operation_contracts::{
    OperationReported, OperationResultKind, OperationResultRef, OperationStatus,
};
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::reparse::{
    ReparseError, ReparsePlan, apply_conversations, compare, current_projection, decode_hex,
    encode_hex, read_parser_evidence,
};
use crate::{
    ArchiveInspector, BlobRef, BlobStore, DigestAlgorithm, Limits, MediaType, ParserCapability,
    ParserExecutionInput, ParserIdentity, ParserRegistry,
};

/// Restart-safe executor for stored raw archives and late operation bindings.
#[derive(Debug, Clone)]
pub struct InitialImportWorker {
    pool: PgPool,
    blobs: BlobStore,
    registry: Arc<ParserRegistry>,
    limits: Limits,
}

#[derive(Debug)]
struct PreparedInitialImport {
    plan: ReparsePlan,
    evidence_ref: BlobRef,
    completeness: crate::ArchiveCompletenessReport,
    detected: crate::DetectedSchema,
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
    ) -> Self {
        Self {
            pool,
            blobs,
            registry,
            limits,
        }
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
                or (r.state in ('completed','partial') and exists (
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
                mark_permanent_failure(&self.pool, run_id).await?;
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
        let completeness = crate::ArchiveCompletenessReport::from_ingest(
            input.archive_id.to_string(),
            &parsed,
            &[],
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
        sqlx::query("insert into claude_archive.extracted_artifacts (extracted_artifact_id,export_id,artifact_index,artifact_kind,blob_ref,content_hash,byte_size) values ($1,$2,0,'entry',$3,$4,$5) on conflict (export_id,artifact_index) do nothing")
            .bind(Uuid::now_v7()).bind(prepared.plan.export_id).bind(format!("sha256/{}",prepared.evidence_ref.digest_hex))
            .bind(decode_hex(&prepared.evidence_ref.digest_hex).ok_or(ReparseError::Conflict)?)
            .bind(i64::try_from(prepared.evidence_ref.length_bytes).map_err(|_| ReparseError::Conflict)?).execute(&mut *tx).await?;
        persist_initial_projection(&mut tx, &prepared.plan).await?;
        sqlx::query("insert into claude_archive.completeness_reports (report_id,run_id,status,discovered_counts,missing_assets,unknown_variants,warnings) values ($1,$2,$3,$4,$5,$6,$7) on conflict (run_id) do nothing")
            .bind(Uuid::now_v7()).bind(run_id).bind(status).bind(counts)
            .bind(i32::try_from(prepared.completeness.counts.missing_knowledge_files).unwrap_or(i32::MAX))
            .bind(i32::try_from(prepared.completeness.counts.unknown_variants).unwrap_or(i32::MAX))
            .bind(&warnings).execute(&mut *tx).await?;
        sqlx::query("update claude_archive.exports set detected_schema=$2,parser_version=$3 where export_id=$1")
            .bind(prepared.plan.export_id).bind(prepared.detected.identifier()).bind(prepared.plan.target.version()).execute(&mut *tx).await?;
        sqlx::query("update claude_archive.import_runs set state=$2,warnings=$3,finished_at=now() where run_id=$1")
            .bind(run_id).bind(terminal_state).bind(warnings).execute(&mut *tx).await?;
        enqueue_reports_in(
            &mut tx,
            run_id,
            prepared.plan.export_id,
            archive_id,
            summary,
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

async fn mark_permanent_failure(pool: &PgPool, run_id: Uuid) -> Result<(), ReparseError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "update claude_archive.import_runs
         set state='failed', warnings='[\"claude.archive.invalid\"]'::jsonb, finished_at=now()
         where run_id=$1 and state not in ('completed','partial','failed')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
    let operations: Vec<Uuid> = sqlx::query_scalar(
        "select operation_id from claude_archive.platform_operation_imports
         where import_run_id=$1 and reported_at is null order by operation_id",
    )
    .bind(run_id)
    .fetch_all(&mut *tx)
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
            .execute(&mut *tx).await?;
        sqlx::query("update claude_archive.platform_operation_imports set reported_at=now() where operation_id=$1 and reported_at is null")
            .bind(operation_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
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
}

fn summary_from(report: &crate::ArchiveCompletenessReport) -> ImportSummary {
    ImportSummary {
        completeness: contract_completeness(report.status),
        conversations: u32::try_from(report.counts.conversations).unwrap_or(u32::MAX),
        messages: u32::try_from(report.counts.messages).unwrap_or(u32::MAX),
        assets: u32::try_from(report.counts.verified_knowledge_files).unwrap_or(u32::MAX),
        gaps: u32::try_from(report.counts.missing_knowledge_files + report.counts.unknown_variants)
            .unwrap_or(u32::MAX)
            .max(u32::from(
                report.status != crate::CompletenessStatus::Complete,
            )),
        warnings: u32::try_from(report.warnings.len()).unwrap_or(u32::MAX),
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
    let gaps = count("missing_knowledge_files")
        .saturating_add(count("unknown_variants"))
        .max(u32::from(completeness != AiArchiveCompleteness::Complete));
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
    Ok(OperationReported {
        operation_id: operation,
        status: if summary.completeness == AiArchiveCompleteness::Complete {
            OperationStatus::Succeeded
        } else {
            OperationStatus::PartiallySucceeded
        },
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
        warnings: Vec::new(),
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

async fn persist_initial_projection(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &ReparsePlan,
) -> Result<(), ReparseError> {
    for project in &plan.parsed.projects {
        let project_id: Uuid = sqlx::query_scalar("insert into claude_archive.projects (project_id,account_id,external_project_id,title,upstream_state) values ($1,$2,$3,$4,'present') on conflict (account_id,external_project_id) do update set title=excluded.title,updated_at=now() returning project_id")
            .bind(Uuid::now_v7()).bind(plan.tenant_id).bind(&project.external_id).bind(&project.name).fetch_one(&mut **tx).await?;
        sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'project',$2) on conflict do nothing")
            .bind(plan.export_id).bind(project_id).execute(&mut **tx).await?;
        for source in plan
            .parsed
            .project_knowledge_files
            .iter()
            .filter(|source| source.project_external_id == project.external_id)
        {
            sqlx::query("insert into claude_archive.project_sources (source_id,project_id,external_source_id,source_kind,title,mime_type,byte_size,locally_backed_up) values ($1,$2,$3,'file',$4,$5,$6,false) on conflict (project_id,external_source_id) do update set title=excluded.title,mime_type=excluded.mime_type,byte_size=excluded.byte_size,last_seen_at=now()")
                .bind(Uuid::now_v7()).bind(project_id).bind(&source.external_id).bind(&source.filename).bind(&source.media_type)
                .bind(source.bytes.as_ref().and_then(|bytes| i64::try_from(bytes.len()).ok())).execute(&mut **tx).await?;
        }
    }
    apply_conversations(tx, plan).await?;
    for conversation in &plan.parsed.conversations {
        let conversation_id: Uuid = sqlx::query_scalar("select conversation_id from claude_archive.conversations where account_id=$1 and external_conversation_id=$2")
            .bind(plan.tenant_id).bind(&conversation.external_id).fetch_one(&mut **tx).await?;
        let mut message_ids = BTreeMap::new();
        for message in &conversation.messages {
            let role = match message.role.as_str() {
                "user" | "assistant" | "system" | "tool" | "internal" => message.role.as_str(),
                _ => "unknown",
            };
            let message_id: Uuid = sqlx::query_scalar("insert into claude_archive.messages (message_id,conversation_id,external_message_id,role,model) values ($1,$2,$3,$4,$5) on conflict (conversation_id,external_message_id) do update set role=excluded.role,model=excluded.model,last_seen_at=now() returning message_id")
                .bind(Uuid::now_v7()).bind(conversation_id).bind(&message.external_id).bind(role).bind(&message.model).fetch_one(&mut **tx).await?;
            message_ids.insert(message.external_id.clone(), message_id);
            sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'message',$2) on conflict do nothing")
                .bind(plan.export_id).bind(message_id).execute(&mut **tx).await?;
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
    }
    Ok(())
}
