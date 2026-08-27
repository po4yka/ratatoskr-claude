//! Executable operator lifecycle command adapters.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ratatoskr_claude_archive::portable_export::{
    PortableArchiveExporter, PortableArchiveState, PortableArtifact, PortableArtifactVersion,
    PortableAsset, PortableAssetAvailability, PortableConversation, PortableExportFilter,
    PortableKnowledgeSource, PortableProject, PortableProvenance,
};
use ratatoskr_claude_archive::privacy_deletion::{
    ConversationDeletionPlanRequest, PrivacyDeletionExecutor, PrivacyDeletionPlanner,
    RawExportDeletionPlanRequest, ResolvedDeletionBlob, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::{
    BlobRef, BlobStore, Config, ConsumerExportParser, Database, DigestAlgorithm, MediaType,
    ParserExecutionError, ParserExecutionInput, ParserExecutor, ParserIdentity,
    ParserMigrationEntry, ParserMigrationEntryStatus, ParserMigrationPlan, ParserMigrationReport,
    ParserRegistry, ReparseChangeKind, ReparseEngine,
};
use secrecy::ExposeSecret as _;
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::lifecycle_commands::{
    ParserMigrateCommand, ParserMigrateExecution, PortableExportCommand, PortableExportExecution,
    PrivacyDeleteExecuteCommand, PrivacyDeleteExecuteExecution, PrivacyDeletePlanCommand,
    PrivacyDeletePlanExecution, PrivacyDeletionScope, ReparseCommand, ReparseExecution,
};

type PortableConversationRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    serde_json::Value,
);
type PortableArtifactRow = (Uuid, String, Option<String>, Option<String>, Option<String>);
type PortableAssetRow = (
    String,
    Option<String>,
    Option<String>,
    Option<Vec<u8>>,
    Option<i64>,
    String,
);

/// Process-owned database, blob store, and parser registry for one command.
#[derive(Debug)]
pub struct OperatorContext {
    database: Database,
    blobs: BlobStore,
    registry: Arc<ParserRegistry>,
    limits: ratatoskr_claude_archive::Limits,
}

impl OperatorContext {
    /// Opens configured operator dependencies and applies the current schema.
    ///
    /// # Errors
    ///
    /// Returns a stable content-free code when configuration or a dependency is unavailable.
    pub async fn open() -> Result<Self, String> {
        let config = Config::load().map_err(|_| "configuration_invalid".to_owned())?;
        let database_url = config
            .storage
            .database_url
            .as_ref()
            .ok_or_else(|| "database_unconfigured".to_owned())?;
        let blob_root = config
            .storage
            .blob_root
            .as_ref()
            .ok_or_else(|| "blob_store_unconfigured".to_owned())?;
        let database = Database::connect(
            database_url.expose_secret(),
            config.limits.database_connections,
            Duration::from_millis(config.limits.database_acquire_timeout_ms),
        )
        .await
        .map_err(|_| "database_unavailable".to_owned())?;
        database
            .apply_schema()
            .await
            .map_err(|_| "schema_unavailable".to_owned())?;
        let blobs = BlobStore::open(blob_root).map_err(|_| "blob_store_unavailable".to_owned())?;
        let mut registry = ParserRegistry::default();
        registry
            .register_compiled(
                ConsumerExportParser::descriptor(),
                Arc::new(ConsumerParserAdapter),
            )
            .map_err(|_| "parser_registry_invalid".to_owned())?;
        Ok(Self {
            database,
            blobs,
            registry: Arc::new(registry),
            limits: config.limits,
        })
    }

    /// Executes deterministic portable export from tenant-scoped database state.
    pub async fn portable_export(
        &self,
        command: &PortableExportCommand,
    ) -> PortableExportExecution {
        match self.load_portable_state(command).await.and_then(|state| {
            PortableArchiveExporter::new()
                .export_to_path_with_assets(&state, &self.blobs, &command.output)
                .map_err(|_| "export_failed".to_owned())?;
            let bytes = std::fs::read(&command.output).map_err(|_| "export_failed".to_owned())?;
            Ok((
                hex(&Sha256::digest(&bytes)),
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            ))
        }) {
            Ok((archive_sha256, byte_size)) => PortableExportExecution::Completed {
                archive_sha256,
                byte_size,
            },
            Err(error_code) => PortableExportExecution::Failed { error_code },
        }
    }

    /// Persists one complete privacy deletion inventory.
    pub async fn privacy_plan(
        &self,
        command: &PrivacyDeletePlanCommand,
    ) -> PrivacyDeletePlanExecution {
        let Ok(account_id) = Uuid::parse_str(&command.tenant) else {
            return PrivacyDeletePlanExecution::Failed {
                error_code: "not_found".to_owned(),
            };
        };
        let request_id =
            match existing_request(self.database.pool(), &command.tenant, &command.request_key)
                .await
            {
                Ok(Some(id)) => id,
                Ok(None) => Uuid::now_v7(),
                Err(()) => {
                    return PrivacyDeletePlanExecution::Failed {
                        error_code: "operation_failed".to_owned(),
                    };
                }
            };
        let planner = PrivacyDeletionPlanner::new(self.database.pool().clone());
        let correlation_id = format!("privacy-delete:{request_id}");
        let result = match command.scope {
            PrivacyDeletionScope::Tenant => {
                planner
                    .plan_tenant(&TenantDeletionPlanRequest {
                        tenant_ref: command.tenant.clone(),
                        account_id,
                        request_id,
                        request_key: command.request_key.clone(),
                        correlation_id,
                    })
                    .await
            }
            PrivacyDeletionScope::Export => {
                planner
                    .plan_raw_export(&RawExportDeletionPlanRequest {
                        tenant_ref: command.tenant.clone(),
                        account_id,
                        request_id,
                        request_key: command.request_key.clone(),
                        correlation_id,
                        export_id: parse_target(command),
                    })
                    .await
            }
            PrivacyDeletionScope::Conversation => {
                planner
                    .plan_conversation(&ConversationDeletionPlanRequest {
                        tenant_ref: command.tenant.clone(),
                        account_id,
                        request_id,
                        request_key: command.request_key.clone(),
                        correlation_id,
                        conversation_id: parse_target(command),
                    })
                    .await
            }
        };
        match result {
            Ok(inventory) => PrivacyDeletePlanExecution::Planned {
                request_id: inventory.request_id.to_string(),
                item_count: u64::try_from(inventory.items.len()).unwrap_or(u64::MAX),
            },
            Err(_) => PrivacyDeletePlanExecution::Failed {
                error_code: "not_found".to_owned(),
            },
        }
    }

    /// Physically erases exact unshared blobs and atomically finalizes deletion.
    pub async fn privacy_execute(
        &self,
        command: &PrivacyDeleteExecuteCommand,
    ) -> PrivacyDeleteExecuteExecution {
        let owned: Result<Option<String>, _> = sqlx::query_scalar(
            "select tenant_ref from claude_archive.privacy_deletion_requests where request_id=$1",
        )
        .bind(command.request_id)
        .fetch_optional(self.database.pool())
        .await;
        if !matches!(owned,Ok(Some(ref tenant)) if tenant==&command.tenant) {
            return PrivacyDeleteExecuteExecution::Failed {
                error_code: "not_found".to_owned(),
            };
        }
        let Ok(resolved) = deletion_blobs(self.database.pool(), command.request_id).await else {
            return PrivacyDeleteExecuteExecution::Failed {
                error_code: "operation_failed".to_owned(),
            };
        };
        match PrivacyDeletionExecutor::new(self.database.pool().clone())
            .finalize_with_blob_store(command.request_id, &self.blobs, &resolved)
            .await
        {
            Ok(report) => PrivacyDeleteExecuteExecution::Completed {
                request_id: report.request_id,
            },
            Err(_) => PrivacyDeleteExecuteExecution::Failed {
                error_code: "operation_failed".to_owned(),
            },
        }
    }

    /// Plans or applies one exact reparse.
    pub async fn reparse(&self, command: &ReparseCommand) -> ReparseExecution {
        let engine = self.reparse_engine();
        let target = ParserIdentity::new(&command.parser_name, &command.parser_version);
        match engine
            .plan(command.tenant_id, command.archive_id, target)
            .await
        {
            Ok(plan) => {
                let report = if command.dry_run {
                    Ok(plan.report.clone())
                } else {
                    engine.apply(&plan).await
                };
                match report.and_then(|value| serde_json::to_value(value).map_err(Into::into)) {
                    Ok(value) => ReparseExecution::Completed(value),
                    Err(_) => ReparseExecution::Failed {
                        error_code: "operation_failed".to_owned(),
                    },
                }
            }
            Err(_) => ReparseExecution::Failed {
                error_code: "not_found".to_owned(),
            },
        }
    }

    /// Classifies and optionally applies all tenant archives for one parser.
    pub async fn parser_migrate(&self, command: &ParserMigrateCommand) -> ParserMigrateExecution {
        if let Ok(Some(value)) = load_migration_report(self.database.pool(), command).await {
            return migration_execution(value);
        }
        let rows:Vec<(Uuid,String)>=match sqlx::query_as("select ai_archive_id,coalesce(parser_version,'') from claude_archive.exports where account_ref=$1 order by ai_archive_id").bind(command.tenant_id).fetch_all(self.database.pool()).await{Ok(rows)=>rows,Err(_)=>return ParserMigrateExecution::Failed{error_code:"operation_failed".to_owned()}};
        let engine = self.reparse_engine();
        let target = ParserIdentity::new(&command.parser_name, &command.parser_version);
        let mut entries = Vec::new();
        let mut plans = BTreeMap::new();
        for (archive, current) in rows {
            if current == command.parser_version {
                entries.push(ParserMigrationEntry {
                    archive_id: archive,
                    status: ParserMigrationEntryStatus::AlreadyCurrent,
                });
                continue;
            }
            match engine
                .plan(command.tenant_id, archive, target.clone())
                .await
            {
                Ok(plan) => {
                    plans.insert(archive, plan);
                    entries.push(ParserMigrationEntry {
                        archive_id: archive,
                        status: ParserMigrationEntryStatus::Eligible,
                    });
                }
                Err(_) => entries.push(ParserMigrationEntry {
                    archive_id: archive,
                    status: ParserMigrationEntryStatus::Unsupported,
                }),
            }
        }
        let planned = ParserMigrationReport::planned(
            Uuid::now_v7(),
            command.tenant_id.to_string(),
            format!("{}@{}", command.parser_name, command.parser_version),
            entries,
        );
        if command.dry_run {
            return ParserMigrateExecution::Completed {
                report: serde_json::to_value(planned).unwrap_or(serde_json::Value::Null),
                partial: false,
            };
        }
        let mut outcomes = BTreeMap::new();
        for (archive, plan) in plans {
            let outcome = engine
                .apply(&plan)
                .await
                .map(|report| {
                    report.changes.iter().any(|change| {
                        matches!(
                            change.kind,
                            ReparseChangeKind::Added | ReparseChangeKind::Changed
                        )
                    })
                })
                .map_err(|_| ());
            outcomes.insert(archive, outcome);
        }
        let final_report = ParserMigrationPlan::new(planned)
            .apply_with(|archive| outcomes.remove(&archive).unwrap_or(Err(())));
        if persist_migration_report(self.database.pool(), command, &final_report)
            .await
            .is_err()
        {
            return ParserMigrateExecution::Failed {
                error_code: "operation_failed".to_owned(),
            };
        }
        migration_execution(serde_json::to_value(final_report).unwrap_or(serde_json::Value::Null))
    }

    fn reparse_engine(&self) -> ReparseEngine {
        ReparseEngine::new(
            self.database.pool().clone(),
            self.blobs.clone(),
            Arc::clone(&self.registry),
            self.limits.clone(),
        )
    }

    async fn load_portable_state(
        &self,
        command: &PortableExportCommand,
    ) -> Result<PortableArchiveState, String> {
        load_portable_state(self.database.pool(), command).await
    }
}

#[derive(Debug)]
struct ConsumerParserAdapter;
impl ParserExecutor for ConsumerParserAdapter {
    fn execute(
        &self,
        input: ParserExecutionInput<'_>,
    ) -> Result<ratatoskr_claude_archive::ParsedExport, ParserExecutionError> {
        ConsumerExportParser::parse(input.evidence).map_err(|_| ParserExecutionError::Failed)
    }
}

fn parse_target(command: &PrivacyDeletePlanCommand) -> Uuid {
    command
        .target
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or(Uuid::nil())
}
async fn existing_request(pool: &PgPool, tenant: &str, key: &str) -> Result<Option<Uuid>, ()> {
    sqlx::query_scalar("select request_id from claude_archive.privacy_deletion_requests where tenant_ref=$1 and request_key=$2").bind(tenant).bind(key).fetch_optional(pool).await.map_err(|_|())
}

async fn deletion_blobs(pool: &PgPool, request_id: Uuid) -> Result<Vec<ResolvedDeletionBlob>, ()> {
    let rows:Vec<(String,i64)>=sqlx::query_as("select distinct i.blob_ref,coalesce(e.byte_size,x.byte_size) from claude_archive.privacy_deletion_items i left join claude_archive.exports e on i.blob_ref=e.blob_ref left join claude_archive.extracted_artifacts x on i.blob_ref=x.blob_ref where i.request_id=$1 and i.action='erase_blob' and i.blob_ref is not null").bind(request_id).fetch_all(pool).await.map_err(|_|())?;
    rows.into_iter()
        .map(|(key, length)| {
            let digest = key.strip_prefix("sha256/").ok_or(())?.to_owned();
            Ok(ResolvedDeletionBlob {
                key,
                reference: BlobRef {
                    owner_service: ratatoskr_claude_archive::blob_store::OWNER_SERVICE.to_owned(),
                    algorithm: DigestAlgorithm::Sha256,
                    digest_hex: digest,
                    media_type: MediaType::parse("application/octet-stream").map_err(|_| ())?,
                    length_bytes: u64::try_from(length).map_err(|_| ())?,
                },
            })
        })
        .collect()
}

#[expect(
    clippy::too_many_lines,
    reason = "split after lifecycle behavior is green"
)]
async fn load_portable_state(
    pool: &PgPool,
    command: &PortableExportCommand,
) -> Result<PortableArchiveState, String> {
    let account:Option<(Uuid,String)>=sqlx::query_as("select account_id,external_account_id from claude_archive.accounts where external_account_id=$1 or account_id::text=$1").bind(&command.tenant).fetch_optional(pool).await.map_err(|_|"database_failed".to_owned())?;
    let Some((account_id, external)) = account else {
        return Err("not_found".to_owned());
    };
    let export:Option<(Uuid,Vec<u8>,String,String)>=sqlx::query_as("select ai_archive_id,archive_hash,coalesce(parser_version,'unknown'),to_char(received_at at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from claude_archive.exports where account_ref=$1 order by received_at desc,export_id desc limit 1").bind(account_id).fetch_optional(pool).await.map_err(|_|"database_failed".to_owned())?;
    let Some((snapshot, raw_hash, parser_version, observed)) = export else {
        return Err("not_found".to_owned());
    };
    let filter = PortableExportFilter {
        account_external_ref: external.clone(),
        project_external_id: command.project.clone(),
        observed_from_rfc3339: command.observed_from.clone(),
        observed_to_rfc3339: command.observed_to.clone(),
    };
    let mut projects = Vec::new();
    let project_rows:Vec<(Uuid,String,Option<String>,String)>=sqlx::query_as("select project_id,external_project_id,title,to_char(updated_at at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from claude_archive.projects where account_id=$1 order by external_project_id").bind(account_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
    for (project_id, id, title, at) in project_rows {
        if !selected(&filter, Some(&id), &at) {
            continue;
        }
        let sources:Vec<(String,String,Option<String>,bool)>=sqlx::query_as("select external_source_id,source_kind,title,locally_backed_up from claude_archive.project_sources where project_id=$1 order by external_source_id").bind(project_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
        projects.push(PortableProject {
            external_id: id.clone(),
            title: title.clone(),
            observed_at_rfc3339: at,
            payload: serde_json::json!({"external_id":id,"title":title}),
            knowledge_sources: sources
                .into_iter()
                .map(
                    |(external_id, source_kind, title, backed)| PortableKnowledgeSource {
                        external_id,
                        source_kind,
                        title,
                        availability: if backed { "verified" } else { "missing" }.to_owned(),
                        payload: serde_json::json!({"locally_backed_up":backed}),
                    },
                )
                .collect(),
        });
    }
    let conversation_rows:Vec<PortableConversationRow>=sqlx::query_as("select c.external_conversation_id,null::text,c.title,to_char(c.last_seen_at at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),jsonb_build_object('external_id',c.external_conversation_id,'messages',coalesce((select jsonb_agg(jsonb_build_object('external_id',m.external_message_id,'role',m.role,'parent_message_id',m.parent_message_id) order by m.provider_created_at,m.message_id) from claude_archive.messages m where m.conversation_id=c.conversation_id),'[]'::jsonb)) from claude_archive.conversations c where c.account_id=$1 order by c.external_conversation_id").bind(account_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
    let conversations = conversation_rows
        .into_iter()
        .filter(|(_, project, _, at, _)| selected(&filter, project.as_deref(), at))
        .map(
            |(external_id, project_external_id, title, at, payload)| PortableConversation {
                external_id,
                project_external_id,
                title,
                observed_at_rfc3339: at,
                payload,
            },
        )
        .collect();
    let artifact_rows:Vec<PortableArtifactRow>=sqlx::query_as("select a.artifact_id,a.external_artifact_id,c.external_conversation_id,p.external_project_id,a.title from claude_archive.artifacts a left join claude_archive.conversations c on c.conversation_id=a.conversation_id left join claude_archive.projects p on p.project_id=a.project_id left join claude_archive.messages m on m.message_id=a.message_id left join claude_archive.conversations mc on mc.conversation_id=m.conversation_id where coalesce(c.account_id,p.account_id,mc.account_id)=$1 order by a.external_artifact_id").bind(account_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
    let mut artifacts = Vec::new();
    for (artifact_id, external_id, conversation_external_id, project_external_id, title) in
        artifact_rows
    {
        if filter
            .project_external_id
            .as_deref()
            .is_some_and(|wanted| project_external_id.as_deref() != Some(wanted))
        {
            continue;
        }
        let versions:Vec<(String,Option<String>,Option<serde_json::Value>)>=sqlx::query_as("select external_version_id,previous_external_version_id,raw_record from claude_archive.artifact_versions where artifact_id=$1 order by version_index,external_version_id").bind(artifact_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
        artifacts.push(PortableArtifact {
            external_id,
            conversation_external_id,
            title,
            versions: versions
                .into_iter()
                .map(
                    |(external_id, previous_external_id, raw)| PortableArtifactVersion {
                        external_id,
                        previous_external_id,
                        payload: raw.unwrap_or_else(|| serde_json::json!({})),
                    },
                )
                .collect(),
        });
    }
    let asset_rows:Vec<PortableAssetRow>=sqlx::query_as("select coalesce(a.external_asset_id,a.asset_id::text),p.external_project_id,a.mime_type,a.content_hash,a.byte_size,to_char(a.last_seen_at at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from claude_archive.assets a left join claude_archive.messages m on m.message_id=a.message_id left join claude_archive.conversations c on c.conversation_id=m.conversation_id left join claude_archive.project_sources s on s.source_id=a.source_id left join claude_archive.projects p on p.project_id=s.project_id where coalesce(c.account_id,p.account_id)=$1 order by coalesce(a.external_asset_id,a.asset_id::text)").bind(account_id).fetch_all(pool).await.map_err(|_|"database_failed".to_owned())?;
    let assets = asset_rows
        .into_iter()
        .filter(|(_, project, _, _, _, at)| selected(&filter, project.as_deref(), at))
        .map(
            |(external_id, project_external_id, mime, hash, length, at)| {
                let media = mime
                    .as_deref()
                    .and_then(|value| MediaType::parse(value).ok());
                let blob = match (hash, length, media) {
                    (Some(hash), Some(length), Some(media_type)) if length >= 0 => Some(BlobRef {
                        owner_service: ratatoskr_claude_archive::blob_store::OWNER_SERVICE
                            .to_owned(),
                        algorithm: DigestAlgorithm::Sha256,
                        digest_hex: hex(&hash),
                        media_type,
                        length_bytes: u64::try_from(length).unwrap_or(0),
                    }),
                    _ => None,
                };
                PortableAsset {
                    external_id,
                    project_external_id,
                    observed_at_rfc3339: at,
                    availability: if blob.is_some() {
                        PortableAssetAvailability::Verified
                    } else {
                        PortableAssetAvailability::Missing
                    },
                    blob,
                    media_type: mime,
                }
            },
        )
        .collect();
    let completeness:Option<String>=sqlx::query_scalar("select cr.status from claude_archive.completeness_reports cr join claude_archive.import_runs ir on ir.run_id=cr.run_id join claude_archive.exports e on e.export_id=ir.export_id where e.account_ref=$1 order by cr.created_at desc limit 1").bind(account_id).fetch_optional(pool).await.map_err(|_|"database_failed".to_owned())?;
    Ok(PortableArchiveState {
        account_external_ref: external,
        provenance: PortableProvenance {
            source_snapshot_ids: vec![snapshot.to_string()],
            archive_sha256: hex(&raw_hash),
            parser_name: "claude-archive".to_owned(),
            parser_version,
            observed_at_rfc3339: observed,
            completeness: completeness.unwrap_or_else(|| "unknown".to_owned()),
        },
        projects,
        conversations,
        artifacts,
        assets,
    })
}

fn selected(filter: &PortableExportFilter, project: Option<&str>, observed: &str) -> bool {
    filter
        .project_external_id
        .as_deref()
        .is_none_or(|wanted| project == Some(wanted))
        && filter
            .observed_from_rfc3339
            .as_deref()
            .is_none_or(|from| observed >= from)
        && filter
            .observed_to_rfc3339
            .as_deref()
            .is_none_or(|to| observed <= to)
}
async fn load_migration_report(
    pool: &PgPool,
    command: &ParserMigrateCommand,
) -> Result<Option<serde_json::Value>, sqlx::Error> {
    sqlx::query_scalar("select report from claude_archive.parser_migration_reports where tenant_ref=$1 and operation_key=$2").bind(command.tenant_id.to_string()).bind(&command.operation_key).fetch_optional(pool).await
}
async fn persist_migration_report(
    pool: &PgPool,
    command: &ParserMigrateCommand,
    report: &ParserMigrationReport,
) -> Result<(), sqlx::Error> {
    let value =
        serde_json::to_value(report).map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
    let digest = Sha256::digest(
        serde_json::to_vec(report).map_err(|error| sqlx::Error::Encode(Box::new(error)))?,
    );
    sqlx::query("insert into claude_archive.parser_migration_reports (migration_report_id,tenant_ref,operation_key,parser_name,parser_version,plan_fingerprint,dry_run,state,report,correlation_id,completed_at) values ($1,$2,$3,$4,$5,$6,false,$7,$8,$9,now()) on conflict (tenant_ref,operation_key) do nothing").bind(report.operation_id).bind(command.tenant_id.to_string()).bind(&command.operation_key).bind(&command.parser_name).bind(&command.parser_version).bind(digest.as_slice()).bind(if report.status==ratatoskr_claude_archive::ParserMigrationStatus::Partial{"partial"}else{"completed"}).bind(value).bind(format!("parser-migrate:{}",report.operation_id)).execute(pool).await.map(|_|())
}
fn migration_execution(report: serde_json::Value) -> ParserMigrateExecution {
    let partial = report.get("status").and_then(serde_json::Value::as_str) == Some("partial");
    ParserMigrateExecution::Completed { report, partial }
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}
