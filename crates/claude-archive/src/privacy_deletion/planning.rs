//! Tenant-scoped privacy inventory planning.

use super::*;

impl PrivacyDeletionPlanner {
    /// Creates a planner over the Claude Archive database.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Plans the complete closure for one authenticated tenant account.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionError`] when inventory persistence fails.
    pub async fn plan_tenant(
        &self,
        request: &TenantDeletionPlanRequest,
    ) -> Result<DeletionInventory, PrivacyDeletionError> {
        let mut transaction = self.pool.begin().await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        require_account(&mut transaction, request.account_id).await?;

        if let Some(inventory) = load_existing(&mut transaction, request).await? {
            transaction.commit().await?;
            return Ok(inventory);
        }

        let mut items = enumerate_tenant(&mut transaction, request).await?;
        items.sort_by(|left, right| {
            (left.kind, &left.subject_id, left.action).cmp(&(
                right.kind,
                &right.subject_id,
                right.action,
            ))
        });
        let inventory = DeletionInventory::from_items(request.request_id, items);
        persist_inventory(&mut transaction, request, &inventory).await?;
        transaction.commit().await?;
        Ok(inventory)
    }

    /// Plans one raw-export scope for an authenticated tenant account.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionError`] when the scope is unavailable or
    /// inventory persistence fails.
    pub async fn plan_raw_export(
        &self,
        request: &RawExportDeletionPlanRequest,
    ) -> Result<DeletionInventory, PrivacyDeletionError> {
        let mut transaction = self.pool.begin().await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        require_account(&mut transaction, request.account_id).await?;
        let owned: bool = sqlx::query_scalar(
            "select exists(select 1 from claude_archive.exports
             where export_id = $1 and account_ref = $2)",
        )
        .bind(request.export_id)
        .bind(request.account_id)
        .fetch_one(&mut *transaction)
        .await?;
        if !owned {
            transaction.rollback().await?;
            return Err(PrivacyDeletionError::NotFound);
        }

        let export_ids = [request.export_id];
        let mut items = Vec::new();
        add_uuid_blob_rows_for_exports(
            &mut transaction,
            &mut items,
            DeletionItemKind::RawArchive,
            "select export_id, blob_ref from claude_archive.exports
             where export_id = any($1) order by export_id",
            &export_ids,
            DeletionAction::EraseBlob,
        )
        .await?;
        add_export_rows(&mut transaction, &export_ids, &mut items).await?;
        add_observed_subjects(&mut transaction, &export_ids, &mut items).await?;
        items.sort_by(|left, right| {
            (left.kind, &left.subject_id, left.action).cmp(&(
                right.kind,
                &right.subject_id,
                right.action,
            ))
        });
        let inventory = DeletionInventory::from_items(request.request_id, items);
        persist_scoped_inventory(
            &mut transaction,
            &request.tenant_ref,
            request.request_id,
            &request.request_key,
            &request.correlation_id,
            "raw_export",
            Some(request.export_id),
            &inventory,
        )
        .await?;
        transaction.commit().await?;
        Ok(inventory)
    }

    /// Plans one conversation and every retained raw archive containing it.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionError`] when the scope is unavailable or
    /// inventory persistence fails.
    pub async fn plan_conversation(
        &self,
        request: &ConversationDeletionPlanRequest,
    ) -> Result<DeletionInventory, PrivacyDeletionError> {
        let mut transaction = self.pool.begin().await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        require_account(&mut transaction, request.account_id).await?;
        let owned: bool = sqlx::query_scalar(
            "select exists(
                 select 1 from claude_archive.conversations
                 where conversation_id = $1 and account_id = $2
             )",
        )
        .bind(request.conversation_id)
        .bind(request.account_id)
        .fetch_one(&mut *transaction)
        .await?;
        if !owned {
            transaction.rollback().await?;
            return Err(PrivacyDeletionError::NotFound);
        }

        let export_ids: Vec<Uuid> = sqlx::query_scalar(
            "select e.export_id from claude_archive.exports e
             join claude_archive.export_observations o on o.export_id = e.export_id
             where e.account_ref = $1 and o.subject_kind = 'conversation'
               and o.subject_id = $2 order by e.export_id",
        )
        .bind(request.account_id)
        .bind(request.conversation_id)
        .fetch_all(&mut *transaction)
        .await?;
        if export_ids.is_empty() {
            transaction.rollback().await?;
            return Err(PrivacyDeletionError::NotFound);
        }

        let mut items = Vec::new();
        add_uuid_blob_rows_for_exports(
            &mut transaction,
            &mut items,
            DeletionItemKind::RawArchive,
            "select export_id, blob_ref from claude_archive.exports
             where export_id = any($1) order by export_id",
            &export_ids,
            DeletionAction::EraseBlob,
        )
        .await?;
        add_export_rows(&mut transaction, &export_ids, &mut items).await?;
        add_observed_subjects(&mut transaction, &export_ids, &mut items).await?;
        items.sort_by(|left, right| {
            (left.kind, &left.subject_id, left.action).cmp(&(
                right.kind,
                &right.subject_id,
                right.action,
            ))
        });
        let inventory = DeletionInventory::from_items(request.request_id, items);
        persist_scoped_inventory(
            &mut transaction,
            &request.tenant_ref,
            request.request_id,
            &request.request_key,
            &request.correlation_id,
            "conversation",
            Some(request.conversation_id),
            &inventory,
        )
        .await?;
        transaction.commit().await?;
        Ok(inventory)
    }
}

impl DeletionInventory {
    fn from_items(request_id: Uuid, items: Vec<DeletionInventoryItem>) -> Self {
        let mut category_totals = BTreeMap::new();
        for item in &items {
            *category_totals.entry(item.kind).or_insert(0) += 1;
        }
        Self {
            request_id,
            items,
            category_totals,
        }
    }
}

impl DeletionItemKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::RawArchive => "raw_archive",
            Self::ExtractedArtifact => "extracted_artifact",
            Self::ImportRun => "import_run",
            Self::CompletenessReport => "completeness_report",
            Self::UnknownRecord => "unknown_record",
            Self::Project => "project",
            Self::ProjectSource => "project_source",
            Self::Conversation => "conversation",
            Self::Message => "message",
            Self::MessageRelation => "message_relation",
            Self::ContentPart => "content_part",
            Self::Artifact => "artifact",
            Self::ArtifactVersion => "artifact_version",
            Self::Asset => "asset",
            Self::ExternalReference => "external_reference",
            Self::Revision => "revision",
            Self::AnalysisLink => "analysis_link",
            Self::Inbox => "inbox",
            Self::Outbox => "outbox",
            Self::DownstreamTombstone => "downstream_tombstone",
            Self::SharedBlob => "shared_blob",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "raw_archive" => Self::RawArchive,
            "extracted_artifact" => Self::ExtractedArtifact,
            "import_run" => Self::ImportRun,
            "completeness_report" => Self::CompletenessReport,
            "unknown_record" => Self::UnknownRecord,
            "project" => Self::Project,
            "project_source" => Self::ProjectSource,
            "conversation" => Self::Conversation,
            "message" => Self::Message,
            "message_relation" => Self::MessageRelation,
            "content_part" => Self::ContentPart,
            "artifact" => Self::Artifact,
            "artifact_version" => Self::ArtifactVersion,
            "asset" => Self::Asset,
            "external_reference" => Self::ExternalReference,
            "revision" => Self::Revision,
            "analysis_link" => Self::AnalysisLink,
            "inbox" => Self::Inbox,
            "outbox" => Self::Outbox,
            "downstream_tombstone" => Self::DownstreamTombstone,
            "shared_blob" => Self::SharedBlob,
            _ => return None,
        })
    }
}

impl DeletionAction {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::EraseBlob => "erase_blob",
            Self::RemoveRecord => "remove_record",
            Self::RetainShared => "retain_shared",
            Self::RetainEvidenced => "retain_evidenced",
            Self::EmitTombstone => "emit_tombstone",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "erase_blob" => Self::EraseBlob,
            "remove_record" => Self::RemoveRecord,
            "retain_shared" => Self::RetainShared,
            "retain_evidenced" => Self::RetainEvidenced,
            "emit_tombstone" => Self::EmitTombstone,
            _ => return None,
        })
    }
}

async fn lock_tenant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(tenant_ref)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn require_account(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
) -> Result<(), PrivacyDeletionError> {
    let exists: bool = sqlx::query_scalar(
        "select exists(select 1 from claude_archive.accounts where account_id = $1)",
    )
    .bind(account_id)
    .fetch_one(&mut **transaction)
    .await?;
    if exists {
        Ok(())
    } else {
        Err(PrivacyDeletionError::NotFound)
    }
}

async fn load_existing(
    transaction: &mut Transaction<'_, Postgres>,
    request: &TenantDeletionPlanRequest,
) -> Result<Option<DeletionInventory>, PrivacyDeletionError> {
    let existing: Option<(Uuid, String, Option<Uuid>, String)> = sqlx::query_as(
        "select request_id, scope_kind, scope_id, correlation_id
         from claude_archive.privacy_deletion_requests
         where tenant_ref = $1 and request_key = $2",
    )
    .bind(&request.tenant_ref)
    .bind(&request.request_key)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((request_id, scope_kind, scope_id, correlation_id)) = existing else {
        return Ok(None);
    };
    if request_id != request.request_id
        || scope_kind != "tenant"
        || scope_id.is_some()
        || correlation_id != request.correlation_id
    {
        return Err(PrivacyDeletionError::Conflict);
    }

    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "select item_kind, subject_id, action, blob_ref
         from claude_archive.privacy_deletion_items
         where request_id = $1 order by item_index",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut items = Vec::with_capacity(rows.len());
    for (kind, subject_id, action, blob_ref) in rows {
        let (Some(kind), Some(action)) = (
            DeletionItemKind::parse(&kind),
            DeletionAction::parse(&action),
        ) else {
            return Err(PrivacyDeletionError::Conflict);
        };
        items.push(DeletionInventoryItem {
            kind,
            subject_id,
            action,
            blob_ref,
        });
    }
    Ok(Some(DeletionInventory::from_items(request_id, items)))
}

async fn enumerate_tenant(
    transaction: &mut Transaction<'_, Postgres>,
    request: &TenantDeletionPlanRequest,
) -> Result<Vec<DeletionInventoryItem>, PrivacyDeletionError> {
    let export_ids: Vec<Uuid> = sqlx::query_scalar(
        "select export_id from claude_archive.exports
         where account_ref = $1 order by export_id",
    )
    .bind(request.account_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut inventory = Vec::new();
    add_blob_rows(
        transaction,
        &mut inventory,
        DeletionItemKind::RawArchive,
        "select export_id, blob_ref from claude_archive.exports
         where account_ref = $1 order by export_id",
        request.account_id,
    )
    .await?;
    add_export_rows(transaction, &export_ids, &mut inventory).await?;
    add_account_rows(transaction, request.account_id, &mut inventory).await?;
    add_delivery_rows(
        transaction,
        &export_ids,
        request.account_id,
        &request.tenant_ref,
        &mut inventory,
    )
    .await?;
    add_shared_blobs(transaction, request.account_id, &mut inventory).await?;
    Ok(inventory)
}

async fn add_export_rows(
    transaction: &mut Transaction<'_, Postgres>,
    export_ids: &[Uuid],
    inventory: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    add_uuid_blob_rows_for_exports(
        transaction,
        inventory,
        DeletionItemKind::ExtractedArtifact,
        "select extracted_artifact_id, blob_ref from claude_archive.extracted_artifacts
         where export_id = any($1) order by extracted_artifact_id",
        export_ids,
        DeletionAction::EraseBlob,
    )
    .await?;
    add_uuid_rows_for_exports(
        transaction,
        inventory,
        DeletionItemKind::ImportRun,
        "select run_id from claude_archive.import_runs
         where export_id = any($1) order by run_id",
        export_ids,
    )
    .await?;
    add_uuid_rows_for_exports(
        transaction,
        inventory,
        DeletionItemKind::CompletenessReport,
        "select report_id from claude_archive.completeness_reports
         where run_id in (select run_id from claude_archive.import_runs where export_id = any($1))
         order by report_id",
        export_ids,
    )
    .await?;
    add_uuid_rows_for_exports(
        transaction,
        inventory,
        DeletionItemKind::Revision,
        "select revision_id from claude_archive.revisions
         where export_id = any($1) order by revision_id",
        export_ids,
    )
    .await
}

async fn add_observed_subjects(
    transaction: &mut Transaction<'_, Postgres>,
    export_ids: &[Uuid],
    inventory: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    let rows: Vec<(String, Uuid, bool)> = sqlx::query_as(
        "select distinct selected.subject_kind, selected.subject_id,
                exists(
                    select 1 from claude_archive.export_observations retained
                    where retained.subject_kind = selected.subject_kind
                      and retained.subject_id = selected.subject_id
                      and not (retained.export_id = any($1))
                ) as retained
         from claude_archive.export_observations selected
         where selected.export_id = any($1)
         order by selected.subject_kind, selected.subject_id",
    )
    .bind(export_ids)
    .fetch_all(&mut **transaction)
    .await?;
    for (subject_kind, subject_id, retained) in rows {
        let Some(kind) = observed_kind(&subject_kind) else {
            continue;
        };
        inventory.push(item(
            kind,
            subject_id,
            if retained {
                DeletionAction::RetainEvidenced
            } else {
                DeletionAction::RemoveRecord
            },
            None,
        ));
    }
    Ok(())
}

fn observed_kind(value: &str) -> Option<DeletionItemKind> {
    Some(match value {
        "project" => DeletionItemKind::Project,
        "project_source" => DeletionItemKind::ProjectSource,
        "conversation" => DeletionItemKind::Conversation,
        "message" => DeletionItemKind::Message,
        "content_part" => DeletionItemKind::ContentPart,
        "artifact" => DeletionItemKind::Artifact,
        "artifact_version" => DeletionItemKind::ArtifactVersion,
        "asset" => DeletionItemKind::Asset,
        "external_reference" => DeletionItemKind::ExternalReference,
        _ => return None,
    })
}

async fn add_account_rows(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
    inventory: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    for (kind, query) in [
        (
            DeletionItemKind::UnknownRecord,
            "select cp.content_part_id from claude_archive.content_parts cp
             join claude_archive.messages m on m.message_id = cp.message_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id = $1 and cp.part_kind = 'unknown' order by cp.content_part_id",
        ),
        (
            DeletionItemKind::Project,
            "select project_id from claude_archive.projects where account_id = $1 order by project_id",
        ),
        (
            DeletionItemKind::ProjectSource,
            "select ps.source_id from claude_archive.project_sources ps
             join claude_archive.projects p on p.project_id = ps.project_id
             where p.account_id = $1 order by ps.source_id",
        ),
        (
            DeletionItemKind::Conversation,
            "select conversation_id from claude_archive.conversations
             where account_id = $1 order by conversation_id",
        ),
        (
            DeletionItemKind::Message,
            "select m.message_id from claude_archive.messages m
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id = $1 order by m.message_id",
        ),
        (
            DeletionItemKind::MessageRelation,
            "select r.relation_id from claude_archive.message_relations r
             join claude_archive.messages m on m.message_id = r.parent_message_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id = $1 order by r.relation_id",
        ),
        (
            DeletionItemKind::ContentPart,
            "select cp.content_part_id from claude_archive.content_parts cp
             join claude_archive.messages m on m.message_id = cp.message_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id = $1 order by cp.content_part_id",
        ),
        (
            DeletionItemKind::Artifact,
            "select a.artifact_id from claude_archive.artifacts a
             left join claude_archive.projects p on p.project_id = a.project_id
             left join claude_archive.conversations c on c.conversation_id = a.conversation_id
             left join claude_archive.messages m on m.message_id = a.message_id
             left join claude_archive.conversations mc on mc.conversation_id = m.conversation_id
             where coalesce(p.account_id, c.account_id, mc.account_id) = $1 order by a.artifact_id",
        ),
        (
            DeletionItemKind::ArtifactVersion,
            "select av.version_id from claude_archive.artifact_versions av
             join claude_archive.artifacts a on a.artifact_id = av.artifact_id
             left join claude_archive.projects p on p.project_id = a.project_id
             left join claude_archive.conversations c on c.conversation_id = a.conversation_id
             left join claude_archive.messages m on m.message_id = a.message_id
             left join claude_archive.conversations mc on mc.conversation_id = m.conversation_id
             where coalesce(p.account_id, c.account_id, mc.account_id) = $1 order by av.version_id",
        ),
        (
            DeletionItemKind::ExternalReference,
            "select er.reference_id from claude_archive.external_references er
             left join claude_archive.projects p on p.project_id = er.project_id
             left join claude_archive.conversations c on c.conversation_id = er.conversation_id
             left join claude_archive.artifacts a on a.artifact_id = er.artifact_id
             left join claude_archive.projects ap on ap.project_id = a.project_id
             left join claude_archive.conversations ac on ac.conversation_id = a.conversation_id
             left join claude_archive.messages am on am.message_id = a.message_id
             left join claude_archive.conversations amc on amc.conversation_id = am.conversation_id
             where coalesce(p.account_id, c.account_id, ap.account_id, ac.account_id, amc.account_id) = $1
             order by er.reference_id",
        ),
    ] {
        add_uuid_rows(transaction, inventory, kind, query, account_id).await?;
    }

    let asset_rows: Vec<(Uuid, Option<String>)> = sqlx::query_as(
        "select a.asset_id, a.blob_ref from claude_archive.assets a
         left join claude_archive.messages m on m.message_id = a.message_id
         left join claude_archive.conversations c on c.conversation_id = m.conversation_id
         left join claude_archive.project_sources ps on ps.source_id = a.source_id
         left join claude_archive.projects p on p.project_id = ps.project_id
         where coalesce(c.account_id, p.account_id) = $1 order by a.asset_id",
    )
    .bind(account_id)
    .fetch_all(&mut **transaction)
    .await?;
    for (asset_id, blob_ref) in asset_rows {
        inventory.push(item(
            DeletionItemKind::Asset,
            asset_id,
            DeletionAction::RemoveRecord,
            blob_ref,
        ));
    }
    Ok(())
}

async fn add_delivery_rows(
    transaction: &mut Transaction<'_, Postgres>,
    export_ids: &[Uuid],
    account_id: Uuid,
    tenant_ref: &str,
    inventory: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    let export_id_strings: Vec<String> = export_ids.iter().map(Uuid::to_string).collect();
    let analysis_rows: Vec<Uuid> = sqlx::query_scalar(
        "select distinct l.completion_event_id
         from claude_archive.knowledge_analysis_links l
         join claude_archive.exports e on e.ai_archive_id = l.ai_archive_id
         where e.export_id = any($1) order by l.completion_event_id",
    )
    .bind(export_ids)
    .fetch_all(&mut **transaction)
    .await?;
    push_ids(inventory, DeletionItemKind::AnalysisLink, &analysis_rows);

    let inbox_rows: Vec<Uuid> = sqlx::query_scalar(
        "select i.event_id from claude_archive.inbox_events i
         where i.event_id = any($1) order by i.event_id",
    )
    .bind(&analysis_rows)
    .fetch_all(&mut **transaction)
    .await?;
    push_ids(inventory, DeletionItemKind::Inbox, &inbox_rows);

    let outbox_rows: Vec<Uuid> = sqlx::query_scalar(
        "select event_id from claude_archive.outbox_events
         where tenant_ref = $1
            or (aggregate_type = 'export' and aggregate_id = any($2))
         order by event_id",
    )
    .bind(tenant_ref)
    .bind(&export_id_strings)
    .fetch_all(&mut **transaction)
    .await?;
    push_ids(inventory, DeletionItemKind::Outbox, &outbox_rows);

    let tombstones: Vec<String> = sqlx::query_scalar(
        "select distinct l.subject_id from claude_archive.knowledge_analysis_links l
         join claude_archive.exports e on e.ai_archive_id = l.ai_archive_id
         where e.account_ref = $1 and l.subject_kind in ('conversation', 'artifact')
         order by l.subject_id",
    )
    .bind(account_id)
    .fetch_all(&mut **transaction)
    .await?;
    for subject_id in tombstones {
        inventory.push(DeletionInventoryItem {
            kind: DeletionItemKind::DownstreamTombstone,
            subject_id,
            action: DeletionAction::EmitTombstone,
            blob_ref: None,
        });
    }
    Ok(())
}

async fn add_shared_blobs(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
    inventory: &mut Vec<DeletionInventoryItem>,
) -> Result<(), sqlx::Error> {
    let shared: Vec<String> = sqlx::query_scalar(
        "with selected(blob_ref) as (
             select ps.blob_ref from claude_archive.project_sources ps
             join claude_archive.projects p on p.project_id = ps.project_id
             where p.account_id = $1 and ps.blob_ref is not null
             union select cp.blob_ref from claude_archive.content_parts cp
             join claude_archive.messages m on m.message_id = cp.message_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id = $1 and cp.blob_ref is not null
             union select a.blob_ref from claude_archive.assets a
             left join claude_archive.messages m on m.message_id = a.message_id
             left join claude_archive.conversations c on c.conversation_id = m.conversation_id
             left join claude_archive.project_sources ps on ps.source_id = a.source_id
             left join claude_archive.projects p on p.project_id = ps.project_id
             where coalesce(c.account_id, p.account_id) = $1 and a.blob_ref is not null
         ), retained(blob_ref) as (
             select ps.blob_ref from claude_archive.project_sources ps
             join claude_archive.projects p on p.project_id = ps.project_id
             where p.account_id <> $1 and ps.blob_ref is not null
             union select cp.blob_ref from claude_archive.content_parts cp
             join claude_archive.messages m on m.message_id = cp.message_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.account_id <> $1 and cp.blob_ref is not null
             union select a.blob_ref from claude_archive.assets a
             left join claude_archive.messages m on m.message_id = a.message_id
             left join claude_archive.conversations c on c.conversation_id = m.conversation_id
             left join claude_archive.project_sources ps on ps.source_id = a.source_id
             left join claude_archive.projects p on p.project_id = ps.project_id
             where coalesce(c.account_id, p.account_id) <> $1 and a.blob_ref is not null
         )
         select selected.blob_ref from selected join retained using (blob_ref)
         order by selected.blob_ref",
    )
    .bind(account_id)
    .fetch_all(&mut **transaction)
    .await?;
    for blob_ref in shared {
        inventory.push(DeletionInventoryItem {
            kind: DeletionItemKind::SharedBlob,
            subject_id: blob_ref.clone(),
            action: DeletionAction::RetainShared,
            blob_ref: Some(blob_ref),
        });
    }
    Ok(())
}

async fn add_blob_rows(
    transaction: &mut Transaction<'_, Postgres>,
    inventory: &mut Vec<DeletionInventoryItem>,
    kind: DeletionItemKind,
    query: &str,
    account_id: Uuid,
) -> Result<(), sqlx::Error> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(query)
        .bind(account_id)
        .fetch_all(&mut **transaction)
        .await?;
    for (id, blob_ref) in rows {
        inventory.push(item(kind, id, DeletionAction::EraseBlob, Some(blob_ref)));
    }
    Ok(())
}

async fn add_uuid_rows(
    transaction: &mut Transaction<'_, Postgres>,
    inventory: &mut Vec<DeletionInventoryItem>,
    kind: DeletionItemKind,
    query: &str,
    account_id: Uuid,
) -> Result<(), sqlx::Error> {
    let ids: Vec<Uuid> = sqlx::query_scalar(query)
        .bind(account_id)
        .fetch_all(&mut **transaction)
        .await?;
    push_ids(inventory, kind, &ids);
    Ok(())
}

async fn add_uuid_rows_for_exports(
    transaction: &mut Transaction<'_, Postgres>,
    inventory: &mut Vec<DeletionInventoryItem>,
    kind: DeletionItemKind,
    query: &str,
    export_ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    let ids: Vec<Uuid> = sqlx::query_scalar(query)
        .bind(export_ids)
        .fetch_all(&mut **transaction)
        .await?;
    push_ids(inventory, kind, &ids);
    Ok(())
}

async fn add_uuid_blob_rows_for_exports(
    transaction: &mut Transaction<'_, Postgres>,
    inventory: &mut Vec<DeletionInventoryItem>,
    kind: DeletionItemKind,
    query: &str,
    export_ids: &[Uuid],
    action: DeletionAction,
) -> Result<(), sqlx::Error> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(query)
        .bind(export_ids)
        .fetch_all(&mut **transaction)
        .await?;
    for (id, blob_ref) in rows {
        inventory.push(item(kind, id, action, Some(blob_ref)));
    }
    Ok(())
}

fn push_ids(inventory: &mut Vec<DeletionInventoryItem>, kind: DeletionItemKind, ids: &[Uuid]) {
    for id in ids {
        inventory.push(item(kind, *id, DeletionAction::RemoveRecord, None));
    }
}

fn item(
    kind: DeletionItemKind,
    id: Uuid,
    action: DeletionAction,
    blob_ref: Option<String>,
) -> DeletionInventoryItem {
    DeletionInventoryItem {
        kind,
        subject_id: id.to_string(),
        action,
        blob_ref,
    }
}

async fn persist_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    request: &TenantDeletionPlanRequest,
    inventory: &DeletionInventory,
) -> Result<(), sqlx::Error> {
    persist_scoped_inventory(
        transaction,
        &request.tenant_ref,
        request.request_id,
        &request.request_key,
        &request.correlation_id,
        "tenant",
        None,
        inventory,
    )
    .await
}
