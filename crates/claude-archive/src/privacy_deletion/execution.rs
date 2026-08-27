//! Atomic privacy-deletion execution and propagation.

use super::*;

struct DurableDeletionRequest {
    tenant_ref: String,
    scope_kind: String,
    scope_id: Option<Uuid>,
    correlation_id: String,
    state: String,
    completion_report: Option<serde_json::Value>,
}

impl PrivacyDeletionExecutor {
    /// Creates an executor over the Claude Archive database.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Finalizes one durable deletion request with an optional test fault.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionExecutionError`] when the request is absent,
    /// persistence fails, or the requested deterministic fault fires.
    #[expect(
        clippy::too_many_lines,
        reason = "the transaction boundary keeps every coupled terminal effect visible"
    )]
    pub async fn finalize(
        &self,
        request_id: Uuid,
        fault: Option<FinalizationFault>,
    ) -> Result<DeletionCompletionReport, PrivacyDeletionExecutionError> {
        let mut transaction = self.pool.begin().await?;
        let request = sqlx::query(
            "select tenant_ref, scope_kind, scope_id, correlation_id, state, completion_report
             from claude_archive.privacy_deletion_requests
             where request_id = $1 for update",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(request) = request else {
            transaction.rollback().await?;
            return Err(PrivacyDeletionExecutionError::NotFound);
        };
        let request = DurableDeletionRequest {
            tenant_ref: request.try_get("tenant_ref")?,
            scope_kind: request.try_get("scope_kind")?,
            scope_id: request.try_get("scope_id")?,
            correlation_id: request.try_get("correlation_id")?,
            state: request.try_get("state")?,
            completion_report: request.try_get("completion_report")?,
        };
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        if request.state == "completed" {
            let report = request
                .completion_report
                .ok_or(PrivacyDeletionExecutionError::NotFound)?;
            let report = serde_json::from_value(report)?;
            transaction.commit().await?;
            return Ok(report);
        }

        sqlx::query(
            "delete from claude_archive.export_observations
             where export_id::text in (
                 select subject_id from claude_archive.privacy_deletion_items
                 where request_id = $1 and item_kind = 'raw_archive'
             )",
        )
        .bind(request_id)
        .execute(&mut *transaction)
        .await?;

        if fault == Some(FinalizationFault::AfterFirstRemoval) {
            transaction.rollback().await?;
            return Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterFirstRemoval,
            ));
        }

        let tenant_accounts = if request.scope_kind == "tenant" {
            tenant_account_ids(&mut transaction, request_id).await?
        } else {
            Vec::new()
        };
        if request.scope_kind == "tenant" {
            delete_tenant_lifecycle_rows(&mut transaction, &request.tenant_ref).await?;
        }
        delete_inventory_rows(&mut transaction, request_id).await?;
        for account_id in tenant_accounts {
            sqlx::query("delete from claude_archive.accounts where account_id = $1")
                .bind(account_id)
                .execute(&mut *transaction)
                .await?;
        }
        insert_deletion_tombstones(
            &mut transaction,
            request_id,
            &request.tenant_ref,
            &request.correlation_id,
        )
        .await?;
        let category_counts: serde_json::Value = sqlx::query_scalar(
            "select coalesce(jsonb_object_agg(item_kind, item_count), '{}'::jsonb)
             from (
                 select item_kind, count(*) as item_count
                 from claude_archive.privacy_deletion_items
                 where request_id = $1 group by item_kind order by item_kind
             ) counts",
        )
        .bind(request_id)
        .fetch_one(&mut *transaction)
        .await?;
        let evidence_ref = format!("privacy-deletion/{request_id}");
        sqlx::query(
            "insert into claude_archive.privacy_deletion_audits
             (request_id, scope_kind, category_counts, correlation_id, outcome,
              evidence_blob_ref, completed_at)
             values ($1, $2, $3, $4, 'completed', $5, now())",
        )
        .bind(request_id)
        .bind(&request.scope_kind)
        .bind(&category_counts)
        .bind(&request.correlation_id)
        .bind(&evidence_ref)
        .execute(&mut *transaction)
        .await?;
        let report = DeletionCompletionReport {
            request_id,
            status: "completed".to_owned(),
            category_counts,
            evidence_blob_ref: evidence_ref,
        };
        let completion_report = serde_json::to_value(&report)?;
        sqlx::query("delete from claude_archive.privacy_deletion_items where request_id = $1")
            .bind(request_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "update claude_archive.privacy_deletion_requests
             set state = 'completed', completion_report = $2,
                 completed_at = now(), error_code = null
             where request_id = $1",
        )
        .bind(request_id)
        .bind(completion_report)
        .execute(&mut *transaction)
        .await?;
        let _ = request.scope_id;
        transaction.commit().await?;
        Ok(report)
    }

    /// Executes physical blob work and finalizes one durable deletion request.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionExecutionError`] when exact blob work or
    /// database finalization fails.
    pub async fn finalize_with_blob_store(
        &self,
        request_id: Uuid,
        blob_store: &crate::BlobStore,
        resolved_blobs: &[ResolvedDeletionBlob],
    ) -> Result<DeletionCompletionReport, PrivacyDeletionExecutionError> {
        let state: Option<String> = sqlx::query_scalar(
            "select state from claude_archive.privacy_deletion_requests where request_id = $1",
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        match state.as_deref() {
            None => return Err(PrivacyDeletionExecutionError::NotFound),
            Some("completed") => return self.finalize(request_id, None).await,
            Some(_) => {}
        }

        let erase_items: Vec<(i32, String)> = sqlx::query_as(
            "select item_index, blob_ref from claude_archive.privacy_deletion_items
             where request_id = $1 and action = 'erase_blob' and blob_ref is not null
             order by item_index",
        )
        .bind(request_id)
        .fetch_all(&self.pool)
        .await?;
        sqlx::query(
            "update claude_archive.privacy_deletion_requests
             set state = 'purging', error_code = null where request_id = $1",
        )
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        for (item_index, key) in erase_items {
            let resolved = resolved_blobs
                .iter()
                .find(|candidate| candidate.key == key)
                .ok_or(PrivacyDeletionExecutionError::UnresolvedBlob)?;
            let expected_key = format!("sha256/{}", resolved.reference.digest_hex);
            if expected_key != key {
                return Err(PrivacyDeletionExecutionError::UnresolvedBlob);
            }
            if blob_has_retained_reference(&self.pool, request_id, &key).await? {
                sqlx::query(
                    "update claude_archive.privacy_deletion_items
                     set action = 'retain_shared', state = 'retained'
                     where request_id = $1 and item_index = $2",
                )
                .bind(request_id)
                .bind(item_index)
                .execute(&self.pool)
                .await?;
                continue;
            }
            let _outcome = blob_store.erase(&resolved.reference)?;
            sqlx::query(
                "update claude_archive.privacy_deletion_items set state = 'purged'
                 where request_id = $1 and item_index = $2",
            )
            .bind(request_id)
            .bind(item_index)
            .execute(&self.pool)
            .await?;
        }
        self.finalize(request_id, None).await
    }
}

async fn tenant_account_ids(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "select distinct account_id from (
             select e.account_ref as account_id from claude_archive.exports e
             join claude_archive.privacy_deletion_items i
               on i.subject_id = e.export_id::text and i.item_kind = 'raw_archive'
             where i.request_id = $1 and e.account_ref is not null
             union
             select p.account_id from claude_archive.projects p
             join claude_archive.privacy_deletion_items i
               on i.subject_id = p.project_id::text and i.item_kind = 'project'
             where i.request_id = $1
             union
             select c.account_id from claude_archive.conversations c
             join claude_archive.privacy_deletion_items i
               on i.subject_id = c.conversation_id::text and i.item_kind = 'conversation'
             where i.request_id = $1
         ) owned order by account_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await
}

async fn delete_tenant_lifecycle_rows(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
) -> Result<(), sqlx::Error> {
    for query in [
        "delete from claude_archive.reparse_runs where tenant_ref = $1",
        "delete from claude_archive.parser_migration_reports where tenant_ref = $1",
        "delete from claude_archive.portable_exports where tenant_ref = $1",
    ] {
        sqlx::query(query)
            .bind(tenant_ref)
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}

async fn blob_has_retained_reference(
    pool: &PgPool,
    request_id: Uuid,
    blob_ref: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "select exists(
             select 1 from claude_archive.exports e
             where e.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'raw_archive'
                   and i.subject_id = e.export_id::text)
             union all
             select 1 from claude_archive.extracted_artifacts e
             where e.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'extracted_artifact'
                   and i.subject_id = e.extracted_artifact_id::text)
             union all
             select 1 from claude_archive.project_sources s
             where s.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'project_source'
                   and i.subject_id = s.source_id::text and i.action = 'remove_record')
             union all
             select 1 from claude_archive.content_parts p
             where p.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'content_part'
                   and i.subject_id = p.content_part_id::text and i.action = 'remove_record')
             union all
             select 1 from claude_archive.artifact_versions v
             where v.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'artifact_version'
                   and i.subject_id = v.version_id::text and i.action = 'remove_record')
             union all
             select 1 from claude_archive.assets a
             where a.blob_ref = $2 and not exists (
                 select 1 from claude_archive.privacy_deletion_items i
                 where i.request_id = $1 and i.item_kind = 'asset'
                   and i.subject_id = a.asset_id::text and i.action = 'remove_record')
         )",
    )
    .bind(request_id)
    .bind(blob_ref)
    .fetch_one(pool)
    .await
}

async fn delete_inventory_rows(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<(), sqlx::Error> {
    for query in [
        "delete from claude_archive.backup_status_audits where reference_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'external_reference' and action = 'remove_record')",
        "delete from claude_archive.external_references where reference_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'external_reference' and action = 'remove_record')",
        "delete from claude_archive.assets where asset_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'asset' and action = 'remove_record')",
        "delete from claude_archive.artifact_versions where version_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'artifact_version' and action = 'remove_record')",
        "delete from claude_archive.artifacts where artifact_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'artifact' and action = 'remove_record')",
        "delete from claude_archive.message_relations where relation_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'message_relation' and action = 'remove_record')",
        "delete from claude_archive.content_parts where content_part_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'content_part' and action = 'remove_record')",
        "delete from claude_archive.messages where message_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'message' and action = 'remove_record')",
        "delete from claude_archive.project_sources where source_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'project_source' and action = 'remove_record')",
        "delete from claude_archive.projects where project_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'project' and action = 'remove_record')",
        "delete from claude_archive.conversations where conversation_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'conversation' and action = 'remove_record')",
        "delete from claude_archive.revisions where revision_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'revision' and action = 'remove_record')",
        "delete from claude_archive.completeness_reports where report_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'completeness_report' and action = 'remove_record')",
        "delete from claude_archive.import_runs where run_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'import_run' and action = 'remove_record')",
        "delete from claude_archive.knowledge_analysis_links where completion_event_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'analysis_link' and action = 'remove_record')",
        "delete from claude_archive.inbox_events where event_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'inbox' and action = 'remove_record')",
        "delete from claude_archive.outbox_events where event_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'outbox' and action = 'remove_record')",
        "delete from claude_archive.extracted_artifacts where extracted_artifact_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'extracted_artifact')",
        "delete from claude_archive.exports where export_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'raw_archive')",
    ] {
        sqlx::query(query)
            .bind(request_id)
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}

async fn insert_deletion_tombstones(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    tenant_ref: &str,
    correlation_id: &str,
) -> Result<(), sqlx::Error> {
    use sha2::{Digest as _, Sha256};

    let subjects: Vec<String> = sqlx::query_scalar(
        "select subject_id from claude_archive.privacy_deletion_items
         where request_id = $1 and item_kind = 'downstream_tombstone'
           and action = 'emit_tombstone' order by subject_id",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    for subject_id in subjects {
        let envelope = serde_json::json!({
            "event_type": "ai_archive.subject.tombstoned.v1",
            "reason": "user_requested",
            "request_id": request_id,
            "subject_id": subject_id,
        });
        let digest = Sha256::digest(serde_json::to_vec(&envelope).unwrap_or_default());
        sqlx::query(
            "insert into claude_archive.outbox_events
             (event_id, event_type, aggregate_type, aggregate_id, envelope,
              payload_digest, correlation_id, tenant_ref, occurred_at)
             values ($1, 'ai_archive.subject.tombstoned.v1', 'tombstone', $2,
                     $3, $4, $5, $6, now()) on conflict do nothing",
        )
        .bind(Uuid::now_v7())
        .bind(&subject_id)
        .bind(&envelope)
        .bind(digest.as_slice())
        .bind(correlation_id)
        .bind(tenant_ref)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}
