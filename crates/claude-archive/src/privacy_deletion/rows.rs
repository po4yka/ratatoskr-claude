//! Phase A of privacy deletion: remove the rows, then record that they are gone.
//!
//! The order of a deletion is rows first, bytes second, complete last (XR-021 CONTRACTS.md S07).
//! This phase is one transaction under the tenant lock. It deletes the scope rows and their
//! dependents, decides which blobs the remaining rows still reference, enqueues the tombstones and
//! moves the request to `purging` with the instant the rows left. The inventory items stay: they
//! carry the blob references the byte phase still has to erase. At no commit point does a
//! surviving row reference a missing blob, and every blob not yet erased is named by an item.

use ratatoskr_identifiers::WireTimestamp;

use super::*;

/// Every `(table, column)` foreign key into a deletion root that is not `ON DELETE CASCADE` and
/// that deletion deals with itself, either by deleting the referencing rows with their parent or
/// by letting the database clear the reference.
///
/// `every_foreign_key_into_deletion_roots_is_handled` compares this list with `pg_constraint`, so
/// a new foreign key cannot reach production without deletion learning about it.
///
/// `entity_versions` has no foreign key (its `entity_id` names a project or a conversation by
/// kind), so it is not listed: `restore_retained_versions` drops the versions of records that no
/// longer exist.
pub(super) const HANDLED_REFERENCES: &[(&str, &str)] = &[
    // Deleted with their export, import run or account by `delete_inventory_rows`.
    ("exports", "account_ref"),
    ("import_runs", "export_id"),
    ("completeness_reports", "run_id"),
    ("revisions", "export_id"),
    ("platform_operation_imports", "export_id"),
    ("platform_operation_imports", "import_run_id"),
    ("reparse_runs", "export_id"),
    ("tombstones", "evidence_export_id"),
    ("projects", "account_id"),
    ("conversations", "account_id"),
    // Deleted with the project, conversation, message, source, Artifact or reference they hang on.
    ("project_sources", "project_id"),
    ("assets", "source_id"),
    ("assets", "message_id"),
    ("messages", "conversation_id"),
    ("messages", "parent_message_id"),
    ("message_relations", "parent_message_id"),
    ("message_relations", "child_message_id"),
    ("content_parts", "message_id"),
    ("artifacts", "account_id"),
    ("artifacts", "project_id"),
    ("artifacts", "conversation_id"),
    ("artifacts", "message_id"),
    ("artifact_versions", "artifact_id"),
    ("external_references", "project_id"),
    ("external_references", "conversation_id"),
    ("external_references", "artifact_id"),
    ("backup_status_audits", "reference_id"),
    // Cleared by the database when a removed project outlives a retained conversation.
    ("conversations", "project_id"),
];

/// The durable request a phase works on.
pub(super) struct DurableDeletionRequest {
    pub(super) tenant_ref: String,
    pub(super) scope_kind: String,
    pub(super) correlation_id: String,
    pub(super) state: String,
    pub(super) completion_report: Option<serde_json::Value>,
}

pub(super) async fn load_request(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<DurableDeletionRequest, PrivacyDeletionExecutionError> {
    let request = sqlx::query(
        "select tenant_ref, scope_kind, correlation_id, state, completion_report
         from claude_archive.privacy_deletion_requests
         where request_id = $1 for update",
    )
    .bind(request_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PrivacyDeletionExecutionError::NotFound)?;
    Ok(DurableDeletionRequest {
        tenant_ref: request.try_get("tenant_ref")?,
        scope_kind: request.try_get("scope_kind")?,
        correlation_id: request.try_get("correlation_id")?,
        state: request.try_get("state")?,
        completion_report: request.try_get("completion_report")?,
    })
}

impl PrivacyDeletionExecutor {
    /// Phase A. Idempotent: a request already past the row phase is left alone.
    ///
    /// `evidence` is the content-free blob the tombstones name. The caller stores it before this
    /// transaction, so a retry reuses the same reference.
    pub(super) async fn remove_rows(
        &self,
        request_id: Uuid,
        evidence: &crate::BlobRef,
        fault: Option<FinalizationFault>,
    ) -> Result<(), PrivacyDeletionExecutionError> {
        let mut transaction = self.pool.begin().await?;
        let request = load_request(&mut transaction, request_id).await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        if request.state != "planned" {
            transaction.commit().await?;
            return Ok(());
        }
        // Imports lock by archive account, so a deletion of that account's rows excludes them too.
        let accounts = tenant_account_ids(&mut transaction, request_id).await?;
        for account_id in &accounts {
            lock_tenant(&mut transaction, &account_id.to_string()).await?;
        }

        // Read before the observations go: a subject is tombstoned under the archive that saw it.
        let subjects = super::tombstones::collect_subjects(&mut transaction, request_id).await?;
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

        let tenant_scope = request.scope_kind == "tenant";
        if tenant_scope {
            delete_tenant_lifecycle_rows(&mut transaction, &request.tenant_ref, &accounts).await?;
        }
        delete_export_dependents(&mut transaction, request_id).await?;
        delete_inventory_rows(&mut transaction, request_id).await?;
        if tenant_scope {
            for account_id in &accounts {
                sqlx::query("delete from claude_archive.accounts where account_id = $1")
                    .bind(account_id)
                    .execute(&mut *transaction)
                    .await?;
            }
        }
        // What a retained export presented is what the survivors hold before any fact is built.
        super::restore::restore_retained_versions(&mut transaction, &accounts).await?;
        retain_blobs_still_referenced(&mut transaction, request_id).await?;

        let removed_at = WireTimestamp::now();
        let archives = super::tombstones::deleted_archives(&subjects);
        super::tombstones::enqueue_tombstones(
            &mut transaction,
            &request,
            request_id,
            subjects,
            evidence,
            removed_at,
        )
        .await?;
        // Knowledge drops what was announced under a tombstoned archive, so what a retained export
        // still holds is announced again, after the tombstone.
        super::tombstones::reannounce_survivors(
            &mut transaction,
            &request,
            request_id,
            &archives,
            removed_at,
        )
        .await?;
        super::tombstones::purge_facts_of_deleted_archives(&mut transaction, &request, &archives)
            .await?;
        sqlx::query(
            "update claude_archive.privacy_deletion_requests
             set state = 'purging', error_code = null, evidence_blob_ref = $2,
                 rows_removed_at = $3::timestamptz
             where request_id = $1",
        )
        .bind(request_id)
        .bind(format!("sha256/{}", evidence.digest_hex))
        .bind(removed_at.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if fault == Some(FinalizationFault::AfterRowsCommitted) {
            return Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterRowsCommitted,
            ));
        }
        Ok(())
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

/// Tenant-scoped lifecycle rows. They are keyed by a tenant string, which is the Platform user
/// reference for rows written through a deletion and the archive account id for rows written by
/// the operator commands, so both spellings are removed.
async fn delete_tenant_lifecycle_rows(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
    accounts: &[Uuid],
) -> Result<(), sqlx::Error> {
    let mut spellings: Vec<String> = accounts.iter().map(Uuid::to_string).collect();
    spellings.push(tenant_ref.to_owned());
    for query in [
        "delete from claude_archive.reparse_runs where tenant_ref = any($1)",
        "delete from claude_archive.parser_migration_reports where tenant_ref = any($1)",
        "delete from claude_archive.portable_exports where tenant_ref = any($1)",
    ] {
        sqlx::query(query)
            .bind(&spellings)
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}

/// Rows that reference a selected export for every scope: the Platform operation bindings and
/// their import runs' reparse history, and tombstones whose evidence is that export.
async fn delete_export_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<(), sqlx::Error> {
    for query in [
        "delete from claude_archive.platform_operation_imports where export_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'raw_archive')",
        "delete from claude_archive.reparse_runs where export_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'raw_archive')",
        "delete from claude_archive.tombstones where evidence_export_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'raw_archive')",
    ] {
        sqlx::query(query)
            .bind(request_id)
            .execute(&mut **transaction)
            .await?;
    }
    Ok(())
}

/// Turns an erase into a retain for every blob a remaining row still references.
///
/// The rows are already gone, so "referenced by something that remains" is a plain lookup, the
/// same one the eraser repeats under the digest lock before it removes any bytes.
async fn retain_blobs_still_referenced(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
) -> Result<(), sqlx::Error> {
    let keys: Vec<String> = sqlx::query_scalar(
        "select distinct blob_ref from claude_archive.privacy_deletion_items
         where request_id = $1 and action = 'erase_blob' and blob_ref is not null",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await?;
    let referenced: Vec<String> = crate::blob_protocol::referenced_among(transaction, &keys)
        .await?
        .into_iter()
        .collect();
    sqlx::query(
        "update claude_archive.privacy_deletion_items
         set action = 'retain_shared', state = 'retained'
         where request_id = $1 and action = 'erase_blob' and blob_ref = any($2)",
    )
    .bind(request_id)
    .bind(&referenced)
    .execute(&mut **transaction)
    .await?;
    Ok(())
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
        "update claude_archive.messages set parent_message_id = null
         where parent_message_id::text in
               (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'message' and action = 'remove_record')
           and message_id::text not in
               (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'message' and action = 'remove_record')",
        "delete from claude_archive.messages where message_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'message' and action = 'remove_record')",
        "delete from claude_archive.assets where source_id::text in
         (select subject_id from claude_archive.privacy_deletion_items where request_id = $1 and item_kind = 'project_source' and action = 'remove_record')",
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
