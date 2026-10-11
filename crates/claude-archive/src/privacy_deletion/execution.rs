//! Privacy-deletion execution: store the evidence, remove the rows, erase the bytes, complete.
//!
//! Phase 0 stores the content-free evidence blob. Phase A (`rows.rs`) removes the rows in one
//! transaction. Phase B erases bytes with no long transaction held. Phase C writes the audit and
//! completes the request. A request resumes from whichever phase it stopped in, because each
//! phase is idempotent and the request state names the next one.

use super::rows::load_request;
use super::{
    DeletionCompletionReport, FinalizationFault, PgPool, PrivacyDeletionExecutionError,
    PrivacyDeletionExecutor, ResolvedDeletionBlob, Uuid, lock_tenant,
};
use crate::blob_protocol::{blob_lock, referenced_among};

impl PrivacyDeletionExecutor {
    /// Creates an executor over the Claude Archive database.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            erase_gate: None,
        }
    }

    /// Parks the eraser at `gate` for every blob it erases.
    ///
    /// This is the blocking fault seam of the interleaving tests (XR-021 CONTRACTS.md R2-07).
    #[must_use]
    pub fn with_erase_gate(mut self, gate: super::EraseGate) -> Self {
        self.erase_gate = Some(gate);
        self
    }

    /// Removes the rows of one durable deletion request and completes it, without byte work.
    ///
    /// `evidence` is the content-free blob the tombstones name; the caller stored it.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionExecutionError`] when the request is absent, persistence fails, or
    /// the requested deterministic fault fires.
    pub async fn finalize(
        &self,
        request_id: Uuid,
        evidence: &crate::BlobRef,
        fault: Option<FinalizationFault>,
    ) -> Result<DeletionCompletionReport, PrivacyDeletionExecutionError> {
        self.remove_rows(request_id, evidence, fault).await?;
        self.complete(request_id).await
    }

    /// Executes every phase of one durable deletion request: evidence, rows, bytes, completion.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyDeletionExecutionError`] when exact blob work or database finalization
    /// fails, or when the requested deterministic fault fires.
    pub async fn finalize_with_blob_store(
        &self,
        request_id: Uuid,
        blob_store: &crate::BlobStore,
        resolved_blobs: &[ResolvedDeletionBlob],
        fault: Option<FinalizationFault>,
    ) -> Result<DeletionCompletionReport, PrivacyDeletionExecutionError> {
        let state: Option<String> = sqlx::query_scalar(
            "select state from claude_archive.privacy_deletion_requests where request_id = $1",
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        match state.as_deref() {
            None => return Err(PrivacyDeletionExecutionError::NotFound),
            Some("completed") => return self.complete(request_id).await,
            Some(_) => {}
        }

        let evidence = self.store_evidence(request_id, blob_store).await?;
        self.remove_rows(request_id, &evidence, fault).await?;
        self.erase_bytes(request_id, blob_store, resolved_blobs)
            .await?;
        self.complete(request_id).await
    }

    /// Phase 0: the evidence every tombstone and the audit point at.
    ///
    /// It holds the request id, the scope and per-category totals, never a timestamp or content,
    /// so repeating a request stores the same bytes under the same digest.
    async fn store_evidence(
        &self,
        request_id: Uuid,
        blob_store: &crate::BlobStore,
    ) -> Result<crate::BlobRef, PrivacyDeletionExecutionError> {
        let (scope_kind, totals): (String, serde_json::Value) = sqlx::query_as(
            "select r.scope_kind,
                    coalesce((select jsonb_object_agg(item_kind, item_count)
                              from (select item_kind, count(*) as item_count
                                    from claude_archive.privacy_deletion_items
                                    where request_id = r.request_id
                                    group by item_kind) counts), '{}'::jsonb)
             from claude_archive.privacy_deletion_requests r where r.request_id = $1",
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await?;
        let evidence = serde_json::to_vec(&serde_json::json!({
            "request_id": request_id,
            "scope": scope_kind,
            "totals": totals,
        }))?;
        Ok(blob_store.store(crate::MediaType::parse("application/json")?, &evidence)?)
    }

    /// Phase B: erase every blob the remaining rows no longer reference.
    ///
    /// Each blob is one transaction: the tenant lock, then the digest lock, then the same
    /// reference check the row phase used, then either keeping the blob (`retain_shared`) or
    /// erasing it (`purged`), committed while the digest lock is held. A registrar that stored the
    /// same bytes after the row phase therefore either shows up in the check or waits and finds
    /// the blob gone (`blob_protocol` module).
    async fn erase_bytes(
        &self,
        request_id: Uuid,
        blob_store: &crate::BlobStore,
        resolved_blobs: &[ResolvedDeletionBlob],
    ) -> Result<(), PrivacyDeletionExecutionError> {
        let erase_items: Vec<(i32, String)> = sqlx::query_as(
            "select item_index, blob_ref from claude_archive.privacy_deletion_items
             where request_id = $1 and action = 'erase_blob' and state <> 'purged'
               and blob_ref is not null
             order by item_index",
        )
        .bind(request_id)
        .fetch_all(&self.pool)
        .await?;
        for (item_index, key) in erase_items {
            let resolved = resolved_blobs
                .iter()
                .find(|candidate| candidate.key == key)
                .ok_or(PrivacyDeletionExecutionError::UnresolvedBlob)?;
            if format!("sha256/{}", resolved.reference.digest_hex) != key {
                return Err(PrivacyDeletionExecutionError::UnresolvedBlob);
            }
            self.erase_blob(request_id, item_index, &key, blob_store, resolved)
                .await?;
        }
        Ok(())
    }

    async fn erase_blob(
        &self,
        request_id: Uuid,
        item_index: i32,
        key: &str,
        blob_store: &crate::BlobStore,
        resolved: &ResolvedDeletionBlob,
    ) -> Result<(), PrivacyDeletionExecutionError> {
        let mut transaction = self.pool.begin().await?;
        let request = load_request(&mut transaction, request_id).await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        blob_lock(&mut transaction, &resolved.reference.digest_hex).await?;
        let still_referenced = referenced_among(&mut transaction, &[key.to_owned()]).await?;
        let outcome = if still_referenced.is_empty() {
            if let Some(gate) = &self.erase_gate {
                gate.park(&resolved.reference.digest_hex).await;
            }
            let _erased = blob_store.erase(&resolved.reference)?;
            ("erase_blob", "purged")
        } else {
            ("retain_shared", "retained")
        };
        sqlx::query(
            "update claude_archive.privacy_deletion_items set action = $3, state = $4
             where request_id = $1 and item_index = $2",
        )
        .bind(request_id)
        .bind(item_index)
        .bind(outcome.0)
        .bind(outcome.1)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Phase C: the audit, the completion report and the end of the item list, in one transaction.
    async fn complete(
        &self,
        request_id: Uuid,
    ) -> Result<DeletionCompletionReport, PrivacyDeletionExecutionError> {
        let mut transaction = self.pool.begin().await?;
        let request = load_request(&mut transaction, request_id).await?;
        lock_tenant(&mut transaction, &request.tenant_ref).await?;
        if request.state == "completed" {
            let report = request
                .completion_report
                .ok_or(PrivacyDeletionExecutionError::NotFound)?;
            let report = serde_json::from_value(report)?;
            transaction.commit().await?;
            return Ok(report);
        }
        let evidence_ref: Option<String> = sqlx::query_scalar(
            "select evidence_blob_ref from claude_archive.privacy_deletion_requests
             where request_id = $1",
        )
        .bind(request_id)
        .fetch_one(&mut *transaction)
        .await?;
        let evidence_ref = evidence_ref.ok_or(PrivacyDeletionExecutionError::NotFound)?;
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
        transaction.commit().await?;
        Ok(report)
    }
}
