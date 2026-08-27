//! Inventory-first privacy deletion boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Row as _, Transaction};
use uuid::Uuid;

mod execution;
mod persistence;
mod planning;

use persistence::persist_scoped_inventory;

/// Authenticated tenant request to inventory all account-owned evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeletionPlanRequest {
    /// Opaque authenticated tenant reference persisted with the operation.
    pub tenant_ref: String,
    /// Internal provider-account identity authorized for this tenant.
    pub account_id: Uuid,
    /// Stable opaque deletion operation identity.
    pub request_id: Uuid,
    /// Idempotency key supplied by the caller.
    pub request_key: String,
    /// Correlation identity carried through audit and propagation.
    pub correlation_id: String,
}

/// Authenticated request to inventory one raw provider export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawExportDeletionPlanRequest {
    /// Opaque authenticated tenant reference persisted with the operation.
    pub tenant_ref: String,
    /// Internal provider-account identity authorized for this tenant.
    pub account_id: Uuid,
    /// Stable opaque deletion operation identity.
    pub request_id: Uuid,
    /// Idempotency key supplied by the caller.
    pub request_key: String,
    /// Correlation identity carried through audit and propagation.
    pub correlation_id: String,
    /// Internal raw-export identity requested for deletion.
    pub export_id: Uuid,
}

/// Authenticated request to inventory one conversation and its raw evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationDeletionPlanRequest {
    /// Opaque authenticated tenant reference persisted with the operation.
    pub tenant_ref: String,
    /// Internal provider-account identity authorized for this tenant.
    pub account_id: Uuid,
    /// Stable opaque deletion operation identity.
    pub request_id: Uuid,
    /// Idempotency key supplied by the caller.
    pub request_key: String,
    /// Correlation identity carried through audit and propagation.
    pub correlation_id: String,
    /// Internal conversation identity requested for deletion.
    pub conversation_id: Uuid,
}

/// Closed deletion-inventory category vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionItemKind {
    /// Immutable source archive bytes.
    RawArchive,
    /// Bounded extraction output bytes.
    ExtractedArtifact,
    /// Import attempt over a source archive.
    ImportRun,
    /// Import completeness evidence.
    CompletenessReport,
    /// Preserved unrecognized provider record.
    UnknownRecord,
    /// Claude Project projection.
    Project,
    /// Claude Project Knowledge source.
    ProjectSource,
    /// Conversation graph root.
    Conversation,
    /// Conversation graph node.
    Message,
    /// Typed edge between graph nodes.
    MessageRelation,
    /// Ordered heterogeneous message part.
    ContentPart,
    /// First-class Artifact.
    Artifact,
    /// Immutable Artifact version.
    ArtifactVersion,
    /// Uploaded or generated file evidence.
    Asset,
    /// Provider external-reference observation.
    ExternalReference,
    /// Normalized observation revision.
    Revision,
    /// Downstream Knowledge analysis linkage.
    AnalysisLink,
    /// Consumed command/event deduplication record.
    Inbox,
    /// Pending or published archive event.
    Outbox,
    /// Subject requiring downstream removal propagation.
    DownstreamTombstone,
    /// Blob retained because surviving evidence still references it.
    SharedBlob,
}

/// Work to perform for one deletion inventory item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionAction {
    /// Erase exact verified bytes from owned storage.
    EraseBlob,
    /// Remove one database record during finalization.
    RemoveRecord,
    /// Retain bytes reached from evidence outside the deletion scope.
    RetainShared,
    /// Retain normalized data with independent raw provenance.
    RetainEvidenced,
    /// Publish an authoritative downstream subject tombstone.
    EmitTombstone,
}

/// One content-free target or retained-evidence decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionInventoryItem {
    /// Category of owned evidence.
    pub kind: DeletionItemKind,
    /// Opaque internal identity or content-addressed blob reference.
    pub subject_id: String,
    /// Planned deletion or retention action.
    pub action: DeletionAction,
    /// Optional exact owned blob reference used by physical execution.
    pub blob_ref: Option<String>,
}

/// Stable complete preflight inventory for one request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionInventory {
    /// Opaque operation identity.
    pub request_id: Uuid,
    /// Items in stable category and identity order.
    pub items: Vec<DeletionInventoryItem>,
    /// Per-category totals derived from [`Self::items`].
    pub category_totals: BTreeMap<DeletionItemKind, usize>,
}

/// Content-free durable result of a completed privacy deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionCompletionReport {
    /// Opaque completed operation identity.
    pub request_id: Uuid,
    /// Stable terminal status.
    pub status: String,
    /// Counts derived from the persisted deletion inventory.
    pub category_counts: serde_json::Value,
    /// Non-sensitive reference to terminal audit evidence.
    pub evidence_blob_ref: String,
}

/// Exact owned `BlobStore` reference resolved for one persisted inventory key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDeletionBlob {
    /// Canonical content-addressed key stored in the deletion inventory.
    pub key: String,
    /// Validated reference used for exact verified physical erasure.
    pub reference: crate::BlobRef,
}

/// Privacy-deletion planning failure.
#[derive(Debug, thiserror::Error)]
pub enum PrivacyDeletionError {
    /// `PostgreSQL` refused an inventory operation.
    #[error("privacy deletion inventory persistence failed")]
    Database(#[from] sqlx::Error),
    /// The authenticated account does not exist in this bounded context.
    #[error("privacy deletion subject was not found")]
    NotFound,
    /// A request identity was reused with different immutable arguments.
    #[error("privacy deletion request identity conflicts with durable state")]
    Conflict,
}

/// Deterministic failure point used to prove finalization transactionality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationFault {
    /// Fail immediately after the first selected database removal.
    AfterFirstRemoval,
}

/// Privacy-deletion execution failure.
#[derive(Debug, thiserror::Error)]
pub enum PrivacyDeletionExecutionError {
    /// `PostgreSQL` refused a finalization operation.
    #[error("privacy deletion finalization persistence failed")]
    Database(#[from] sqlx::Error),
    /// The requested durable deletion operation does not exist.
    #[error("privacy deletion operation was not found")]
    NotFound,
    /// A deterministic test fault interrupted finalization.
    #[error("privacy deletion finalization fault injected")]
    Injected(FinalizationFault),
    /// Durable terminal evidence cannot be decoded by this build.
    #[error("privacy deletion completion report is invalid")]
    InvalidReport(#[from] serde_json::Error),
    /// Exact locally owned blob verification or erasure failed.
    #[error("privacy deletion blob operation failed")]
    Blob(#[from] crate::StoreError),
    /// Durable inventory names a blob the caller did not resolve exactly.
    #[error("privacy deletion blob inventory is unresolved")]
    UnresolvedBlob,
}

/// PostgreSQL-backed inventory planner.
#[derive(Debug, Clone)]
pub struct PrivacyDeletionPlanner {
    pool: PgPool,
}

/// Database finalizer for a previously persisted deletion inventory.
#[derive(Debug, Clone)]
pub struct PrivacyDeletionExecutor {
    pool: PgPool,
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
