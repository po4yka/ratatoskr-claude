//! Authenticated tenant-scoped archive receipt.
//!
//! A delivery arrives with tenant claims and an acquisition mode. Receipt
//! verifies the claims against tenants this archive actually knows before
//! the first byte is read, streams the archive through the capped,
//! content-addressed blob store while hashing it, then records the export
//! and its initial import run in one transaction. Re-delivering a known
//! digest is not an error and not a second snapshot: it reports the existing
//! export as an explicit duplicate outcome.

use uuid::Uuid;

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

use crate::blob_store::{BlobRef, BlobStore, MediaType, StoreError};
use crate::database::Database;
use crate::import_state::ImportState;

/// The media type every raw provider archive carries until parser detection
/// (a later plan item) proves a finer type from the stored bytes.
const RAW_ARCHIVE_MEDIA_TYPE: &str = "application/zip";

/// How a provider snapshot was obtained. Closed vocabulary mirroring the
/// schema CHECK; an unknown mode cannot be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionMode {
    /// A personal export from Claude Free, Pro, or Max.
    ConsumerExport,
    /// An organization/workspace export.
    OrganizationExport,
    /// Enterprise Compliance API evidence.
    ComplianceApi,
    /// A conversation captured manually by its owner.
    ManualConversationCapture,
    /// Data brought in from a predecessor system.
    LegacyImport,
}

impl AcquisitionMode {
    /// The database text for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConsumerExport => "consumer_export",
            Self::OrganizationExport => "organization_export",
            Self::ComplianceApi => "compliance_api",
            Self::ManualConversationCapture => "manual_conversation_capture",
            Self::LegacyImport => "legacy_import",
        }
    }

    /// Parses the database text for a mode.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let mode = match text {
            "consumer_export" => Self::ConsumerExport,
            "organization_export" => Self::OrganizationExport,
            "compliance_api" => Self::ComplianceApi,
            "manual_conversation_capture" => Self::ManualConversationCapture,
            "legacy_import" => Self::LegacyImport,
            _ => return None,
        };
        Some(mode)
    }
}

/// Receipt failure. Text names classes and identifiers, never archive bytes
/// or filenames.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReceiptError {
    /// The claimed tenant resolves to no known account or organization.
    #[error("the delivery names no tenant this archive knows")]
    UnknownTenant,
    /// The claim names an account and an organization at the same time, or
    /// names neither; receipt needs exactly one scope.
    #[error("the delivery must name exactly one account or organization scope")]
    AmbiguousScope,
    /// The delivered archive passed its declared maximum size mid-stream.
    #[error("the archive exceeded its declared limit of {limit_bytes} bytes")]
    ArchiveTooLarge {
        /// The declared maximum, named by the refusal.
        limit_bytes: u64,
    },
    /// The delivery carried no bytes at all.
    #[error("the delivery carried no bytes")]
    EmptyDelivery,
    /// The blob store refused or could not hold the delivery.
    #[error("the archive could not be stored")]
    Storage(#[from] StoreError),
    /// An archive-owned database operation failed.
    #[error("a receipt database operation failed")]
    Query(#[from] sqlx::Error),
    /// Platform's declared raw archive identity differs from the stream.
    #[error("the delivered archive differs from Platform's declared identity")]
    DeclaredIdentityMismatch,
    /// A typed Platform operation report could not be encoded.
    #[error("the terminal operation report could not be encoded")]
    ReportEncoding(#[from] serde_json::Error),
    /// A terminal operation report could not be placed in its full event envelope.
    #[error("the terminal operation report could not be enveloped")]
    ReportEnvelope(#[source] ratatoskr_event_envelope::EnvelopeError),
    /// The published operation-contract vocabulary rejected a fixed value.
    #[error("the terminal operation report could not satisfy its contract")]
    ReportContract,
}

/// Platform's independently computed raw archive identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveIdentity {
    /// Lowercase SHA-256 hexadecimal digest expected for the stream.
    pub sha256: String,
    /// Exact byte length expected for the stream.
    pub byte_size: u64,
}

/// Platform operation waiting for the terminal result this receipt can prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlatformOperation {
    /// Platform-owned operation identity from the trusted receipt boundary.
    pub operation_id: Uuid,
}

/// What one receipt attempt actually did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptOutcome {
    /// The archive was new: it was stored, and an export plus its initial
    /// import run were recorded.
    Stored {
        /// The fresh export record.
        export_id: Uuid,
        /// The initial import run created for that export.
        run_id: Uuid,
        /// Where the raw bytes live.
        blob_ref: crate::blob_store::BlobRef,
    },
    /// The digest already exists: nothing was re-stored, and the existing
    /// export is named instead.
    Duplicate {
        /// The pre-existing export holding these bytes.
        existing_export_id: Uuid,
    },
}

/// The raw tenant claims a delivery arrives with, before verification. Both
/// scopes may be claimed at once - refusing that combination is exactly the
/// authenticator's job, so the ambiguity stays constructible and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantClaim {
    /// A claimed personal-account identifier.
    pub account: Option<Uuid>,
    /// A claimed organization/workspace identifier.
    pub organization: Option<Uuid>,
}

/// One verified tenant identity. Construction is verification: the claim
/// resolved to exactly one known account or organization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantScope {
    /// A personal-account tenant.
    Account(Uuid),
    /// An organization/workspace tenant.
    Organization(Uuid),
}

/// The receipt pipeline entry point.
///
/// Order of operations is the safety story: tenant claims are verified
/// before any byte is read; the archive then streams through the capped
/// blob store, so a hostile or oversized delivery never becomes durable
/// state; only after raw placement does one transaction record the export
/// and its initial import run. A digest that already exists resolves to an
/// explicit duplicate outcome naming the existing export - the unique
/// constraint arbitrates concurrent deliveries of identical bytes.
///
/// # Errors
///
/// Returns [`ReceiptError::UnknownTenant`] or [`ReceiptError::
/// AmbiguousScope`] before storage, [`ReceiptError::ArchiveTooLarge`] and
/// [`ReceiptError::EmptyDelivery`] for refused streams (leaving nothing
/// durable behind), [`ReceiptError::Storage`] for blob-store failures, and
/// [`ReceiptError::Query`] for database failures.
pub async fn receive_archive(
    database: &Database,
    store: &BlobStore,
    claim: &TenantClaim,
    mode: AcquisitionMode,
    mut reader: impl std::io::Read,
    max_bytes: u64,
) -> Result<ReceiptOutcome, ReceiptError> {
    let scope = authenticate_claim(database, claim).await?;
    let media_type = MediaType::parse(RAW_ARCHIVE_MEDIA_TYPE)?;
    let blob_ref = store
        .store_stream(media_type, &mut reader, max_bytes)
        .map_err(|error| match error {
            StoreError::LimitExceeded { limit_bytes } => {
                ReceiptError::ArchiveTooLarge { limit_bytes }
            }
            StoreError::EmptyInput => ReceiptError::EmptyDelivery,
            other => ReceiptError::Storage(other),
        })?;
    record_stored_archive(database, scope, mode, blob_ref, None).await
}

/// Receives a Platform-bound archive only when declared identity matches.
///
/// # Errors
///
/// Returns [`ReceiptError::DeclaredIdentityMismatch`] without publishing raw
/// bytes when the completed stream differs from Platform's supplied identity.
pub async fn receive_platform_archive(
    database: &Database,
    store: &BlobStore,
    claim: &TenantClaim,
    mode: AcquisitionMode,
    mut reader: impl std::io::Read,
    max_bytes: u64,
    identity: &ArchiveIdentity,
) -> Result<ReceiptOutcome, ReceiptError> {
    let scope = authenticate_claim(database, claim).await?;
    let media_type = MediaType::parse(RAW_ARCHIVE_MEDIA_TYPE)?;
    let blob_ref = store
        .store_stream_with_identity(
            media_type,
            &mut reader,
            max_bytes,
            &identity.sha256,
            identity.byte_size,
        )
        .map_err(|error| match error {
            StoreError::LimitExceeded { limit_bytes } => {
                ReceiptError::ArchiveTooLarge { limit_bytes }
            }
            StoreError::EmptyInput => ReceiptError::EmptyDelivery,
            StoreError::DeclaredIdentityMismatch => ReceiptError::DeclaredIdentityMismatch,
            other => ReceiptError::Storage(other),
        })?;
    record_stored_archive(database, scope, mode, blob_ref, None).await
}

/// Receives one Platform archive and durably records its terminal raw-receipt result.
///
/// # Errors
///
/// Returns a receipt error when scope verification, identity-checked storage,
/// or the atomic export/run/report transaction cannot complete.
#[allow(
    clippy::too_many_arguments,
    reason = "the public Platform receipt contract carries the explicit archive and operation boundaries"
)]
pub async fn receive_platform_operation_archive(
    database: &Database,
    store: &BlobStore,
    claim: &TenantClaim,
    mode: AcquisitionMode,
    reader: impl std::io::Read,
    max_bytes: u64,
    identity: &ArchiveIdentity,
    operation: PlatformOperation,
) -> Result<ReceiptOutcome, ReceiptError> {
    let scope = authenticate_claim(database, claim).await?;
    let media_type = MediaType::parse(RAW_ARCHIVE_MEDIA_TYPE)?;
    let blob_ref = store
        .store_stream_with_identity(
            media_type,
            reader,
            max_bytes,
            &identity.sha256,
            identity.byte_size,
        )
        .map_err(|error| match error {
            StoreError::LimitExceeded { limit_bytes } => {
                ReceiptError::ArchiveTooLarge { limit_bytes }
            }
            StoreError::EmptyInput => ReceiptError::EmptyDelivery,
            StoreError::DeclaredIdentityMismatch => ReceiptError::DeclaredIdentityMismatch,
            other => ReceiptError::Storage(other),
        })?;
    record_stored_archive(database, scope, mode, blob_ref, Some(operation)).await
}

/// Records a previously verified Platform archive and its terminal raw-receipt report.
///
/// Callers must authenticate the scope before streaming and pass only a
/// [`BlobRef`] produced by the identity-verifying `BlobStore` operation.
///
/// # Errors
///
/// Returns a receipt error when the durable export, run, or outbox report
/// cannot be committed atomically.
pub async fn record_verified_platform_archive(
    database: &Database,
    scope: TenantScope,
    blob_ref: BlobRef,
    operation: PlatformOperation,
) -> Result<ReceiptOutcome, ReceiptError> {
    record_stored_archive(
        database,
        scope,
        AcquisitionMode::ConsumerExport,
        blob_ref,
        Some(operation),
    )
    .await
}

/// Records a verified raw archive and starts its import run.
async fn record_stored_archive(
    database: &Database,
    scope: TenantScope,
    mode: AcquisitionMode,
    blob_ref: BlobRef,
    platform_operation: Option<PlatformOperation>,
) -> Result<ReceiptOutcome, ReceiptError> {
    let digest_bytes = decode_digest_hex(&blob_ref.digest_hex)
        .ok_or(ReceiptError::Storage(StoreError::InvalidIdentity))?;
    let (account_ref, organization_ref) = match scope {
        TenantScope::Account(account) => (Some(account), None),
        TenantScope::Organization(organization) => (None, Some(organization)),
    };

    let export_id = Uuid::now_v7();
    let ai_archive_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    let mut transaction = database.pool().begin().await.map_err(ReceiptError::Query)?;
    let insert = sqlx::query(
        "insert into claude_archive.exports
             (export_id, ai_archive_id, account_ref, organization_ref, acquisition, archive_hash,
              blob_ref, byte_size, received_at)
         values ($1, $2, $3, $4, $5, $6, $7, $8, now())",
    )
    .bind(export_id)
    .bind(ai_archive_id)
    .bind(account_ref)
    .bind(organization_ref)
    .bind(mode.as_str())
    .bind(digest_bytes.clone())
    .bind(blob_key(&blob_ref))
    .bind(i64::try_from(blob_ref.length_bytes).unwrap_or(i64::MAX))
    .execute(&mut *transaction)
    .await;

    if let Err(error) = insert {
        drop(transaction);
        if is_archive_hash_conflict(&error) {
            return duplicate_of(database, &digest_bytes).await;
        }
        return Err(ReceiptError::Query(error));
    }

    sqlx::query(
        "insert into claude_archive.import_runs (run_id, export_id, state)
         values ($1, $2, $3)",
    )
    .bind(run_id)
    .bind(export_id)
    .bind(ImportState::Received.as_str())
    .execute(&mut *transaction)
    .await
    .map_err(ReceiptError::Query)?;

    if let Some(operation) = platform_operation {
        let report = raw_stored_partial(operation, ai_archive_id)?;
        let envelope = operation_report_envelope(&report)?;
        let payload_digest = Sha256::digest(
            serde_json::to_vec(&envelope.payload).map_err(ReceiptError::ReportEncoding)?,
        );
        sqlx::query(
            "insert into claude_archive.outbox_events
                 (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
                  correlation_id, occurred_at)
             values ($1, $2, 'operation', $3, $4, $5, $6, $7::timestamptz)",
        )
        .bind(envelope.event_id.0)
        .bind(envelope.event_type.to_wire())
        .bind(operation.operation_id.to_string())
        .bind(serde_json::to_value(&envelope).map_err(ReceiptError::ReportEncoding)?)
        .bind(payload_digest.as_slice())
        .bind(envelope.correlation_id.to_string())
        .bind(envelope.occurred_at.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(ReceiptError::Query)?;
    }

    transaction.commit().await.map_err(ReceiptError::Query)?;

    Ok(ReceiptOutcome::Stored {
        export_id,
        run_id,
        blob_ref,
    })
}

/// Builds the only terminal fact raw receipt can honestly establish.
fn raw_stored_partial(
    operation: PlatformOperation,
    ai_archive_id: Uuid,
) -> Result<OperationReported, ReceiptError> {
    let archive =
        AiArchiveId::parse(&ai_archive_id.to_string()).map_err(|_| ReceiptError::ReportContract)?;
    let operation_id = OperationId::parse(&operation.operation_id.to_string())
        .map_err(|_| ReceiptError::ReportContract)?;
    let provider = AiProvider::parse("claude").map_err(|_| ReceiptError::ReportContract)?;
    let result_kind = OperationResultKind::parse("ai_archive.import")
        .map_err(|_| ReceiptError::ReportContract)?;
    let report = OperationReported {
        operation_id,
        status: OperationStatus::PartiallySucceeded,
        stage: None,
        progress_percent: None,
        results: vec![OperationResultRef {
            result_kind,
            target: EntityRef::from(archive),
            blob: None,
            ai_archive_import_summary: Some(AiArchiveOperationSummary {
                ai_archive_id: archive,
                provider,
                completeness: AiArchiveCompleteness::Unknown,
                conversation_count: 0,
                message_count: 0,
                asset_count: 0,
                gap_count: 1,
                warning_count: 1,
            }),
            extensions: Extensions::new(),
        }],
        error: None,
        warnings: Vec::new(),
        extensions: Extensions::new(),
    };
    Ok(report)
}

fn operation_report_envelope(report: &OperationReported) -> Result<EventEnvelope, ReceiptError> {
    let event_id = EventId::new_v7();
    let aggregate_id = EntityRef::from(report.operation_id);
    let mut envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
        "event_id": event_id,
        "event_type": OperationReported::EVENT_TYPE,
        "occurred_at": WireTimestamp::now(),
        "producer": "ratatoskr-claude",
        "aggregate_id": aggregate_id,
        "correlation_id": event_id.as_entity_ref(),
        "schema_version": 1,
        "payload": {}
    }))
    .map_err(ReceiptError::ReportEncoding)?;
    envelope
        .set_payload(report)
        .map_err(ReceiptError::ReportEnvelope)?;
    Ok(envelope)
}

/// Verifies a claim against the tenants this archive knows.
///
/// Exactly one scope variant may be claimed, and it must resolve to exactly
/// one existing row; anything else is refused without touching storage.
///
/// # Errors
///
/// Returns [`ReceiptError::AmbiguousScope`] when the claim names both scopes
/// or neither, [`ReceiptError::UnknownTenant`] when the claimed identifier
/// resolves to no row, and [`ReceiptError::Query`] on database failure.
pub async fn authenticate_claim(
    database: &Database,
    claim: &TenantClaim,
) -> Result<TenantScope, ReceiptError> {
    match (claim.account, claim.organization) {
        (Some(account), None) => {
            ensure_account(database, account).await?;
            Ok(TenantScope::Account(account))
        }
        (None, Some(organization)) => {
            ensure_organization(database, organization).await?;
            Ok(TenantScope::Organization(organization))
        }
        _ => Err(ReceiptError::AmbiguousScope),
    }
}

/// Answers whether one account identifier belongs to this archive.
async fn ensure_account(database: &Database, account: Uuid) -> Result<(), ReceiptError> {
    let present: Option<Uuid> =
        sqlx::query_scalar("select account_id from claude_archive.accounts where account_id = $1")
            .bind(account)
            .fetch_optional(database.pool())
            .await
            .map_err(ReceiptError::Query)?;
    if present.is_some() {
        Ok(())
    } else {
        Err(ReceiptError::UnknownTenant)
    }
}

/// Answers whether one organization identifier belongs to this archive.
async fn ensure_organization(database: &Database, organization: Uuid) -> Result<(), ReceiptError> {
    let present: Option<Uuid> = sqlx::query_scalar(
        "select organization_id from claude_archive.organizations where organization_id = $1",
    )
    .bind(organization)
    .fetch_optional(database.pool())
    .await
    .map_err(ReceiptError::Query)?;
    if present.is_some() {
        Ok(())
    } else {
        Err(ReceiptError::UnknownTenant)
    }
}

/// Whether the failure was the exports table refusing a second row for one
/// archive digest.
fn is_archive_hash_conflict(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database_error| {
        database_error.code().is_some_and(|code| code == "23505")
            && database_error
                .constraint()
                .is_some_and(|name| name == "exports_archive_hash_key")
    })
}

/// Loads the export that already holds `digest_bytes` and reports it as the
/// duplicate outcome; absence is a corruption the caller must see loudly.
async fn duplicate_of(
    database: &Database,
    digest_bytes: &[u8],
) -> Result<ReceiptOutcome, ReceiptError> {
    let existing: Option<Uuid> =
        sqlx::query_scalar("select export_id from claude_archive.exports where archive_hash = $1")
            .bind(digest_bytes)
            .fetch_optional(database.pool())
            .await
            .map_err(ReceiptError::Query)?;
    match existing {
        Some(existing_export_id) => Ok(ReceiptOutcome::Duplicate { existing_export_id }),
        None => Err(ReceiptError::Query(sqlx::Error::ColumnNotFound(
            "the duplicate digest vanished between conflict and lookup".to_owned(),
        ))),
    }
}

/// The durable storage key recorded alongside the snapshot: algorithm and
/// digest, resolvable by this service's own blob store.
fn blob_key(reference: &BlobRef) -> String {
    format!(
        "{}/{}",
        reference.algorithm.identifier(),
        reference.digest_hex
    )
}

/// Decodes lowercase hexadecimal into bytes; `None` for anything else.
fn decode_digest_hex(digest_hex: &str) -> Option<Vec<u8>> {
    if digest_hex.len() != 64 {
        return None;
    }
    let bytes = digest_hex.as_bytes();
    let mut decoded = Vec::with_capacity(32);
    for pair in bytes.chunks(2) {
        let high = hex_value(*pair.first()?)?;
        let low = hex_value(*pair.get(1)?)?;
        decoded.push(u8::try_from(high * 16 + low).ok()?);
    }
    Some(decoded)
}

/// The numeric value of one lowercase hexadecimal digit.
fn hex_value(byte: u8) -> Option<u32> {
    match byte {
        b'0'..=b'9' => Some(u32::from(byte - b'0')),
        b'a'..=b'f' => Some(u32::from(byte - b'a') + 10),
        _ => None,
    }
}
