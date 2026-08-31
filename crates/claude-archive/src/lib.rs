#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Domain library for the Ratatoskr Claude bounded context.
//!
//! The foundation owns process configuration, structured telemetry, the
//! content-addressed blob store, and application of the first-version
//! `claude_archive` schema. Export receipt, conservative parsing, Project
//! Knowledge evidence, completeness reporting, contract-native outbox publication,
//! and revision-specific Knowledge analysis linkage are implemented.

pub mod archive_inspection;
pub mod artifact;
pub mod blob_store;
pub mod completeness;
pub mod config;
pub mod database;
pub mod events;
pub mod export_projection;
pub mod external_reference;
pub mod fixture_admission;
pub mod import_state;
mod initial_import;
pub mod knowledge_link;
pub mod operation_outbox;
pub mod parser_migration;
pub mod parser_registry;
pub mod portable_artifact;
pub mod portable_export;
pub mod privacy_deletion;
pub mod project_knowledge;
pub mod receipt;
pub mod reparse;
pub mod telemetry;

pub use archive_inspection::{
    ArchiveEntry, ArchiveError, ArchiveExtraction, ArchiveExtractor, ArchiveInspector,
    ArchiveInventory, ArchiveLimit, ExtractedArtifact, MediaDisposition, RawArchiveProvenance,
    UnsafeEntryKind,
};
pub use artifact::{
    ArtifactReconciler, ArtifactReconciliationError, ArtifactVersionAnomaly,
    ArtifactVersionAvailability, ReconciledArtifact, ReconciledArtifactVersion,
};
pub use blob_store::{BlobRef, BlobStore, DigestAlgorithm, MediaType, StoreError};
pub use completeness::{
    ArchiveCompletenessReport, CompletenessCounts, CompletenessStatus, CompletenessWarning,
    CumulativeCompletenessReport,
};
pub use config::{
    AdminConfig, Config, ConfigError, Limits, ReceiptConfig, StorageConfig, TelemetryConfig,
};
pub use database::{Database, PersistenceError};
pub use events::{ArchiveEventError, ArchiveEventFact, ArchiveOutbox, OutboxEvent};
pub use export_projection::{
    Artifact, ArtifactVersion, ConsumerExportParser, ContentPart, Conversation, ExportParseError,
    Message, ParsedExport, ParserStamp, Project, ProjectInstruction, ProjectKnowledgeFile,
    UnknownField,
};
pub use external_reference::{
    AuthorizationStatus, BackupStatusAudit, BackupStatusLedger, ExternalReference,
    ExternalReferenceKind, LocalBackupStatus, LocalEvidence, derive_local_backup_status,
};
pub use import_state::{ImportError, ImportRunStore, ImportState, TransitionOutcome};
pub use initial_import::InitialImportWorker;
pub use knowledge_link::{KnowledgeLinkError, KnowledgeLinkOutcome, KnowledgeLinkStore};
pub use operation_outbox::OperationReportOutbox;
pub use parser_migration::{
    ParserMigrationEntry, ParserMigrationEntryStatus, ParserMigrationPlan, ParserMigrationReport,
    ParserMigrationStatus,
};
pub use parser_registry::{
    CompiledParser, DetectedSchema, ParserCapability, ParserDescriptor, ParserExecutionError,
    ParserExecutionInput, ParserExecutor, ParserIdentity, ParserRegistry, ParserRegistryError,
    ParserSelectionError,
};
pub use portable_artifact::{
    ArtifactPortableError, ArtifactPortableExporter, PortableArtifactRepresentation,
};
pub use project_knowledge::{
    KnowledgeFileAnomaly, KnowledgeFileAvailability, ProjectKnowledgeIngestResult,
    ProjectKnowledgeIngestor,
};
pub use receipt::{
    AcquisitionMode, ArchiveIdentity, PlatformOperation, ReceiptError, ReceiptOutcome, TenantClaim,
    TenantScope,
};
pub use reparse::{
    ReparseChange, ReparseChangeKind, ReparseEngine, ReparseError, ReparsePlan, ReparseReport,
    ReparseWarning,
};
pub use telemetry::{TelemetryError, TelemetryGuard, init_telemetry};

#[cfg(feature = "test-support")]
pub mod test_support;
