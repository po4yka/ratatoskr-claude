#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Domain library for the Ratatoskr Claude bounded context.
//!
//! The foundation owns process configuration, structured telemetry, the
//! content-addressed blob store, and application of the first-version
//! `claude_archive` schema. Export receipt, parsing, completeness, and event
//! publication arrive with later implementation plan items.

pub mod archive_inspection;
pub mod blob_store;
pub mod config;
pub mod database;
pub mod import_state;
pub mod parser_registry;
pub mod receipt;
pub mod telemetry;

pub use archive_inspection::{
    ArchiveEntry, ArchiveError, ArchiveExtraction, ArchiveExtractor, ArchiveInspector,
    ArchiveInventory, ArchiveLimit, ExtractedArtifact, MediaDisposition, RawArchiveProvenance,
    UnsafeEntryKind,
};
pub use blob_store::{BlobRef, BlobStore, DigestAlgorithm, MediaType, StoreError};
pub use config::{AdminConfig, Config, ConfigError, Limits, StorageConfig, TelemetryConfig};
pub use database::{Database, PersistenceError};
pub use import_state::{ImportError, ImportRunStore, ImportState, TransitionOutcome};
pub use parser_registry::{
    DetectedSchema, ParserCapability, ParserDescriptor, ParserRegistry, ParserRegistryError,
    ParserSelectionError,
};
pub use receipt::{AcquisitionMode, ReceiptError, ReceiptOutcome, TenantClaim, TenantScope};
pub use telemetry::{TelemetryError, TelemetryGuard, init_telemetry};

#[cfg(feature = "test-support")]
pub mod test_support;
