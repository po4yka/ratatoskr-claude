#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Domain library for the Ratatoskr Claude bounded context.
//!
//! The foundation owns process configuration, structured telemetry, the
//! content-addressed blob store, and application of the first-version
//! `claude_archive` schema. Export receipt, parsing, completeness, and event
//! publication arrive with later implementation plan items.

pub mod blob_store;
pub mod config;
pub mod database;
pub mod telemetry;

pub use blob_store::{BlobRef, BlobStore, DigestAlgorithm, MediaType, StoreError};
pub use config::{AdminConfig, Config, ConfigError, Limits, StorageConfig, TelemetryConfig};
pub use database::{Database, PersistenceError};
pub use telemetry::{TelemetryError, TelemetryGuard, init_telemetry};

#[cfg(feature = "test-support")]
pub mod test_support;
