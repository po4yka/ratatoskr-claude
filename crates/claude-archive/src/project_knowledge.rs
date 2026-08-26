//! Safe Project Knowledge file ingest classification.

use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

use crate::{BlobRef, BlobStore, MediaType, ProjectKnowledgeFile, StoreError};

/// The locally observable availability of one Project Knowledge file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeFileAvailability {
    /// A reference whose source did not supply bytes.
    ReferencedOnly,
    /// A locally verified content-addressed backup.
    Verified {
        /// Reference to the verified local bytes.
        blob_ref: BlobRef,
    },
    /// Retained but inert bytes that failed a verification control.
    Quarantined {
        /// Stored evidence retained for controlled investigation.
        evidence_blob_ref: BlobRef,
        /// Content-free anomaly classification.
        reason: KnowledgeFileAnomaly,
    },
}

/// A stable, content-free classification for a knowledge-file anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeFileAnomaly {
    /// The provider omitted a digest for supplied bytes.
    MissingDeclaredDigest,
    /// The provider digest did not match supplied bytes.
    DigestMismatch,
    /// The provider media type was malformed.
    InvalidMediaType,
}

/// One knowledge-file ingestion outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectKnowledgeIngestResult {
    /// Provider identifier of the knowledge file.
    pub external_id: String,
    /// Provider identifier of the owning project.
    pub project_external_id: String,
    /// Local availability and integrity classification.
    pub availability: KnowledgeFileAvailability,
}

/// BlobStore-backed Project Knowledge ingest boundary.
#[derive(Debug, Clone)]
pub struct ProjectKnowledgeIngestor {
    store: BlobStore,
}

impl ProjectKnowledgeIngestor {
    /// Creates an ingest boundary over the service-owned `BlobStore`.
    #[must_use]
    pub fn new(store: &BlobStore) -> Self {
        Self {
            store: store.clone(),
        }
    }

    /// Classifies a Project Knowledge file for local retention.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the configured `BlobStore` cannot accept an
    /// eventual verified or quarantined evidence record.
    pub fn ingest(
        &self,
        file: &ProjectKnowledgeFile,
    ) -> Result<ProjectKnowledgeIngestResult, StoreError> {
        let availability = match &file.bytes {
            None => KnowledgeFileAvailability::ReferencedOnly,
            Some(bytes) => self.ingest_bytes(file, bytes)?,
        };
        Ok(ProjectKnowledgeIngestResult {
            external_id: file.external_id.clone(),
            project_external_id: file.project_external_id.clone(),
            availability,
        })
    }

    fn ingest_bytes(
        &self,
        file: &ProjectKnowledgeFile,
        bytes: &[u8],
    ) -> Result<KnowledgeFileAvailability, StoreError> {
        let source_digest = sha256_hex(bytes);
        let (media_type, invalid_media_type) = match MediaType::parse(&file.media_type) {
            Ok(media_type) => (media_type, false),
            Err(StoreError::InvalidMediaType) => {
                (MediaType::parse("application/octet-stream")?, true)
            }
            Err(error) => return Err(error),
        };
        let evidence_blob_ref = self.store.store(media_type, bytes)?;
        self.store.verify(&evidence_blob_ref)?;

        if invalid_media_type {
            return Ok(quarantined(
                evidence_blob_ref,
                KnowledgeFileAnomaly::InvalidMediaType,
            ));
        }
        let Some(declared_sha256) = &file.declared_sha256 else {
            return Ok(quarantined(
                evidence_blob_ref,
                KnowledgeFileAnomaly::MissingDeclaredDigest,
            ));
        };
        if declared_sha256 != &source_digest || evidence_blob_ref.digest_hex != source_digest {
            return Ok(quarantined(
                evidence_blob_ref,
                KnowledgeFileAnomaly::DigestMismatch,
            ));
        }

        Ok(KnowledgeFileAvailability::Verified {
            blob_ref: evidence_blob_ref,
        })
    }
}

fn quarantined(blob_ref: BlobRef, reason: KnowledgeFileAnomaly) -> KnowledgeFileAvailability {
    KnowledgeFileAvailability::Quarantined {
        evidence_blob_ref: blob_ref,
        reason,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut encoded = String::new();
    for byte in hasher.finalize() {
        let _ignored = write!(encoded, "{byte:02x}");
    }
    encoded
}
