//! Artifact-version reconciliation over immutable parser observations.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::{
    Artifact, ArtifactVersion, BlobRef, BlobStore, MediaType, ParsedExport, ParserStamp,
    StoreError, UnknownField,
};

/// First-class Artifact reconciliation boundary.
#[derive(Debug, Clone)]
pub struct ArtifactReconciler {
    store: BlobStore,
}

/// A reconciled Artifact identity with its observed versions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReconciledArtifact {
    /// Provider Artifact identifier.
    pub external_id: String,
    /// Provider-declared Artifact type.
    pub artifact_type: String,
    /// Optional provider display title.
    pub title: Option<String>,
    /// Optional provider language identifier.
    pub language: Option<String>,
    /// Optional provider project relationship.
    pub project_external_id: Option<String>,
    /// Optional provider conversation relationship.
    pub conversation_external_id: Option<String>,
    /// Optional provider message relationship.
    pub message_external_id: Option<String>,
    /// JSON Pointer for the first source record.
    pub location: String,
    /// Inert original provider Artifact record.
    pub raw: serde_json::Value,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
    /// Ordered immutable version observations.
    pub versions: Vec<ReconciledArtifactVersion>,
}

/// One reconciled Artifact version.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReconciledArtifactVersion {
    /// Provider version identifier.
    pub external_id: String,
    /// Optional provider identifier for the predecessor version.
    pub previous_external_id: Option<String>,
    /// Provider-declared media type of the available payload.
    pub media_type: String,
    /// Provider-declared SHA-256 when observed.
    pub declared_sha256: Option<String>,
    /// JSON Pointer for the source record.
    pub location: String,
    /// Inert original provider Artifact-version record.
    pub raw: serde_json::Value,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
    /// Local availability and integrity classification.
    pub availability: ArtifactVersionAvailability,
}

/// The locally observable availability of an Artifact version payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactVersionAvailability {
    /// A version whose source did not supply bytes.
    ReferencedOnly,
    /// A locally verified content-addressed payload.
    Verified {
        /// Reference to the verified local bytes.
        blob_ref: BlobRef,
    },
    /// Retained but inert bytes that failed a verification control.
    Quarantined {
        /// Stored evidence retained for controlled investigation.
        evidence_blob_ref: BlobRef,
        /// Content-free anomaly classification.
        reason: ArtifactVersionAnomaly,
    },
}

/// A stable, content-free classification for an Artifact-version anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactVersionAnomaly {
    /// The provider omitted a digest for supplied bytes.
    MissingDeclaredDigest,
    /// The provider digest did not match supplied bytes.
    DigestMismatch,
    /// The provider media type was malformed.
    InvalidMediaType,
}

/// Artifact reconciliation failure without Artifact titles or content.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactReconciliationError {
    /// An immutable Artifact identity changed within the same provider identifier.
    #[error("artifact identity evidence conflicts for {external_id}")]
    IdentityConflict {
        /// Provider Artifact identifier that conflicts.
        external_id: String,
    },
    /// The same provider version identifier carried divergent evidence.
    #[error("artifact version evidence conflicts for {artifact_external_id}/{version_external_id}")]
    VersionConflict {
        /// Provider Artifact identifier that owns the version.
        artifact_external_id: String,
        /// Provider version identifier that conflicts.
        version_external_id: String,
    },
    /// A version named a predecessor absent from the observed chain.
    #[error(
        "artifact version predecessor is missing for {artifact_external_id}/{version_external_id}"
    )]
    MissingPredecessor {
        /// Provider Artifact identifier that owns the version.
        artifact_external_id: String,
        /// Provider version identifier with the missing predecessor.
        version_external_id: String,
    },
    /// Provider predecessor links formed a cycle.
    #[error(
        "artifact version lineage contains a cycle for {artifact_external_id}/{version_external_id}"
    )]
    VersionCycle {
        /// Provider Artifact identifier that owns the cycle.
        artifact_external_id: String,
        /// Provider version identifier encountered while visiting the cycle.
        version_external_id: String,
    },
    /// The `BlobStore` refused Artifact payload evidence.
    #[error("artifact payload storage failed")]
    Store(#[from] StoreError),
}

impl ArtifactReconciler {
    /// Creates a reconciliation boundary over the service-owned `BlobStore`.
    #[must_use]
    pub fn new(store: &BlobStore) -> Self {
        Self {
            store: store.clone(),
        }
    }

    /// Reconciles immutable parsed observations into Artifact chains.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactReconciliationError`] when identity or lineage evidence
    /// conflicts, a predecessor is missing, a cycle is observed, or the
    /// `BlobStore` cannot retain a supplied payload.
    pub fn reconcile(
        &self,
        exports: &[ParsedExport],
    ) -> Result<Vec<ReconciledArtifact>, ArtifactReconciliationError> {
        let mut artifacts = BTreeMap::new();
        for export in exports {
            for artifact in &export.artifacts {
                self.merge_artifact(&mut artifacts, artifact)?;
            }
        }

        artifacts
            .into_iter()
            .map(|(external_id, artifact)| Self::order_versions(&external_id, artifact))
            .collect()
    }

    fn merge_artifact(
        &self,
        artifacts: &mut BTreeMap<String, ReconciledArtifact>,
        artifact: &Artifact,
    ) -> Result<(), ArtifactReconciliationError> {
        if let Some(existing) = artifacts.get_mut(&artifact.external_id) {
            self.merge_versions(existing, artifact)
        } else {
            let mut reconciled = ReconciledArtifact {
                external_id: artifact.external_id.clone(),
                artifact_type: artifact.artifact_type.clone(),
                title: artifact.title.clone(),
                language: artifact.language.clone(),
                project_external_id: artifact.project_external_id.clone(),
                conversation_external_id: artifact.conversation_external_id.clone(),
                message_external_id: artifact.message_external_id.clone(),
                location: artifact.location.clone(),
                raw: artifact.raw.clone(),
                parser: artifact.parser.clone(),
                unknown_fields: artifact.unknown_fields.clone(),
                versions: Vec::new(),
            };
            self.merge_versions(&mut reconciled, artifact)?;
            artifacts.insert(artifact.external_id.clone(), reconciled);
            Ok(())
        }
    }

    fn merge_versions(
        &self,
        reconciled: &mut ReconciledArtifact,
        artifact: &Artifact,
    ) -> Result<(), ArtifactReconciliationError> {
        if !same_identity(reconciled, artifact) {
            return Err(ArtifactReconciliationError::IdentityConflict {
                external_id: artifact.external_id.clone(),
            });
        }

        for version in &artifact.versions {
            let candidate = self.reconcile_version(version)?;
            if let Some(existing) = reconciled
                .versions
                .iter()
                .find(|existing| existing.external_id == candidate.external_id)
            {
                if existing != &candidate {
                    return Err(ArtifactReconciliationError::VersionConflict {
                        artifact_external_id: artifact.external_id.clone(),
                        version_external_id: candidate.external_id,
                    });
                }
            } else {
                reconciled.versions.push(candidate);
            }
        }
        Ok(())
    }

    fn reconcile_version(
        &self,
        version: &ArtifactVersion,
    ) -> Result<ReconciledArtifactVersion, ArtifactReconciliationError> {
        Ok(ReconciledArtifactVersion {
            external_id: version.external_id.clone(),
            previous_external_id: version.previous_external_id.clone(),
            media_type: version.media_type.clone(),
            declared_sha256: version.declared_sha256.clone(),
            location: version.location.clone(),
            raw: version.raw.clone(),
            parser: version.parser.clone(),
            unknown_fields: version.unknown_fields.clone(),
            availability: self.ingest_payload(version)?,
        })
    }

    fn ingest_payload(
        &self,
        version: &ArtifactVersion,
    ) -> Result<ArtifactVersionAvailability, StoreError> {
        let Some(bytes) = &version.bytes else {
            return Ok(ArtifactVersionAvailability::ReferencedOnly);
        };

        let (media_type, invalid_media_type) = match MediaType::parse(&version.media_type) {
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
                ArtifactVersionAnomaly::InvalidMediaType,
            ));
        }
        let Some(declared_sha256) = &version.declared_sha256 else {
            return Ok(quarantined(
                evidence_blob_ref,
                ArtifactVersionAnomaly::MissingDeclaredDigest,
            ));
        };
        if declared_sha256 != &sha256_hex(bytes)
            || evidence_blob_ref.digest_hex != declared_sha256.as_str()
        {
            return Ok(quarantined(
                evidence_blob_ref,
                ArtifactVersionAnomaly::DigestMismatch,
            ));
        }

        Ok(ArtifactVersionAvailability::Verified {
            blob_ref: evidence_blob_ref,
        })
    }

    fn order_versions(
        external_id: &str,
        mut artifact: ReconciledArtifact,
    ) -> Result<ReconciledArtifact, ArtifactReconciliationError> {
        let versions: BTreeMap<String, ReconciledArtifactVersion> = artifact
            .versions
            .drain(..)
            .map(|version| (version.external_id.clone(), version))
            .collect();
        let mut ordered = Vec::with_capacity(versions.len());
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();

        for version_external_id in versions.keys() {
            visit_version(
                version_external_id,
                external_id,
                &versions,
                &mut visiting,
                &mut visited,
                &mut ordered,
            )?;
        }
        artifact.versions = ordered;
        Ok(artifact)
    }
}

impl ReconciledArtifactVersion {
    /// Answers whether the version has verified payload evidence.
    #[must_use]
    pub const fn is_verified(&self) -> bool {
        matches!(
            self.availability,
            ArtifactVersionAvailability::Verified { .. }
        )
    }

    /// Returns the verified local payload reference when it exists.
    #[must_use]
    pub const fn verified_blob_ref(&self) -> Option<&BlobRef> {
        match &self.availability {
            ArtifactVersionAvailability::Verified { blob_ref } => Some(blob_ref),
            ArtifactVersionAvailability::ReferencedOnly
            | ArtifactVersionAvailability::Quarantined { .. } => None,
        }
    }
}

fn same_identity(existing: &ReconciledArtifact, incoming: &Artifact) -> bool {
    existing.artifact_type == incoming.artifact_type
        && existing.project_external_id == incoming.project_external_id
        && existing.conversation_external_id == incoming.conversation_external_id
        && existing.message_external_id == incoming.message_external_id
}

fn visit_version(
    version_external_id: &str,
    artifact_external_id: &str,
    versions: &BTreeMap<String, ReconciledArtifactVersion>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
    ordered: &mut Vec<ReconciledArtifactVersion>,
) -> Result<(), ArtifactReconciliationError> {
    if visited.contains(version_external_id) {
        return Ok(());
    }
    if !visiting.insert(version_external_id.to_owned()) {
        return Err(ArtifactReconciliationError::VersionCycle {
            artifact_external_id: artifact_external_id.to_owned(),
            version_external_id: version_external_id.to_owned(),
        });
    }

    let version = versions.get(version_external_id).ok_or_else(|| {
        ArtifactReconciliationError::MissingPredecessor {
            artifact_external_id: artifact_external_id.to_owned(),
            version_external_id: version_external_id.to_owned(),
        }
    })?;
    if let Some(previous_external_id) = &version.previous_external_id {
        if !versions.contains_key(previous_external_id) {
            return Err(ArtifactReconciliationError::MissingPredecessor {
                artifact_external_id: artifact_external_id.to_owned(),
                version_external_id: version_external_id.to_owned(),
            });
        }
        visit_version(
            previous_external_id,
            artifact_external_id,
            versions,
            visiting,
            visited,
            ordered,
        )?;
    }
    visiting.remove(version_external_id);
    if visited.insert(version_external_id.to_owned()) {
        ordered.push(version.clone());
    }
    Ok(())
}

fn quarantined(
    evidence_blob_ref: BlobRef,
    reason: ArtifactVersionAnomaly,
) -> ArtifactVersionAvailability {
    ArtifactVersionAvailability::Quarantined {
        evidence_blob_ref,
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
