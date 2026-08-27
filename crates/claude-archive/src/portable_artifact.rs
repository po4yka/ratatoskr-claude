//! Safe in-memory portable representation of reconciled Artifact evidence.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::{BlobStore, ReconciledArtifact, ReconciledArtifactVersion, StoreError};

/// Deterministic portable Artifact representation keyed by relative path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortableArtifactRepresentation {
    files: BTreeMap<String, Vec<u8>>,
}

/// Builds a safe portable representation from verified Artifact evidence.
#[derive(Debug, Clone)]
pub struct ArtifactPortableExporter {
    store: BlobStore,
}

/// Failure while creating portable Artifact output.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactPortableError {
    /// The `BlobStore` could not read a verified Artifact payload.
    #[error("portable Artifact payload access failed")]
    Store(#[from] StoreError),
    /// A normalized record could not be serialized into portable JSON.
    #[error("portable Artifact JSON serialization failed")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Serialize)]
struct PortableArtifactVersion<'a> {
    artifact_external_id: &'a str,
    version: &'a ReconciledArtifactVersion,
    rendering: ArtifactRendering,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ArtifactRendering {
    Rendered { path: String },
    Unrenderable { reason: &'static str },
}

impl PortableArtifactRepresentation {
    /// Returns all relative paths and their exact portable bytes.
    #[must_use]
    pub const fn files(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.files
    }
}

impl ArtifactPortableExporter {
    /// Creates a portable-representation boundary over the service-owned `BlobStore`.
    #[must_use]
    pub fn new(store: &BlobStore) -> Self {
        Self {
            store: store.clone(),
        }
    }

    /// Creates deterministic safe output for reconciled Artifacts.
    ///
    /// Text and Markdown derivatives are created only when the Artifact type
    /// and verified payload media type are both explicitly allowlisted. All
    /// other Artifact types remain normalized JSON evidence with an
    /// `unrenderable` status.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPortableError`] when verified payload evidence cannot
    /// be read from the `BlobStore` or normalized output cannot be serialized.
    pub fn represent(
        &self,
        artifacts: &[ReconciledArtifact],
    ) -> Result<PortableArtifactRepresentation, ArtifactPortableError> {
        let mut ordered: Vec<&ReconciledArtifact> = artifacts.iter().collect();
        ordered.sort_by(|left, right| left.external_id.cmp(&right.external_id));

        let mut representation = PortableArtifactRepresentation::default();
        for artifact in ordered {
            self.add_artifact(&mut representation.files, artifact)?;
        }
        Ok(representation)
    }

    fn add_artifact(
        &self,
        files: &mut BTreeMap<String, Vec<u8>>,
        artifact: &ReconciledArtifact,
    ) -> Result<(), ArtifactPortableError> {
        let root = format!(
            "artifacts/{}",
            portable_component("artifact", &artifact.external_id)
        );
        files.insert(
            format!("{root}/artifact.json"),
            serde_json::to_vec_pretty(artifact)?,
        );

        for version in &artifact.versions {
            self.add_version(files, artifact, version, &root)?;
        }
        Ok(())
    }

    fn add_version(
        &self,
        files: &mut BTreeMap<String, Vec<u8>>,
        artifact: &ReconciledArtifact,
        version: &ReconciledArtifactVersion,
        root: &str,
    ) -> Result<(), ArtifactPortableError> {
        let version_root = format!(
            "{root}/versions/{}",
            portable_component("version", &version.external_id)
        );
        let rendering = self.render_version(files, artifact, version, &version_root)?;
        let record = PortableArtifactVersion {
            artifact_external_id: &artifact.external_id,
            version,
            rendering,
        };
        files.insert(
            format!("{version_root}.json"),
            serde_json::to_vec_pretty(&record)?,
        );
        Ok(())
    }

    fn render_version(
        &self,
        files: &mut BTreeMap<String, Vec<u8>>,
        artifact: &ReconciledArtifact,
        version: &ReconciledArtifactVersion,
        version_root: &str,
    ) -> Result<ArtifactRendering, ArtifactPortableError> {
        let Some(extension) = text_extension(artifact, version) else {
            return Ok(ArtifactRendering::Unrenderable {
                reason: rendering_reason(artifact, version),
            });
        };
        let Some(blob_ref) = version.verified_blob_ref() else {
            return Ok(ArtifactRendering::Unrenderable {
                reason: "payload_not_verified",
            });
        };
        let bytes = self.store.read(blob_ref)?;
        if std::str::from_utf8(&bytes).is_err() {
            return Ok(ArtifactRendering::Unrenderable {
                reason: "payload_not_utf8",
            });
        }

        let path = format!("{version_root}.{extension}");
        files.insert(path.clone(), bytes);
        Ok(ArtifactRendering::Rendered { path })
    }
}

fn text_extension(
    artifact: &ReconciledArtifact,
    version: &ReconciledArtifactVersion,
) -> Option<&'static str> {
    match (artifact.artifact_type.as_str(), version.media_type.as_str()) {
        ("text/plain", "text/plain") => Some("txt"),
        ("text/markdown", "text/markdown") => Some("md"),
        _ => None,
    }
}

fn rendering_reason(
    artifact: &ReconciledArtifact,
    version: &ReconciledArtifactVersion,
) -> &'static str {
    if matches!(
        artifact.artifact_type.as_str(),
        "text/plain" | "text/markdown"
    ) && !matches!(version.media_type.as_str(), "text/plain" | "text/markdown")
    {
        "unsupported_artifact_media_type"
    } else {
        "unsupported_artifact_type"
    }
}

fn portable_component(prefix: &str, provider_identifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(provider_identifier.as_bytes());
    let mut digest = String::new();
    for byte in hasher.finalize() {
        let _ignored = write!(digest, "{byte:02x}");
    }
    format!("{prefix}-{digest}")
}
