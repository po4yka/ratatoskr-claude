//! Structural ZIP inspection for received Claude archive evidence.

use std::io::{BufRead as _, Read as _};

use crate::blob_store::{BlobRef, BlobStore, StoreError};
use crate::config::Limits;

/// Structural archive inspection with a finite resource budget.
#[derive(Debug, Clone)]
pub struct ArchiveInspector {
    limits: Limits,
}

impl ArchiveInspector {
    /// Creates an inspector using a copy of the validated process limits.
    #[must_use]
    pub fn new(limits: &Limits) -> Self {
        Self {
            limits: limits.clone(),
        }
    }

    /// Inspects raw ZIP evidence without extracting entry bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] when the raw archive cannot be safely inspected.
    pub fn inspect(
        &self,
        store: &BlobStore,
        raw_archive: &BlobRef,
    ) -> Result<ArchiveInventory, ArchiveError> {
        let file = store.open_verified(raw_archive)?;
        let mut archive = zip::ZipArchive::new(file).map_err(|_| ArchiveError::InvalidZip)?;
        let mut entries = Vec::new();
        let mut seen_paths = std::collections::HashSet::new();
        let mut total_size = 0_u64;
        let mut entry_count = 0_u32;

        for index in 0..archive.len() {
            let entry = archive
                .by_index_raw(index)
                .map_err(|_| ArchiveError::InvalidZip)?;
            let path = validate_path(entry.name_raw())?;
            if !seen_paths.insert(path.clone()) {
                return Err(ArchiveError::UnsafePath);
            }
            validate_entry_kind(&entry)?;
            if entry.is_dir() {
                continue;
            }

            entry_count = entry_count
                .checked_add(1)
                .ok_or(ArchiveError::LimitExceeded {
                    limit: ArchiveLimit::EntryCount,
                })?;
            if entry_count > self.limits.max_archive_entries {
                return Err(ArchiveError::LimitExceeded {
                    limit: ArchiveLimit::EntryCount,
                });
            }
            validate_entry_size(entry.size(), entry.compressed_size(), &self.limits)?;
            total_size =
                total_size
                    .checked_add(entry.size())
                    .ok_or(ArchiveError::LimitExceeded {
                        limit: ArchiveLimit::TotalBytes,
                    })?;
            if total_size > self.limits.max_total_extracted_bytes {
                return Err(ArchiveError::LimitExceeded {
                    limit: ArchiveLimit::TotalBytes,
                });
            }
            entries.push(ArchiveEntry {
                index,
                path,
                uncompressed_size: entry.size(),
                compressed_size: entry.compressed_size(),
            });
        }

        Ok(ArchiveInventory { entries })
    }
}

/// Bounded extraction of an inspected ZIP inventory.
#[derive(Debug, Clone)]
pub struct ArchiveExtractor {
    limits: Limits,
}

impl ArchiveExtractor {
    /// Creates an extractor using a copy of the validated process limits.
    #[must_use]
    pub fn new(limits: &Limits) -> Self {
        Self {
            limits: limits.clone(),
        }
    }

    /// Streams accepted archive entries to `BlobStore` without rendering them.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveError`] when an entry cannot be safely extracted.
    pub fn extract(
        &self,
        store: &BlobStore,
        raw_archive: &BlobRef,
        inventory: &ArchiveInventory,
    ) -> Result<ArchiveExtraction, ArchiveError> {
        let file = store.open_verified(raw_archive)?;
        let mut archive = zip::ZipArchive::new(file).map_err(|_| ArchiveError::InvalidZip)?;
        let mut artifacts = Vec::new();
        let mut total_bytes = 0_u64;

        for planned in inventory.entries() {
            let entry = archive
                .by_index(planned.index)
                .map_err(|_| ArchiveError::InvalidZip)?;
            validate_extraction_entry(&entry, planned)?;

            let remaining = self
                .limits
                .max_total_extracted_bytes
                .checked_sub(total_bytes)
                .ok_or(ArchiveError::LimitExceeded {
                    limit: ArchiveLimit::TotalBytes,
                })?;
            let (entry_limit, exceeded_limit) = extraction_limit(&self.limits, remaining)?;
            let mut reader = std::io::BufReader::new(entry.take(entry_limit.saturating_add(1)));
            let (disposition, media_type) = classify_media(reader.fill_buf()?);
            let media_type = crate::MediaType::parse(media_type)?;
            let blob_ref = store
                .store_stream(media_type, &mut reader, entry_limit)
                .map_err(|error| map_extraction_error(error, exceeded_limit))?;
            total_bytes = total_bytes.checked_add(blob_ref.length_bytes).ok_or(
                ArchiveError::LimitExceeded {
                    limit: ArchiveLimit::TotalBytes,
                },
            )?;
            artifacts.push(ExtractedArtifact {
                blob_ref,
                provenance: RawArchiveProvenance {
                    raw_digest_hex: raw_archive.digest_hex.clone(),
                    entry_path: planned.path.clone(),
                },
                disposition,
            });
        }

        Ok(ArchiveExtraction { artifacts })
    }
}

/// A validated archive inventory with no entry body bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveInventory {
    entries: Vec<ArchiveEntry>,
}

impl ArchiveInventory {
    /// Returns entry metadata in stable central-directory order.
    #[must_use]
    pub fn entries(&self) -> &[ArchiveEntry] {
        &self.entries
    }
}

/// Metadata for one accepted non-directory ZIP entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEntry {
    index: usize,
    path: String,
    uncompressed_size: u64,
    compressed_size: u64,
}

impl ArchiveEntry {
    /// Returns the validated portable archive path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the declared uncompressed byte count.
    #[must_use]
    pub fn uncompressed_size(&self) -> u64 {
        self.uncompressed_size
    }

    /// Returns the declared compressed byte count.
    #[must_use]
    pub fn compressed_size(&self) -> u64 {
        self.compressed_size
    }
}

/// The class of unsafe entry refused by structural inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsafeEntryKind {
    /// A symbolic or hard link entry.
    Link,
    /// A device, FIFO, socket, or another special filesystem entry.
    Special,
    /// An entry that requests decryption.
    Encrypted,
}

/// The configured resource limit exceeded by a hostile archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveLimit {
    /// Too many non-directory entries.
    EntryCount,
    /// An entry exceeds its decompressed byte limit.
    EntryBytes,
    /// Declared decompressed bytes exceed the archive aggregate limit.
    TotalBytes,
    /// Declared compression ratio exceeds the configured maximum.
    CompressionRatio,
}

/// The handling class assigned after conservative byte sniffing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDisposition {
    /// JSON-shaped bytes may be handed to a future structured parser.
    StructuredCandidate,
    /// Active, nested-archive, or unknown bytes stay retained but inert.
    Quarantined,
}

/// Raw evidence that one derived `BlobRef` came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawArchiveProvenance {
    raw_digest_hex: String,
    entry_path: String,
}

impl RawArchiveProvenance {
    /// Returns the raw ZIP SHA-256 digest.
    #[must_use]
    pub fn raw_digest_hex(&self) -> &str {
        &self.raw_digest_hex
    }

    /// Returns the validated path of the source ZIP entry.
    #[must_use]
    pub fn entry_path(&self) -> &str {
        &self.entry_path
    }
}

/// One retained archive entry with its raw provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedArtifact {
    blob_ref: BlobRef,
    provenance: RawArchiveProvenance,
    disposition: MediaDisposition,
}

impl ExtractedArtifact {
    /// Returns the stored derived bytes reference.
    #[must_use]
    pub fn blob_ref(&self) -> &BlobRef {
        &self.blob_ref
    }

    /// Returns the immutable raw archive provenance.
    #[must_use]
    pub fn provenance(&self) -> &RawArchiveProvenance {
        &self.provenance
    }

    /// Returns the inert handling disposition.
    #[must_use]
    pub fn disposition(&self) -> MediaDisposition {
        self.disposition
    }
}

/// All successfully retained entries from one extraction pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveExtraction {
    artifacts: Vec<ExtractedArtifact>,
}

impl ArchiveExtraction {
    /// Returns successful entries in validated inventory order.
    #[must_use]
    pub fn artifacts(&self) -> &[ExtractedArtifact] {
        &self.artifacts
    }
}

/// Archive inspection failure without provider content in its display form.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ArchiveError {
    /// Raw archive bytes cannot be resolved or verified.
    #[error("the raw archive evidence is unavailable")]
    Store(#[from] StoreError),
    /// An archive reader could not safely read an entry stream.
    #[error("the archive entry stream could not be read")]
    Io(#[from] std::io::Error),
    /// The bytes are not a structurally valid ZIP archive.
    #[error("the raw archive is not a valid ZIP container")]
    InvalidZip,
    /// An entry path is unsafe or ambiguous.
    #[error("the archive contains an unsafe entry path")]
    UnsafePath,
    /// An entry kind cannot be safely archived.
    #[error("the archive contains an unsafe entry kind")]
    UnsafeEntry {
        /// The refused category.
        kind: UnsafeEntryKind,
    },
    /// An archive crosses a configured inspection limit.
    #[error("the archive exceeds a configured inspection limit")]
    LimitExceeded {
        /// The exceeded limit class.
        limit: ArchiveLimit,
    },
    /// Inspection has not been implemented for this archive type yet.
    #[error("archive inspection is not implemented")]
    Unsupported,
}

fn validate_path(raw_path: &[u8]) -> Result<String, ArchiveError> {
    let path = std::str::from_utf8(raw_path).map_err(|_| ArchiveError::UnsafePath)?;
    let is_windows_drive = path.as_bytes().get(1).is_some_and(|byte| *byte == b':')
        && path.as_bytes().first().is_some_and(u8::is_ascii_alphabetic);
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || is_windows_drive
        || path.chars().any(char::is_control)
    {
        return Err(ArchiveError::UnsafePath);
    }

    let mut normalized = Vec::new();
    for component in path.split('/').filter(|component| !component.is_empty()) {
        match component {
            "." | ".." => return Err(ArchiveError::UnsafePath),
            safe => normalized.push(safe),
        }
    }
    if normalized.is_empty() {
        return Err(ArchiveError::UnsafePath);
    }
    Ok(normalized.join("/"))
}

fn validate_entry_kind(entry: &zip::read::ZipFile<'_, std::fs::File>) -> Result<(), ArchiveError> {
    if entry.encrypted() {
        return Err(ArchiveError::UnsafeEntry {
            kind: UnsafeEntryKind::Encrypted,
        });
    }
    if entry.is_symlink() {
        return Err(ArchiveError::UnsafeEntry {
            kind: UnsafeEntryKind::Link,
        });
    }
    let is_special = entry.unix_mode().is_some_and(|mode| {
        let file_type = mode & 0o170_000;
        file_type != 0 && file_type != 0o100_000 && file_type != 0o040_000
    });
    if is_special {
        return Err(ArchiveError::UnsafeEntry {
            kind: UnsafeEntryKind::Special,
        });
    }
    Ok(())
}

fn validate_entry_size(
    uncompressed_size: u64,
    compressed_size: u64,
    limits: &Limits,
) -> Result<(), ArchiveError> {
    if uncompressed_size > limits.max_entry_bytes {
        return Err(ArchiveError::LimitExceeded {
            limit: ArchiveLimit::EntryBytes,
        });
    }
    if uncompressed_size > compressed_size.saturating_mul(u64::from(limits.max_compression_ratio)) {
        return Err(ArchiveError::LimitExceeded {
            limit: ArchiveLimit::CompressionRatio,
        });
    }
    Ok(())
}

fn validate_extraction_entry(
    entry: &zip::read::ZipFile<'_, std::fs::File>,
    planned: &ArchiveEntry,
) -> Result<(), ArchiveError> {
    let path = validate_path(entry.name_raw())?;
    validate_entry_kind(entry)?;
    if path != planned.path
        || entry.size() != planned.uncompressed_size
        || entry.compressed_size() != planned.compressed_size
    {
        return Err(ArchiveError::InvalidZip);
    }
    Ok(())
}

fn extraction_limit(limits: &Limits, remaining: u64) -> Result<(u64, ArchiveLimit), ArchiveError> {
    if remaining == 0 {
        return Err(ArchiveError::LimitExceeded {
            limit: ArchiveLimit::TotalBytes,
        });
    }
    if limits.max_entry_bytes <= remaining {
        Ok((limits.max_entry_bytes, ArchiveLimit::EntryBytes))
    } else {
        Ok((remaining, ArchiveLimit::TotalBytes))
    }
}

fn map_extraction_error(error: StoreError, limit: ArchiveLimit) -> ArchiveError {
    match error {
        StoreError::LimitExceeded { .. } => ArchiveError::LimitExceeded { limit },
        other => ArchiveError::Store(other),
    }
}

fn classify_media(bytes: &[u8]) -> (MediaDisposition, &'static str) {
    let trimmed = trim_ascii_whitespace(bytes);
    if looks_like_json(trimmed) {
        (MediaDisposition::StructuredCandidate, "application/json")
    } else if looks_like_html(trimmed) {
        (MediaDisposition::Quarantined, "text/html")
    } else if trimmed.starts_with(b"PK\x03\x04") {
        (MediaDisposition::Quarantined, "application/zip")
    } else {
        (MediaDisposition::Quarantined, "application/octet-stream")
    }
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    bytes.get(start..).unwrap_or_default()
}

fn looks_like_json(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
        && bytes.first().is_some_and(|byte| {
            matches!(
                byte,
                b'{' | b'[' | b'"' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n'
            )
        })
}

fn looks_like_html(bytes: &[u8]) -> bool {
    bytes
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"<html"))
        || bytes
            .get(..14)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"<!doctype html"))
        || bytes.starts_with(b"<script")
}
