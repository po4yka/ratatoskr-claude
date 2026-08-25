//! Content-addressed immutable blob storage owned by this service.
//!
//! Keys are SHA-256 digests beneath this service's own root, so identical
//! bytes deduplicate and an existing object can never be rewritten by a
//! write: a diverging object under an existing digest is a collision, not an
//! overwrite. Reads re-hash what they find, so silently altered storage is
//! reported instead of returned.

use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// The owning service recorded in every reference this store produces.
pub const OWNER_SERVICE: &str = crate::telemetry::SERVICE_NAME;

/// Hash algorithm named by every reference this version produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DigestAlgorithm {
    /// SHA-256, the fleet's archive digest.
    Sha256,
}

impl DigestAlgorithm {
    /// The wire identifier carried inside a reference.
    #[must_use]
    pub fn identifier(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
        }
    }
}

/// A validated `type/subtype` media type string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaType(String);

impl MediaType {
    /// Validates the shape: non-empty `segment/segment`, token characters
    /// only. No parameter section: stored objects carry their type plainly.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidMediaType`] when the shape does not match.
    pub fn parse(value: &str) -> Result<Self, StoreError> {
        let valid = |part: &str| {
            !part.is_empty()
                && part.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'+' | b'~')
                })
        };
        let Some((kind, subtype)) = value.split_once('/') else {
            return Err(StoreError::InvalidMediaType);
        };
        if valid(kind) && valid(subtype) && !subtype.contains('/') {
            Ok(Self(value.to_owned()))
        } else {
            Err(StoreError::InvalidMediaType)
        }
    }

    /// The validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A durable reference to stored bytes, shaped like the fleet
/// `blob-references` contract: owner, digest, media type, length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobRef {
    /// The owning service. Only the owner resolves the bytes.
    pub owner_service: String,
    /// The digest algorithm.
    pub algorithm: DigestAlgorithm,
    /// Lowercase hexadecimal digest of the bytes.
    pub digest_hex: String,
    /// The declared media type.
    pub media_type: MediaType,
    /// Exact byte length.
    pub length_bytes: u64,
}

/// Blob storage failure. Text carries classes, paths under the root, and
/// digests only — never stored bytes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The storage root could not be prepared.
    #[error("the blob store root is unavailable")]
    Unavailable,
    /// An unexpected filesystem failure.
    #[error("the blob store hit a filesystem error")]
    Io(#[from] io::Error),
    /// Bytes exist under this digest and do not match it.
    #[error("an object under digest {digest_hex} already exists and does not match its digest")]
    Collision {
        /// Lowercase hexadecimal digest of the refused write.
        digest_hex: String,
    },
    /// Stored bytes no longer hash to their digest.
    #[error("stored bytes under digest {digest_hex} failed integrity verification")]
    Mismatch {
        /// Lowercase hexadecimal digest the bytes were verified against.
        digest_hex: String,
    },
    /// No object exists for the reference.
    #[error("no object exists for digest {digest_hex}")]
    Missing {
        /// Lowercase hexadecimal digest that was looked up.
        digest_hex: String,
    },
    /// The declared media type is malformed.
    #[error("the media type must be a plain type/subtype pair")]
    InvalidMediaType,
    /// The reference does not belong to this store.
    #[error("the reference does not belong to this blob store")]
    InvalidIdentity,
}

/// The content-addressed store rooted at this service's own directory.
///
/// Writes land through a staging file so a crash leaves either nothing or a
/// complete object behind, and are placed with `hard_link`, which refuses to
/// replace an existing directory entry: immutability holds even if another
/// writer created the object between hashing and placement.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Opens (and prepares) the store beneath `root`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the directory structure cannot be created.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        fs::create_dir_all(root.join("sha256")).map_err(|_| StoreError::Unavailable)?;
        fs::create_dir_all(root.join("staging")).map_err(|_| StoreError::Unavailable)?;
        Ok(Self { root })
    }

    /// Stores bytes, returning their durable reference.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the media type is invalid, an object under
    /// the digest already exists with different bytes ([`StoreError::
    /// Collision`]), or the store cannot hold the bytes faithfully.
    pub fn store(&self, media_type: MediaType, bytes: &[u8]) -> Result<BlobRef, StoreError> {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest_hex = encode_digest(&hasher.finalize());
        let target = self.path_for_digest(&digest_hex);

        if target.exists() {
            // Deduplicate only when the resident bytes really are these bytes;
            // anything else under the key is foreign content that must never
            // be replaced.
            if Self::verify_resident(&target, bytes.len(), &digest_hex)? {
                return Ok(Self::reference(media_type, &digest_hex, bytes.len()));
            }
            return Err(StoreError::Collision {
                digest_hex: digest_hex.clone(),
            });
        }

        let staging_path = self
            .root
            .join("staging")
            .join(format!("{}", Uuid::now_v7()));
        let staged = Self::stage(&staging_path, bytes)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Err(error) = fs::hard_link(&staging_path, &target) {
            if error.kind() != io::ErrorKind::AlreadyExists {
                let _ignored = fs::remove_file(&staging_path);
                return Err(error.into());
            }
            // Another writer placed an object here first. It is acceptable
            // only when it is exactly these bytes.
            let matching = Self::verify_resident(&target, bytes.len(), &digest_hex)?;
            let _ignored = fs::remove_file(&staging_path);
            if !matching {
                return Err(StoreError::Collision {
                    digest_hex: digest_hex.clone(),
                });
            }
        }
        drop(staged);
        let _ignored = fs::remove_file(&staging_path);

        Ok(Self::reference(media_type, &digest_hex, bytes.len()))
    }

    /// Reads back the bytes a reference names, verifying their integrity.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the object is missing or no longer hashes
    /// to its recorded digest.
    pub fn read(&self, reference: &BlobRef) -> Result<Vec<u8>, StoreError> {
        let target = self.resolve(reference)?;
        let bytes = fs::read(target)?;
        if !Self::matches_digest(&bytes, reference) {
            return Err(StoreError::Mismatch {
                digest_hex: reference.digest_hex.clone(),
            });
        }
        Ok(bytes)
    }

    /// Verifies integrity without materializing the bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] like [`BlobStore::read`].
    pub fn verify(&self, reference: &BlobRef) -> Result<(), StoreError> {
        let target = self.resolve(reference)?;
        let bytes = fs::read(target)?;
        if Self::matches_digest(&bytes, reference) {
            Ok(())
        } else {
            Err(StoreError::Mismatch {
                digest_hex: reference.digest_hex.clone(),
            })
        }
    }

    /// Answers whether the store can hold bytes right now.
    ///
    /// A write-read-delete round trip of one probe file through staging: a
    /// root that exists but no longer accepts writes reports itself down
    /// instead of failing the first real archive later.
    #[must_use]
    pub fn writable_probe(&self) -> bool {
        let probe_path = self
            .root
            .join("staging")
            .join(format!("probe-{}", Uuid::now_v7()));
        match Self::stage(&probe_path, b"ratatoskr-claude writability probe") {
            Ok(file) => {
                drop(file);
                let _ignored = fs::remove_file(&probe_path);
                true
            }
            Err(_) => false,
        }
    }

    /// The filesystem location of one digest inside this store.
    fn path_for_digest(&self, digest_hex: &str) -> PathBuf {
        let (prefix, rest) = split_digest(digest_hex);
        self.root.join("sha256").join(prefix).join(rest)
    }

    /// Checks a reference belongs here and names its object.
    fn resolve(&self, reference: &BlobRef) -> Result<PathBuf, StoreError> {
        if reference.owner_service != OWNER_SERVICE
            || reference.algorithm != DigestAlgorithm::Sha256
            || !is_canonical_sha256(&reference.digest_hex)
        {
            return Err(StoreError::InvalidIdentity);
        }
        let target = self.path_for_digest(&reference.digest_hex);
        if !target.is_file() {
            return Err(StoreError::Missing {
                digest_hex: reference.digest_hex.clone(),
            });
        }
        Ok(target)
    }

    /// Whether resident bytes at `path` have exactly this length and digest.
    fn verify_resident(
        path: &Path,
        expected_len: usize,
        digest_hex: &str,
    ) -> Result<bool, StoreError> {
        let resident = fs::read(path)?;
        let resident_len = resident.len();
        Ok(resident_len == expected_len && Self::hex_matches(&resident, digest_hex))
    }

    fn matches_digest(bytes: &[u8], reference: &BlobRef) -> bool {
        bytes.len() == usize::try_from(reference.length_bytes).unwrap_or(usize::MAX)
            && Self::hex_matches(bytes, &reference.digest_hex)
    }

    fn hex_matches(bytes: &[u8], digest_hex: &str) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        encode_digest(&hasher.finalize()) == digest_hex
    }

    fn reference(media_type: MediaType, digest_hex: &str, length: usize) -> BlobRef {
        BlobRef {
            owner_service: OWNER_SERVICE.to_owned(),
            algorithm: DigestAlgorithm::Sha256,
            digest_hex: digest_hex.to_owned(),
            media_type,
            length_bytes: u64::try_from(length).unwrap_or(u64::MAX),
        }
    }

    /// Writes bytes to a staging file in bounded chunks and flushes the file
    /// contents to the device before any consumer can see them.
    fn stage(staging_path: &Path, bytes: &[u8]) -> Result<fs::File, StoreError> {
        let mut file = fs::File::create(staging_path)?;
        for chunk in bytes.chunks(8192) {
            file.write_all(chunk)?;
        }
        file.sync_all()?;
        Ok(file)
    }
}

fn split_digest(digest_hex: &str) -> (&str, &str) {
    let boundary = 2.min(digest_hex.len());
    digest_hex.split_at(boundary)
}

/// Whether `digest_hex` is 64 lowercase hexadecimal characters.
fn is_canonical_sha256(digest_hex: &str) -> bool {
    digest_hex.len() == 64
        && digest_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Lowercase hexadecimal encoding, hand-rolled: the fleet carries no hex crate.
fn encode_digest(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ignored = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Creates a uniquely named temporary directory for one test.
///
/// # Panics
///
/// Never in normal operation; a host without a temporary directory cannot run
/// the suite at all.
#[cfg(feature = "test-support")]
pub mod scratch {
    use std::fs;
    use std::path::{Path, PathBuf};

    use uuid::Uuid;

    /// A fresh directory no other test has touched.
    ///
    /// # Panics
    ///
    /// When the host cannot create it; the suite cannot run at all then.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "test scaffolding: a host that cannot create a scratch directory cannot run the suite at all"
    )]
    pub fn temp_root(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("ratatoskr-claude-{label}-{}", Uuid::now_v7()));
        fs::create_dir_all(&root).expect("the temporary root can be created");
        root
    }

    /// Best-effort removal of a scratch tree, ignoring absence.
    pub fn remove(root: &Path) {
        let _ignored = fs::remove_dir_all(root);
    }
}
