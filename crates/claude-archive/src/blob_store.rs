//! Content-addressed immutable blob storage owned by this service.
//!
//! Keys are SHA-256 digests beneath this service's own root, so identical
//! bytes deduplicate and an existing object can never be rewritten by a
//! write: a diverging object under an existing digest is a collision, not an
//! overwrite. Reads re-hash what they find, so silently altered storage is
//! reported instead of returned.

use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read as _, Seek as _, Write as _};
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

/// Result of an exact idempotent `BlobStore` erasure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EraseOutcome {
    /// The exact owned object was removed.
    Erased,
    /// The exact owned object was already absent.
    AlreadyAbsent,
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
    /// A streamed ingest delivered more bytes than its declared maximum.
    #[error("the stream exceeded its declared limit of {limit_bytes} bytes")]
    LimitExceeded {
        /// The declared maximum, named by the refusal.
        limit_bytes: u64,
    },
    /// A streamed ingest delivered no bytes at all.
    #[error("the stream delivered no bytes")]
    EmptyInput,
    /// A caller-supplied digest or length does not match the completed stream.
    #[error("the completed stream differs from its declared identity")]
    DeclaredIdentityMismatch,
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

        let staging_path = self.root.join("staging").join(Uuid::now_v7().to_string());
        let staged = Self::stage(&staging_path, bytes)?;
        let placed = self.publish_staged(media_type, &staging_path, &digest_hex, bytes.len());
        drop(staged);
        let _ignored = fs::remove_file(&staging_path);
        placed
    }

    /// Stores a byte stream, hashing and counting it while it arrives.
    ///
    /// The stream is consumed in bounded chunks, so memory stays flat no
    /// matter how large the archive claims to be. The declared maximum is
    /// enforced mid-stream: the moment it is exceeded the ingest fails, the
    /// staged file is removed, and nothing durable exists for those bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::LimitExceeded`] when the stream passes
    /// `max_bytes`, [`StoreError::EmptyInput`] when it delivers nothing,
    /// [`StoreError::Collision`] when an object under the digest already
    /// holds different bytes, and [`StoreError`] propagates filesystem
    /// failures from staging and placement.
    pub fn store_stream(
        &self,
        media_type: MediaType,
        mut reader: impl io::Read,
        max_bytes: u64,
    ) -> Result<BlobRef, StoreError> {
        let staging_path = self.root.join("staging").join(Uuid::now_v7().to_string());
        let (digest_hex, total) = match stage_stream(&staging_path, &mut reader, max_bytes) {
            Ok(pair) => pair,
            Err(error) => {
                let _ignored = fs::remove_file(&staging_path);
                return Err(error);
            }
        };

        let length = usize::try_from(total).unwrap_or(usize::MAX);
        let placed = self.publish_staged(media_type, &staging_path, &digest_hex, length);
        let _ignored = fs::remove_file(&staging_path);
        placed
    }

    /// Stores a stream only when its completed digest and length equal the
    /// independently declared identity.
    ///
    /// The comparison happens while the bytes are still private staging data,
    /// so a declaration mismatch cannot publish a raw object.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::DeclaredIdentityMismatch`] when the completed
    /// stream differs from either declared field, with no durable object.
    pub fn store_stream_with_identity(
        &self,
        media_type: MediaType,
        mut reader: impl io::Read,
        max_bytes: u64,
        expected_digest_hex: &str,
        expected_length_bytes: u64,
    ) -> Result<BlobRef, StoreError> {
        let staging_path = self.root.join("staging").join(Uuid::now_v7().to_string());
        let (digest_hex, total) = match stage_stream(&staging_path, &mut reader, max_bytes) {
            Ok(pair) => pair,
            Err(error) => {
                let _ignored = fs::remove_file(&staging_path);
                return Err(error);
            }
        };
        if digest_hex != expected_digest_hex || total != expected_length_bytes {
            let _ignored = fs::remove_file(&staging_path);
            return Err(StoreError::DeclaredIdentityMismatch);
        }

        let length = usize::try_from(total).unwrap_or(usize::MAX);
        let placed = self.publish_staged(media_type, &staging_path, &digest_hex, length);
        let _ignored = fs::remove_file(&staging_path);
        placed
    }

    /// Reads back the bytes a reference names, verifying their integrity.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the object is missing or no longer hashes
    /// to its recorded digest.
    pub fn read(&self, reference: &BlobRef) -> Result<Vec<u8>, StoreError> {
        let mut file = self.open_verified(reference)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    /// Verifies integrity without materializing the bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] like [`BlobStore::read`].
    pub fn verify(&self, reference: &BlobRef) -> Result<(), StoreError> {
        self.open_verified(reference).map(|_| ())
    }

    /// Erases exactly the locally owned object named by `reference`.
    ///
    /// Repeating an erasure is successful and reports
    /// [`EraseOutcome::AlreadyAbsent`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the reference is not a valid locally owned
    /// content address or storage cannot complete the erasure.
    pub fn erase(&self, reference: &BlobRef) -> Result<EraseOutcome, StoreError> {
        Self::validate_identity(reference)?;
        let target = self.path_for_digest(&reference.digest_hex);
        self.validate_target_parent(&target)?;
        match fs::symlink_metadata(&target) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(EraseOutcome::AlreadyAbsent);
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(StoreError::InvalidIdentity);
            }
            Ok(_) => {}
        }
        drop(self.open_verified(reference)?);
        self.validate_target_parent(&target)?;
        match fs::remove_file(target) {
            Ok(()) => Ok(EraseOutcome::Erased),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(EraseOutcome::AlreadyAbsent)
            }
            Err(error) => Err(error.into()),
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

    /// Places a fully staged, fully hashed file at its content address.
    ///
    /// Both ingest paths converge here. A resident object under the digest is
    /// accepted only when it holds exactly these bytes (idempotent
    /// deduplication); anything else is a collision that never rewrites it.
    fn publish_staged(
        &self,
        media_type: MediaType,
        staging_path: &Path,
        digest_hex: &str,
        length: usize,
    ) -> Result<BlobRef, StoreError> {
        let target = self.path_for_digest(digest_hex);
        if target.exists() {
            // Deduplicate only when the resident bytes really are these bytes;
            // anything else under the key is foreign content that must never
            // be replaced.
            let matching = Self::verify_resident(&target, length, digest_hex)?;
            if matching {
                return Ok(Self::reference(media_type, digest_hex, length));
            }
            return Err(StoreError::Collision {
                digest_hex: digest_hex.to_owned(),
            });
        }

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Err(error) = fs::hard_link(staging_path, &target) {
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error.into());
            }
            // Another writer placed an object here first. It is acceptable
            // only when it is exactly these bytes.
            let matching = Self::verify_resident(&target, length, digest_hex)?;
            if !matching {
                return Err(StoreError::Collision {
                    digest_hex: digest_hex.to_owned(),
                });
            }
        }

        Ok(Self::reference(media_type, digest_hex, length))
    }

    /// The filesystem location of one digest inside this store.
    fn path_for_digest(&self, digest_hex: &str) -> PathBuf {
        let (prefix, rest) = split_digest(digest_hex);
        self.root.join("sha256").join(prefix).join(rest)
    }

    /// Checks a reference belongs here and names its object.
    fn resolve(&self, reference: &BlobRef) -> Result<PathBuf, StoreError> {
        Self::validate_identity(reference)?;
        let target = self.path_for_digest(&reference.digest_hex);
        self.validate_target_parent(&target)?;
        if !target.is_file() {
            return Err(StoreError::Missing {
                digest_hex: reference.digest_hex.clone(),
            });
        }
        Ok(target)
    }

    fn validate_target_parent(&self, target: &Path) -> Result<(), StoreError> {
        let object_root = fs::canonicalize(self.root.join("sha256"))?;
        let parent = target.parent().ok_or(StoreError::InvalidIdentity)?;
        let canonical_parent = match fs::canonicalize(parent) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if !canonical_parent.starts_with(object_root) {
            return Err(StoreError::InvalidIdentity);
        }
        Ok(())
    }

    fn validate_identity(reference: &BlobRef) -> Result<(), StoreError> {
        if reference.owner_service != OWNER_SERVICE
            || reference.algorithm != DigestAlgorithm::Sha256
            || !is_canonical_sha256(&reference.digest_hex)
        {
            return Err(StoreError::InvalidIdentity);
        }
        Ok(())
    }

    /// Opens one verified object at its beginning without materializing it.
    ///
    /// Archive consumers use the returned handle to apply their own streaming
    /// and seeking limits while preserving the `BlobStore` integrity check.
    pub(crate) fn open_verified(&self, reference: &BlobRef) -> Result<fs::File, StoreError> {
        let target = self.resolve(reference)?;
        let mut file = fs::File::open(target)?;
        if !Self::matches_file(&mut file, reference)? {
            return Err(StoreError::Mismatch {
                digest_hex: reference.digest_hex.clone(),
            });
        }
        file.rewind()?;
        Ok(file)
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

    fn matches_file(file: &mut fs::File, reference: &BlobRef) -> Result<bool, StoreError> {
        if file.metadata()?.len() != reference.length_bytes {
            return Ok(false);
        }
        let mut hasher = Sha256::new();
        let mut chunk = vec![0_u8; INGEST_CHUNK_BYTES];
        loop {
            let filled = file.read(&mut chunk)?;
            if filled == 0 {
                break;
            }
            let Some(filled_bytes) = chunk.get(..filled) else {
                return Ok(false);
            };
            hasher.update(filled_bytes);
        }
        Ok(encode_digest(&hasher.finalize()) == reference.digest_hex)
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

/// The size of one streaming ingest chunk: large enough that syscall count
/// stays modest for archive-sized uploads, small enough that memory stays
/// flat no matter how large the stream claims to be.
const INGEST_CHUNK_BYTES: usize = 65_536;

/// Consumes `reader` into a staging file at `staging_path`, folding an
/// incremental SHA-256 over everything delivered. Refuses mid-stream once
/// the total passes `max_bytes`, and refuses a stream that delivers nothing.
///
/// The caller owns the staged file: it is removed on every error path the
/// caller sees, and left only behind a crashed process, where its staging
/// location keeps it inert.
fn stage_stream(
    staging_path: &Path,
    reader: &mut impl io::Read,
    max_bytes: u64,
) -> Result<(String, u64), StoreError> {
    let mut file = fs::File::create(staging_path)?;
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let mut chunk = vec![0u8; INGEST_CHUNK_BYTES];
    loop {
        let filled = reader.read(&mut chunk)?;
        if filled == 0 {
            break;
        }
        total += u64::try_from(filled).unwrap_or(u64::MAX);
        if total > max_bytes {
            return Err(StoreError::LimitExceeded {
                limit_bytes: max_bytes,
            });
        }
        let filled_bytes = chunk.get(..filled).ok_or_else(|| {
            StoreError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "a read reported more bytes than its buffer holds",
            ))
        })?;
        hasher.update(filled_bytes);
        file.write_all(filled_bytes)?;
    }
    if total == 0 {
        return Err(StoreError::EmptyInput);
    }
    file.sync_all()?;
    Ok((encode_digest(&hasher.finalize()), total))
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
