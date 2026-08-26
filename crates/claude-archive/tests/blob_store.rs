//! The blob store contract: content addressing, durable references,
//! write-once immutability, and integrity-verified reads.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{BlobRef, BlobStore, DigestAlgorithm, MediaType, StoreError};

const OWNER: &str = "ratatoskr-claude-archive";
const MEDIA_TYPE: &str = "application/zip";

/// Independent one-shot SHA-256 hex encoding, computed outside the
/// implementation under test and used to predict where objects must land.
fn one_shot_digest_hex(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let mut encoded = String::new();
    for byte in hasher.finalize() {
        let _ignored = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn open_store(label: &str) -> (std::path::PathBuf, BlobStore) {
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the store opens against a fresh directory");
    (root, store)
}

/// Every regular file beneath `root/sha256`, discovered without recursion
/// assumptions about depth.
fn object_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let buckets = fs::read_dir(root.join("sha256")).expect("the digest bucket layer exists");
    for bucket in buckets {
        let bucket = bucket.expect("bucket entries are readable");
        if !bucket.file_type().expect("entry type is readable").is_dir() {
            continue;
        }
        for object in fs::read_dir(bucket.path()).expect("objects are listable") {
            let object = object.expect("object entries are readable");
            if object.file_type().expect("type is readable").is_file() {
                found.push(object.path());
            }
        }
    }
    found
}

#[test]
fn roundtrip_returns_original_bytes() {
    let (root, store) = open_store("roundtrip");
    let payload = b"an immutable archive of some kind".to_vec();

    let reference = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("storing succeeds");
    let read_back = store.read(&reference).expect("reading succeeds");

    assert_eq!(read_back, payload, "the bytes survive the round trip");
    remove(&root);
}

#[test]
fn reference_fields_match_contract() {
    // SHA-256 of b"contract payload", computed outside the implementation
    // under test and pinned here as the golden anchor.
    const EXPECTED_DIGEST: &str =
        "27740f5a7b475e00de5f4045ad83108384dada72a84ce7adf40e7dbbc087f077";

    let (root, store) = open_store("reference");
    let payload = b"contract payload".to_vec();

    let reference = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("storing succeeds");

    assert_eq!(reference.owner_service, OWNER);
    assert_eq!(reference.algorithm, DigestAlgorithm::Sha256);
    assert_eq!(
        reference.algorithm.identifier(),
        "sha256",
        "the algorithm carries its wire identifier"
    );
    assert_eq!(
        reference.digest_hex, EXPECTED_DIGEST,
        "the digest is the lowercase hexadecimal SHA-256 of the payload"
    );
    assert_eq!(reference.media_type.as_str(), MEDIA_TYPE);
    assert_eq!(
        reference.length_bytes,
        u64::try_from(payload.len()).expect("length fits")
    );
    remove(&root);
}

#[test]
fn identical_bytes_deduplicate_to_one_object() {
    let (root, store) = open_store("dedup");
    let payload = b"the very same bytes".to_vec();

    let first = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("the first store succeeds");
    let second = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("re-storing identical bytes is idempotent success");

    assert_eq!(first, second, "both stores return the same reference");
    let files = object_files(&root);
    assert_eq!(files.len(), 1, "exactly one object exists for that digest");
    assert_eq!(
        fs::read(&files[0]).expect("the object is readable"),
        payload,
        "the stored bytes are intact"
    );
    remove(&root);
}

#[test]
fn different_bytes_produce_different_references() {
    let (root, store) = open_store("distinct");

    let first = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), b"first payload")
        .expect("the first store succeeds");
    let second = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), b"second payload")
        .expect("the second store succeeds");

    assert_ne!(first.digest_hex, second.digest_hex);
    assert_ne!(first, second);
    remove(&root);
}

#[test]
fn overwrite_refused_and_divergence_reported_without_echoing_payloads() {
    let (root, store) = open_store("immutable");
    let original = b"original immutable bytes".to_vec();
    let divergent = b"totally different divergent bytes".to_vec();

    let reference = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &original)
        .expect("the first store succeeds");
    let (bucket, remainder) = reference.digest_hex.split_at(2);
    let path = root.join("sha256").join(bucket).join(remainder);

    // Simulate a foreign writer replacing the object out-of-band.
    fs::write(&path, &divergent).expect("the test can plant divergent bytes");
    let planted = fs::read(&path).expect("the planted file is readable");
    assert_eq!(planted, divergent);

    // Storing the original again must refuse rather than rewrite the object.
    let refused = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &original)
        .expect_err("a mismatching existing object refuses the write");
    assert!(
        matches!(refused, StoreError::Collision { .. }),
        "the refusal names a collision, got: {refused}"
    );
    let after = fs::read(&path).expect("the object survives the refused write");
    assert_eq!(
        after, divergent,
        "the refused write left the existing object exactly as it was"
    );

    // Neither payload may appear in the error text.
    let rendered = refused.to_string();
    assert!(!rendered.contains(std::str::from_utf8(&original).unwrap_or_default()));
    assert!(!rendered.contains(std::str::from_utf8(&divergent).unwrap_or_default()));

    remove(&root);
}

#[test]
fn tampered_storage_fails_read() {
    let (root, store) = open_store("tamper");
    let payload = b"bytes that will be tampered with on disk".to_vec();

    let reference = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("storing succeeds");
    let (bucket, remainder) = reference.digest_hex.split_at(2);
    let path = root.join("sha256").join(bucket).join(remainder);

    let mut on_disk = fs::read(&path).expect("the object is readable");
    on_disk[0] ^= 0xff;
    fs::write(&path, &on_disk).expect("the test can flip one byte");

    let error = store
        .read(&reference)
        .expect_err("reading altered bytes must fail");
    match error {
        StoreError::Mismatch { digest_hex } => {
            assert_eq!(digest_hex, reference.digest_hex);
        }
        other => panic!("expected an integrity mismatch, got: {other}"),
    }
    remove(&root);
}

#[test]
fn rejects_invalid_media_type() {
    for candidate in ["", "notamediatype", "has spaces/inside"] {
        let parsed = MediaType::parse(candidate);
        assert!(
            matches!(parsed, Err(StoreError::InvalidMediaType)),
            "{candidate:?} must be refused as a media type"
        );
    }
    // A different well-formed pair parses and round-trips its string.
    let valid = MediaType::parse("text/markdown").expect("a plain pair is valid");
    assert_eq!(valid.as_str(), "text/markdown");
}

#[test]
fn foreign_reference_is_refused() {
    let (root, store) = open_store("foreign");
    let reference = BlobRef {
        owner_service: "ratatoskr-someone-else".to_owned(),
        algorithm: DigestAlgorithm::Sha256,
        digest_hex: "00".repeat(32),
        media_type: MediaType::parse(MEDIA_TYPE).unwrap(),
        length_bytes: 0,
    };

    let error = store
        .read(&reference)
        .expect_err("a reference from another owner must not resolve here");
    assert!(matches!(error, StoreError::InvalidIdentity));
    remove(&root);
}

/// A deterministic payload several internal chunks long plus a ragged
/// remainder, so incremental hashing crosses chunk boundaries unevenly.
fn multi_chunk_payload(chunks: usize, remainder: usize) -> Vec<u8> {
    let length = chunks * 65_536 + remainder;
    (0..length)
        .map(|index| u8::try_from(index % 251).unwrap_or(0))
        .collect()
}

#[test]
fn streamed_ingest_matches_buffered_reference_across_chunk_boundaries() {
    let (root, store) = open_store("stream-match");
    let payload = multi_chunk_payload(3, 137);

    let streamed = store
        .store_stream(
            MediaType::parse(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(&payload),
            u64::try_from(payload.len()).unwrap(),
        )
        .expect("streaming succeeds within the cap");
    let buffered = store
        .store(MediaType::parse(MEDIA_TYPE).unwrap(), &payload)
        .expect("buffered storing succeeds");

    assert_eq!(
        streamed, buffered,
        "both paths address identical bytes identically"
    );
    let files = object_files(&root);
    assert_eq!(files.len(), 1, "exactly one object backs both paths");
    remove(&root);
}

#[test]
fn streamed_ingest_refuses_oversized_stream_and_leaves_nothing_complete() {
    let (root, store) = open_store("stream-cap");
    let payload = multi_chunk_payload(1, 5001); // well past a 1024-byte cap
    let expected_digest = one_shot_digest_hex(&payload);

    let refused = store
        .store_stream(
            MediaType::parse(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(&payload),
            1024,
        )
        .expect_err("a stream past its cap must be refused");

    match &refused {
        StoreError::LimitExceeded { limit_bytes } => {
            assert_eq!(*limit_bytes, 1024, "the refusal names the cap");
        }
        other => panic!("expected a limit-exceeded refusal, got: {other}"),
    }

    // Nothing durable: no object under the would-be digest, staging empty.
    let would_be = BlobRef {
        owner_service: OWNER.to_owned(),
        algorithm: DigestAlgorithm::Sha256,
        digest_hex: expected_digest.clone(),
        media_type: MediaType::parse(MEDIA_TYPE).unwrap(),
        length_bytes: u64::try_from(payload.len()).unwrap(),
    };
    let read_attempt = store.read(&would_be);
    assert!(
        matches!(read_attempt, Err(StoreError::Missing { .. })),
        "no object may exist for the refused stream"
    );
    let staging_entries =
        fs::read_dir(root.join("staging")).expect("staging directory is listable");
    assert_eq!(
        staging_entries.count(),
        0,
        "the refused attempt leaves no staged files behind"
    );

    remove(&root);
}

#[test]
fn streamed_ingest_refuses_empty_stream() {
    let (root, store) = open_store("stream-empty");
    let empty: Vec<u8> = Vec::new();

    let refused = store
        .store_stream(
            MediaType::parse(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(&empty),
            1024,
        )
        .expect_err("an empty stream has nothing worth archiving");

    assert!(
        matches!(refused, StoreError::EmptyInput),
        "expected an empty-input refusal, got: {refused}"
    );
    remove(&root);
}

#[test]
fn streamed_ingest_preserves_write_once_deduplication() {
    let (root, store) = open_store("stream-dedup");
    let payload = multi_chunk_payload(2, 91);

    let first = store
        .store_stream(
            MediaType::parse(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(&payload),
            u64::try_from(payload.len()).unwrap(),
        )
        .expect("the first streamed ingest succeeds");
    let second = store
        .store_stream(
            MediaType::parse(MEDIA_TYPE).unwrap(),
            std::io::Cursor::new(&payload),
            u64::try_from(payload.len()).unwrap(),
        )
        .expect("re-streaming identical bytes is idempotent success");

    assert_eq!(first, second, "both ingests return the same reference");
    assert_eq!(
        object_files(&root).len(),
        1,
        "exactly one object exists for the digest"
    );
    remove(&root);
}
