//! Hostile ZIP inventory tests against the public inspection boundary.
#![expect(
    clippy::expect_used,
    reason = "integration-test setup must stop loudly when synthetic fixtures cannot be built"
)]

use std::io::Write as _;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{
    ArchiveError, ArchiveExtractor, ArchiveInspector, ArchiveLimit, BlobRef, BlobStore, Config,
    MediaDisposition, MediaType, UnsafeEntryKind,
};
use sha2::{Digest as _, Sha256};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const ARCHIVE_MEDIA_TYPE: &str = "application/zip";

fn inspector_with(
    mut change: impl FnMut(&mut ratatoskr_claude_archive::Limits),
) -> ArchiveInspector {
    let mut limits = Config::default().limits;
    change(&mut limits);
    ArchiveInspector::new(&limits)
}

fn extractor_with(
    mut change: impl FnMut(&mut ratatoskr_claude_archive::Limits),
) -> ArchiveExtractor {
    let mut limits = Config::default().limits;
    change(&mut limits);
    ArchiveExtractor::new(&limits)
}

fn open_store(label: &str) -> (std::path::PathBuf, BlobStore) {
    let root = temp_root(label);
    let store = BlobStore::open(&root).expect("the test store opens");
    (root, store)
}

fn store_archive(store: &BlobStore, bytes: &[u8]) -> BlobRef {
    store
        .store(
            MediaType::parse(ARCHIVE_MEDIA_TYPE).expect("the archive type is valid"),
            bytes,
        )
        .expect("synthetic archive stores")
}

fn zip_with_files(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    for (path, bytes) in entries {
        writer
            .start_file(*path, SimpleFileOptions::default())
            .expect("synthetic file starts");
        writer.write_all(bytes).expect("synthetic file writes");
    }
    writer
        .finish()
        .expect("synthetic archive finishes")
        .into_inner()
}

fn stored_zip_with_files(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    for (path, bytes) in entries {
        writer
            .start_file(
                *path,
                SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
            )
            .expect("synthetic stored file starts");
        writer
            .write_all(bytes)
            .expect("synthetic stored file writes");
    }
    writer
        .finish()
        .expect("synthetic archive finishes")
        .into_inner()
}

fn zip_with_link() -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    writer
        .add_symlink("link", "target", SimpleFileOptions::default())
        .expect("synthetic link writes");
    writer
        .finish()
        .expect("synthetic archive finishes")
        .into_inner()
}

fn zip_with_special_entry() -> Vec<u8> {
    let mut bytes = zip_with_files(&[("device", b"unused")]);
    let central_header = bytes
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .expect("the synthetic archive contains a central header");
    *bytes
        .get_mut(central_header + 5)
        .expect("the central header has a version byte") = 3;
    bytes
        .get_mut(central_header + 38..central_header + 42)
        .expect("the central header has an external-attributes field")
        .copy_from_slice(&(0o020_777_u32 << 16).to_le_bytes());
    bytes
}

fn encrypted_zip() -> Vec<u8> {
    let mut bytes = zip_with_files(&[("secret.json", br"{}")]);
    let central_header = bytes
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .expect("the synthetic archive contains a central header");
    *bytes.get_mut(6).expect("the local header has a flags byte") |= 1;
    *bytes
        .get_mut(central_header + 8)
        .expect("the central header has a flags byte") |= 1;
    bytes
}

fn compressed_zip() -> Vec<u8> {
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    writer
        .start_file(
            "large.json",
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )
        .expect("compressed file starts");
    writer
        .write_all(&vec![b'x'; 4096])
        .expect("compressed file writes");
    writer
        .finish()
        .expect("synthetic archive finishes")
        .into_inner()
}

#[test]
fn inspection_rejects_traversal_absolute_and_duplicate_normalized_paths() {
    for bytes in [
        zip_with_files(&[("../escape.json", br"{}")]),
        zip_with_files(&[("/absolute.json", br"{}")]),
        zip_with_files(&[
            ("folder//entry.json", br"{}"),
            ("folder/entry.json", br"{}"),
        ]),
    ] {
        let (root, store) = open_store("unsafe-path");
        let raw = store_archive(&store, &bytes);

        let error = inspector_with(|_| {})
            .inspect(&store, &raw)
            .expect_err("unsafe paths must be refused");

        assert!(matches!(error, ArchiveError::UnsafePath));
        remove(&root);
    }
}

#[test]
fn inspection_rejects_link_encrypted_and_special_entries() {
    for (bytes, expected) in [
        (zip_with_link(), UnsafeEntryKind::Link),
        (encrypted_zip(), UnsafeEntryKind::Encrypted),
        (zip_with_special_entry(), UnsafeEntryKind::Special),
    ] {
        let (root, store) = open_store("unsafe-entry");
        let raw = store_archive(&store, &bytes);

        let error = inspector_with(|_| {})
            .inspect(&store, &raw)
            .expect_err("unsafe entry kinds must be refused");

        assert!(
            matches!(error, ArchiveError::UnsafeEntry { kind } if kind == expected),
            "expected {expected:?}, got {error:?}"
        );
        remove(&root);
    }
}

#[test]
fn inspection_rejects_entry_count_declared_expansion_and_compression_ratio_bombs() {
    let cases = [
        (
            zip_with_files(&[("one.json", br"{}"), ("two.json", br"{}")]),
            ArchiveLimit::EntryCount,
            Box::new(|limits: &mut ratatoskr_claude_archive::Limits| limits.max_archive_entries = 1)
                as Box<dyn Fn(&mut ratatoskr_claude_archive::Limits)>,
        ),
        (
            zip_with_files(&[("large.json", b"1234")]),
            ArchiveLimit::EntryBytes,
            Box::new(|limits: &mut ratatoskr_claude_archive::Limits| limits.max_entry_bytes = 1),
        ),
        (
            compressed_zip(),
            ArchiveLimit::CompressionRatio,
            Box::new(|limits: &mut ratatoskr_claude_archive::Limits| {
                limits.max_compression_ratio = 1;
            }),
        ),
    ];

    for (bytes, expected, configure) in cases {
        let (root, store) = open_store("archive-limit");
        let raw = store_archive(&store, &bytes);

        let error = inspector_with(configure)
            .inspect(&store, &raw)
            .expect_err("declared hostile limits must be refused");

        assert!(matches!(error, ArchiveError::LimitExceeded { limit } if limit == expected));
        remove(&root);
    }
}

#[test]
fn inspection_accepts_a_bounded_json_inventory_without_reading_or_executing_it() {
    let (root, store) = open_store("json-inventory");
    let raw = store_archive(&store, &zip_with_files(&[("manifest.json", br"{}")]));

    let inventory = inspector_with(|_| {})
        .inspect(&store, &raw)
        .expect("a bounded JSON entry produces an inventory");

    assert_eq!(inventory.entries().len(), 1);
    let entry = &inventory.entries()[0];
    assert_eq!(entry.path(), "manifest.json");
    assert_eq!(entry.uncompressed_size(), 2);
    assert!(entry.compressed_size() > 0);
    remove(&root);
}

#[test]
fn extraction_stores_validated_entry_with_raw_digest_provenance() {
    let (root, store) = open_store("provenance");
    let source = br#"{\"archive\":true}"#;
    let archive = zip_with_files(&[("manifest.json", source)]);
    let raw = store_archive(&store, &archive);
    let inventory = inspector_with(|_| {})
        .inspect(&store, &raw)
        .expect("the source archive inspects");

    let extraction = extractor_with(|_| {})
        .extract(&store, &raw, &inventory)
        .expect("the inspected entry extracts");

    assert_eq!(extraction.artifacts().len(), 1);
    let artifact = &extraction.artifacts()[0];
    assert_eq!(
        store
            .read(artifact.blob_ref())
            .expect("derived bytes read back"),
        source
    );
    assert_eq!(
        artifact.provenance().raw_digest_hex(),
        format!("{:x}", Sha256::digest(&archive))
    );
    assert_eq!(artifact.provenance().entry_path(), "manifest.json");
    assert_eq!(
        artifact.disposition(),
        MediaDisposition::StructuredCandidate
    );
    remove(&root);
}

#[test]
fn extraction_refuses_actual_entry_expansion_past_the_streaming_cap_without_a_blob_ref() {
    let (root, store) = open_store("expansion-cap");
    let archive = stored_zip_with_files(&[("large.json", &vec![b'x'; 4096])]);
    let raw = store_archive(&store, &archive);
    let inventory = inspector_with(|_| {})
        .inspect(&store, &raw)
        .expect("the archive passes its inspection budget");

    let error = extractor_with(|limits| limits.max_entry_bytes = 1024)
        .extract(&store, &raw, &inventory)
        .expect_err("streaming must refuse bytes past the entry cap");

    assert!(matches!(
        error,
        ArchiveError::LimitExceeded {
            limit: ArchiveLimit::EntryBytes
        }
    ));
    remove(&root);
}

#[test]
fn html_and_nested_archives_are_quarantined_without_execution() {
    let (root, store) = open_store("quarantine");
    let nested = zip_with_files(&[("nested.json", br"{}")]);
    let archive = zip_with_files(&[
        ("viewer.html", b"<script>alert('never execute')</script>"),
        ("nested.zip", &nested),
    ]);
    let raw = store_archive(&store, &archive);
    let inventory = inspector_with(|_| {})
        .inspect(&store, &raw)
        .expect("the archive structurally inspects");

    let extraction = extractor_with(|_| {})
        .extract(&store, &raw, &inventory)
        .expect("inert entries retain as quarantined data");

    assert_eq!(extraction.artifacts().len(), 2);
    assert!(
        extraction
            .artifacts()
            .iter()
            .all(|artifact| artifact.disposition() == MediaDisposition::Quarantined)
    );
    remove(&root);
}
