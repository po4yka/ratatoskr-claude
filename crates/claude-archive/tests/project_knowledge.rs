//! Project Knowledge `BlobStore` ingest contracts.

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{
    BlobStore, ConsumerExportParser, KnowledgeFileAnomaly, KnowledgeFileAvailability,
    ProjectKnowledgeIngestor,
};

const SYNTHETIC_EXPORT: &str = include_str!("fixtures/synthetic_consumer_export.json");

#[test]
fn stores_matching_knowledge_bytes_as_a_verified_blob() {
    let root = temp_root("project-knowledge-verified");
    let store = BlobStore::open(&root).expect("the test store opens");
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");
    let file = parsed
        .project_knowledge_files
        .first()
        .expect("the fixture provides a file with matching bytes");

    let result = ProjectKnowledgeIngestor::new(&store)
        .ingest(file)
        .expect("the available file ingests");

    let KnowledgeFileAvailability::Verified { blob_ref } = result.availability else {
        panic!("matching bytes must become a verified local backup");
    };
    assert_eq!(
        blob_ref.digest_hex,
        "fe75f1cc18ee987f97d34faaded87de72b2ea0c96bca98e0b6c183b30f9c49fe"
    );
    assert_eq!(blob_ref.length_bytes, 11);
    remove(&root);
}

#[test]
fn quarantines_digest_mismatch_without_publishing_a_backup() {
    let root = temp_root("project-knowledge-quarantine");
    let store = BlobStore::open(&root).expect("the test store opens");
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");
    let file = parsed
        .project_knowledge_files
        .get(2)
        .expect("the fixture provides a digest-mismatched file");

    let result = ProjectKnowledgeIngestor::new(&store)
        .ingest(file)
        .expect("the mismatched file remains retained evidence");

    assert!(
        matches!(
            result.availability,
            KnowledgeFileAvailability::Quarantined {
                reason: KnowledgeFileAnomaly::DigestMismatch,
                ..
            }
        ),
        "a digest mismatch must remain quarantined rather than become a backup"
    );
    remove(&root);
}
