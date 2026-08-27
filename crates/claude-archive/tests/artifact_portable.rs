//! Safe portable Artifact representation contracts.

use std::path::Path;

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{
    ArtifactPortableExporter, ArtifactReconciler, BlobStore, ConsumerExportParser,
};

const ARTIFACT_V1_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_artifact_v1.json");
const ARTIFACT_V2_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_artifact_v2.json");
const UNKNOWN_ARTIFACT_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_unknown_artifact.json");

#[test]
fn unknown_artifact_type_is_unrenderable_without_a_derivative() {
    let root = temp_root("portable-unknown-artifact");
    let store = BlobStore::open(&root).expect("the test store opens");
    let parsed = ConsumerExportParser::parse(UNKNOWN_ARTIFACT_EXPORT.as_bytes())
        .expect("the unknown Artifact fixture parses");
    let artifacts = ArtifactReconciler::new(&store)
        .reconcile(&[parsed])
        .expect("the unknown Artifact reconciles");

    let portable = ArtifactPortableExporter::new(&store)
        .represent(&artifacts)
        .expect("portable representation succeeds");
    assert_eq!(portable.files().len(), 2);
    assert!(
        !portable
            .files()
            .keys()
            .any(|path| has_text_derivative(path)),
        "an unknown Artifact type must not gain a rendered derivative"
    );
    let version_record = portable
        .files()
        .iter()
        .find_map(|(path, bytes)| {
            path.contains("versions/")
                .then(|| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                .flatten()
        })
        .expect("portable output contains a version JSON record");
    assert_eq!(
        version_record.pointer("/rendering/status"),
        Some(&serde_json::json!("unrenderable"))
    );
    assert_eq!(
        version_record.pointer("/rendering/reason"),
        Some(&serde_json::json!("unsupported_artifact_type"))
    );
    let artifact_record = portable
        .files()
        .iter()
        .find_map(|(path, bytes)| {
            path.ends_with("/artifact.json")
                .then(|| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                .flatten()
        })
        .expect("portable output contains an Artifact JSON record");
    assert_eq!(
        artifact_record.pointer("/raw/type"),
        Some(&serde_json::json!("provider/canvas")),
        "portable unknown Artifact evidence must retain the original provider record"
    );

    remove(&root);
}

fn has_text_derivative(path: &str) -> bool {
    Path::new(path).extension().is_some_and(|extension| {
        extension.eq_ignore_ascii_case("md") || extension.eq_ignore_ascii_case("txt")
    })
}

#[test]
fn portable_artifact_output_is_byte_deterministic() {
    let root = temp_root("portable-artifact-determinism");
    let store = BlobStore::open(&root).expect("the test store opens");
    let first = ConsumerExportParser::parse(ARTIFACT_V1_EXPORT.as_bytes())
        .expect("the first Artifact observation parses");
    let second = ConsumerExportParser::parse(ARTIFACT_V2_EXPORT.as_bytes())
        .expect("the second Artifact observation parses");
    let artifacts = ArtifactReconciler::new(&store)
        .reconcile(&[first, second])
        .expect("the Artifact chain reconciles");
    let exporter = ArtifactPortableExporter::new(&store);

    let first_output = exporter
        .represent(&artifacts)
        .expect("the first portable representation succeeds");
    let second_output = exporter
        .represent(&artifacts)
        .expect("the second portable representation succeeds");

    assert_eq!(first_output.files().len(), 5);
    assert_eq!(first_output, second_output);
    remove(&root);
}
