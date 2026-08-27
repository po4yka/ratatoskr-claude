//! Artifact-version reconciliation contracts.

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{
    ArtifactReconciler, BlobStore, ConsumerExportParser, ReconciledArtifactVersion,
};

const ARTIFACT_V1_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_artifact_v1.json");
const ARTIFACT_V2_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_artifact_v2.json");

#[test]
fn reconciles_repeated_and_changed_artifact_versions_into_one_chain() {
    let first = ConsumerExportParser::parse(ARTIFACT_V1_EXPORT.as_bytes())
        .expect("the first Artifact observation parses");
    let repeated = ConsumerExportParser::parse(ARTIFACT_V1_EXPORT.as_bytes())
        .expect("the repeated Artifact observation parses");
    let changed = ConsumerExportParser::parse(ARTIFACT_V2_EXPORT.as_bytes())
        .expect("the changed Artifact observation parses");
    let root = temp_root("artifact-reconciliation");
    let store = BlobStore::open(&root).expect("the test store opens");

    let reconciled = ArtifactReconciler::new(&store)
        .reconcile(&[first, repeated, changed])
        .expect("the Artifact observations reconcile");

    assert_eq!(reconciled.len(), 1);
    assert_eq!(reconciled[0].external_id, "artifact-garden-plan");
    assert_eq!(reconciled[0].versions.len(), 2);
    assert_eq!(
        reconciled[0].versions[0].external_id,
        "artifact-garden-plan-v1"
    );
    assert_eq!(
        reconciled[0].versions[1].previous_external_id.as_deref(),
        Some("artifact-garden-plan-v1")
    );
    assert!(
        reconciled[0]
            .versions
            .iter()
            .all(ReconciledArtifactVersion::is_verified)
    );

    remove(&root);
}
