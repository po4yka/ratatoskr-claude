//! Completeness-reporting contracts for synthetic archive evidence.

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::{
    ArchiveCompletenessReport, AuthorizationStatus, BackupStatusLedger, BlobStore,
    CompletenessCounts, CompletenessStatus, ConsumerExportParser, CumulativeCompletenessReport,
    ExternalReference, ExternalReferenceKind, LocalEvidence, ProjectKnowledgeIngestor,
};

const SYNTHETIC_EXPORT: &str = include_str!("fixtures/synthetic_consumer_export.json");

#[test]
fn reports_fixture_counts_gaps_and_cumulative_math() {
    let root = temp_root("completeness-report");
    let store = BlobStore::open(&root).expect("the test store opens");
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");
    let ingestor = ProjectKnowledgeIngestor::new(&store);
    let outcomes = parsed
        .project_knowledge_files
        .iter()
        .map(|file| {
            ingestor
                .ingest(file)
                .expect("fixture file ingestion succeeds")
        })
        .collect::<Vec<_>>();

    let report = ArchiveCompletenessReport::from_ingest("archive-fixture", &parsed, &outcomes);

    assert_eq!(report.counts.projects, 2);
    assert_eq!(report.counts.project_instructions, 1);
    assert_eq!(report.counts.knowledge_references, 3);
    assert_eq!(report.counts.verified_knowledge_files, 1);
    assert_eq!(report.counts.missing_knowledge_files, 1);
    assert_eq!(report.counts.quarantined_knowledge_files, 1);
    assert_eq!(report.status, CompletenessStatus::AssetsPartial);
    assert_eq!(
        report
            .warnings
            .iter()
            .map(|warning| &warning.code)
            .collect::<Vec<_>>(),
        [
            "knowledge_file_missing_bytes",
            "knowledge_file_digest_mismatch"
        ]
    );

    let cumulative = CumulativeCompletenessReport::from_reports(&[report.clone(), report]);
    assert_eq!(cumulative.archives, 2);
    assert_eq!(cumulative.counts.projects, 4);
    assert_eq!(cumulative.counts.verified_knowledge_files, 2);
    assert_eq!(cumulative.counts.missing_knowledge_files, 2);
    assert_eq!(cumulative.counts.quarantined_knowledge_files, 2);
    assert_eq!(cumulative.status, CompletenessStatus::AssetsPartial);
    assert_eq!(cumulative.warnings.len(), 4);
    remove(&root);
}

#[test]
fn backup_status_counts_ignore_expired_authorization() {
    let backed_up = ExternalReference::new(ExternalReferenceKind::Conversation, "conversation-1");
    let reference_only =
        ExternalReference::new(ExternalReferenceKind::Conversation, "conversation-2");
    let mut ledger = BackupStatusLedger::default();

    let _ = ledger.observe_evidence(&backed_up, LocalEvidence::Verified);
    let _ = ledger.observe_evidence(&reference_only, LocalEvidence::Missing);
    let _ = ledger.observe_authorization(&backed_up, AuthorizationStatus::Expired);
    let statuses = [
        ledger
            .status(&backed_up)
            .expect("verified evidence records a status"),
        ledger
            .status(&reference_only)
            .expect("missing evidence records a status"),
    ];

    let counts = CompletenessCounts::from_backup_statuses(statuses);
    assert_eq!(counts.locally_backed_up_entities, 1);
    assert_eq!(counts.reference_only_entities, 1);
}
