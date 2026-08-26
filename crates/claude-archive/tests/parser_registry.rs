//! Exact selection outcomes for versioned parser declarations.
use ratatoskr_claude_archive::{
    AcquisitionMode, DetectedSchema, ParserCapability, ParserDescriptor, ParserRegistry,
    ParserRegistryError, ParserSelectionError,
};

fn consumer_descriptor(capabilities: Vec<ParserCapability>) -> ParserDescriptor {
    ParserDescriptor::new(
        "claude-personal-export",
        "2026-08-26",
        vec![AcquisitionMode::ConsumerExport],
        vec!["claude-export-2026-08".to_owned()],
        capabilities,
    )
}

#[test]
fn registry_selects_exact_schema_mode_and_capability_match() {
    let registry = ParserRegistry::new(vec![consumer_descriptor(vec![
        ParserCapability::ArchiveStructure,
        ParserCapability::Projects,
    ])])
    .expect("one declaration is unambiguous");
    let detected = DetectedSchema::new(AcquisitionMode::ConsumerExport, "claude-export-2026-08");

    let selected = registry
        .select(&detected, &[ParserCapability::Projects])
        .expect("the exact declared parser selects");

    assert_eq!(selected.identifier(), "claude-personal-export");
    assert_eq!(selected.version(), "2026-08-26");
}

#[test]
fn registry_reports_unsupported_detected_schema_version() {
    let registry = ParserRegistry::new(vec![consumer_descriptor(vec![
        ParserCapability::ArchiveStructure,
    ])])
    .expect("one declaration is unambiguous");
    let detected = DetectedSchema::new(AcquisitionMode::ConsumerExport, "claude-export-2099-01");

    let error = registry
        .select(&detected, &[ParserCapability::ArchiveStructure])
        .expect_err("a future detected schema must not select a nearby parser");

    assert_eq!(error, ParserSelectionError::UnsupportedSchema);
}

#[test]
fn registry_reports_missing_capability_without_fallback() {
    let registry = ParserRegistry::new(vec![consumer_descriptor(vec![
        ParserCapability::ArchiveStructure,
    ])])
    .expect("one declaration is unambiguous");
    let detected = DetectedSchema::new(AcquisitionMode::ConsumerExport, "claude-export-2026-08");

    let error = registry
        .select(&detected, &[ParserCapability::Projects])
        .expect_err("a parser missing Projects must not be selected");

    assert_eq!(error, ParserSelectionError::UnsupportedCapabilities);
}

#[test]
fn registry_refuses_overlapping_parser_declarations() {
    let first = consumer_descriptor(vec![ParserCapability::ArchiveStructure]);
    let second = consumer_descriptor(vec![ParserCapability::Projects]);

    let error = ParserRegistry::new(vec![first, second])
        .expect_err("overlapping mode and schema declarations are ambiguous");

    assert_eq!(error, ParserRegistryError::OverlappingDeclaration);
}
