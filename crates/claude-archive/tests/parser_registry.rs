//! Exact selection outcomes for versioned parser declarations.
use ratatoskr_claude_archive::{
    AcquisitionMode, DetectedSchema, ParsedExport, ParserCapability, ParserDescriptor,
    ParserExecutionError, ParserExecutionInput, ParserExecutor, ParserIdentity, ParserRegistry,
    ParserRegistryError, ParserSelectionError, ParserStamp,
};
use std::sync::Arc;

fn consumer_descriptor(capabilities: Vec<ParserCapability>) -> ParserDescriptor {
    ParserDescriptor::new(
        "claude-personal-export",
        "2026-08-26",
        vec![AcquisitionMode::ConsumerExport],
        vec!["claude-export-2026-08".to_owned()],
        capabilities,
    )
}

fn versioned_descriptor(version: &str) -> ParserDescriptor {
    ParserDescriptor::new(
        "claude-personal-export",
        version,
        vec![AcquisitionMode::ConsumerExport],
        vec!["claude-export-2026-08".to_owned()],
        vec![ParserCapability::ArchiveStructure],
    )
}

#[derive(Debug)]
struct EmptyParser {
    version: String,
}

impl ParserExecutor for EmptyParser {
    fn execute(
        &self,
        input: ParserExecutionInput<'_>,
    ) -> Result<ParsedExport, ParserExecutionError> {
        Ok(ParsedExport {
            parser: ParserStamp {
                schema_identifier: input.detected_schema.identifier().to_owned(),
                parser_identifier: "claude-personal-export".to_owned(),
                parser_version: self.version.clone(),
            },
            ..ParsedExport::default()
        })
    }
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

#[derive(Debug, PartialEq, Eq)]
struct VersionRegistryObservation {
    compatible_versions: Vec<String>,
    exact_execution_version: Option<String>,
    ordinary_selection_is_ambiguous: bool,
}

fn observe_version_registry(registry: &ParserRegistry) -> VersionRegistryObservation {
    let detected = DetectedSchema::new(AcquisitionMode::ConsumerExport, "claude-export-2026-08");
    let capabilities = [ParserCapability::ArchiveStructure];
    let compatible_versions = registry
        .compatible_versions(&detected, &capabilities)
        .into_iter()
        .map(|identity| identity.version().to_owned())
        .collect();
    let exact_execution_version = registry
        .find_exact(
            &ParserIdentity::new("claude-personal-export", "1.10"),
            &detected,
            &capabilities,
        )
        .and_then(|parser| {
            parser
                .execute(ParserExecutionInput {
                    detected_schema: &detected,
                    evidence: b"{}",
                })
                .ok()
        })
        .map(|parsed| parsed.parser.parser_version);
    let ordinary_selection_is_ambiguous = matches!(
        registry.select(&detected, &capabilities),
        Err(ParserSelectionError::AmbiguousDeclaration)
    );

    VersionRegistryObservation {
        compatible_versions,
        exact_execution_version,
        ordinary_selection_is_ambiguous,
    }
}

#[test]
fn compatible_versions_and_exact_lookup_are_deterministic() {
    let mut forward = ParserRegistry::default();
    let mut reverse = ParserRegistry::default();
    for version in ["1.2", "1.10", "2.0"] {
        forward
            .register_compiled(
                versioned_descriptor(version),
                Arc::new(EmptyParser {
                    version: version.to_owned(),
                }),
            )
            .expect("each exact parser identity is unique");
    }
    for version in ["2.0", "1.10", "1.2"] {
        reverse
            .register_compiled(
                versioned_descriptor(version),
                Arc::new(EmptyParser {
                    version: version.to_owned(),
                }),
            )
            .expect("each exact parser identity is unique");
    }

    let expected = VersionRegistryObservation {
        compatible_versions: vec!["1.2".to_owned(), "1.10".to_owned(), "2.0".to_owned()],
        exact_execution_version: Some("1.10".to_owned()),
        ordinary_selection_is_ambiguous: true,
    };

    assert_eq!(observe_version_registry(&forward), expected);
    assert_eq!(observe_version_registry(&reverse), expected);
}
