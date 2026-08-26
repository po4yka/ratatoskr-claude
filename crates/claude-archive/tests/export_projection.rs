//! Deterministic contract tests for the synthetic consumer-export parser.

use ratatoskr_claude_archive::{
    AcquisitionMode, ConsumerExportParser, ContentPart, DetectedSchema, ExportParseError,
    ParserCapability, ParserRegistry, ParserStamp,
};

const SYNTHETIC_EXPORT: &str = include_str!("fixtures/synthetic_consumer_export.json");
const UNKNOWN_FIELDS_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_unknown_fields.json");
const UNKNOWN_CONTENT_EXPORT: &str =
    include_str!("fixtures/synthetic_consumer_export_unknown_content.json");
const GOLDEN_PROJECTION: &str = include_str!("golden/synthetic_consumer_export_projection.json");

#[test]
fn maps_all_supported_records_and_relationships() {
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");

    assert_eq!(parsed.projects.len(), 2);
    assert_eq!(parsed.projects[0].external_id, "project-garden");
    assert_eq!(parsed.projects[0].name, "Garden plans");
    assert_eq!(
        parsed.projects[0].description.as_deref(),
        Some("Seasonal notes")
    );
    assert_eq!(
        parsed.projects[0].instructions.as_deref(),
        Some("Use metric units.")
    );
    assert_eq!(parsed.projects[1].external_id, "project-recipes");
    assert_eq!(parsed.conversations.len(), 1);

    let conversation = &parsed.conversations[0];
    assert_eq!(conversation.external_id, "conversation-planting");
    assert_eq!(
        conversation.project_external_id.as_deref(),
        Some("project-garden")
    );
    assert_eq!(conversation.messages.len(), 2);
    assert_eq!(conversation.messages[1].external_id, "message-assistant-1");
    assert_eq!(
        conversation.messages[1].parent_external_id.as_deref(),
        Some("message-user-1")
    );
    assert_eq!(conversation.messages[1].role, "assistant");
    assert_eq!(
        conversation.messages[1].model.as_deref(),
        Some("claude-synthetic")
    );
    assert!(matches!(
        conversation.messages[0].content.as_slice(),
        [ContentPart::Text { text, .. }, ContentPart::Markdown { markdown, .. }]
            if text == "When should basil be planted?" && markdown == "## Climate\nWarm season"
    ));
}

#[test]
fn stamps_every_projection_record_with_parser_provenance() {
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");

    assert_stamp(&parsed.parser);
    for project in &parsed.projects {
        assert_stamp(&project.parser);
    }
    for conversation in &parsed.conversations {
        assert_stamp(&conversation.parser);
        for message in &conversation.messages {
            assert_stamp(&message.parser);
            for part in &message.content {
                assert_stamp(content_stamp(part));
            }
        }
    }
}

#[test]
fn declares_exact_consumer_export_parser_capabilities() {
    let registry = ParserRegistry::new(vec![ConsumerExportParser::descriptor()])
        .expect("one parser declaration is unambiguous");
    let detected = DetectedSchema::new(
        AcquisitionMode::ConsumerExport,
        "claude-export-2026-08-synthetic",
    );

    let selected = registry
        .select(
            &detected,
            &[
                ParserCapability::Projects,
                ParserCapability::Conversations,
                ParserCapability::Messages,
                ParserCapability::ContentParts,
            ],
        )
        .expect("the projection parser declares every implemented capability");

    assert_eq!(selected.identifier(), "claude-synthetic-consumer-export");
    assert_eq!(selected.version(), "2026-08-26");
}

#[test]
fn preserves_unknown_fields() {
    let parsed = ConsumerExportParser::parse(UNKNOWN_FIELDS_EXPORT.as_bytes())
        .expect("unknown fields do not make the supported fixture fail");

    assert_unknown_field(
        &parsed.unknown_fields,
        "",
        "future_root",
        &serde_json::json!({ "edition": 2 }),
    );
    assert_unknown_field(
        &parsed.projects[0].unknown_fields,
        "/projects/0",
        "future_project",
        &serde_json::json!({ "region": "west" }),
    );
    assert_unknown_field(
        &parsed.conversations[0].unknown_fields,
        "/conversations/0",
        "future_conversation",
        &serde_json::json!(42),
    );
    assert_unknown_field(
        &parsed.conversations[0].messages[0].unknown_fields,
        "/conversations/0/messages/0",
        "future_message",
        &serde_json::json!("retain me"),
    );
}

#[test]
fn preserves_unknown_content_variants() {
    let parsed = ConsumerExportParser::parse(UNKNOWN_CONTENT_EXPORT.as_bytes())
        .expect("a future content variant remains retained evidence");

    assert!(matches!(
        parsed.conversations[0].messages[0].content.as_slice(),
        [ContentPart::Unknown { raw, .. }]
            if raw == &serde_json::json!({
                "type": "provider_future_content",
                "reference": "asset-42",
                "provider_flag": true
            })
    ));
}

#[test]
fn parses_identical_bytes_deterministically() {
    let first = ConsumerExportParser::parse(UNKNOWN_FIELDS_EXPORT.as_bytes())
        .expect("the first parse succeeds");
    let second = ConsumerExportParser::parse(UNKNOWN_FIELDS_EXPORT.as_bytes())
        .expect("the second parse succeeds");

    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_string(&first).expect("the first projection serializes"),
        serde_json::to_string(&second).expect("the second projection serializes")
    );
}

#[test]
fn refuses_missing_required_message_identifier() {
    let error = ConsumerExportParser::parse(
        br#"{
            "schema": "claude-export-2026-08-synthetic",
            "projects": [],
            "conversations": [{
                "id": "conversation-missing-message-id",
                "title": "Missing identifier",
                "messages": [{ "role": "user", "content": [] }]
            }]
        }"#,
    )
    .expect_err("a message identifier is required");

    assert_eq!(
        error,
        ExportParseError::InvalidStructure {
            location: "/conversations/0/messages/0/id".to_owned(),
            reason: "a string is required".to_owned(),
        }
    );
}

#[test]
fn maps_synthetic_fixture_to_read_only_golden() {
    let parsed = ConsumerExportParser::parse(SYNTHETIC_EXPORT.as_bytes())
        .expect("the documented synthetic fixture parses");
    let actual = serde_json::to_string_pretty(&parsed).expect("the projection serializes");

    assert_eq!(actual, GOLDEN_PROJECTION.trim_end());
}

fn assert_stamp(stamp: &ParserStamp) {
    assert_eq!(stamp.schema_identifier, "claude-export-2026-08-synthetic");
    assert_eq!(stamp.parser_identifier, "claude-synthetic-consumer-export");
    assert_eq!(stamp.parser_version, "2026-08-26");
}

fn content_stamp(part: &ContentPart) -> &ParserStamp {
    match part {
        ContentPart::Text { parser, .. }
        | ContentPart::Markdown { parser, .. }
        | ContentPart::Unknown { parser, .. } => parser,
    }
}

fn assert_unknown_field(
    fields: &[ratatoskr_claude_archive::UnknownField],
    location: &str,
    name: &str,
    value: &serde_json::Value,
) {
    assert!(fields.iter().any(|field| {
        field.location == location && field.name == name && &field.value == value
    }));
}
