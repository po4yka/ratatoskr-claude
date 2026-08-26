//! Conservative projection types for the documented synthetic consumer export.

use serde::Serialize;
use serde_json::{Map, Value};

use crate::parser_registry::{ParserCapability, ParserDescriptor};
use crate::receipt::AcquisitionMode;

const SYNTHETIC_SCHEMA_IDENTIFIER: &str = "claude-export-2026-08-synthetic";
const PARSER_IDENTIFIER: &str = "claude-synthetic-consumer-export";
const PARSER_VERSION: &str = "2026-08-26";

/// Parser provenance carried by a normalized record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ParserStamp {
    /// Detected source schema identifier.
    pub schema_identifier: String,
    /// Stable parser identifier.
    pub parser_identifier: String,
    /// Parser implementation version.
    pub parser_version: String,
}

/// A provider field that no current parser rule understands.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnknownField {
    /// JSON Pointer for the object containing the unrecognized field.
    pub location: String,
    /// Original provider field name.
    pub name: String,
    /// Inert original provider value.
    pub value: Value,
}

/// A normalized project observation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Project {
    /// Provider project identifier.
    pub external_id: String,
    /// Project display name.
    pub name: String,
    /// Optional project description.
    pub description: Option<String>,
    /// Optional project instructions.
    pub instructions: Option<String>,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// A normalized conversation observation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Conversation {
    /// Provider conversation identifier.
    pub external_id: String,
    /// Optional provider project relationship.
    pub project_external_id: Option<String>,
    /// Conversation title.
    pub title: String,
    /// Provider creation timestamp text when observed.
    pub created_at: Option<String>,
    /// Ordered message observations.
    pub messages: Vec<Message>,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// A normalized message node.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Message {
    /// Provider message identifier.
    pub external_id: String,
    /// Optional provider parent message identifier.
    pub parent_external_id: Option<String>,
    /// Provider-declared message role.
    pub role: String,
    /// Optional model identifier.
    pub model: Option<String>,
    /// Provider creation timestamp text when observed.
    pub created_at: Option<String>,
    /// Ordered heterogeneous content parts.
    pub content: Vec<ContentPart>,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// An ordered normalized message content part.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContentPart {
    /// Plain text content.
    Text {
        /// Text body.
        text: String,
        /// Parser provenance.
        parser: ParserStamp,
        /// Unrecognized provider fields retained as evidence.
        unknown_fields: Vec<UnknownField>,
    },
    /// Markdown content.
    Markdown {
        /// Markdown body.
        markdown: String,
        /// Parser provenance.
        parser: ParserStamp,
        /// Unrecognized provider fields retained as evidence.
        unknown_fields: Vec<UnknownField>,
    },
    /// A provider content variant that this parser cannot interpret.
    Unknown {
        /// Original content-part object.
        raw: Value,
        /// Parser provenance.
        parser: ParserStamp,
    },
}

/// A complete normalized synthetic export projection.
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct ParsedExport {
    /// Parser provenance for the record set.
    pub parser: ParserStamp,
    /// Project observations in source order.
    pub projects: Vec<Project>,
    /// Conversation observations in source order.
    pub conversations: Vec<Conversation>,
    /// Unrecognized root fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// A malformed or unsupported synthetic export.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExportParseError {
    /// The document could not be decoded as JSON.
    #[error("the export is not valid JSON")]
    InvalidJson,
    /// A structural requirement was not met.
    #[error("invalid export structure at {location}: {reason}")]
    InvalidStructure {
        /// JSON Pointer for the invalid value.
        location: String,
        /// Stable validation reason.
        reason: String,
    },
}

/// The parser for the documented synthetic consumer-export schema.
#[derive(Debug, Default)]
pub struct ConsumerExportParser;

impl ConsumerExportParser {
    /// Declares the parser's exact supported source boundary.
    #[must_use]
    pub fn descriptor() -> ParserDescriptor {
        ParserDescriptor::new(
            PARSER_IDENTIFIER,
            PARSER_VERSION,
            vec![AcquisitionMode::ConsumerExport],
            vec![SYNTHETIC_SCHEMA_IDENTIFIER.to_owned()],
            vec![
                ParserCapability::Projects,
                ParserCapability::Conversations,
                ParserCapability::Messages,
                ParserCapability::ContentParts,
            ],
        )
    }

    /// Parses a synthetic export into normalized records.
    ///
    /// # Errors
    ///
    /// Returns [`ExportParseError`] when the input cannot be parsed.
    pub fn parse(input: &[u8]) -> Result<ParsedExport, ExportParseError> {
        let value = serde_json::from_slice(input).map_err(|_| ExportParseError::InvalidJson)?;
        let root = object_at(&value, "")?;
        let schema = required_string(root, "schema", "")?;
        if schema != SYNTHETIC_SCHEMA_IDENTIFIER {
            return Err(invalid("/schema", "the schema identifier is unsupported"));
        }
        let parser = parser_stamp();

        let mut projects = Vec::new();
        for (index, project) in required_array(root, "projects", "")?.iter().enumerate() {
            projects.push(parse_project(project, index, &parser)?);
        }

        let mut conversations = Vec::new();
        for (index, conversation) in required_array(root, "conversations", "")?
            .iter()
            .enumerate()
        {
            conversations.push(parse_conversation(conversation, index, &parser)?);
        }

        Ok(ParsedExport {
            parser,
            projects,
            conversations,
            unknown_fields: unknown_fields(root, &["schema", "projects", "conversations"], ""),
        })
    }
}

fn parse_project(
    value: &Value,
    index: usize,
    parser: &ParserStamp,
) -> Result<Project, ExportParseError> {
    let location = format!("/projects/{index}");
    let object = object_at(value, &location)?;
    Ok(Project {
        external_id: required_string(object, "id", &location)?.to_owned(),
        name: required_string(object, "name", &location)?.to_owned(),
        description: optional_string(object, "description", &location)?,
        instructions: optional_string(object, "instructions", &location)?,
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &["id", "name", "description", "instructions"],
            &location,
        ),
    })
}

fn parse_conversation(
    value: &Value,
    index: usize,
    parser: &ParserStamp,
) -> Result<Conversation, ExportParseError> {
    let location = format!("/conversations/{index}");
    let object = object_at(value, &location)?;
    let mut messages = Vec::new();
    for (message_index, message) in required_array(object, "messages", &location)?
        .iter()
        .enumerate()
    {
        messages.push(parse_message(message, &location, message_index, parser)?);
    }

    Ok(Conversation {
        external_id: required_string(object, "id", &location)?.to_owned(),
        project_external_id: optional_string(object, "project_id", &location)?,
        title: required_string(object, "title", &location)?.to_owned(),
        created_at: optional_string(object, "created_at", &location)?,
        messages,
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &["id", "project_id", "title", "created_at", "messages"],
            &location,
        ),
    })
}

fn parse_message(
    value: &Value,
    conversation_location: &str,
    index: usize,
    parser: &ParserStamp,
) -> Result<Message, ExportParseError> {
    let location = format!("{conversation_location}/messages/{index}");
    let object = object_at(value, &location)?;
    let mut content = Vec::new();
    for (content_index, part) in required_array(object, "content", &location)?
        .iter()
        .enumerate()
    {
        content.push(parse_content_part(part, &location, content_index, parser)?);
    }

    Ok(Message {
        external_id: required_string(object, "id", &location)?.to_owned(),
        parent_external_id: optional_string(object, "parent_id", &location)?,
        role: required_string(object, "role", &location)?.to_owned(),
        model: optional_string(object, "model", &location)?,
        created_at: optional_string(object, "created_at", &location)?,
        content,
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &["id", "parent_id", "role", "model", "created_at", "content"],
            &location,
        ),
    })
}

fn parse_content_part(
    value: &Value,
    message_location: &str,
    index: usize,
    parser: &ParserStamp,
) -> Result<ContentPart, ExportParseError> {
    let location = format!("{message_location}/content/{index}");
    let object = object_at(value, &location)?;
    match required_string(object, "type", &location)? {
        "text" => Ok(ContentPart::Text {
            text: required_string(object, "text", &location)?.to_owned(),
            parser: parser.clone(),
            unknown_fields: unknown_fields(object, &["type", "text"], &location),
        }),
        "markdown" => Ok(ContentPart::Markdown {
            markdown: required_string(object, "markdown", &location)?.to_owned(),
            parser: parser.clone(),
            unknown_fields: unknown_fields(object, &["type", "markdown"], &location),
        }),
        _ => Ok(ContentPart::Unknown {
            raw: value.clone(),
            parser: parser.clone(),
        }),
    }
}

fn parser_stamp() -> ParserStamp {
    ParserStamp {
        schema_identifier: SYNTHETIC_SCHEMA_IDENTIFIER.to_owned(),
        parser_identifier: PARSER_IDENTIFIER.to_owned(),
        parser_version: PARSER_VERSION.to_owned(),
    }
}

fn unknown_fields(
    object: &Map<String, Value>,
    known: &[&str],
    location: &str,
) -> Vec<UnknownField> {
    object
        .iter()
        .filter(|(name, _)| !known.contains(&name.as_str()))
        .map(|(name, value)| UnknownField {
            location: location.to_owned(),
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

fn object_at<'a>(
    value: &'a Value,
    location: &str,
) -> Result<&'a Map<String, Value>, ExportParseError> {
    value
        .as_object()
        .ok_or_else(|| invalid(location, "an object is required"))
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    location: &str,
) -> Result<&'a [Value], ExportParseError> {
    object
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| invalid(&format!("{location}/{key}"), "an array is required"))
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    location: &str,
) -> Result<&'a str, ExportParseError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(&format!("{location}/{key}"), "a string is required"))
}

fn optional_string(
    object: &Map<String, Value>,
    key: &str,
    location: &str,
) -> Result<Option<String>, ExportParseError> {
    match object.get(key) {
        Some(value) => value
            .as_str()
            .map(|text| Some(text.to_owned()))
            .ok_or_else(|| invalid(&format!("{location}/{key}"), "a string is required")),
        None => Ok(None),
    }
}

fn invalid(location: &str, reason: &str) -> ExportParseError {
    ExportParseError::InvalidStructure {
        location: location.to_owned(),
        reason: reason.to_owned(),
    }
}
