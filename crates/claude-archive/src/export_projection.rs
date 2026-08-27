//! Conservative projection types for the documented synthetic consumer export.

use serde::Serialize;
use serde_json::{Map, Value};

use crate::parser_registry::{ParserCapability, ParserDescriptor};
use crate::receipt::AcquisitionMode;

const SYNTHETIC_SCHEMA_IDENTIFIER: &str = "claude-export-2026-08-synthetic";
const PARSER_IDENTIFIER: &str = "claude-synthetic-consumer-export";
const PARSER_VERSION: &str = "2026-08-27";

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
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// An instruction observed for a project.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectInstruction {
    /// Provider identifier of the project that owns this instruction.
    pub project_external_id: String,
    /// Instruction text exactly as observed.
    pub text: String,
    /// Parser provenance.
    pub parser: ParserStamp,
}

/// A Project Knowledge file reference observed in an export.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectKnowledgeFile {
    /// Provider identifier of this knowledge file.
    pub external_id: String,
    /// Provider identifier of the project that owns the file.
    pub project_external_id: String,
    /// Provider filename retained as evidence.
    pub filename: String,
    /// Provider-declared plain media type.
    pub media_type: String,
    /// Provider-declared SHA-256 when observed.
    pub declared_sha256: Option<String>,
    /// Supplied bytes before their separate `BlobStore` ingest classification.
    pub bytes: Option<Vec<u8>>,
    /// JSON Pointer for the source record.
    pub location: String,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// A first-class Claude Artifact observation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Artifact {
    /// Provider Artifact identifier.
    pub external_id: String,
    /// Provider-declared Artifact type.
    pub artifact_type: String,
    /// Optional provider display title.
    pub title: Option<String>,
    /// Optional provider language identifier.
    pub language: Option<String>,
    /// Optional provider project relationship.
    pub project_external_id: Option<String>,
    /// Optional provider conversation relationship.
    pub conversation_external_id: Option<String>,
    /// Optional provider message relationship.
    pub message_external_id: Option<String>,
    /// Immutable version evidence in source order.
    pub versions: Vec<ArtifactVersion>,
    /// JSON Pointer for the source record.
    pub location: String,
    /// Inert original provider Artifact record.
    pub raw: Value,
    /// Parser provenance.
    pub parser: ParserStamp,
    /// Unrecognized provider fields retained as evidence.
    pub unknown_fields: Vec<UnknownField>,
}

/// One immutable version observation of an Artifact.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArtifactVersion {
    /// Provider version identifier.
    pub external_id: String,
    /// Optional provider identifier for the predecessor version.
    pub previous_external_id: Option<String>,
    /// Provider-declared media type of the available payload.
    pub media_type: String,
    /// Provider-declared SHA-256 when observed.
    pub declared_sha256: Option<String>,
    /// Supplied payload bytes before their separate `BlobStore` ingest classification.
    pub bytes: Option<Vec<u8>>,
    /// JSON Pointer for the source record.
    pub location: String,
    /// Inert original provider Artifact-version record.
    pub raw: Value,
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
    /// Project instructions in source order.
    pub project_instructions: Vec<ProjectInstruction>,
    /// Project Knowledge references in source order.
    pub project_knowledge_files: Vec<ProjectKnowledgeFile>,
    /// Conversation observations in source order.
    pub conversations: Vec<Conversation>,
    /// Artifact observations in source order.
    pub artifacts: Vec<Artifact>,
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
                ParserCapability::ProjectInstructions,
                ParserCapability::ProjectKnowledgeFiles,
                ParserCapability::Conversations,
                ParserCapability::Messages,
                ParserCapability::ContentParts,
                ParserCapability::Artifacts,
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
        let mut project_instructions = Vec::new();
        let mut project_knowledge_files = Vec::new();
        for (index, project) in required_array(root, "projects", "")?.iter().enumerate() {
            let parsed_project = parse_project(project, index, &parser)?;
            if let Some(instruction) = parsed_project.instruction {
                project_instructions.push(instruction);
            }
            project_knowledge_files.extend(parsed_project.knowledge_files);
            projects.push(parsed_project.project);
        }

        let mut conversations = Vec::new();
        for (index, conversation) in required_array(root, "conversations", "")?
            .iter()
            .enumerate()
        {
            conversations.push(parse_conversation(conversation, index, &parser)?);
        }

        let mut artifacts = Vec::new();
        if let Some(records) = optional_array(root, "artifacts", "")? {
            for (index, artifact) in records.iter().enumerate() {
                artifacts.push(parse_artifact(artifact, index, &parser)?);
            }
        }

        Ok(ParsedExport {
            parser,
            projects,
            project_instructions,
            project_knowledge_files,
            conversations,
            artifacts,
            unknown_fields: unknown_fields(
                root,
                &["schema", "projects", "conversations", "artifacts"],
                "",
            ),
        })
    }
}

struct ParsedProject {
    project: Project,
    instruction: Option<ProjectInstruction>,
    knowledge_files: Vec<ProjectKnowledgeFile>,
}

fn parse_project(
    value: &Value,
    index: usize,
    parser: &ParserStamp,
) -> Result<ParsedProject, ExportParseError> {
    let location = format!("/projects/{index}");
    let object = object_at(value, &location)?;
    let external_id = required_string(object, "id", &location)?.to_owned();
    let instruction =
        optional_string(object, "instructions", &location)?.map(|text| ProjectInstruction {
            project_external_id: external_id.clone(),
            text,
            parser: parser.clone(),
        });
    let mut knowledge_files = Vec::new();
    if let Some(files) = optional_array(object, "knowledge_files", &location)? {
        for (file_index, file) in files.iter().enumerate() {
            knowledge_files.push(parse_knowledge_file(
                file,
                &location,
                file_index,
                &external_id,
                parser,
            )?);
        }
    }

    Ok(ParsedProject {
        project: Project {
            external_id,
            name: required_string(object, "name", &location)?.to_owned(),
            description: optional_string(object, "description", &location)?,
            parser: parser.clone(),
            unknown_fields: unknown_fields(
                object,
                &[
                    "id",
                    "name",
                    "description",
                    "instructions",
                    "knowledge_files",
                ],
                &location,
            ),
        },
        instruction,
        knowledge_files,
    })
}

fn parse_knowledge_file(
    value: &Value,
    project_location: &str,
    index: usize,
    project_external_id: &str,
    parser: &ParserStamp,
) -> Result<ProjectKnowledgeFile, ExportParseError> {
    let location = format!("{project_location}/knowledge_files/{index}");
    let object = object_at(value, &location)?;
    Ok(ProjectKnowledgeFile {
        external_id: required_string(object, "id", &location)?.to_owned(),
        project_external_id: project_external_id.to_owned(),
        filename: required_string(object, "filename", &location)?.to_owned(),
        media_type: required_string(object, "media_type", &location)?.to_owned(),
        declared_sha256: optional_string(object, "sha256", &location)?,
        bytes: optional_bytes(object, "bytes", &location)?,
        location: location.clone(),
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &["id", "filename", "media_type", "sha256", "bytes"],
            &location,
        ),
    })
}

fn parse_artifact(
    value: &Value,
    index: usize,
    parser: &ParserStamp,
) -> Result<Artifact, ExportParseError> {
    let location = format!("/artifacts/{index}");
    let object = object_at(value, &location)?;
    let mut versions = Vec::new();
    for (version_index, version) in required_array(object, "versions", &location)?
        .iter()
        .enumerate()
    {
        versions.push(parse_artifact_version(
            version,
            &location,
            version_index,
            parser,
        )?);
    }

    Ok(Artifact {
        external_id: required_string(object, "id", &location)?.to_owned(),
        artifact_type: required_string(object, "type", &location)?.to_owned(),
        title: optional_string(object, "title", &location)?,
        language: optional_string(object, "language", &location)?,
        project_external_id: optional_string(object, "project_id", &location)?,
        conversation_external_id: optional_string(object, "conversation_id", &location)?,
        message_external_id: optional_string(object, "message_id", &location)?,
        versions,
        location: location.clone(),
        raw: value.clone(),
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &[
                "id",
                "type",
                "title",
                "language",
                "project_id",
                "conversation_id",
                "message_id",
                "versions",
            ],
            &location,
        ),
    })
}

fn parse_artifact_version(
    value: &Value,
    artifact_location: &str,
    index: usize,
    parser: &ParserStamp,
) -> Result<ArtifactVersion, ExportParseError> {
    let location = format!("{artifact_location}/versions/{index}");
    let object = object_at(value, &location)?;
    Ok(ArtifactVersion {
        external_id: required_string(object, "id", &location)?.to_owned(),
        previous_external_id: optional_string(object, "previous_version_id", &location)?,
        media_type: required_string(object, "media_type", &location)?.to_owned(),
        declared_sha256: optional_string(object, "sha256", &location)?,
        bytes: optional_bytes(object, "bytes", &location)?,
        location: location.clone(),
        raw: value.clone(),
        parser: parser.clone(),
        unknown_fields: unknown_fields(
            object,
            &["id", "previous_version_id", "media_type", "sha256", "bytes"],
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

fn optional_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    location: &str,
) -> Result<Option<&'a [Value]>, ExportParseError> {
    match object.get(key) {
        Some(value) => value
            .as_array()
            .map(Vec::as_slice)
            .map(Some)
            .ok_or_else(|| invalid(&format!("{location}/{key}"), "an array is required")),
        None => Ok(None),
    }
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

fn optional_bytes(
    object: &Map<String, Value>,
    key: &str,
    location: &str,
) -> Result<Option<Vec<u8>>, ExportParseError> {
    let Some(values) = optional_array(object, key, location)? else {
        return Ok(None);
    };
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_u64()
                .and_then(|number| u8::try_from(number).ok())
                .ok_or_else(|| {
                    invalid(
                        &format!("{location}/{key}/{index}"),
                        "an unsigned byte is required",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn invalid(location: &str, reason: &str) -> ExportParseError {
    ExportParseError::InvalidStructure {
        location: location.to_owned(),
        reason: reason.to_owned(),
    }
}
