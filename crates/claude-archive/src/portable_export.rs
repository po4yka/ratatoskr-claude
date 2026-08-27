//! Portable archive export boundary.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::{Cursor, Write as _};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};
use sha2::Digest as _;
use zip::write::SimpleFileOptions;

use crate::{BlobRef, BlobStore};

/// Deterministic portable archive writer.
#[derive(Debug, Default)]
pub struct PortableArchiveExporter;

impl PortableArchiveExporter {
    /// Creates an exporter.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Produces a portable archive for one selected state.
    ///
    /// # Errors
    ///
    /// Returns [`PortableExportError`] when output cannot be produced.
    pub fn export_to_bytes(
        &self,
        state: &PortableArchiveState,
    ) -> Result<Vec<u8>, PortableExportError> {
        archive_bytes(state, normalized_members(state)?, None)
    }

    /// Produces a portable archive including selected verified asset bytes.
    ///
    /// # Errors
    ///
    /// Returns [`PortableExportError`] when output cannot be produced.
    pub fn export_to_bytes_with_assets(
        &self,
        state: &PortableArchiveState,
        blob_store: &BlobStore,
    ) -> Result<Vec<u8>, PortableExportError> {
        let mut members = normalized_members(state)?;
        for asset in verified_assets(state) {
            let reference = asset
                .blob
                .as_ref()
                .ok_or(PortableExportError::MissingVerifiedAssetReference)?;
            let bytes = blob_store.read(reference)?;
            members.push(Member::asset(asset, bytes));
        }
        archive_bytes(state, members, None)
    }

    /// Publishes a portable archive through an owned temporary sibling.
    ///
    /// # Errors
    ///
    /// Returns [`PortableExportError`] when assembly or atomic publication
    /// fails. A failed publication removes only its owned temporary file.
    pub fn export_to_path(
        &self,
        state: &PortableArchiveState,
        output: &Path,
    ) -> Result<(), PortableExportError> {
        let bytes = self.export_to_bytes(state)?;
        publish_bytes(output, &bytes)
    }

    /// Publishes a portable archive including selected verified asset bytes.
    ///
    /// # Errors
    ///
    /// Returns [`PortableExportError`] when assembly, asset verification, or
    /// atomic publication fails.
    pub fn export_to_path_with_assets(
        &self,
        state: &PortableArchiveState,
        blob_store: &BlobStore,
        output: &Path,
    ) -> Result<(), PortableExportError> {
        let bytes = self.export_to_bytes_with_assets(state, blob_store)?;
        publish_bytes(output, &bytes)
    }

    /// Produces an asset-aware portable archive from a selected state set.
    ///
    /// # Errors
    ///
    /// Returns [`PortableExportError`] when no state is available or output
    /// cannot be produced.
    pub fn export_selected_to_bytes_with_assets(
        &self,
        states: &[PortableArchiveState],
        filter: &PortableExportFilter,
        blob_store: &BlobStore,
    ) -> Result<Vec<u8>, PortableExportError> {
        let state = states
            .iter()
            .find(|state| state.account_external_ref == filter.account_external_ref)
            .ok_or(PortableExportError::EmptySelection)?;
        let selected = select_state(state, filter);
        let mut members = normalized_members(&selected)?;
        for asset in verified_assets(&selected) {
            let reference = asset
                .blob
                .as_ref()
                .ok_or(PortableExportError::MissingVerifiedAssetReference)?;
            members.push(Member::asset(asset, blob_store.read(reference)?));
        }
        archive_bytes(&selected, members, Some(filter))
    }
}

/// Export failure.
#[derive(Debug, thiserror::Error)]
pub enum PortableExportError {
    /// The filter selected no tenant state.
    #[error("the portable export filter selected no state")]
    EmptySelection,
    /// Two selected records claimed the same stable provider identity.
    #[error("duplicate portable {kind} identity: {external_id}")]
    DuplicateStableIdentity {
        /// Selected record category.
        kind: &'static str,
        /// Repeated stable provider identity.
        external_id: String,
    },
    /// A canonical member could not be encoded.
    #[error("portable archive JSON encoding failed")]
    Json(#[from] serde_json::Error),
    /// The ZIP stream could not be encoded.
    #[error("portable archive ZIP encoding failed")]
    Zip(#[from] zip::result::ZipError),
    /// The archive stream or destination could not be written.
    #[error("portable archive I/O failed")]
    Io(#[from] std::io::Error),
    /// A verified asset had no integrity-checkable local reference.
    #[error("a verified portable asset has no local reference")]
    MissingVerifiedAssetReference,
    /// A verified asset could not be read through the owned `BlobStore`.
    #[error("a verified portable asset is unavailable")]
    Blob(#[from] crate::StoreError),
    /// The destination has no parent directory.
    #[error("portable archive output path has no parent directory")]
    InvalidOutputPath,
}

/// Tenant-scoped normalized evidence for one export.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableArchiveState {
    /// Owning account external reference.
    pub account_external_ref: String,
    /// Immutable source provenance.
    pub provenance: PortableProvenance,
    /// Selected projects and their Project Knowledge.
    pub projects: Vec<PortableProject>,
    /// Selected conversations.
    pub conversations: Vec<PortableConversation>,
    /// Selected Artifacts and all observed versions.
    pub artifacts: Vec<PortableArtifact>,
    /// Selected asset references.
    pub assets: Vec<PortableAsset>,
}

/// Exact tenant, project, and inclusive observation predicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableExportFilter {
    /// Required authenticated account identity.
    pub account_external_ref: String,
    /// Optional exact provider project identity.
    pub project_external_id: Option<String>,
    /// Optional inclusive lower RFC 3339 observation bound.
    pub observed_from_rfc3339: Option<String>,
    /// Optional inclusive upper RFC 3339 observation bound.
    pub observed_to_rfc3339: Option<String>,
}

/// Non-sensitive source identity carried by every rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableProvenance {
    /// Stable source snapshot identities.
    pub source_snapshot_ids: Vec<String>,
    /// SHA-256 of the immutable raw archive.
    pub archive_sha256: String,
    /// Exact parser name.
    pub parser_name: String,
    /// Exact parser version.
    pub parser_version: String,
    /// Stable observation timestamp in RFC 3339 form.
    pub observed_at_rfc3339: String,
    /// Evidence-based completeness classification.
    pub completeness: String,
}

/// One selected normalized project and its Project Knowledge.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableProject {
    /// Provider project identity.
    pub external_id: String,
    /// Optional provider title, retained only as inert data.
    pub title: Option<String>,
    /// Stable observation timestamp in RFC 3339 form.
    pub observed_at_rfc3339: String,
    /// Normalized project evidence.
    pub payload: serde_json::Value,
    /// Project Knowledge entries related to this project.
    pub knowledge_sources: Vec<PortableKnowledgeSource>,
}

/// One selected Project Knowledge source.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableKnowledgeSource {
    /// Provider source identity.
    pub external_id: String,
    /// Provider source kind.
    pub source_kind: String,
    /// Optional inert provider title or filename.
    pub title: Option<String>,
    /// Evidence availability classification.
    pub availability: String,
    /// Normalized source evidence.
    pub payload: serde_json::Value,
}

/// One selected normalized conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableConversation {
    /// Provider conversation identity.
    pub external_id: String,
    /// Optional owning provider project identity.
    pub project_external_id: Option<String>,
    /// Optional provider title, retained only as inert data.
    pub title: Option<String>,
    /// Stable observation timestamp in RFC 3339 form.
    pub observed_at_rfc3339: String,
    /// Normalized graph payload.
    pub payload: serde_json::Value,
}

/// One selected first-class Artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableArtifact {
    /// Provider Artifact identity.
    pub external_id: String,
    /// Owning provider conversation identity when observed.
    pub conversation_external_id: Option<String>,
    /// Optional inert provider title.
    pub title: Option<String>,
    /// All observed Artifact versions.
    pub versions: Vec<PortableArtifactVersion>,
}

/// One immutable selected Artifact version.
#[derive(Debug, Clone, PartialEq)]
pub struct PortableArtifactVersion {
    /// Provider version identity.
    pub external_id: String,
    /// Previous provider version identity when observed.
    pub previous_external_id: Option<String>,
    /// Normalized version evidence.
    pub payload: serde_json::Value,
}

/// One selected asset reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableAsset {
    /// Provider asset identity.
    pub external_id: String,
    /// Linked provider project identity when observed.
    pub project_external_id: Option<String>,
    /// Stable observation timestamp in RFC 3339 form.
    pub observed_at_rfc3339: String,
    /// Evidence availability classification.
    pub availability: PortableAssetAvailability,
    /// Verified local bytes when available.
    pub blob: Option<BlobRef>,
    /// Observed media type.
    pub media_type: Option<String>,
}

/// Whether asset bytes are available for portable export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortableAssetAvailability {
    /// Bytes were verified and can be copied.
    Verified,
    /// The provider named an asset without locally archived bytes.
    Missing,
    /// Candidate bytes failed integrity or safety validation.
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Member {
    path: String,
    bytes: Vec<u8>,
    media_type: String,
    availability: &'static str,
}

impl Member {
    fn normalized(path: String, bytes: Vec<u8>, media_type: &str) -> Self {
        Self {
            path,
            bytes,
            media_type: media_type.to_owned(),
            availability: "normalized",
        }
    }

    fn asset(asset: &PortableAsset, bytes: Vec<u8>) -> Self {
        Self {
            path: format!("assets/{}", path_component(&asset.external_id)),
            bytes,
            media_type: asset
                .media_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_owned()),
            availability: "verified",
        }
    }
}

fn normalized_members(state: &PortableArchiveState) -> Result<Vec<Member>, PortableExportError> {
    let mut members = project_members(state)?;
    members.extend(artifact_members(state)?);
    let mut conversations = state.conversations.iter().collect::<Vec<_>>();
    conversations.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    for conversation in conversations {
        let value = json!({
            "provenance": provenance_value(&state.provenance),
            "conversation": {
                "external_id": conversation.external_id,
                "project_external_id": conversation.project_external_id,
                "title": conversation.title,
                "observed_at_rfc3339": conversation.observed_at_rfc3339,
                "payload": canonicalize(&conversation.payload),
            },
        });
        let component = path_component(&conversation.external_id);
        members.push(Member::normalized(
            format!("conversations/{component}.json"),
            canonical_json(&value)?,
            "application/json",
        ));
        members.push(Member::normalized(
            format!("conversations/{component}.md"),
            conversation_markdown(conversation, &state.provenance).into_bytes(),
            "text/markdown",
        ));
    }
    members.sort();
    Ok(members)
}

fn project_members(state: &PortableArchiveState) -> Result<Vec<Member>, PortableExportError> {
    let mut projects = state.projects.iter().collect::<Vec<_>>();
    projects.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    let mut members = Vec::new();
    for project in projects {
        let component = path_component(&project.external_id);
        members.push(Member::normalized(
            format!("projects/{component}.json"),
            canonical_json(&json!({
                "provenance": provenance_value(&state.provenance),
                "project": project_value(project),
            }))?,
            "application/json",
        ));
        members.push(Member::normalized(
            format!("projects/{component}.md"),
            project_markdown(project, &state.provenance).into_bytes(),
            "text/markdown",
        ));
        members.extend(knowledge_members(project, &state.provenance)?);
    }
    Ok(members)
}

fn knowledge_members(
    project: &PortableProject,
    provenance: &PortableProvenance,
) -> Result<Vec<Member>, PortableExportError> {
    let mut sources = project.knowledge_sources.iter().collect::<Vec<_>>();
    sources.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    sources
        .into_iter()
        .map(|source| {
            Ok(Member::normalized(
                format!("knowledge/{}.json", path_component(&source.external_id)),
                canonical_json(&json!({
                    "provenance": provenance_value(provenance),
                    "project_external_id": project.external_id,
                    "source": {
                        "external_id": source.external_id,
                        "source_kind": source.source_kind,
                        "title": source.title,
                        "availability": source.availability,
                        "payload": canonicalize(&source.payload),
                    },
                }))?,
                "application/json",
            ))
        })
        .collect()
}

fn artifact_members(state: &PortableArchiveState) -> Result<Vec<Member>, PortableExportError> {
    let mut artifacts = state.artifacts.iter().collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    let mut members = Vec::with_capacity(artifacts.len() * 2);
    for artifact in artifacts {
        let component = path_component(&artifact.external_id);
        members.push(Member::normalized(
            format!("artifacts/{component}.json"),
            canonical_json(&json!({
                "provenance": provenance_value(&state.provenance),
                "artifact": artifact_value(artifact),
            }))?,
            "application/json",
        ));
        members.push(Member::normalized(
            format!("artifacts/{component}.md"),
            artifact_markdown(artifact, &state.provenance).into_bytes(),
            "text/markdown",
        ));
    }
    Ok(members)
}

fn canonical_json(value: &Value) -> Result<Vec<u8>, PortableExportError> {
    let mut bytes = serde_json::to_vec(&canonicalize(value))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        Value::Object(items) => Value::Object(Map::from_iter(
            items
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize(value)))
                .collect::<BTreeMap<_, _>>(),
        )),
        scalar => scalar.clone(),
    }
}

fn provenance_value(provenance: &PortableProvenance) -> Value {
    let mut snapshots = provenance.source_snapshot_ids.clone();
    snapshots.sort();
    json!({
        "source_snapshot_ids": snapshots,
        "archive_sha256": provenance.archive_sha256,
        "parser_name": provenance.parser_name,
        "parser_version": provenance.parser_version,
        "observed_at_rfc3339": provenance.observed_at_rfc3339,
        "completeness": provenance.completeness,
    })
}

fn project_value(project: &PortableProject) -> Value {
    json!({
        "external_id": project.external_id,
        "title": project.title,
        "observed_at_rfc3339": project.observed_at_rfc3339,
        "payload": canonicalize(&project.payload),
    })
}

fn artifact_value(artifact: &PortableArtifact) -> Value {
    let mut versions = artifact.versions.iter().collect::<Vec<_>>();
    versions.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    json!({
        "external_id": artifact.external_id,
        "conversation_external_id": artifact.conversation_external_id,
        "title": artifact.title,
        "versions": versions.into_iter().map(|version| json!({
            "external_id": version.external_id,
            "previous_external_id": version.previous_external_id,
            "payload": canonicalize(&version.payload),
        })).collect::<Vec<_>>(),
    })
}

fn provenance_header(provenance: &PortableProvenance) -> String {
    format!(
        "<!-- provenance: archive-sha256={}; parser={}; parser-version={} -->\n\n",
        provenance.archive_sha256, provenance.parser_name, provenance.parser_version,
    )
}

fn project_markdown(project: &PortableProject, provenance: &PortableProvenance) -> String {
    let title = project
        .title
        .as_deref()
        .map_or_else(|| "Untitled project".to_owned(), escape_active_html);
    format!(
        "{}# {}\n\n`project_id`: `{}`\n\n`observed_at`: `{}`\n",
        provenance_header(provenance),
        title,
        escape_active_html(&project.external_id),
        project.observed_at_rfc3339,
    )
}

fn artifact_markdown(artifact: &PortableArtifact, provenance: &PortableProvenance) -> String {
    let title = artifact
        .title
        .as_deref()
        .map_or_else(|| "Untitled Artifact".to_owned(), escape_active_html);
    let mut versions = artifact.versions.iter().collect::<Vec<_>>();
    versions.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    let mut markdown = format!(
        "{}# {}\n\n`artifact_id`: `{}`\n",
        provenance_header(provenance),
        title,
        escape_active_html(&artifact.external_id),
    );
    for version in versions {
        markdown.push_str("\n## Version `");
        markdown.push_str(&escape_active_html(&version.external_id));
        markdown.push_str("`\n");
        if let Some(previous) = &version.previous_external_id {
            markdown.push_str("\nPrevious: `");
            markdown.push_str(&escape_active_html(previous));
            markdown.push_str("`\n");
        }
    }
    markdown
}

fn conversation_markdown(
    conversation: &PortableConversation,
    provenance: &PortableProvenance,
) -> String {
    let title = conversation
        .title
        .as_deref()
        .map_or_else(|| "Untitled conversation".to_owned(), escape_active_html);
    let mut markdown = format!(
        "{}# {}\n\n`conversation_id`: `{}`\n\n`observed_at`: `{}`\n",
        provenance_header(provenance),
        title,
        escape_active_html(&conversation.external_id),
        conversation.observed_at_rfc3339,
    );
    append_messages(&mut markdown, &conversation.payload);
    markdown
}

fn append_messages(markdown: &mut String, payload: &Value) {
    let Some(messages) = payload.get("messages").and_then(Value::as_array) else {
        return;
    };
    for message in messages {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        markdown.push_str("\n## ");
        markdown.push_str(&escape_active_html(role));
        markdown.push('\n');
        append_content(markdown, message);
    }
}

fn append_content(markdown: &mut String, message: &Value) {
    let parts = message
        .get("content")
        .or_else(|| message.get("parts"))
        .and_then(Value::as_array);
    let Some(parts) = parts else {
        return;
    };
    for part in parts {
        let text = part
            .as_str()
            .or_else(|| part.get("text").and_then(Value::as_str));
        if let Some(text) = text {
            markdown.push('\n');
            markdown.push_str(&escape_active_html(text));
            markdown.push('\n');
        } else {
            markdown.push_str("\n```json\n");
            markdown.push_str(&canonicalize(part).to_string());
            markdown.push_str("\n```\n");
        }
    }
}

fn escape_active_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn path_component(external_id: &str) -> String {
    let readable = external_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let digest = sha256_hex(external_id.as_bytes());
    let suffix = digest.chars().take(16).collect::<String>();
    format!("{readable}-{suffix}")
}

fn sha256_hex(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _write_result = write!(output, "{byte:02x}");
            output
        })
}

fn verified_assets(state: &PortableArchiveState) -> Vec<&PortableAsset> {
    let mut assets = state
        .assets
        .iter()
        .filter(|asset| asset.availability == PortableAssetAvailability::Verified)
        .collect::<Vec<_>>();
    assets.sort_by(|left, right| left.external_id.cmp(&right.external_id));
    assets
}

fn archive_bytes(
    state: &PortableArchiveState,
    members: Vec<Member>,
    filter: Option<&PortableExportFilter>,
) -> Result<Vec<u8>, PortableExportError> {
    let mut members = resolve_member_collisions(members);
    members.push(manifest_member(state, &members, filter)?);
    write_zip(members)
}

fn resolve_member_collisions(members: Vec<Member>) -> Vec<Member> {
    let mut groups = BTreeMap::<String, Vec<Member>>::new();
    for member in members {
        groups.entry(member.path.clone()).or_default().push(member);
    }
    let mut resolved = Vec::new();
    for mut group in groups.into_values() {
        group.sort();
        group.dedup();
        if group.len() > 1 {
            for member in &mut group {
                member.path = disambiguated_path(member);
            }
        }
        resolved.extend(group);
    }
    resolved.sort();
    resolved
}

fn disambiguated_path(member: &Member) -> String {
    let mut identity = member.bytes.clone();
    identity.extend_from_slice(member.media_type.as_bytes());
    identity.extend_from_slice(member.availability.as_bytes());
    let suffix = sha256_hex(&identity);
    let (stem, extension) = member
        .path
        .rsplit_once('.')
        .map_or((member.path.as_str(), None), |(stem, extension)| {
            (stem, Some(extension))
        });
    extension.map_or_else(
        || format!("{stem}-{suffix}"),
        |extension| format!("{stem}-{suffix}.{extension}"),
    )
}

fn manifest_member(
    state: &PortableArchiveState,
    members: &[Member],
    filter: Option<&PortableExportFilter>,
) -> Result<Member, PortableExportError> {
    let entries = members
        .iter()
        .map(|member| {
            json!({
                "path": member.path,
                "sha256": sha256_hex(&member.bytes),
                "byte_length": member.bytes.len(),
                "media_type": member.media_type,
                "availability": member.availability,
                "completeness": state.provenance.completeness,
                "provenance": provenance_value(&state.provenance),
            })
        })
        .collect::<Vec<_>>();
    let mut warnings = state
        .assets
        .iter()
        .filter(|asset| asset.availability != PortableAssetAvailability::Verified)
        .map(|asset| {
            json!({
                "code": "asset_unavailable",
                "asset_external_id": asset.external_id,
                "availability": asset.availability.as_str(),
            })
        })
        .collect::<Vec<_>>();
    warnings.sort_by_key(Value::to_string);
    let mut manifest = json!({
        "format": "ratatoskr-claude-portable-archive",
        "account_external_ref": state.account_external_ref,
        "provenance": provenance_value(&state.provenance),
        "members": entries,
        "warnings": warnings,
    });
    if let (Some(filter), Some(object)) = (filter, manifest.as_object_mut()) {
        object.insert("filters".to_owned(), filter_value(filter));
    }
    Ok(Member::normalized(
        "manifest.json".to_owned(),
        canonical_json(&manifest)?,
        "application/json",
    ))
}

fn filter_value(filter: &PortableExportFilter) -> Value {
    json!({
        "account_external_ref": filter.account_external_ref,
        "project_external_id": filter.project_external_id,
        "observed_from_rfc3339": filter.observed_from_rfc3339,
        "observed_to_rfc3339": filter.observed_to_rfc3339,
    })
}

fn select_state(
    state: &PortableArchiveState,
    filter: &PortableExportFilter,
) -> PortableArchiveState {
    let projects = state
        .projects
        .iter()
        .filter(|project| {
            matches_selection(
                Some(&project.external_id),
                &project.observed_at_rfc3339,
                filter,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let conversations = state
        .conversations
        .iter()
        .filter(|conversation| {
            matches_selection(
                conversation.project_external_id.as_ref(),
                &conversation.observed_at_rfc3339,
                filter,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let selected_conversations = conversations
        .iter()
        .map(|conversation| conversation.external_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    PortableArchiveState {
        account_external_ref: state.account_external_ref.clone(),
        provenance: state.provenance.clone(),
        projects,
        artifacts: state
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact
                    .conversation_external_id
                    .as_deref()
                    .is_some_and(|identity| selected_conversations.contains(identity))
            })
            .cloned()
            .collect(),
        assets: state
            .assets
            .iter()
            .filter(|asset| {
                matches_selection(
                    asset.project_external_id.as_ref(),
                    &asset.observed_at_rfc3339,
                    filter,
                )
            })
            .cloned()
            .collect(),
        conversations,
    }
}

fn matches_selection(
    project_external_id: Option<&String>,
    observed_at_rfc3339: &str,
    filter: &PortableExportFilter,
) -> bool {
    filter
        .project_external_id
        .as_ref()
        .is_none_or(|project| project_external_id == Some(project))
        && filter
            .observed_from_rfc3339
            .as_deref()
            .is_none_or(|from| observed_at_rfc3339 >= from)
        && filter
            .observed_to_rfc3339
            .as_deref()
            .is_none_or(|to| observed_at_rfc3339 <= to)
}

impl PortableAssetAvailability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Missing => "missing",
            Self::Quarantined => "quarantined",
        }
    }
}

mod io;

use io::{publish_bytes, write_zip};
