//! Read-only admission checks for minimized owner-derived parser fixtures.

use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

const MAX_FILES: usize = 32;
const MAX_FILE_BYTES: u64 = 1_048_576;

/// Fixture admission outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureAdmissionStatus {
    /// Candidate passed every required gate.
    Admitted,
    /// Candidate is unsafe or lacks required evidence.
    Rejected,
}

/// Deterministic content-free admission report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureAdmissionReport {
    /// Non-sensitive case identity when a manifest supplies one.
    pub case_id: Option<String>,
    /// Final gate decision.
    pub status: FixtureAdmissionStatus,
    /// Sorted machine-readable rejection codes.
    pub findings: Vec<String>,
}

/// Candidate read or manifest decoding failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FixtureAdmissionError {
    /// Candidate files could not be read safely.
    #[error("fixture candidate could not be read")]
    Io(#[from] std::io::Error),
}

/// Stateless read-only fixture admission gate.
#[derive(Debug, Default, Clone, Copy)]
pub struct FixtureAdmission;

impl FixtureAdmission {
    /// Inspects a candidate tree without writing or blessing it.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureAdmissionError`] if the candidate cannot be read.
    pub fn inspect(candidate: &Path) -> Result<FixtureAdmissionReport, FixtureAdmissionError> {
        if !candidate.is_dir() {
            return Ok(FixtureAdmissionReport {
                case_id: None,
                status: FixtureAdmissionStatus::Rejected,
                findings: vec!["candidate_not_directory".to_owned()],
            });
        }
        let mut findings = Vec::new();
        let files = inspect_tree(candidate, &mut findings)?;
        let manifest_path = candidate.join("manifest.json");
        let manifest = match std::fs::read(&manifest_path) {
            Ok(bytes) => {
                if let Ok(manifest) = serde_json::from_slice::<FixtureManifest>(&bytes) {
                    Some(manifest)
                } else {
                    findings.push("manifest_invalid".to_owned());
                    None
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                findings.push("manifest_missing".to_owned());
                None
            }
            Err(error) => return Err(error.into()),
        };
        let case_id = manifest.as_ref().map(|manifest| manifest.case_id.clone());
        if let Some(manifest) = manifest {
            inspect_manifest(candidate, &files, &manifest, &mut findings)?;
        }
        findings.sort();
        findings.dedup();
        Ok(FixtureAdmissionReport {
            case_id,
            status: if findings.is_empty() {
                FixtureAdmissionStatus::Admitted
            } else {
                FixtureAdmissionStatus::Rejected
            },
            findings,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureManifest {
    format: String,
    case_id: String,
    acquisition_mode: String,
    schema_id: String,
    private_evidence_record: String,
    fixture: PathBuf,
    observed_structure: PathBuf,
    expected_structure: PathBuf,
    synthetic_only: bool,
    reviews: FixtureReviews,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureReviews {
    consent: ReviewGate,
    license: ReviewGate,
    secret_scan: ReviewGate,
    personal_data_scan: ReviewGate,
    path_safety: ReviewGate,
    deterministic_comparison: ReviewGate,
    independent_review: ReviewGate,
    owner_approval: ReviewGate,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct ReviewGate(bool);

impl ReviewGate {
    fn passed(&self) -> bool {
        self.0
    }
}

impl FixtureReviews {
    fn all_passed(&self) -> bool {
        self.consent.passed()
            && self.license.passed()
            && self.secret_scan.passed()
            && self.personal_data_scan.passed()
            && self.path_safety.passed()
            && self.deterministic_comparison.passed()
            && self.independent_review.passed()
            && self.owner_approval.passed()
    }
}

fn inspect_tree(
    candidate: &Path,
    findings: &mut Vec<String>,
) -> Result<Vec<PathBuf>, FixtureAdmissionError> {
    if !candidate.is_dir() {
        findings.push("candidate_not_directory".to_owned());
        return Ok(Vec::new());
    }
    let mut pending = vec![(candidate.to_path_buf(), 0_usize)];
    let mut files = Vec::new();
    while let Some((directory, depth)) = pending.pop() {
        if depth > 4 {
            findings.push("tree_too_deep".to_owned());
            continue;
        }
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                findings.push("symlink_forbidden".to_owned());
                continue;
            }
            if file_type.is_dir() {
                pending.push((entry.path(), depth + 1));
                continue;
            }
            if !file_type.is_file() {
                findings.push("special_file_forbidden".to_owned());
                continue;
            }
            files.push(entry.path());
        }
    }
    files.sort();
    if files.len() > MAX_FILES {
        findings.push("too_many_files".to_owned());
    }
    for path in &files {
        let metadata = std::fs::metadata(path)?;
        if metadata.len() > MAX_FILE_BYTES {
            findings.push("file_too_large".to_owned());
            continue;
        }
        inspect_file(path, findings)?;
    }
    Ok(files)
}

fn inspect_file(path: &Path, findings: &mut Vec<String>) -> Result<(), FixtureAdmissionError> {
    let extension = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(extension.as_str(), "zip" | "tar" | "tgz" | "gz" | "7z") {
        findings.push("raw_archive_forbidden".to_owned());
        return Ok(());
    }
    if !matches!(extension.as_str(), "json" | "md") {
        findings.push("unsupported_candidate_file".to_owned());
        return Ok(());
    }
    let bytes = std::fs::read(path)?;
    let Ok(text) = std::str::from_utf8(&bytes) else {
        findings.push("non_utf8_forbidden".to_owned());
        return Ok(());
    };
    let lower = text.to_ascii_lowercase();
    for marker in [
        "sk-",
        "bearer ",
        "account_email",
        "owner_email",
        "owner_name",
        "source_digest",
        "original_filename",
        "private_path",
    ] {
        if lower.contains(marker) {
            findings.push("private_or_secret_value".to_owned());
            break;
        }
    }
    if text
        .split(|character: char| character.is_whitespace() || matches!(character, '"' | '<' | '>'))
        .any(looks_like_email)
    {
        findings.push("private_or_secret_value".to_owned());
    }
    Ok(())
}

fn looks_like_email(value: &str) -> bool {
    let trimmed = value.trim_matches(|character: char| {
        !character.is_ascii_alphanumeric()
            && character != '@'
            && character != '.'
            && character != '-'
            && character != '_'
    });
    let Some((local, domain)) = trimmed.split_once('@') else {
        return false;
    };
    !local.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')
}

fn inspect_manifest(
    candidate: &Path,
    files: &[PathBuf],
    manifest: &FixtureManifest,
    findings: &mut Vec<String>,
) -> Result<(), FixtureAdmissionError> {
    if manifest.format != "ratatoskr-owner-derived-fixture" {
        findings.push("manifest_format_unsupported".to_owned());
    }
    if !safe_identifier(&manifest.case_id)
        || !safe_identifier(&manifest.schema_id)
        || !safe_identifier(&manifest.private_evidence_record)
    {
        findings.push("manifest_identifier_invalid".to_owned());
    }
    if !matches!(
        manifest.acquisition_mode.as_str(),
        "consumer_export"
            | "organization_export"
            | "compliance_api"
            | "manual_conversation_capture"
            | "legacy_import"
    ) {
        findings.push("acquisition_mode_invalid".to_owned());
    }
    if !manifest.synthetic_only {
        findings.push("source_values_not_minimized".to_owned());
    }
    if !manifest.reviews.all_passed() {
        findings.push("review_or_approval_missing".to_owned());
    }
    for path in [
        &manifest.fixture,
        &manifest.observed_structure,
        &manifest.expected_structure,
    ] {
        if !safe_relative_path(path) || !files.contains(&candidate.join(path)) {
            findings.push("manifest_path_unsafe_or_missing".to_owned());
        }
    }
    let listed = [
        Path::new("manifest.json"),
        manifest.fixture.as_path(),
        manifest.observed_structure.as_path(),
        manifest.expected_structure.as_path(),
    ];
    if files.iter().any(|path| {
        path.strip_prefix(candidate)
            .map_or(true, |relative| !listed.contains(&relative))
    }) {
        findings.push("unlisted_candidate_file".to_owned());
    }
    if safe_relative_path(&manifest.observed_structure)
        && safe_relative_path(&manifest.expected_structure)
    {
        let observed = read_json(candidate.join(&manifest.observed_structure), findings)?;
        let expected = read_json(candidate.join(&manifest.expected_structure), findings)?;
        if observed.is_some() && expected.is_some() && observed != expected {
            findings.push("structure_comparison_mismatch".to_owned());
        }
    }
    if safe_relative_path(&manifest.fixture) {
        let _ = read_json(candidate.join(&manifest.fixture), findings)?;
    }
    Ok(())
}

fn read_json(
    path: PathBuf,
    findings: &mut Vec<String>,
) -> Result<Option<serde_json::Value>, FixtureAdmissionError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            if let Ok(value) = serde_json::from_slice(&bytes) {
                Ok(Some(value))
            } else {
                findings.push("candidate_json_invalid".to_owned());
                Ok(None)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
