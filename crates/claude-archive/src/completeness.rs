//! Conservative completeness summaries for normalized archive evidence.

use crate::{
    ContentPart, KnowledgeFileAnomaly, KnowledgeFileAvailability, ParsedExport,
    ProjectKnowledgeIngestResult,
};

/// Conservative completeness classification for archive evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletenessStatus {
    /// All expected evidence was positively verified.
    Complete,
    /// Conversation coverage was positively verified while other categories remain unknown.
    ConversationsComplete,
    /// Unknown provider structure prevented a complete claim.
    StructurallyPartial,
    /// Referenced or supplied asset evidence is missing or anomalous.
    AssetsPartial,
    /// Evidence cannot support a more specific status.
    Unknown,
    /// Validation made the archive report unusable.
    FailedValidation,
}

/// Counted archive evidence used by per-archive and cumulative reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletenessCounts {
    /// Normalized project records.
    pub projects: u64,
    /// First-class project instruction records.
    pub project_instructions: u64,
    /// Referenced Project Knowledge files.
    pub knowledge_references: u64,
    /// Verified locally backed-up knowledge files.
    pub verified_knowledge_files: u64,
    /// Knowledge files without a verified local backup.
    pub missing_knowledge_files: u64,
    /// Retained inert knowledge files with an anomaly.
    pub quarantined_knowledge_files: u64,
    /// Normalized conversations.
    pub conversations: u64,
    /// Normalized messages.
    pub messages: u64,
    /// Retained unknown provider variants.
    pub unknown_variants: u64,
}

/// One content-free warning emitted by completeness calculation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletenessWarning {
    /// Stable warning code without provider content.
    pub code: String,
}

/// Completeness evidence for one immutable archive observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveCompletenessReport {
    /// Caller-supplied opaque archive identity.
    pub archive_identity: String,
    /// Conservative coverage status.
    pub status: CompletenessStatus,
    /// Counted evidence and gaps.
    pub counts: CompletenessCounts,
    /// Source-order content-free warnings.
    pub warnings: Vec<CompletenessWarning>,
}

impl ArchiveCompletenessReport {
    /// Calculates completeness from one parsed export and its file outcomes.
    #[must_use]
    pub fn from_ingest(
        archive_identity: impl Into<String>,
        parsed: &ParsedExport,
        outcomes: &[ProjectKnowledgeIngestResult],
    ) -> Self {
        let mut counts = counts_for(parsed);
        let mut warnings = Vec::new();
        let mut used_outcomes = vec![false; outcomes.len()];

        for file in &parsed.project_knowledge_files {
            let outcome = next_outcome(
                file.external_id.as_str(),
                file.project_external_id.as_str(),
                outcomes,
                &mut used_outcomes,
            );
            match outcome.map(|result| &result.availability) {
                Some(KnowledgeFileAvailability::Verified { .. }) => {
                    counts.verified_knowledge_files += 1;
                }
                Some(KnowledgeFileAvailability::Quarantined { reason, .. }) => {
                    counts.quarantined_knowledge_files += 1;
                    warnings.push(warning_for_anomaly(*reason));
                }
                Some(KnowledgeFileAvailability::ReferencedOnly) | None => {
                    counts.missing_knowledge_files += 1;
                    warnings.push(warning("knowledge_file_missing_bytes"));
                }
            }
        }

        let status = status_for(&counts);
        Self {
            archive_identity: archive_identity.into(),
            status,
            counts,
            warnings,
        }
    }
}

/// A count-wise cumulative view over immutable per-archive reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CumulativeCompletenessReport {
    /// Number of report observations included in the fold.
    pub archives: u64,
    /// Count-wise sum of constituent report evidence.
    pub counts: CompletenessCounts,
    /// Most conservative constituent status.
    pub status: CompletenessStatus,
    /// Constituent warnings in caller-provided archive order.
    pub warnings: Vec<CompletenessWarning>,
}

impl CumulativeCompletenessReport {
    /// Folds reports into one cumulative evidence summary.
    #[must_use]
    pub fn from_reports(reports: &[ArchiveCompletenessReport]) -> Self {
        let mut counts = CompletenessCounts::default();
        let mut warnings = Vec::new();
        let mut status = CompletenessStatus::Complete;
        for report in reports {
            add_counts(&mut counts, &report.counts);
            warnings.extend(report.warnings.iter().cloned());
            status = most_conservative(status, report.status);
        }
        Self {
            archives: u64::try_from(reports.len()).unwrap_or(u64::MAX),
            counts,
            status: if reports.is_empty() {
                CompletenessStatus::Unknown
            } else {
                status
            },
            warnings,
        }
    }
}

fn counts_for(parsed: &ParsedExport) -> CompletenessCounts {
    CompletenessCounts {
        projects: count(parsed.projects.len()),
        project_instructions: count(parsed.project_instructions.len()),
        knowledge_references: count(parsed.project_knowledge_files.len()),
        conversations: count(parsed.conversations.len()),
        messages: parsed
            .conversations
            .iter()
            .map(|conversation| count(conversation.messages.len()))
            .sum(),
        unknown_variants: unknown_variants(parsed),
        ..CompletenessCounts::default()
    }
}

fn next_outcome<'a>(
    external_id: &str,
    project_external_id: &str,
    outcomes: &'a [ProjectKnowledgeIngestResult],
    used_outcomes: &mut [bool],
) -> Option<&'a ProjectKnowledgeIngestResult> {
    outcomes
        .iter()
        .zip(used_outcomes.iter_mut())
        .find_map(|(outcome, used)| {
            if !*used
                && outcome.external_id == external_id
                && outcome.project_external_id == project_external_id
            {
                *used = true;
                Some(outcome)
            } else {
                None
            }
        })
}

fn unknown_variants(parsed: &ParsedExport) -> u64 {
    let project_fields = parsed
        .projects
        .iter()
        .map(|project| count(project.unknown_fields.len()));
    let knowledge_fields = parsed
        .project_knowledge_files
        .iter()
        .map(|file| count(file.unknown_fields.len()));
    let conversation_fields = parsed.conversations.iter().flat_map(|conversation| {
        let message_fields = conversation.messages.iter().flat_map(|message| {
            let content_fields = message.content.iter().map(|part| match part {
                ContentPart::Text { unknown_fields, .. }
                | ContentPart::Markdown { unknown_fields, .. } => count(unknown_fields.len()),
                ContentPart::Unknown { .. } => 1,
            });
            std::iter::once(count(message.unknown_fields.len())).chain(content_fields)
        });
        std::iter::once(count(conversation.unknown_fields.len())).chain(message_fields)
    });
    count(parsed.unknown_fields.len())
        + project_fields.sum::<u64>()
        + knowledge_fields.sum::<u64>()
        + conversation_fields.sum::<u64>()
}

fn status_for(counts: &CompletenessCounts) -> CompletenessStatus {
    if counts.missing_knowledge_files > 0 || counts.quarantined_knowledge_files > 0 {
        CompletenessStatus::AssetsPartial
    } else if counts.unknown_variants > 0 {
        CompletenessStatus::StructurallyPartial
    } else {
        CompletenessStatus::Complete
    }
}

fn warning_for_anomaly(anomaly: KnowledgeFileAnomaly) -> CompletenessWarning {
    let code = match anomaly {
        KnowledgeFileAnomaly::MissingDeclaredDigest => "knowledge_file_missing_declared_digest",
        KnowledgeFileAnomaly::DigestMismatch => "knowledge_file_digest_mismatch",
        KnowledgeFileAnomaly::InvalidMediaType => "knowledge_file_invalid_media_type",
    };
    warning(code)
}

fn warning(code: &str) -> CompletenessWarning {
    CompletenessWarning {
        code: code.to_owned(),
    }
}

fn add_counts(total: &mut CompletenessCounts, addition: &CompletenessCounts) {
    total.projects = total.projects.saturating_add(addition.projects);
    total.project_instructions = total
        .project_instructions
        .saturating_add(addition.project_instructions);
    total.knowledge_references = total
        .knowledge_references
        .saturating_add(addition.knowledge_references);
    total.verified_knowledge_files = total
        .verified_knowledge_files
        .saturating_add(addition.verified_knowledge_files);
    total.missing_knowledge_files = total
        .missing_knowledge_files
        .saturating_add(addition.missing_knowledge_files);
    total.quarantined_knowledge_files = total
        .quarantined_knowledge_files
        .saturating_add(addition.quarantined_knowledge_files);
    total.conversations = total.conversations.saturating_add(addition.conversations);
    total.messages = total.messages.saturating_add(addition.messages);
    total.unknown_variants = total
        .unknown_variants
        .saturating_add(addition.unknown_variants);
}

fn most_conservative(left: CompletenessStatus, right: CompletenessStatus) -> CompletenessStatus {
    if status_rank(left) >= status_rank(right) {
        left
    } else {
        right
    }
}

const fn status_rank(status: CompletenessStatus) -> u8 {
    match status {
        CompletenessStatus::Complete => 1,
        CompletenessStatus::ConversationsComplete => 2,
        CompletenessStatus::Unknown => 3,
        CompletenessStatus::StructurallyPartial => 4,
        CompletenessStatus::AssetsPartial => 5,
        CompletenessStatus::FailedValidation => 6,
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
