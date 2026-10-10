//! Projection of the persisted import into `ai_archive.*` facts.
//!
//! The facts are built from the rows the import persisted, never from the in-memory parse, and
//! enqueued in the transaction that persisted them, so no consumer sees a fact whose rows rolled
//! back (XR-021 CONTRACTS.md S07). Order is the import head, then projects, conversations and
//! Artifacts. A subject is published as added when no earlier fact exists for its identity, as
//! updated when its current record differs from the latest earlier fact, and not at all when it is
//! equal to it.
//!
//! The owner of every fact is the Platform user of the archive account, resolved through the
//! configured account mapping. An account without a mapping has no Knowledge tenant, so it emits
//! no fact; the operation reports still flow.
//!
//! Verified only against the documented synthetic export schema.

use std::collections::{HashMap, HashSet};

use ratatoskr_ai_archive_contracts::{
    AiArchiveCompleteness, AiArchiveImport, AiArchiveProvenance, AiArtifact, AiArtifactAdded,
    AiArtifactUpdated, AiAuthorRole, AiCompletenessReport, AiContentPart, AiConversation,
    AiConversationAdded, AiConversationUpdated, AiGap, AiGapKind, AiMessage, AiModelName,
    AiProject, AiProjectAdded, AiProjectUpdated, AiProvider, AiText, AiTitle, ParserName,
    ParserVersion,
};
use ratatoskr_error_contracts::WarningEnvelope;
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::{
    AiArchiveId, AiConversationId, AiProjectId, ContentDigest, DigestAlgorithm, DigestHex,
    EntityLocalId, EntityRef, Extensions, SafeMessage, TenantRef, WireTimestamp, canonical_json,
};
use sha2::{Digest as _, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

mod loaders;
mod reannounce;

use crate::events::contract_blob_ref;
use crate::{
    ArchiveEventError, ArchiveEventFact, ArchiveOutbox, BlobRef, ParserIdentity, PlatformUsers,
    ReparseError,
};
use loaders::{
    artifact_fact, conversation_fact, head_already_announced, load_artifacts, load_conversations,
    load_projects, project_fact,
};
pub(crate) use reannounce::{Deletion, Survivors, reannounce};

/// What the import head needs to know about the report the caller just persisted.
#[derive(Debug, Clone)]
pub(crate) struct ReportView {
    /// The persisted completeness status, in its schema spelling.
    pub(crate) status: String,
    /// The persisted discovered counts.
    pub(crate) counts: serde_json::Value,
    /// The persisted content-free warning codes.
    pub(crate) warnings: Vec<String>,
}

impl ReportView {
    /// The report the export's first import persisted.
    ///
    /// A reparse re-announces the archive head under the same archive id, and what Knowledge was
    /// told about the completeness of that archive is a property of the import, not of one
    /// parser release: the status, counts and warnings stay those of the first import. An export
    /// with no persisted report is announced as `unknown`, which names a gap, rather than being
    /// promoted to a status nobody measured.
    pub(crate) async fn original(
        transaction: &mut Transaction<'_, Postgres>,
        export_id: Uuid,
    ) -> Result<Self, ReparseError> {
        let row: Option<(String, serde_json::Value, Option<serde_json::Value>)> = sqlx::query_as(
            "select r.status, r.discovered_counts, r.warnings
             from claude_archive.completeness_reports r
             join claude_archive.import_runs i on i.run_id = r.run_id
             where i.export_id = $1 order by r.created_at, r.report_id limit 1",
        )
        .bind(export_id)
        .fetch_optional(&mut **transaction)
        .await?;
        Ok(match row {
            Some((status, counts, warnings)) => Self {
                status,
                counts,
                warnings: warnings
                    .as_ref()
                    .and_then(serde_json::Value::as_array)
                    .map(|codes| {
                        codes
                            .iter()
                            .filter_map(|code| code.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
            },
            None => Self {
                status: "unknown".to_owned(),
                counts: serde_json::json!({}),
                warnings: Vec::new(),
            },
        })
    }
}

/// The subjects a projection could not announce, by kind.
///
/// A subject that cannot be projected (a row with no parser stamp, a message with no usable
/// provider id, an Artifact with no verified bytes) is named on the import head instead of
/// vanishing, so Knowledge can tell that its fact set is not the whole archive (XR-021
/// CONTRACTS.md S07).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Omitted {
    pub(crate) projects: u64,
    pub(crate) conversations: u64,
    pub(crate) artifacts: u64,
}

impl Omitted {
    pub(crate) const fn is_empty(self) -> bool {
        self.projects == 0 && self.conversations == 0 && self.artifacts == 0
    }

    /// One gap per kind that lost a subject.
    pub(crate) fn gaps(self) -> Vec<AiGap> {
        let mut gaps = Vec::new();
        for (count, kind, detail) in [
            (
                self.projects,
                "unprojected_project",
                "Projects that could not be projected into facts were not announced.",
            ),
            (
                self.conversations,
                "unprojected_conversation",
                "Conversations that could not be projected into facts were not announced.",
            ),
            (
                self.artifacts,
                "unprojected_artifact",
                "Artifacts without verified bytes could not be projected into facts.",
            ),
        ] {
            if count > 0 {
                gaps.extend(gap(kind, detail, Some(count)));
            }
        }
        gaps
    }

    /// An archive that lost a subject is never announced as complete.
    pub(crate) fn completeness(self, status: AiArchiveCompleteness) -> AiArchiveCompleteness {
        if !self.is_empty() && status.is_complete() {
            AiArchiveCompleteness::StructurallyPartial
        } else {
            status
        }
    }
}

/// One import or reparse whose persisted rows are projected into facts.
pub(crate) struct ImportProjection<'a> {
    pub(crate) users: &'a PlatformUsers,
    pub(crate) account_id: Uuid,
    pub(crate) export_id: Uuid,
    pub(crate) archive_id: Uuid,
    pub(crate) raw: &'a BlobRef,
    pub(crate) parser: &'a ParserIdentity,
    pub(crate) report: ReportView,
    /// The initial import reports its head even when it holds nothing; a reparse only when it
    /// changed something.
    pub(crate) head_always: bool,
}

/// The record counts that decide which gaps an import must name.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct GapInput {
    pub(crate) complete: bool,
    pub(crate) unknown_variants: u64,
    pub(crate) missing_files: u64,
    pub(crate) quarantined_files: u64,
}

impl GapInput {
    /// Reads the counts from a persisted `discovered_counts` document.
    pub(crate) fn from_counts(status: &str, counts: &serde_json::Value) -> Self {
        let count = |name: &str| {
            counts
                .get(name)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        };
        Self {
            complete: status == "complete",
            unknown_variants: count("unknown_variants"),
            missing_files: count("missing_knowledge_files"),
            quarantined_files: count("quarantined_knowledge_files"),
        }
    }
}

/// The named holes of an import, one entry per kind.
///
/// An import that is not complete always names at least one gap (invariant A1 of the contract);
/// a status without a specific cause is named as such instead of being left blank.
pub(crate) fn gaps_for(input: GapInput) -> Vec<AiGap> {
    let mut gaps = Vec::new();
    for (count, kind, detail) in [
        (
            input.unknown_variants,
            "unrecognized_record",
            "Provider records this parser does not recognize were preserved without interpretation.",
        ),
        (
            input.missing_files,
            "missing_file",
            "Project knowledge files were referenced without bytes in the archive.",
        ),
        (
            input.quarantined_files,
            "quarantined_file",
            "Project knowledge files failed verification and were kept inert.",
        ),
    ] {
        if count > 0 {
            gaps.extend(gap(kind, detail, Some(count)));
        }
    }
    if gaps.is_empty() && !input.complete {
        gaps.extend(gap(
            "incomplete_import",
            "The archive was not proven complete.",
            None,
        ));
    }
    gaps
}

fn gap(kind: &str, detail: &str, count: Option<u64>) -> Option<AiGap> {
    Some(AiGap {
        gap_kind: AiGapKind::parse(kind).ok()?,
        detail: SafeMessage::parse(detail).ok()?,
        external_ref: None,
        affected_count: count.map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        extensions: Extensions::new(),
    })
}

pub(crate) fn contract_completeness(status: &str) -> AiArchiveCompleteness {
    match status {
        "complete" => AiArchiveCompleteness::Complete,
        "conversations_complete" => AiArchiveCompleteness::ConversationsComplete,
        "assets_partial" => AiArchiveCompleteness::AssetsPartial,
        "unknown" => AiArchiveCompleteness::Unknown,
        "failed_validation" => AiArchiveCompleteness::FailedValidation,
        _ => AiArchiveCompleteness::StructurallyPartial,
    }
}

impl ImportProjection<'_> {
    /// Enqueues every fact this import changed and returns the subjects it could not announce.
    pub(crate) async fn publish(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<Omitted, ReparseError> {
        let Some(owner) = self.users.owner_of(self.account_id) else {
            tracing::info!(
                class = "owner_unmapped",
                "no archive fact for an unmapped account"
            );
            return Ok(Omitted::default());
        };
        let imported_at = WireTimestamp::now();
        let provider = claude()?;
        let source_export = contract_blob_ref(self.raw)?;
        let mut subjects = Vec::new();
        let mut omitted = Omitted::default();
        for project in
            load_projects(transaction, self.export_id, &provider, &mut omitted, None).await?
        {
            if let Some(fact) = project_fact(
                transaction,
                self.provenance(
                    owner,
                    &provider,
                    &source_export,
                    imported_at,
                    &project.stamp,
                ),
                project,
            )
            .await?
            {
                subjects.push(fact);
            }
        }
        let conversations = load_conversations(
            transaction,
            self.export_id,
            owner,
            &provider,
            &mut omitted,
            None,
        )
        .await?;
        let counts = (
            u32::try_from(conversations.len()).unwrap_or(u32::MAX),
            conversations
                .iter()
                .map(|conversation| u32::try_from(conversation.messages.len()).unwrap_or(u32::MAX))
                .fold(0_u32, u32::saturating_add),
        );
        for conversation in conversations {
            if let Some(fact) = conversation_fact(
                transaction,
                self.provenance(
                    owner,
                    &provider,
                    &source_export,
                    imported_at,
                    &(
                        conversation.parser_name.clone(),
                        conversation.parser_version.clone(),
                    ),
                ),
                conversation,
            )
            .await?
            {
                subjects.push(fact);
            }
        }
        subjects.extend(
            self.artifact_facts(
                transaction,
                (owner, &provider),
                (&source_export, imported_at),
                &mut omitted,
            )
            .await?,
        );
        // An omission alone is reason enough to re-announce the head: it is the only place the
        // loss is visible.
        if subjects.is_empty() && !self.head_always && omitted.is_empty() {
            return Ok(omitted);
        }

        let head = self.head(owner, provider, source_export, imported_at, counts, omitted)?;
        // A repeated reparse of an archive with the same omissions would announce the same gaps
        // again; only a change in what the head says is worth an event.
        if subjects.is_empty()
            && !self.head_always
            && head_already_announced(transaction, &head).await?
        {
            return Ok(omitted);
        }
        ArchiveOutbox::enqueue_in(transaction, &ArchiveEventFact::Imported(head), imported_at)
            .await?;
        for fact in subjects {
            // Each fact is stamped when it is enqueued, so the pump's order by occurrence time
            // is the order of this function even where event ids share a millisecond.
            ArchiveOutbox::enqueue_in(transaction, &fact, WireTimestamp::now()).await?;
        }
        Ok(omitted)
    }

    /// The Artifact facts of the export that changed, in the order the import announces them.
    async fn artifact_facts(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        (owner, provider): (TenantRef, &AiProvider),
        (source_export, imported_at): (&ratatoskr_identifiers::BlobRef, WireTimestamp),
        omitted: &mut Omitted,
    ) -> Result<Vec<ArchiveEventFact>, ReparseError> {
        let mut facts = Vec::new();
        for artifact in
            load_artifacts(transaction, self.export_id, owner, provider, omitted, None).await?
        {
            let stamp = (
                artifact.parser_name.clone(),
                artifact.parser_version.clone(),
            );
            let provenance = self.provenance(owner, provider, source_export, imported_at, &stamp);
            if let Some(fact) = artifact_fact(transaction, provenance, artifact).await? {
                facts.push(fact);
            }
        }
        Ok(facts)
    }

    fn provenance(
        &self,
        owner: TenantRef,
        provider: &AiProvider,
        source_export: &ratatoskr_identifiers::BlobRef,
        imported_at: WireTimestamp,
        stamp: &(ParserName, ParserVersion),
    ) -> AiArchiveProvenance {
        AiArchiveProvenance {
            ai_archive_id: AiArchiveId(self.archive_id),
            provider: provider.clone(),
            owner,
            source_export: source_export.clone(),
            imported_at,
            parser_name: stamp.0.clone(),
            parser_version: stamp.1.clone(),
            extensions: Extensions::new(),
        }
    }

    fn head(
        &self,
        owner: TenantRef,
        provider: AiProvider,
        source_export: ratatoskr_identifiers::BlobRef,
        imported_at: WireTimestamp,
        (conversations, messages): (u32, u32),
        omitted: Omitted,
    ) -> Result<AiArchiveImport, ReparseError> {
        let mut gaps = gaps_for(GapInput::from_counts(
            &self.report.status,
            &self.report.counts,
        ));
        gaps.extend(omitted.gaps());
        let count = |name: &str| {
            self.report
                .counts
                .get(name)
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or_default()
        };
        let warnings = self
            .report
            .warnings
            .iter()
            .filter_map(|code| {
                warning(
                    &format!("ai_archive.{code}"),
                    "A non-blocking problem was recorded.",
                )
            })
            .collect();
        Ok(AiArchiveImport {
            ai_archive_id: AiArchiveId(self.archive_id),
            provider,
            owner,
            source_export,
            imported_at,
            parser_name: parser_name(self.parser.identifier())?,
            parser_version: parser_version(self.parser.version())?,
            completeness_report: AiCompletenessReport {
                completeness: omitted.completeness(contract_completeness(&self.report.status)),
                conversation_count: conversations,
                message_count: messages,
                asset_count: count("verified_knowledge_files"),
                gap_count: u32::try_from(gaps.len()).unwrap_or(u32::MAX),
                gaps,
            },
            warnings,
            extensions: Extensions::new(),
        })
    }
}

fn claude() -> Result<AiProvider, ArchiveEventError> {
    AiProvider::parse("claude").map_err(|_| ArchiveEventError::Identifier)
}

/// The contract spelling of a parser identifier: lowercase snake case, at most 32 characters.
fn parser_name(identifier: &str) -> Result<ParserName, ArchiveEventError> {
    let mut token: String = identifier
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    token.truncate(32);
    ParserName::parse(&token).map_err(|_| ArchiveEventError::Identifier)
}

fn parser_version(version: &str) -> Result<ParserVersion, ArchiveEventError> {
    ParserVersion::parse(version).map_err(|_| ArchiveEventError::Identifier)
}

fn stamp(
    name: Option<String>,
    version: Option<String>,
) -> Result<Option<(ParserName, ParserVersion)>, ArchiveEventError> {
    match (name, version) {
        (Some(name), Some(version)) => Ok(Some((parser_name(&name)?, parser_version(&version)?))),
        _ => Ok(None),
    }
}

fn warning(code: &str, message: &str) -> Option<WarningEnvelope> {
    Some(WarningEnvelope {
        code: ratatoskr_error_contracts::ErrorCode::parse(code).ok()?,
        message: SafeMessage::parse(message).ok()?,
        field_path: None,
        extensions: Extensions::new(),
    })
}

/// A database timestamp rendered as the canonical wire instant, trailing zeros trimmed.
fn instant(raw: Option<String>) -> Option<WireTimestamp> {
    let raw = raw?;
    let (whole, fraction) = raw.strip_suffix('Z')?.split_once('.')?;
    let fraction = fraction.trim_end_matches('0');
    let canonical = if fraction.is_empty() {
        format!("{whole}Z")
    } else {
        format!("{whole}.{fraction}Z")
    };
    WireTimestamp::parse(&canonical).ok()
}

fn digest(value: &impl serde::Serialize) -> Result<ContentDigest, ArchiveEventError> {
    let canonical = canonical_json(value)?;
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        hex: DigestHex::parse(&format!("{:x}", Sha256::digest(canonical.as_bytes())))
            .map_err(|_| ArchiveEventError::Identifier)?,
    })
}
