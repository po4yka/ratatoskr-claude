//! Validation and persistence of Knowledge completion links to archive revisions.

use ratatoskr_ai_archive_contracts::{
    AiArchiveAnalysisCompleted, AiArchiveSubject, AiArtifactAdded, AiArtifactUpdated,
    AiConversationAdded, AiConversationUpdated,
};
use ratatoskr_event_envelope::EventEnvelope;

use crate::Database;

/// Outcome of consuming one at-least-once Knowledge completion delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeLinkOutcome {
    /// One exact published revision was linked to its completed analysis.
    Linked,
    /// The exact delivery was already stored.
    Duplicate,
}

/// Completion-link validation error without archive content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KnowledgeLinkError {
    /// No published archive revision has the completion's exact archive, subject, and digest.
    #[error("the Knowledge completion does not match a published archive revision")]
    RevisionNotPublished,
    /// A persisted outbox envelope cannot be decoded as its published contract.
    #[error("a persisted archive outbox envelope is invalid")]
    Envelope(#[from] ratatoskr_event_envelope::EnvelopeError),
    /// Owned linkage persistence failed.
    #[error("the Knowledge completion linkage could not be persisted")]
    Persistence(#[from] sqlx::Error),
    /// Owned outbox JSON is corrupt.
    #[error("a persisted archive outbox envelope is corrupt")]
    Json(#[from] serde_json::Error),
}

/// Consumer-side linkage store for Knowledge's archive completion facts.
#[derive(Debug)]
pub struct KnowledgeLinkStore<'a> {
    database: &'a Database,
}

impl<'a> KnowledgeLinkStore<'a> {
    /// Creates a linkage store over Claude Archive-owned state.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Validates and records one Knowledge completion delivery.
    ///
    /// # Errors
    ///
    /// Returns [`KnowledgeLinkError::RevisionNotPublished`] when the exact immutable revision was
    /// not previously published by this archive.
    pub async fn accept(
        &self,
        completion_event_id: uuid::Uuid,
        completion: &AiArchiveAnalysisCompleted,
    ) -> Result<KnowledgeLinkOutcome, KnowledgeLinkError> {
        if !self.matches_published_revision(completion).await? {
            return Err(KnowledgeLinkError::RevisionNotPublished);
        }
        let (subject_kind, subject_id) = completion_subject(&completion.subject)
            .ok_or(KnowledgeLinkError::RevisionNotPublished)?;
        let inserted = sqlx::query_scalar::<_, uuid::Uuid>(
            "insert into claude_archive.knowledge_analysis_links
                 (completion_event_id, ai_archive_id, subject_kind, subject_id,
                  content_digest_hex, completed_at)
             values ($1, $2, $3, $4, $5, $6::timestamptz)
             on conflict (completion_event_id) do nothing returning completion_event_id",
        )
        .bind(completion_event_id)
        .bind(completion.ai_archive_id.0)
        .bind(subject_kind)
        .bind(subject_id)
        .bind(completion.content_digest.hex.as_str())
        .bind(completion.completed_at.to_string())
        .fetch_optional(self.database.pool())
        .await?;
        Ok(if inserted.is_some() {
            KnowledgeLinkOutcome::Linked
        } else {
            KnowledgeLinkOutcome::Duplicate
        })
    }

    /// Validates and records the complete event envelope delivered by Knowledge.
    ///
    /// # Errors
    ///
    /// Returns [`KnowledgeLinkError::Envelope`] when the delivery is not the published
    /// `knowledge.ai_archive_analysis.completed.v1` contract.
    pub async fn accept_envelope(
        &self,
        envelope: &EventEnvelope,
    ) -> Result<KnowledgeLinkOutcome, KnowledgeLinkError> {
        let completion = envelope.payload_as::<AiArchiveAnalysisCompleted>()?;
        self.accept(envelope.event_id.0, &completion).await
    }

    async fn matches_published_revision(
        &self,
        completion: &AiArchiveAnalysisCompleted,
    ) -> Result<bool, KnowledgeLinkError> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("select envelope from claude_archive.outbox_events")
                .fetch_all(self.database.pool())
                .await?;
        for (raw,) in rows {
            let envelope: EventEnvelope = serde_json::from_value(raw)?;
            if completion_matches_envelope(completion, &envelope) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn completion_matches_envelope(
    completion: &AiArchiveAnalysisCompleted,
    envelope: &EventEnvelope,
) -> bool {
    if let Ok(value) = envelope.payload_as::<AiConversationAdded>() {
        return matches_conversation(
            completion,
            value.import_provenance.ai_archive_id,
            &value.conversation,
        );
    }
    if let Ok(value) = envelope.payload_as::<AiConversationUpdated>() {
        return matches_conversation(
            completion,
            value.import_provenance.ai_archive_id,
            &value.conversation,
        );
    }
    if let Ok(value) = envelope.payload_as::<AiArtifactAdded>() {
        return matches_artifact(
            completion,
            value.import_provenance.ai_archive_id,
            &value.artifact,
        );
    }
    if let Ok(value) = envelope.payload_as::<AiArtifactUpdated>() {
        return matches_artifact(
            completion,
            value.import_provenance.ai_archive_id,
            &value.artifact,
        );
    }
    false
}

fn matches_conversation(
    completion: &AiArchiveAnalysisCompleted,
    archive_id: ratatoskr_identifiers::AiArchiveId,
    conversation: &ratatoskr_ai_archive_contracts::AiConversation,
) -> bool {
    matches!(&completion.subject, AiArchiveSubject::Conversation { ai_conversation_id }
        if completion.ai_archive_id == archive_id
            && completion.owner == conversation.owner
            && *ai_conversation_id == conversation.ai_conversation_id
            && completion.content_digest == conversation.content_digest)
}

fn matches_artifact(
    completion: &AiArchiveAnalysisCompleted,
    archive_id: ratatoskr_identifiers::AiArchiveId,
    artifact: &ratatoskr_ai_archive_contracts::AiArtifact,
) -> bool {
    matches!(&completion.subject, AiArchiveSubject::Artifact { external_artifact_id }
        if completion.ai_archive_id == archive_id
            && completion.owner == artifact.owner
            && *external_artifact_id == artifact.external_artifact_id
            && completion.content_digest == artifact.content_digest)
}

fn completion_subject(subject: &AiArchiveSubject) -> Option<(&'static str, String)> {
    match subject {
        AiArchiveSubject::Conversation { ai_conversation_id } => {
            Some(("conversation", ai_conversation_id.to_string()))
        }
        AiArchiveSubject::Artifact {
            external_artifact_id,
        } => Some(("artifact", external_artifact_id.to_string())),
        AiArchiveSubject::Project { .. } => None,
    }
}
