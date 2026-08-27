//! Contract-native Claude Archive event envelopes and transactional outbox persistence.

use ratatoskr_ai_archive_contracts::{
    AiArchiveImport, AiArchiveTombstone, AiArchiveTombstoneSubject, AiArtifactAdded,
    AiArtifactUpdated, AiConversationAdded, AiConversationUpdated, AiProjectAdded,
    AiProjectUpdated,
};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_identifiers::{EntityRef, EventId, TenantRef, WireTimestamp};
use sha2::{Digest as _, Sha256};

use crate::Database;

/// One normalized archive fact eligible for durable publication.
#[derive(Debug, Clone, PartialEq)]
pub enum ArchiveEventFact {
    /// An immutable provider export finished importing.
    Imported(AiArchiveImport),
    /// A conversation was first observed.
    ConversationAdded(AiConversationAdded),
    /// A conversation revision was observed.
    ConversationUpdated(AiConversationUpdated),
    /// A project was first observed.
    ProjectAdded(AiProjectAdded),
    /// A project revision was observed.
    ProjectUpdated(AiProjectUpdated),
    /// An Artifact was first observed.
    ArtifactAdded(AiArtifactAdded),
    /// An Artifact revision was observed.
    ArtifactUpdated(AiArtifactUpdated),
    /// Authoritative removal evidence was observed.
    Tombstoned(AiArchiveTombstone),
}

/// A complete, durable event ready for at-least-once delivery.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEvent {
    /// The exact wire envelope that a transport must deliver unchanged.
    pub envelope: EventEnvelope,
}

/// Safe failure while producing or persisting an archive event.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ArchiveEventError {
    /// A required contract identifier could not be composed.
    #[error("an archive event identifier is invalid")]
    Identifier,
    /// A contract payload could not be encoded as a complete envelope.
    #[error("the archive event envelope could not be encoded")]
    Envelope(#[from] ratatoskr_event_envelope::EnvelopeError),
    /// A complete envelope could not be serialized from its contract values.
    #[error("the archive event envelope could not be serialized")]
    Json(#[from] serde_json::Error),
    /// The owned outbox transaction failed.
    #[error("the archive outbox could not be persisted")]
    Persistence(#[from] sqlx::Error),
    /// A normalized object was paired with another import's provenance.
    #[error("a normalized archive event belongs to another import")]
    ProvenanceMismatch,
}

/// Persists complete archive event envelopes in the owning transactional outbox.
#[derive(Debug)]
pub struct ArchiveOutbox<'a> {
    database: &'a Database,
}

impl<'a> ArchiveOutbox<'a> {
    /// Creates an outbox writer over Claude Archive-owned storage.
    #[must_use]
    pub const fn new(database: &'a Database) -> Self {
        Self { database }
    }

    /// Builds and stores one event. Callers that also persist normalized rows should use
    /// [`Self::enqueue_in`] inside their normalization transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveEventError`] when the event cannot be formed or persisted.
    pub async fn enqueue(
        &self,
        fact: &ArchiveEventFact,
        occurred_at: WireTimestamp,
    ) -> Result<OutboxEvent, ArchiveEventError> {
        let mut transaction = self.database.pool().begin().await?;
        let event = Self::enqueue_in(&mut transaction, fact, occurred_at).await?;
        transaction.commit().await?;
        Ok(event)
    }

    /// Atomically publishes one completed import and every normalized subject observed in it.
    ///
    /// For a projection and its events in one transaction, use [`Self::publish_import_in`].
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveEventError::ProvenanceMismatch`] when a subject does not name the import
    /// whose completion is being published.
    pub async fn publish_import(
        &self,
        import: &AiArchiveImport,
        subjects: &[ArchiveEventFact],
        occurred_at: WireTimestamp,
    ) -> Result<Vec<OutboxEvent>, ArchiveEventError> {
        let mut transaction = self.database.pool().begin().await?;
        let events =
            Self::publish_import_in(&mut transaction, import, subjects, occurred_at).await?;
        transaction.commit().await?;
        Ok(events)
    }

    /// Appends an import head and its normalized subjects to a caller-owned transaction.
    ///
    /// The caller must persist the matching normalized rows in this transaction before it commits,
    /// so no consumer can observe an event whose archive projection rolled back.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveEventError::ProvenanceMismatch`] when a subject's complete immutable
    /// provenance differs from the supplied import.
    pub async fn publish_import_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        import: &AiArchiveImport,
        subjects: &[ArchiveEventFact],
        occurred_at: WireTimestamp,
    ) -> Result<Vec<OutboxEvent>, ArchiveEventError> {
        if subjects
            .iter()
            .any(|subject| !subject_belongs_to_import(subject, import))
        {
            return Err(ArchiveEventError::ProvenanceMismatch);
        }
        let mut events = Vec::with_capacity(subjects.len() + 1);
        events.push(
            Self::enqueue_in(
                transaction,
                &ArchiveEventFact::Imported(import.clone()),
                occurred_at,
            )
            .await?,
        );
        for subject in subjects {
            events.push(Self::enqueue_in(transaction, subject, occurred_at).await?);
        }
        Ok(events)
    }

    /// Builds and stores one event in a caller-owned transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveEventError`] when the fact cannot be encoded or the insert fails.
    pub async fn enqueue_in(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        fact: &ArchiveEventFact,
        occurred_at: WireTimestamp,
    ) -> Result<OutboxEvent, ArchiveEventError> {
        let event_id = EventId::new_v7();
        let envelope = build_envelope(event_id, fact, occurred_at)?;
        let (aggregate_type, aggregate_id) = fact.aggregate()?;
        let envelope_json =
            serde_json::to_value(&envelope).map_err(|_| ArchiveEventError::Identifier)?;
        let payload_digest = Sha256::digest(serde_json::to_vec(&envelope.payload)?).to_vec();
        let stored: Option<serde_json::Value> = sqlx::query_scalar(
            "insert into claude_archive.outbox_events
                 (event_id, event_type, aggregate_type, aggregate_id, envelope,
                  payload_digest, correlation_id, causation_id, tenant_ref, occurred_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
             on conflict (event_type, payload_digest) do nothing
             returning envelope",
        )
        .bind(event_id.0)
        .bind(envelope.event_type.to_wire())
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(envelope_json)
        .bind(&payload_digest)
        .bind(envelope.correlation_id.to_string())
        .bind(envelope.causation_id.as_ref().map(ToString::to_string))
        .bind(envelope.tenant_id.as_ref().map(ToString::to_string))
        .bind(occurred_at.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
        let envelope = match stored {
            Some(_) => envelope,
            None => sqlx::query_scalar(
                "select envelope from claude_archive.outbox_events
                 where event_type = $1 and payload_digest = $2",
            )
            .bind(envelope.event_type.to_wire())
            .bind(payload_digest)
            .fetch_one(&mut **transaction)
            .await
            .and_then(|raw: serde_json::Value| {
                serde_json::from_value(raw).map_err(sqlx::Error::decode)
            })?,
        };
        Ok(OutboxEvent { envelope })
    }
}

fn subject_belongs_to_import(subject: &ArchiveEventFact, import: &AiArchiveImport) -> bool {
    let expected = ratatoskr_ai_archive_contracts::AiArchiveProvenance::from_import(import);
    match subject {
        ArchiveEventFact::ConversationAdded(value) => value.import_provenance == expected,
        ArchiveEventFact::ConversationUpdated(value) => value.import_provenance == expected,
        ArchiveEventFact::ProjectAdded(value) => value.import_provenance == expected,
        ArchiveEventFact::ProjectUpdated(value) => value.import_provenance == expected,
        ArchiveEventFact::ArtifactAdded(value) => value.import_provenance == expected,
        ArchiveEventFact::ArtifactUpdated(value) => value.import_provenance == expected,
        ArchiveEventFact::Imported(_) | ArchiveEventFact::Tombstoned(_) => false,
    }
}

fn build_envelope(
    event_id: EventId,
    fact: &ArchiveEventFact,
    occurred_at: WireTimestamp,
) -> Result<EventEnvelope, ArchiveEventError> {
    let (aggregate_id, tenant_id) = fact.aggregate_reference()?;
    let event_reference = event_id.as_entity_ref();
    let mut envelope: EventEnvelope = serde_json::from_value(serde_json::json!({
        "event_id": event_id,
        "event_type": "ai_archive.archive.imported.v1",
        "occurred_at": occurred_at,
        "producer": "ratatoskr-claude",
        "aggregate_id": aggregate_id,
        "correlation_id": event_reference,
        "tenant_id": tenant_id,
        "schema_version": 1,
        "payload": {}
    }))?;
    match fact {
        ArchiveEventFact::Imported(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ConversationAdded(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ConversationUpdated(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ProjectAdded(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ProjectUpdated(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ArtifactAdded(value) => envelope.set_payload(value)?,
        ArchiveEventFact::ArtifactUpdated(value) => envelope.set_payload(value)?,
        ArchiveEventFact::Tombstoned(value) => envelope.set_payload(value)?,
    }
    Ok(envelope)
}

impl ArchiveEventFact {
    fn aggregate(&self) -> Result<(&'static str, String), ArchiveEventError> {
        let (reference, _) = self.aggregate_reference()?;
        let kind = match self {
            Self::Imported(_) => "archive",
            Self::ConversationAdded(_) | Self::ConversationUpdated(_) => "conversation",
            Self::ProjectAdded(_) | Self::ProjectUpdated(_) => "project",
            Self::ArtifactAdded(_) | Self::ArtifactUpdated(_) => "artifact",
            Self::Tombstoned(_) => "tombstone",
        };
        Ok((kind, reference.to_string()))
    }

    fn aggregate_reference(&self) -> Result<(EntityRef, TenantRef), ArchiveEventError> {
        match self {
            Self::Imported(value) => Ok((value.ai_archive_id.as_entity_ref(), value.owner)),
            Self::ConversationAdded(value) => Ok((
                value.conversation.ai_conversation_id.as_entity_ref(),
                value.conversation.owner,
            )),
            Self::ConversationUpdated(value) => Ok((
                value.conversation.ai_conversation_id.as_entity_ref(),
                value.conversation.owner,
            )),
            Self::ProjectAdded(value) => Ok((
                value.project.ai_project_id.as_entity_ref(),
                value.import_provenance.owner,
            )),
            Self::ProjectUpdated(value) => Ok((
                value.project.ai_project_id.as_entity_ref(),
                value.import_provenance.owner,
            )),
            Self::ArtifactAdded(value) => Ok((
                artifact_reference(&value.artifact.external_artifact_id.to_string())?,
                value.artifact.owner,
            )),
            Self::ArtifactUpdated(value) => Ok((
                artifact_reference(&value.artifact.external_artifact_id.to_string())?,
                value.artifact.owner,
            )),
            Self::Tombstoned(value) => Ok((tombstone_reference(value)?, value.owner)),
        }
    }
}

fn artifact_reference(id: &str) -> Result<EntityRef, ArchiveEventError> {
    EntityRef::parse(&format!("artifact:{id}")).map_err(|_| ArchiveEventError::Identifier)
}

fn tombstone_reference(value: &AiArchiveTombstone) -> Result<EntityRef, ArchiveEventError> {
    match &value.subject {
        AiArchiveTombstoneSubject::Archive => Ok(value.ai_archive_id.as_entity_ref()),
        AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
            Ok(ai_conversation_id.as_entity_ref())
        }
        AiArchiveTombstoneSubject::Project { ai_project_id } => Ok(ai_project_id.as_entity_ref()),
        AiArchiveTombstoneSubject::Artifact {
            external_artifact_id,
        } => artifact_reference(&external_artifact_id.to_string()),
    }
}
