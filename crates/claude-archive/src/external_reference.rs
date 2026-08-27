//! External Claude-entity references and local backup status.

use std::collections::BTreeMap;
use std::time::SystemTime;

/// The kind of Claude entity named by an external reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub enum ExternalReferenceKind {
    /// A Claude Project.
    Project,
    /// A Claude conversation.
    Conversation,
    /// A Claude Artifact.
    Artifact,
}

/// The local preservation result for an upstream entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBackupStatus {
    /// The entity has no locally verified preservation evidence.
    ReferenceOnly,
    /// The entity is locally preserved by verified evidence.
    LocallyBackedUp,
}

/// Evidence relevant to a local backup result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalEvidence {
    /// No local evidence is available.
    Missing,
    /// The locally retained evidence is verified.
    Verified,
    /// Retained evidence did not pass verification and remains quarantined.
    Quarantined,
}

/// One upstream Claude entity that has an independently derived local status.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ExternalReference {
    /// The provider entity category.
    pub kind: ExternalReferenceKind,
    /// The provider identity, without a title or URL.
    pub external_id: String,
}

impl ExternalReference {
    /// Creates a reference from a provider identity.
    #[must_use]
    pub fn new(kind: ExternalReferenceKind, external_id: impl Into<String>) -> Self {
        Self {
            kind,
            external_id: external_id.into(),
        }
    }
}

/// A non-evidentiary authorization outcome from an acquisition attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationStatus {
    /// Authorization was valid for this attempt.
    Valid,
    /// Authorization has expired.
    Expired,
    /// Authorization was explicitly revoked.
    Revoked,
    /// The service could not determine authorization state.
    Unknown,
}

/// Reconciles evidence-derived statuses independently from authorization.
#[derive(Debug, Default)]
pub struct BackupStatusLedger {
    statuses: BTreeMap<ExternalReference, LocalBackupStatus>,
    audits: Vec<BackupStatusAudit>,
}

/// A content-free record of one local-backup-status transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupStatusAudit {
    /// The entity whose status changed.
    pub reference: ExternalReference,
    /// The previously derived status.
    pub previous_status: LocalBackupStatus,
    /// The newly derived status.
    pub new_status: LocalBackupStatus,
    /// The local evidence classification that produced the new status.
    pub evidence: LocalEvidence,
    /// The caller-observed time of the evidence update.
    pub observed_at: SystemTime,
}

impl BackupStatusLedger {
    /// Records local evidence for an external reference.
    #[must_use]
    pub fn observe_evidence(
        &mut self,
        reference: &ExternalReference,
        evidence: LocalEvidence,
    ) -> LocalBackupStatus {
        self.observe_evidence_at(reference, evidence, SystemTime::now())
    }

    /// Records local evidence observed at a caller-provided time.
    #[must_use]
    pub fn observe_evidence_at(
        &mut self,
        reference: &ExternalReference,
        evidence: LocalEvidence,
        observed_at: SystemTime,
    ) -> LocalBackupStatus {
        let status = derive_local_backup_status(evidence);
        let prior = self.statuses.insert(reference.clone(), status);
        if let Some(previous_status) = prior.filter(|previous| *previous != status) {
            self.audits.push(BackupStatusAudit {
                reference: reference.clone(),
                previous_status,
                new_status: status,
                evidence,
                observed_at,
            });
        }
        status
    }

    /// Reads the last evidence-derived status after an authorization outcome.
    #[must_use]
    pub fn observe_authorization(
        &self,
        reference: &ExternalReference,
        _authorization: AuthorizationStatus,
    ) -> Option<LocalBackupStatus> {
        self.status(reference)
    }

    /// Returns the latest status derived from local evidence.
    #[must_use]
    pub fn status(&self, reference: &ExternalReference) -> Option<LocalBackupStatus> {
        self.statuses.get(reference).copied()
    }

    /// Returns transition audits in observation order.
    #[must_use]
    pub fn audits(&self) -> &[BackupStatusAudit] {
        &self.audits
    }
}

/// Derives a local backup status from local evidence.
#[must_use]
pub const fn derive_local_backup_status(evidence: LocalEvidence) -> LocalBackupStatus {
    match evidence {
        LocalEvidence::Verified => LocalBackupStatus::LocallyBackedUp,
        LocalEvidence::Missing | LocalEvidence::Quarantined => LocalBackupStatus::ReferenceOnly,
    }
}
