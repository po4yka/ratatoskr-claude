//! External-reference backup-status contracts.

use std::time::{Duration, UNIX_EPOCH};

use ratatoskr_claude_archive::{
    AuthorizationStatus, BackupStatusLedger, ExternalReference, ExternalReferenceKind,
    LocalBackupStatus, LocalEvidence, derive_local_backup_status,
};

#[test]
fn verified_evidence_derives_backed_up_and_missing_evidence_derives_reference_only() {
    assert_eq!(
        derive_local_backup_status(LocalEvidence::Missing),
        LocalBackupStatus::ReferenceOnly,
    );
    assert_eq!(
        derive_local_backup_status(LocalEvidence::Verified),
        LocalBackupStatus::LocallyBackedUp,
    );
}

#[test]
fn expired_authorization_does_not_mutate_last_successful_backup_evidence() {
    let reference = ExternalReference::new(ExternalReferenceKind::Project, "project-1");
    let mut ledger = BackupStatusLedger::default();

    assert_eq!(
        ledger.observe_evidence(&reference, LocalEvidence::Verified),
        LocalBackupStatus::LocallyBackedUp,
    );
    assert_eq!(
        ledger.observe_authorization(&reference, AuthorizationStatus::Expired),
        Some(LocalBackupStatus::LocallyBackedUp),
    );
}

#[test]
fn verified_evidence_transition_appends_one_content_free_audit_entry() {
    let reference = ExternalReference::new(ExternalReferenceKind::Artifact, "artifact-1");
    let observed_at = UNIX_EPOCH + Duration::from_mins(28_750_000);
    let mut ledger = BackupStatusLedger::default();

    let _ = ledger.observe_evidence_at(&reference, LocalEvidence::Missing, observed_at);
    let _ = ledger.observe_evidence_at(&reference, LocalEvidence::Verified, observed_at);
    let _ = ledger.observe_evidence_at(&reference, LocalEvidence::Verified, observed_at);

    assert_eq!(
        ledger.audits().len(),
        1,
        "only the status transition audits"
    );
    assert_eq!(
        ledger.audits()[0].previous_status,
        LocalBackupStatus::ReferenceOnly,
    );
    assert_eq!(
        ledger.audits()[0].new_status,
        LocalBackupStatus::LocallyBackedUp,
    );
    assert_eq!(ledger.audits()[0].reference, reference);
    assert_eq!(ledger.audits()[0].evidence, LocalEvidence::Verified);
    assert_eq!(ledger.audits()[0].observed_at, observed_at);
}
