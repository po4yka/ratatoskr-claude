//! Deterministic reports for parser-version migration planning.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Classification of one archive in a migration report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParserMigrationEntryStatus {
    /// Reparse can be applied.
    Eligible,
    /// The archive already uses the exact target.
    AlreadyCurrent,
    /// The target parser is incompatible.
    Unsupported,
    /// Preserved raw evidence is unavailable.
    RawMissing,
    /// An overlapping privacy operation blocks work.
    PrivacyBlocked,
    /// Reparse completed with changes.
    Applied,
    /// Reparse completed without normalized changes.
    Unchanged,
    /// One archive-local apply failed.
    Failed,
}

/// One archive classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParserMigrationEntry {
    /// Stable archive identity.
    pub archive_id: Uuid,
    /// Exactly one result class.
    pub status: ParserMigrationEntryStatus,
}

/// Terminal migration state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParserMigrationStatus {
    /// Side-effect-free plan.
    Planned,
    /// Every eligible archive succeeded.
    Completed,
    /// At least one archive-local apply failed.
    Partial,
}

/// Stable tenant-scoped report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParserMigrationReport {
    /// Stable operation identity.
    pub operation_id: Uuid,
    /// Opaque authenticated tenant reference.
    pub tenant_ref: String,
    /// Exact `NAME@VERSION` target.
    pub target_parser: String,
    /// Planning or terminal state.
    pub status: ParserMigrationStatus,
    /// Deterministic per-archive classifications.
    pub entries: Vec<ParserMigrationEntry>,
    /// Counts derived from entries.
    pub totals: BTreeMap<String, usize>,
}

impl ParserMigrationReport {
    /// Constructs a planning report.
    #[must_use]
    pub fn planned(
        operation_id: Uuid,
        tenant_ref: impl Into<String>,
        target_parser: impl Into<String>,
        mut entries: Vec<ParserMigrationEntry>,
    ) -> Self {
        entries.sort_by_key(|entry| (entry.archive_id, entry.status));
        entries.dedup_by_key(|entry| entry.archive_id);
        let totals = derive_totals(&entries);
        Self {
            operation_id,
            tenant_ref: tenant_ref.into(),
            target_parser: target_parser.into(),
            status: ParserMigrationStatus::Planned,
            entries,
            totals,
        }
    }
}

/// Immutable migration plan with independently executable eligible entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserMigrationPlan {
    /// Deterministic planning report.
    pub report: ParserMigrationReport,
}

impl ParserMigrationPlan {
    /// Wraps a validated planning report.
    #[must_use]
    pub const fn new(report: ParserMigrationReport) -> Self {
        Self { report }
    }

    /// Applies eligible archives independently through a caller-owned reparse boundary.
    #[must_use]
    pub fn apply_with(
        &self,
        mut apply: impl FnMut(Uuid) -> Result<bool, ()>,
    ) -> ParserMigrationReport {
        let mut entries = self.report.entries.clone();
        for entry in &mut entries {
            if entry.status == ParserMigrationEntryStatus::Eligible {
                entry.status = match apply(entry.archive_id) {
                    Ok(true) => ParserMigrationEntryStatus::Applied,
                    Ok(false) => ParserMigrationEntryStatus::Unchanged,
                    Err(()) => ParserMigrationEntryStatus::Failed,
                };
            }
        }
        let status = if entries
            .iter()
            .any(|entry| entry.status == ParserMigrationEntryStatus::Failed)
        {
            ParserMigrationStatus::Partial
        } else {
            ParserMigrationStatus::Completed
        };
        ParserMigrationReport {
            operation_id: self.report.operation_id,
            tenant_ref: self.report.tenant_ref.clone(),
            target_parser: self.report.target_parser.clone(),
            status,
            totals: derive_totals(&entries),
            entries,
        }
    }
}

fn derive_totals(entries: &[ParserMigrationEntry]) -> BTreeMap<String, usize> {
    let mut totals = BTreeMap::new();
    for entry in entries {
        let key = match entry.status {
            ParserMigrationEntryStatus::Eligible => "eligible",
            ParserMigrationEntryStatus::AlreadyCurrent => "already_current",
            ParserMigrationEntryStatus::Unsupported => "unsupported",
            ParserMigrationEntryStatus::RawMissing => "raw_missing",
            ParserMigrationEntryStatus::PrivacyBlocked => "privacy_blocked",
            ParserMigrationEntryStatus::Applied => "applied",
            ParserMigrationEntryStatus::Unchanged => "unchanged",
            ParserMigrationEntryStatus::Failed => "failed",
        };
        *totals.entry(key.to_owned()).or_insert(0) += 1;
    }
    totals
}
