//! Deterministic parser migration report contracts.

use ratatoskr_claude_archive::parser_migration::{
    ParserMigrationEntry, ParserMigrationEntryStatus, ParserMigrationPlan, ParserMigrationReport,
    ParserMigrationStatus,
};
use uuid::Uuid;

#[test]
fn migration_report_classifies_each_archive_once_and_derives_totals() {
    let operation_id = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1000);
    let eligible = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1001);
    let current = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1002);
    let unsupported = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1003);
    let missing = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1004);
    let blocked = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1005);
    let entries = vec![
        entry(blocked, ParserMigrationEntryStatus::PrivacyBlocked),
        entry(unsupported, ParserMigrationEntryStatus::Unsupported),
        entry(eligible, ParserMigrationEntryStatus::Eligible),
        entry(missing, ParserMigrationEntryStatus::RawMissing),
        entry(current, ParserMigrationEntryStatus::AlreadyCurrent),
        entry(eligible, ParserMigrationEntryStatus::Eligible),
    ];
    let first = ParserMigrationReport::planned(
        operation_id,
        "tenant-alpha",
        "claude-personal@2.0.0",
        entries.clone(),
    );
    let second = ParserMigrationReport::planned(
        operation_id,
        "tenant-alpha",
        "claude-personal@2.0.0",
        entries.into_iter().rev().collect(),
    );
    assert_eq!(first, second, "source order must not affect the report");
    assert_eq!(first.entries.len(), 5, "each archive must appear once");
    assert!(
        first
            .entries
            .windows(2)
            .all(|pair| pair[0].archive_id < pair[1].archive_id)
    );
    assert_eq!(first.totals.get("eligible"), Some(&1));
    assert_eq!(first.totals.values().sum::<usize>(), first.entries.len());
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
}

#[test]
fn migration_apply_reports_partial_when_one_archive_fails() {
    let operation = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1100);
    let first = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1101);
    let failed = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1102);
    let later = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1103);
    let unsupported = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_1104);
    let plan = ParserMigrationPlan::new(ParserMigrationReport::planned(
        operation,
        "tenant-alpha",
        "claude-personal@2.0",
        vec![
            entry(later, ParserMigrationEntryStatus::Eligible),
            entry(unsupported, ParserMigrationEntryStatus::Unsupported),
            entry(failed, ParserMigrationEntryStatus::Eligible),
            entry(first, ParserMigrationEntryStatus::Eligible),
        ],
    ));
    let mut attempted = Vec::new();
    let report = plan.apply_with(|archive| {
        attempted.push(archive);
        if archive == failed {
            Err(())
        } else {
            Ok(archive == first)
        }
    });
    assert_eq!(
        attempted,
        vec![first, failed, later],
        "failure must not stop later archives"
    );
    assert_eq!(report.status, ParserMigrationStatus::Partial);
    assert_eq!(
        report
            .entries
            .iter()
            .find(|entry| entry.archive_id == first)
            .map(|entry| entry.status),
        Some(ParserMigrationEntryStatus::Applied)
    );
    assert_eq!(
        report
            .entries
            .iter()
            .find(|entry| entry.archive_id == failed)
            .map(|entry| entry.status),
        Some(ParserMigrationEntryStatus::Failed)
    );
    assert_eq!(
        report
            .entries
            .iter()
            .find(|entry| entry.archive_id == later)
            .map(|entry| entry.status),
        Some(ParserMigrationEntryStatus::Unchanged)
    );
    assert_eq!(
        report
            .entries
            .iter()
            .find(|entry| entry.archive_id == unsupported)
            .map(|entry| entry.status),
        Some(ParserMigrationEntryStatus::Unsupported)
    );
}

fn entry(archive_id: Uuid, status: ParserMigrationEntryStatus) -> ParserMigrationEntry {
    ParserMigrationEntry { archive_id, status }
}
