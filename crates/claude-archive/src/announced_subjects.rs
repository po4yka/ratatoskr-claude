//! The owner every subject was announced under (XR-021 CONTRACTS.md R5-06).
//!
//! A privacy deletion addresses each tombstone to the Platform user Knowledge holds the subject
//! under. That user is recorded here when the subject is announced, in the transaction that
//! enqueues the fact, so a deletion never reads the configured account mapping.

use crate::ArchiveEventFact;

/// Records the owner `fact` announces its subject under.
///
/// A pair `(subject, owner)` has one row. A later fact for the same pair keeps the row and names
/// the archive that fact was announced under, so a survivor announced again under a retained
/// archive moves its row off the archive a deletion tombstones. `account_id` is the account of the
/// export the fact's archive was imported from, and stays null when no export row exists.
///
/// # Errors
///
/// Returns [`crate::ArchiveEventError`] when the fact cannot be placed in an envelope or the row
/// cannot be written.
pub(crate) async fn record(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    fact: &ArchiveEventFact,
) -> Result<(), crate::ArchiveEventError> {
    let Some(announcement) = fact.announcement()? else {
        return Ok(());
    };
    sqlx::query(
        "insert into claude_archive.announced_subjects
             (subject_kind, subject_id, ai_archive_id, owner_ref, account_id)
         values ($1, $2, $3, $4,
                 (select account_ref from claude_archive.exports
                  where ai_archive_id = $3 order by export_id limit 1))
         on conflict (subject_kind, subject_id, owner_ref) do update
             set ai_archive_id = excluded.ai_archive_id,
                 account_id = coalesce(excluded.account_id,
                                       claude_archive.announced_subjects.account_id)",
    )
    .bind(announcement.subject_kind)
    .bind(&announcement.subject_id)
    .bind(announcement.ai_archive_id)
    .bind(announcement.owner.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
