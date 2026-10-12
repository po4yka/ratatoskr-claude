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

/// The distinct owners the ledger holds for the announcements of these accounts.
///
/// A deletion finds the outbox rows of an account through them, never through the request's
/// tenant reference or the configured mapping.
pub(crate) async fn owners_of_accounts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    accounts: &[uuid::Uuid],
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "select distinct owner_ref from claude_archive.announced_subjects
         where account_id = any($1) order by owner_ref",
    )
    .bind(accounts)
    .fetch_all(&mut **transaction)
    .await
}

/// The owners one subject was announced to, restricted to the announcements of these accounts so
/// a provider Artifact id that recurs across tenants never reaches another tenant's owner.
pub(crate) async fn owners_of_subject(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    subject_kind: &str,
    subject_id: &str,
    accounts: &[uuid::Uuid],
) -> Result<Vec<ratatoskr_identifiers::TenantRef>, sqlx::Error> {
    let owners: Vec<String> = sqlx::query_scalar(
        "select owner_ref from claude_archive.announced_subjects
         where subject_kind = $1 and subject_id = $2 and account_id = any($3)
         order by owner_ref",
    )
    .bind(subject_kind)
    .bind(subject_id)
    .bind(accounts)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(owners
        .iter()
        .filter_map(|owner| ratatoskr_identifiers::TenantRef::parse(owner).ok())
        .collect())
}

/// The owner an archive was last announced under, or `None` when it never was.
pub(crate) async fn owner_of_archive(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    archive: uuid::Uuid,
) -> Result<Option<ratatoskr_identifiers::TenantRef>, sqlx::Error> {
    let owner: Option<String> = sqlx::query_scalar(
        "select owner_ref from claude_archive.announced_subjects
         where subject_kind = 'archive' and subject_id = $1
         order by announced_at desc, owner_ref limit 1",
    )
    .bind(archive.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(owner.and_then(|owner| ratatoskr_identifiers::TenantRef::parse(&owner).ok()))
}

/// Deletes the rows of the subjects a deletion tombstoned, for every owner.
pub(crate) async fn forget_subjects(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    subjects: &[(String, String)],
    accounts: &[uuid::Uuid],
) -> Result<(), sqlx::Error> {
    let (kinds, ids): (Vec<&str>, Vec<&str>) = subjects
        .iter()
        .map(|(kind, id)| (kind.as_str(), id.as_str()))
        .unzip();
    sqlx::query(
        "delete from claude_archive.announced_subjects l
         using unnest($1::text[], $2::text[]) as s(kind, id)
         where l.subject_kind = s.kind and l.subject_id = s.id and l.account_id = any($3)",
    )
    .bind(&kinds)
    .bind(&ids)
    .bind(accounts)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Deletes the rows that still name an archive the deletion tombstoned.
///
/// Knowledge drops everything announced under a tombstoned archive. A survivor announced again
/// under a retained archive has moved its row off the deleted one, so the rows left are exactly
/// the subjects Knowledge no longer holds.
pub(crate) async fn forget_archives(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    archives: &[uuid::Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "delete from claude_archive.announced_subjects
         where ai_archive_id = any($1) or (subject_kind = 'archive' and subject_id = any($2))",
    )
    .bind(archives)
    .bind(archives.iter().map(ToString::to_string).collect::<Vec<_>>())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Deletes every row of these accounts, which a tenant-scope deletion removes with the account.
pub(crate) async fn forget_accounts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    accounts: &[uuid::Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from claude_archive.announced_subjects where account_id = any($1)")
        .bind(accounts)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}
