//! The one blob reference protocol for registration and erasure (XR-021 CONTRACTS.md R2-07).
//!
//! The blob store is content-addressed, so two actors can race on one digest: a deletion that asks
//! whether anything still references a digest and then erases it, and an import that stores
//! identical bytes (a no-op, because they exist) and registers a reference afterwards. Both follow
//! one protocol over one lock per digest, `pg_advisory_xact_lock(hashtextextended('blob:' ||
//! digest_hex, 0))` in this service's own database.
//!
//! - The eraser, per blob and in one transaction, takes the digest lock, recomputes the references
//!   with [`referenced_among`], then either keeps the blob or erases it, and commits while still
//!   holding the lock.
//! - The registrar, in the transaction that inserts the first row referencing a digest, takes the
//!   digest lock before that insert and checks the blob is present. A missing blob means an eraser
//!   won: the registrar stores the bytes again when it still holds them and otherwise fails with
//!   [`BlobRegistrationError::Erased`], a retryable class, and registers nothing.
//!
//! The invariant is that no committed row references a missing blob.
//!
//! Acquisition order, for every transaction in this crate: the tenant lock first (the archive
//! account id for importers, the request's tenant reference for deletion), then digest locks in
//! ascending digest order. No path takes a digest lock and then a tenant lock, so the order has no
//! cycle.

use std::collections::BTreeSet;

use sha2::{Digest as _, Sha256};
use sqlx::{Postgres, Transaction};

use crate::{BlobRef, BlobStore, StoreError};

/// Why a blob could not be registered.
#[derive(Debug, thiserror::Error)]
pub enum BlobRegistrationError {
    /// `PostgreSQL` refused the lock.
    #[error("the blob lock could not be taken")]
    Database(#[from] sqlx::Error),
    /// The blob store failed.
    #[error("the blob store could not confirm the blob")]
    Store(#[from] StoreError),
    /// An erasure removed the blob and the registrar holds no bytes to store it again. The
    /// operation is safe to retry from the beginning.
    #[error("the blob was erased and cannot be stored again")]
    Erased {
        /// Lowercase hexadecimal digest of the missing blob.
        digest_hex: String,
    },
}

/// Takes the transaction-scoped lock of one digest.
pub(crate) async fn blob_lock(
    tx: &mut Transaction<'_, Postgres>,
    digest_hex: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("select pg_advisory_xact_lock(hashtextextended('blob:' || $1, 0))")
        .bind(digest_hex)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The keys (`sha256/<hex>`) among `keys` that a surviving row still references.
///
/// This is the one reference check: the row-removal phase of a deletion and the eraser both call
/// it, so what planning retains and what erasure re-checks cannot disagree.
pub(crate) async fn referenced_among(
    tx: &mut Transaction<'_, Postgres>,
    keys: &[String],
) -> Result<BTreeSet<String>, sqlx::Error> {
    let referenced: Vec<String> = sqlx::query_scalar(
        "select k.key from unnest($1::text[]) as k(key)
         where exists (select 1 from claude_archive.exports e where e.blob_ref = k.key)
            or exists (select 1 from claude_archive.extracted_artifacts x where x.blob_ref = k.key)
            or exists (select 1 from claude_archive.project_sources s where s.blob_ref = k.key)
            or exists (select 1 from claude_archive.content_parts p where p.blob_ref = k.key)
            or exists (select 1 from claude_archive.artifact_versions v where v.blob_ref = k.key)
            or exists (select 1 from claude_archive.assets a where a.blob_ref = k.key)
            or exists (select 1 from claude_archive.projects pr where pr.instructions_blob_ref = k.key)",
    )
    .bind(keys)
    .fetch_all(&mut **tx)
    .await?;
    Ok(referenced.into_iter().collect())
}

/// Registers the references a transaction is about to insert.
///
/// Call it after the tenant lock and before the first insert that references any of
/// `references`. It takes the digest locks in ascending order and checks every blob is present
/// (by metadata, so a multi-gigabyte archive is not hashed again). `staged` is every byte
/// sequence the caller still holds; a missing blob whose digest one of them has is stored again.
///
/// # Errors
///
/// Returns [`BlobRegistrationError`] when a lock or the store fails, or when a blob is missing and
/// no staged bytes can restore it.
pub async fn register(
    tx: &mut Transaction<'_, Postgres>,
    store: &BlobStore,
    references: &[&BlobRef],
    staged: &[&[u8]],
) -> Result<(), BlobRegistrationError> {
    let mut ordered: Vec<&BlobRef> = references.to_vec();
    ordered.sort_by(|left, right| left.digest_hex.cmp(&right.digest_hex));
    ordered.dedup_by(|left, right| left.digest_hex == right.digest_hex);
    for reference in &ordered {
        blob_lock(tx, &reference.digest_hex).await?;
    }
    for reference in ordered {
        match store.locate(&reference.digest_hex) {
            Ok(_) => {}
            Err(StoreError::Missing { digest_hex }) => {
                restore(store, reference, staged, digest_hex)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Stores the bytes of `reference` again from the staged bytes that hash to its digest.
fn restore(
    store: &BlobStore,
    reference: &BlobRef,
    staged: &[&[u8]],
    digest_hex: String,
) -> Result<(), BlobRegistrationError> {
    let bytes = staged
        .iter()
        .find(|bytes| format!("{:x}", Sha256::digest(bytes)) == reference.digest_hex)
        .ok_or(BlobRegistrationError::Erased { digest_hex })?;
    store.store(reference.media_type.clone(), bytes)?;
    Ok(())
}
