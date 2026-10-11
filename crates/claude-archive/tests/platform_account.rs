//! The first receipt from a mapped Platform user creates the archive account, and a tenant-token
//! receipt still requires one that exists (XR-021 CONTRACTS.md R3-04 rule 1).
//!
//! A separate file from `receipt.rs`, which sits at the 850 line cap.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, ArchiveIdentity, BlobRef, BlobStore, MediaType, PlatformOperation,
    ReceiptError, ReceiptOutcome, TenantClaim, TenantScope,
};
use sha2::Digest as _;
use sqlx::Row as _;
use uuid::Uuid;

fn store_blob(store: &BlobStore, bytes: &[u8]) -> BlobRef {
    store
        .store_stream(
            MediaType::parse("application/zip").expect("the media type parses"),
            &mut std::io::Cursor::new(bytes.to_vec()),
            1_048_576,
        )
        .expect("the archive bytes are stored")
}

/// Every account row; `observed_first_at` is read as text so the test needs no time crate.
async fn account_rows(pool: &sqlx::PgPool) -> Vec<(Uuid, String, String)> {
    sqlx::query("select account_id, external_account_id, observed_first_at::text from claude_archive.accounts")
        .fetch_all(pool)
        .await
        .expect("the accounts are readable")
        .iter()
        .map(|row| {
            (
                row.get("account_id"),
                row.get("external_account_id"),
                row.get("observed_first_at"),
            )
        })
        .collect()
}

#[tokio::test]
async fn the_first_receipt_from_a_mapped_user_creates_the_account() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let root = temp_root("platform-account-first");
    let store = BlobStore::open(&root).expect("the store opens");
    let account = Uuid::now_v7();
    let scope = TenantScope::Account(account);
    assert!(
        account_rows(db.database.pool()).await.is_empty(),
        "no account row exists before the first receipt"
    );

    let first = ratatoskr_claude_archive::receipt::record_verified_platform_archive(
        &db.database,
        &store,
        scope,
        store_blob(&store, b"first archive"),
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await;
    let after_first = account_rows(db.database.pool()).await;
    let second = ratatoskr_claude_archive::receipt::record_verified_platform_archive(
        &db.database,
        &store,
        scope,
        store_blob(&store, b"second archive"),
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await;
    let after_second = account_rows(db.database.pool()).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(first, Ok(ReceiptOutcome::Stored { .. })),
        "the first receipt is accepted without an operator-seeded row: {first:?}"
    );
    assert_eq!(
        after_first
            .iter()
            .map(|row| (row.0, row.1.as_str()))
            .collect::<Vec<_>>(),
        vec![(account, format!("platform-account:{account}").as_str())],
        "the receipt created the mapped account with its derived external id"
    );
    assert!(
        matches!(second, Ok(ReceiptOutcome::Stored { .. })),
        "the second receipt is accepted: {second:?}"
    );
    assert_eq!(
        after_second, after_first,
        "a second receipt changes nothing about the account row"
    );
}

#[tokio::test]
async fn a_tenant_token_receipt_still_requires_an_existing_account() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let root = temp_root("platform-account-token");
    let store = BlobStore::open(&root).expect("the store opens");
    let payload = b"token archive".to_vec();
    let digest = format!("{:x}", sha2::Sha256::digest(&payload));

    let result = ratatoskr_claude_archive::receipt::receive_platform_operation_archive(
        &db.database,
        &store,
        &TenantClaim {
            account: Some(Uuid::now_v7()),
            organization: None,
        },
        AcquisitionMode::ConsumerExport,
        std::io::Cursor::new(&payload),
        1_048_576,
        &ArchiveIdentity {
            sha256: digest,
            byte_size: u64::try_from(payload.len()).expect("fixture length fits"),
        },
        PlatformOperation {
            operation_id: Uuid::now_v7(),
        },
    )
    .await;
    let rows = account_rows(db.database.pool()).await;
    remove(&root);
    db.cleanup().await.expect("cleanup succeeds");

    assert!(
        matches!(result, Err(ReceiptError::UnknownTenant)),
        "a tenant claim naming no account is refused: {result:?}"
    );
    assert!(
        rows.is_empty(),
        "the tenant-token path never creates an account"
    );
}
