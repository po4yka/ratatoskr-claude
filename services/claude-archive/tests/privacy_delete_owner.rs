//! The real binary plans and executes a deletion without consulting the configured account
//! mapping, and the tombstones name the owner the subject was announced under (XR-021
//! CONTRACTS.md R5-06 rule 4).
//!
//! The mapping the binary reads differs between the plan and the execute, and neither lists the
//! owner the archive was announced under. Before the change the plan was refused for an unmapped
//! account and the execute answered `not_found` once the mapping changed.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::process::Command;

use ratatoskr_claude_archive::BlobStore;
use ratatoskr_claude_archive::MediaType;
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::test_support::{TestDatabase, database_url};
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_ratatoskr-claude-archive");
const ANNOUNCED_TO: &str = "018f0000-0000-7000-8000-0000000000a1";
const MAPPED_LATER_TO: &str = "018f0000-0000-7000-8000-0000000000b1";

fn command(
    database: &str,
    blob_root: &std::path::Path,
    mapping: Option<(&std::path::Path, Uuid)>,
    arguments: &[&str],
) -> (i32, serde_json::Value) {
    let mut process = Command::new(BIN);
    process
        .env_clear()
        .env("RATATOSKR__STORAGE__DATABASE_URL", database)
        .env("RATATOSKR__STORAGE__BLOB_ROOT", blob_root);
    if let Some((seed, account)) = mapping {
        process
            .env(
                "RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS",
                format!("{MAPPED_LATER_TO}={account}"),
            )
            .env("RATATOSKR__RECEIPT__EVENT_BUS_URL", "nats://127.0.0.1:1")
            .env("RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH", seed);
    }
    let output = process
        .args(arguments)
        .output()
        .expect("the service binary runs");
    let stdout = serde_json::from_slice(&output.stdout).unwrap_or(serde_json::Value::Null);
    (output.status.code().unwrap_or(-1), stdout)
}

/// An account with one export holding `raw`, and an archive head announced to [`ANNOUNCED_TO`].
async fn seed_announced_account(
    db: &TestDatabase,
    raw: &ratatoskr_claude_archive::BlobRef,
) -> Uuid {
    let account = Uuid::now_v7();
    let archive = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, 'owner-command')",
    )
    .bind(account)
    .execute(db.database.pool())
    .await
    .expect("the account seeds");
    sqlx::query(
        "insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
          received_at)
         values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
    )
    .bind(Uuid::now_v7())
    .bind(archive)
    .bind(account)
    .bind(&raw.digest_hex)
    .bind(format!("sha256/{}", raw.digest_hex))
    .bind(i64::try_from(raw.length_bytes).expect("length fits"))
    .execute(db.database.pool())
    .await
    .expect("the export seeds");
    sqlx::query(
        "insert into claude_archive.announced_subjects
          (subject_kind, subject_id, ai_archive_id, owner_ref, account_id)
         values ('archive', $4, $1, $2, $3)",
    )
    .bind(archive)
    .bind(format!("user:{ANNOUNCED_TO}"))
    .bind(account)
    .bind(archive.to_string())
    .execute(db.database.pool())
    .await
    .expect("the announcement seeds");
    account
}

#[tokio::test]
async fn a_mapping_change_between_plan_and_execute_changes_nothing() {
    let db = TestDatabase::create().await.expect("a disposable database");
    let root = temp_root("owner-command");
    let store = BlobStore::open(&root).expect("the blob store opens");
    let raw = store
        .store(
            MediaType::parse("application/zip").expect("canonical media type"),
            b"owner command raw archive",
        )
        .expect("the raw bytes are stored");
    let account = seed_announced_account(&db, &raw).await;
    let seed = root.join("seed.nkey");
    std::fs::write(
        &seed,
        "SUAEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLE",
    )
    .expect("the seed file is written");
    let database = database_url(db.name());
    let tenant = account.to_string();

    // The plan runs with no mapping at all, so the account is unmapped.
    let (plan_code, plan) = command(
        &database,
        &root,
        None,
        &[
            "privacy-delete",
            "plan",
            "--tenant",
            &tenant,
            "--request-key",
            "owner-command",
            "--scope",
            "tenant",
        ],
    );
    let request_id = plan
        .get("request_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    // The execute runs with a mapping that names another Platform user for the same account.
    let (execute_code, execute) = command(
        &database,
        &root,
        Some((&seed, account)),
        &[
            "privacy-delete",
            "execute",
            "--tenant",
            &tenant,
            "--request-id",
            &request_id,
            "--confirm",
        ],
    );
    let owners: Vec<String> = sqlx::query_scalar(
        "select tenant_ref from claude_archive.outbox_events
         where event_type = 'ai_archive.subject.tombstoned.v1'",
    )
    .fetch_all(db.database.pool())
    .await
    .expect("the tombstones are readable");
    let recorded: Vec<String> =
        sqlx::query_scalar("select tenant_ref from claude_archive.privacy_deletion_requests")
            .fetch_all(db.database.pool())
            .await
            .expect("the request is readable");
    db.cleanup().await.expect("cleanup succeeds");
    remove(&root);

    assert_eq!(
        (plan_code, plan.get("status")),
        (0, Some(&"planned".into())),
        "{plan}"
    );
    assert_eq!(
        (execute_code, execute.get("status")),
        (0, Some(&"completed".into())),
        "{execute}"
    );
    assert_eq!(
        recorded,
        vec![tenant],
        "the supplied reference is stored as given"
    );
    assert_eq!(
        owners,
        vec![format!("user:{ANNOUNCED_TO}")],
        "the tombstone names the owner the archive was announced under, not the later mapping"
    );
}
