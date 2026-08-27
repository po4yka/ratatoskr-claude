//! The `claude_archive` schema contract: inventory, boundary, and identity
//! constraints, proven against disposable databases created from the one
//! definition file.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::collections::BTreeSet;

use sqlx::Row as _;
use uuid::Uuid;

use ratatoskr_claude_archive::test_support::TestDatabase;

/// The tables the service owns, from the AGENTS.md conceptual data list.
const OWNED_TABLES: [&str; 20] = [
    "accounts",
    "organizations",
    "exports",
    "import_runs",
    "projects",
    "project_sources",
    "conversations",
    "messages",
    "message_relations",
    "content_parts",
    "artifacts",
    "artifact_versions",
    "external_references",
    "backup_status_audits",
    "assets",
    "revisions",
    "tombstones",
    "completeness_reports",
    "outbox_events",
    "inbox_events",
];

async fn table_inventory(pool: &sqlx::PgPool) -> BTreeSet<String> {
    let rows = sqlx::query(
        "select table_name from information_schema.tables
         where table_schema = 'claude_archive' and table_type = 'BASE TABLE'",
    )
    .fetch_all(pool)
    .await
    .expect("the catalog query succeeds");
    rows.into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

async fn column_inventory(pool: &sqlx::PgPool, table: &str) -> BTreeSet<String> {
    let rows = sqlx::query(
        "select column_name from information_schema.columns
         where table_schema = 'claude_archive' and table_name = $1",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .expect("the column catalog query succeeds");
    rows.into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

#[tokio::test]
async fn fresh_database_provisions_every_owned_table() {
    let db = TestDatabase::create()
        .await
        .expect("a fresh disposable database applies the definition");

    let inventory = table_inventory(db.database.pool()).await;
    let expected: BTreeSet<String> = OWNED_TABLES.iter().map(ToString::to_string).collect();
    assert_eq!(
        inventory, expected,
        "the definition provisions exactly the owned tables"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn second_database_has_same_inventory() {
    let first = TestDatabase::create()
        .await
        .expect("the first database provisions");
    let second = TestDatabase::create()
        .await
        .expect("an independent second database provisions");

    let a = table_inventory(first.database.pool()).await;
    let b = table_inventory(second.database.pool()).await;
    assert_eq!(a, b, "independent databases expose the same inventory");
    assert_eq!(a.len(), OWNED_TABLES.len());

    first.cleanup().await.expect("first cleanup succeeds");
    second.cleanup().await.expect("second cleanup succeeds");
}

#[tokio::test]
async fn artifact_versions_retain_provider_lineage_evidence() {
    let db = TestDatabase::create()
        .await
        .expect("a fresh disposable database applies the definition");

    let columns = column_inventory(db.database.pool(), "artifact_versions").await;
    for expected in [
        "artifact_id",
        "external_version_id",
        "previous_external_version_id",
        "blob_ref",
        "content_hash",
        "raw_record",
    ] {
        assert!(
            columns.contains(expected),
            "Artifact-version lineage needs the {expected} column"
        );
    }

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn fresh_schema_exposes_backup_status_and_transition_audits() {
    let db = TestDatabase::create()
        .await
        .expect("a fresh disposable database applies the definition");
    let pool = db.database.pool();
    let inventory = table_inventory(pool).await;

    for table in ["external_references", "backup_status_audits"] {
        assert!(
            inventory.contains(table),
            "backup status requires the {table} table"
        );
    }
    for table in ["projects", "conversations", "artifacts"] {
        let columns = column_inventory(pool, table).await;
        assert!(
            columns.contains("local_backup_status"),
            "{table} must expose its evidence-derived local backup status"
        );
    }
    let audit_columns = column_inventory(pool, "backup_status_audits").await;
    for column in [
        "reference_id",
        "external_entity_id",
        "previous_status",
        "new_status",
        "evidence_kind",
        "observed_at",
    ] {
        assert!(
            audit_columns.contains(column),
            "backup-status audit requires the {column} column"
        );
    }

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn duplicate_archive_hash_rejected() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let pool = db.database.pool();
    let account_id = Uuid::now_v7();

    let insert_export = |export_id: Uuid, hash: Vec<u8>| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "insert into claude_archive.exports
                     (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size, received_at)
                 values ($1, $2, $3, 'consumer_export', $4, 'sha256/aa/bb', 10, now())",
            )
            .bind(export_id)
            .bind(Uuid::now_v7())
            .bind(account_id)
            .bind(hash)
            .execute(&pool)
            .await
        }
    };

    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account_id)
    .bind("acct-dup-hash")
    .execute(pool)
    .await
    .expect("the account inserts");

    insert_export(Uuid::now_v7(), vec![1, 2, 3])
        .await
        .expect("the first export with a given archive hash inserts");
    let refused = insert_export(Uuid::now_v7(), vec![1, 2, 3]).await;
    assert!(
        refused.is_err(),
        "a second export row with the same archive content hash must violate uniqueness"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn duplicate_conversation_external_id_within_account_rejected() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let pool = db.database.pool();

    let account_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account_id)
    .bind("acct-dup-conv")
    .execute(pool)
    .await
    .expect("the account inserts");

    for attempt in 0..2 {
        let inserted = sqlx::query(
            "insert into claude_archive.conversations
                 (conversation_id, account_id, external_conversation_id, upstream_state)
             values ($1, $2, 'chat-same-id', 'present')",
        )
        .bind(Uuid::now_v7())
        .bind(account_id)
        .execute(pool)
        .await;
        if attempt == 0 {
            inserted.expect("the first conversation with an identity inserts");
        } else {
            assert!(
                inserted.is_err(),
                "a duplicate conversation external id inside one account must violate uniqueness"
            );
        }
    }

    // The same identity under a DIFFERENT account is not a collision.
    let other_account = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(other_account)
    .bind("acct-other")
    .execute(pool)
    .await
    .expect("the second account inserts");
    sqlx::query(
        "insert into claude_archive.conversations
             (conversation_id, account_id, external_conversation_id, upstream_state)
         values ($1, $2, 'chat-same-id', 'present')",
    )
    .bind(Uuid::now_v7())
    .bind(other_account)
    .execute(pool)
    .await
    .expect("scoping makes the same provider id distinct across accounts");

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn definition_creates_nothing_outside_claude_archive() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let pool = db.database.pool();

    let rows = sqlx::query(
        "select table_schema, table_name from information_schema.tables
         where table_schema not in ('pg_catalog', 'information_schema')
           and table_type = 'BASE TABLE'",
    )
    .fetch_all(pool)
    .await
    .expect("the catalog query succeeds");
    assert!(
        !rows.is_empty(),
        "the definition creates the owned tables in the first place"
    );
    for row in rows {
        assert_eq!(
            row.get::<String, _>("table_schema"),
            "claude_archive",
            "no table appears outside the owned schema"
        );
    }

    // Every foreign key resolves inside the schema: no cross-schema edge.
    let fk_rows = sqlx::query(
        "select distinct ccu.table_schema as referenced_schema
         from information_schema.table_constraints tc
         join information_schema.constraint_column_usage ccu
           on tc.constraint_name = ccu.constraint_name
          and tc.constraint_schema = ccu.constraint_schema
         where tc.constraint_type = 'FOREIGN KEY'
           and tc.constraint_schema = 'claude_archive'",
    )
    .fetch_all(pool)
    .await
    .expect("the constraint query succeeds");
    for row in fk_rows {
        assert_eq!(
            row.get::<String, _>("referenced_schema"),
            "claude_archive",
            "no foreign key crosses the schema boundary"
        );
    }

    db.cleanup().await.expect("cleanup succeeds");
}
