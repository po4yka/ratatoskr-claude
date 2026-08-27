//! The durable import-run state machine: guarded transitions, idempotent
//! replays, restart recovery, and finality of terminal states, proven against
//! disposable databases created from the one schema definition.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::time::Duration;

use ratatoskr_claude_archive::test_support::{TestDatabase, database_url};
use ratatoskr_claude_archive::{
    Database, ImportError, ImportRunStore, ImportState, TransitionOutcome,
};
use sqlx::Row as _;
use uuid::Uuid;

/// The documented linear pipeline, in order, ending at the terminal state.
const PIPELINE: [ImportState; 10] = [
    ImportState::Received,
    ImportState::Stored,
    ImportState::Inspecting,
    ImportState::SchemaDetected,
    ImportState::Extracting,
    ImportState::Staging,
    ImportState::Validating,
    ImportState::Reconciling,
    ImportState::Publishing,
    ImportState::Completed,
];

/// Seeds one account and one export, returning the export id.
///
/// Rows are inserted directly because tenant provisioning belongs to later
/// plan items; the state machine only needs a referentially valid export.
async fn seeded_export(pool: &sqlx::PgPool) -> Uuid {
    let account_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1, $2)",
    )
    .bind(account_id)
    .bind(format!("acct-import-state-{account_id}"))
    .execute(pool)
    .await
    .expect("the account inserts");

    let export_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.exports
             (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size, received_at)
         values ($1, $2, $3, 'consumer_export', $4, 'sha256/aa/bb', 10, now())",
    )
    .bind(export_id)
    .bind(Uuid::now_v7())
    .bind(account_id)
    .bind(account_id.as_bytes().to_vec())
    .execute(pool)
    .await
    .expect("the export inserts");

    export_id
}

/// Reads the recorded state text straight from the table, bypassing the
/// store under test, so persistence itself is observed independently.
async fn raw_state(pool: &sqlx::PgPool, run_id: Uuid) -> Option<String> {
    sqlx::query("select state from claude_archive.import_runs where run_id = $1")
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .expect("the state query succeeds")
        .map(|row| row.get::<String, _>("state"))
}

/// Inserts an import run directly in an arbitrary legal state, for tests
/// that need a starting point no legitimate sequence would produce.
async fn seed_run_in_state(pool: &sqlx::PgPool, export_id: Uuid, state: &str) -> Uuid {
    let run_id = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.import_runs (run_id, export_id, state) values ($1, $2, $3)",
    )
    .bind(run_id)
    .bind(export_id)
    .bind(state)
    .execute(pool)
    .await
    .expect("the direct run insert succeeds");
    run_id
}

#[tokio::test]
async fn fresh_run_starts_at_received_and_reads_back() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let store = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();

    let created = store
        .create_initial(run_id, export_id)
        .await
        .expect("creating the initial run succeeds");

    assert_eq!(
        created,
        ImportState::Received,
        "a fresh run begins in the initial pipeline state"
    );
    assert_eq!(
        store.current(run_id).await.expect("reading back succeeds"),
        ImportState::Received,
        "the initial state is durably recorded"
    );
    assert_eq!(
        raw_state(db.database.pool(), run_id).await.as_deref(),
        Some("received"),
        "the database holds exactly the initial vocabulary text"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn pipeline_advances_through_documented_successors_persistently() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let store = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();
    store
        .create_initial(run_id, export_id)
        .await
        .expect("the run starts");

    for window in PIPELINE.windows(2) {
        let (from, to) = (window[0], window[1]);

        let outcome = store
            .advance(run_id, from, to)
            .await
            .unwrap_or_else(|error| panic!("advancing {from:?} to {to:?} succeeds: {error}"));
        assert_eq!(outcome, TransitionOutcome::Advanced, "the edge applies");

        let recorded = store.current(run_id).await.expect("reading back succeeds");
        assert_eq!(recorded, to, "the successor is the recorded state");
        assert_eq!(
            raw_state(db.database.pool(), run_id).await.as_deref(),
            Some(to.as_str()),
            "the successor survives an independent raw read"
        );
    }

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn transition_from_unexpected_origin_is_refused_without_change() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let store = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();
    store
        .create_initial(run_id, export_id)
        .await
        .expect("the run starts");
    // Walk to `extracting` through legal edges.
    for pair in [
        (ImportState::Received, ImportState::Stored),
        (ImportState::Stored, ImportState::Inspecting),
        (ImportState::Inspecting, ImportState::SchemaDetected),
        (ImportState::SchemaDetected, ImportState::Extracting),
    ] {
        store
            .advance(run_id, pair.0, pair.1)
            .await
            .expect("each pipeline edge applies");
    }

    // A command whose expected origin (`staging`) differs from the recorded
    // state (`extracting`) and whose target differs from it too must refuse.
    let refused = store
        .advance(run_id, ImportState::Staging, ImportState::Validating)
        .await
        .expect_err("a divergent origin must conflict");

    match refused {
        ImportError::Conflict { current, .. } => {
            assert_eq!(
                current,
                ImportState::Extracting,
                "the conflict names the recorded state"
            );
        }
        other => panic!("expected a conflict, got: {other}"),
    }
    assert_eq!(
        raw_state(db.database.pool(), run_id).await.as_deref(),
        Some("extracting"),
        "the refusal leaves the recorded state unchanged"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn undocumented_edge_is_refused_without_touching_the_record() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let store = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();
    store
        .create_initial(run_id, export_id)
        .await
        .expect("the run starts");

    // `received -> publishing` skips every documented intermediate state.
    let refused = store
        .advance(run_id, ImportState::Received, ImportState::Publishing)
        .await
        .expect_err("an undocumented edge must be refused before any query");

    match refused {
        ImportError::InvalidTransition { from, to } => {
            assert_eq!(from, ImportState::Received);
            assert_eq!(to, ImportState::Publishing);
        }
        other => panic!("expected an invalid-transition refusal, got: {other}"),
    }
    assert_eq!(
        raw_state(db.database.pool(), run_id).await.as_deref(),
        Some("received"),
        "the refusal leaves the recorded state unchanged"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn replaying_applied_transition_reports_already_applied() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let store = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();
    store
        .create_initial(run_id, export_id)
        .await
        .expect("the run starts");

    let first = store
        .advance(run_id, ImportState::Received, ImportState::Stored)
        .await
        .expect("the first application advances");
    assert_eq!(first, TransitionOutcome::Advanced);

    let replay = store
        .advance(run_id, ImportState::Received, ImportState::Stored)
        .await
        .expect("replaying the same command is success, never an error");
    assert_eq!(
        replay,
        TransitionOutcome::AlreadyApplied,
        "the replay reports applied-no-change"
    );
    assert_eq!(
        raw_state(db.database.pool(), run_id).await.as_deref(),
        Some("stored"),
        "the replay changes nothing"
    );

    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn run_resumes_after_simulated_crash_mid_pipeline() {
    let db = TestDatabase::create()
        .await
        .expect("a disposable database applies the definition");
    let before = ImportRunStore::new(&db.database);
    let export_id = seeded_export(db.database.pool()).await;
    let run_id = Uuid::now_v7();
    before
        .create_initial(run_id, export_id)
        .await
        .expect("the run starts");
    // Advance partway: received -> ... -> extracting.
    for pair in [
        (ImportState::Received, ImportState::Stored),
        (ImportState::Stored, ImportState::Inspecting),
        (ImportState::Inspecting, ImportState::SchemaDetected),
        (ImportState::SchemaDetected, ImportState::Extracting),
    ] {
        before
            .advance(run_id, pair.0, pair.1)
            .await
            .expect("edges apply");
    }

    // Simulate the crash: drop every pooled connection to the database.
    db.database.close().await;

    // A brand-new process opens its own connection and reads the record.
    let reopened = Database::connect(&database_url(db.name()), 2, Duration::from_secs(5))
        .await
        .expect("a fresh process connects");
    let after = ImportRunStore::new(&reopened);

    let resumed = after
        .current(run_id)
        .await
        .expect("the fresh process reads the last recorded state");
    assert_eq!(
        resumed,
        ImportState::Extracting,
        "progress survived the crash exactly as recorded"
    );

    // The remaining edges apply with no revisited state.
    for pair in [
        (ImportState::Extracting, ImportState::Staging),
        (ImportState::Staging, ImportState::Validating),
        (ImportState::Validating, ImportState::Reconciling),
        (ImportState::Reconciling, ImportState::Publishing),
        (ImportState::Publishing, ImportState::Completed),
    ] {
        after
            .advance(run_id, pair.0, pair.1)
            .await
            .expect("resume edges apply");
    }
    assert_eq!(
        raw_state(reopened.pool(), run_id).await.as_deref(),
        Some("completed"),
        "the resumed run finishes at completion"
    );

    reopened.close().await;
    db.cleanup().await.expect("cleanup succeeds");
}

#[tokio::test]
async fn terminal_states_accept_no_further_transitions() {
    let terminals = [
        ("completed", ImportState::Completed),
        ("partial", ImportState::Partial),
        ("failed", ImportState::Failed),
        ("quarantined", ImportState::Quarantined),
    ];

    for (text, state) in terminals {
        let db = TestDatabase::create()
            .await
            .expect("a disposable database applies the definition");
        let store = ImportRunStore::new(&db.database);
        let export_id = seeded_export(db.database.pool()).await;
        let run_id = seed_run_in_state(db.database.pool(), export_id, text).await;

        // Any move to a different state must fail; the origin named here is
        // deliberately plausible so only finality can refuse it.
        let refused = store
            .advance(run_id, ImportState::Received, ImportState::Stored)
            .await
            .expect_err("a terminal run refuses movement to another state");

        match refused {
            ImportError::Conflict { current, .. } => {
                assert_eq!(
                    current, state,
                    "the conflict names the terminal state {text}"
                );
            }
            other => panic!("expected a conflict for {text}, got: {other}"),
        }
        assert_eq!(
            raw_state(db.database.pool(), run_id).await.as_deref(),
            Some(text),
            "the terminal run stays exactly where it was"
        );

        db.cleanup().await.expect("cleanup succeeds");
    }
}
