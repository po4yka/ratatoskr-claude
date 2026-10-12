//! The production import ingests the project knowledge files of an archive, so a verified file
//! reaches `completed` and its bytes are kept, and every other outcome stays an explicit gap
//! (XR-021 CONTRACTS.md R4-02).
//!
//! Every case drives the real receipt and `process_pending_once` path with a ZIP archive, never
//! `from_ingest` directly. Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod cumulative;

use cumulative::Series;
use ratatoskr_claude_archive::privacy_deletion::{
    EraseGate, FinalizationFault, PrivacyDeletionExecutionError, PrivacyDeletionExecutor,
    PrivacyDeletionPlanner, RawExportDeletionPlanRequest, ResolvedDeletionBlob,
};
use ratatoskr_claude_archive::test_support::database_url;
use ratatoskr_claude_archive::{Database, PlatformUsers};
use ratatoskr_event_envelope::EventEnvelope;
use ratatoskr_operation_contracts::{OperationReported, OperationStatus};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const GARDEN_PLAN: &[u8] = b"garden plan";
const TAMPERED_PLAN: &[u8] = b"tampered plan";

fn digest_of(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// One knowledge file of the documented synthetic export.
fn file(
    id: &str,
    media_type: &str,
    declared: Option<&str>,
    bytes: Option<&[u8]>,
) -> serde_json::Value {
    let mut file = serde_json::json!({
        "id": id,
        "filename": format!("{id}.txt"),
        "media_type": media_type,
    });
    let object = file.as_object_mut().expect("a file is an object");
    if let Some(declared) = declared {
        object.insert("sha256".to_owned(), serde_json::json!(declared));
    }
    if let Some(bytes) = bytes {
        object.insert("bytes".to_owned(), serde_json::json!(bytes));
    }
    file
}

/// The garden project alone, carrying exactly `files`, plus its conversation.
fn export_with(files: Vec<serde_json::Value>) -> serde_json::Value {
    let mut export = cumulative::base_export();
    let projects = export
        .get_mut("projects")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the base export lists projects");
    projects.truncate(1);
    projects
        .first_mut()
        .and_then(serde_json::Value::as_object_mut)
        .expect("the garden project exists")
        .insert(
            "knowledge_files".to_owned(),
            serde_json::Value::Array(files),
        );
    export
}

fn verified_file() -> serde_json::Value {
    file(
        "knowledge-garden-plan",
        "text/plain",
        Some(&digest_of(GARDEN_PLAN)),
        Some(GARDEN_PLAN),
    )
}

/// What the import recorded for one archive: the run, its report and the Platform operation.
struct Recorded {
    state: String,
    status: String,
    missing_assets: i32,
    counts: serde_json::Value,
    warnings: Vec<String>,
    operation: OperationReported,
}

async fn recorded(series: &Series, operation_id: Uuid, export_id: Uuid) -> Recorded {
    let (state, status, missing_assets, counts, warnings): (
        String,
        String,
        i32,
        serde_json::Value,
        Option<serde_json::Value>,
    ) = sqlx::query_as(
        "select r.state, c.status, c.missing_assets, c.discovered_counts, c.warnings
         from claude_archive.import_runs r
         join claude_archive.completeness_reports c on c.run_id = r.run_id
         where r.export_id = $1",
    )
    .bind(export_id)
    .fetch_one(series.pool())
    .await
    .expect("the run and its completeness report are readable");
    let envelope: serde_json::Value = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1' and aggregate_id like $1",
    )
    .bind(format!("%{operation_id}"))
    .fetch_one(series.pool())
    .await
    .expect("the operation was reported");
    let envelope: EventEnvelope = serde_json::from_value(envelope).expect("a complete envelope");
    Recorded {
        state,
        status,
        missing_assets,
        counts,
        warnings: serde_json::from_value(warnings.unwrap_or_default()).unwrap_or_default(),
        operation: envelope
            .payload_as::<OperationReported>()
            .expect("the payload is an operation report"),
    }
}

/// The columns an import writes for one source.
#[derive(Debug, PartialEq, Eq)]
struct Source {
    id: Uuid,
    blob_ref: Option<String>,
    content_hash: Option<Vec<u8>>,
    byte_size: Option<i64>,
    locally_backed_up: bool,
}

async fn source(series: &Series, external: &str) -> Source {
    let (source_id, blob_ref, content_hash, byte_size, locally_backed_up): (
        Uuid,
        Option<String>,
        Option<Vec<u8>>,
        Option<i64>,
        bool,
    ) = sqlx::query_as(
        "select source_id, blob_ref, content_hash, byte_size, locally_backed_up
         from claude_archive.project_sources where external_source_id = $1",
    )
    .bind(external)
    .fetch_one(series.pool())
    .await
    .expect("the source row exists");
    Source {
        id: source_id,
        blob_ref,
        content_hash,
        byte_size,
        locally_backed_up,
    }
}

async fn project_backup_status(series: &Series) -> String {
    sqlx::query_scalar(
        "select local_backup_status from claude_archive.projects
         where external_project_id = 'project-garden'",
    )
    .fetch_one(series.pool())
    .await
    .expect("the garden project exists")
}

#[tokio::test]
async fn an_archive_with_a_verified_knowledge_file_imports_complete() {
    let series = Series::open("knowledge-verified").await;
    let (export_id, operation_id) = series.import(&export_with(vec![verified_file()])).await;

    let result = recorded(&series, operation_id, export_id).await;
    let row = source(&series, "knowledge-garden-plan").await;
    let observed: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.export_observations
         where export_id = $1 and subject_kind = 'project_source' and subject_id = $2",
    )
    .bind(export_id)
    .bind(row.id)
    .fetch_one(series.pool())
    .await
    .expect("the observation is countable");
    let status = project_backup_status(&series).await;
    let digest = digest_of(GARDEN_PLAN);
    let verified = series
        .store
        .locate(&digest)
        .and_then(|blob| series.store.verify(&blob));
    series.finish().await;

    assert_eq!(result.state, "completed", "the only file is verified");
    assert_eq!(result.status, "complete");
    assert_eq!(result.missing_assets, 0);
    assert_eq!(
        result.counts.get("verified_knowledge_files"),
        Some(&serde_json::json!(1))
    );
    assert!(
        result.warnings.is_empty(),
        "no anomaly: {:?}",
        result.warnings
    );
    assert_eq!(
        row.blob_ref.as_deref(),
        Some(format!("sha256/{digest}").as_str())
    );
    assert_eq!(row.content_hash, Some(Sha256::digest(GARDEN_PLAN).to_vec()));
    assert_eq!(row.byte_size, Some(11));
    assert!(row.locally_backed_up, "a verified file is a local backup");
    assert!(verified.is_ok(), "the blob store verifies the reference");
    assert_eq!(observed, 1, "the importing export observed the source");
    assert_eq!(status, "locally_backed_up");
    assert_eq!(result.operation.status, OperationStatus::Succeeded);
    assert!(
        result.operation.warnings.is_empty(),
        "a complete import carries no warning"
    );
}

/// The shared expectations of a file that is kept as inert evidence and keeps the import partial.
async fn assert_quarantined(label: &str, file: serde_json::Value, evidence: &[u8], warning: &str) {
    let series = Series::open(label).await;
    let (export_id, operation_id) = series.import(&export_with(vec![file])).await;

    let result = recorded(&series, operation_id, export_id).await;
    let row = source(&series, "knowledge-file").await;
    let status = project_backup_status(&series).await;
    let evidence_present = series.store.locate(&digest_of(evidence)).is_ok();
    series.finish().await;

    assert_eq!(result.state, "partial");
    assert_eq!(result.status, "assets_partial");
    assert_eq!(
        result.counts.get("quarantined_knowledge_files"),
        Some(&serde_json::json!(1))
    );
    assert_eq!(result.warnings, vec![warning.to_owned()]);
    assert_eq!(
        row.blob_ref.as_deref(),
        Some(format!("sha256/{}", digest_of(evidence)).as_str()),
        "the evidence blob is kept in the row"
    );
    assert_eq!(row.content_hash, Some(Sha256::digest(evidence).to_vec()));
    assert!(!row.locally_backed_up, "a quarantined file is no backup");
    assert!(evidence_present, "the evidence bytes are retained");
    assert_eq!(status, "reference_only");
    assert_eq!(result.operation.status, OperationStatus::PartiallySucceeded);
    assert!(
        !result.operation.warnings.is_empty(),
        "a partial success carries the incomplete-import warning"
    );
}

#[tokio::test]
async fn a_digest_mismatch_is_quarantined_and_the_import_is_partial() {
    assert_quarantined(
        "knowledge-mismatch",
        file(
            "knowledge-file",
            "text/plain",
            Some(&digest_of(GARDEN_PLAN)),
            Some(TAMPERED_PLAN),
        ),
        TAMPERED_PLAN,
        "knowledge_file_digest_mismatch",
    )
    .await;
}

#[tokio::test]
async fn a_missing_declared_digest_is_quarantined() {
    assert_quarantined(
        "knowledge-undeclared",
        file("knowledge-file", "text/plain", None, Some(GARDEN_PLAN)),
        GARDEN_PLAN,
        "knowledge_file_missing_declared_digest",
    )
    .await;
}

#[tokio::test]
async fn an_invalid_media_type_is_quarantined() {
    assert_quarantined(
        "knowledge-media",
        file(
            "knowledge-file",
            "not a media type",
            Some(&digest_of(GARDEN_PLAN)),
            Some(GARDEN_PLAN),
        ),
        GARDEN_PLAN,
        "knowledge_file_invalid_media_type",
    )
    .await;
}

#[tokio::test]
async fn a_referenced_only_file_stays_a_missing_gap_without_a_blob() {
    let series = Series::open("knowledge-referenced").await;
    let reference = file("knowledge-file", "application/pdf", None, None);
    let (export_id, operation_id) = series.import(&export_with(vec![reference])).await;

    let result = recorded(&series, operation_id, export_id).await;
    let row = source(&series, "knowledge-file").await;
    let status = project_backup_status(&series).await;
    series.finish().await;

    assert_eq!(result.state, "partial");
    assert_eq!(result.status, "assets_partial");
    assert_eq!(result.missing_assets, 1);
    assert_eq!(result.warnings, vec!["knowledge_file_missing_bytes"]);
    assert_eq!(row.blob_ref, None, "no bytes were supplied, so no blob");
    assert_eq!(row.content_hash, None);
    assert!(!row.locally_backed_up);
    assert_eq!(status, "reference_only");
    assert_eq!(result.operation.status, OperationStatus::PartiallySucceeded);
}

#[tokio::test]
async fn a_later_export_that_carries_the_bytes_upgrades_a_referenced_source() {
    let series = Series::open("knowledge-upgrade").await;
    let referenced = file("knowledge-garden-plan", "text/plain", None, None);
    series.import(&export_with(vec![referenced])).await;
    let before = source(&series, "knowledge-garden-plan").await;

    let (export_id, operation_id) = series.import(&export_with(vec![verified_file()])).await;

    let after = source(&series, "knowledge-garden-plan").await;
    let rows: i64 = sqlx::query_scalar("select count(*) from claude_archive.project_sources")
        .fetch_one(series.pool())
        .await
        .expect("the sources are countable");
    let result = recorded(&series, operation_id, export_id).await;
    let status = project_backup_status(&series).await;
    series.finish().await;

    assert!(!before.locally_backed_up && before.blob_ref.is_none());
    assert_eq!(rows, 1, "the second export updates the same source row");
    assert_eq!(after.id, before.id);
    assert!(after.locally_backed_up, "the bytes upgrade the source");
    assert_eq!(
        after.blob_ref.as_deref(),
        Some(format!("sha256/{}", digest_of(GARDEN_PLAN)).as_str())
    );
    assert_eq!(result.state, "completed");
    assert_eq!(status, "locally_backed_up");
}

#[tokio::test]
async fn a_store_failure_while_ingesting_is_retried_not_failed() {
    let series = Series::open("knowledge-store-fault").await;
    let operation_id = series.receive(&export_with(vec![verified_file()])).await;
    // A regular file where the shard directory of the knowledge blob belongs makes the store fail
    // for that one digest and for nothing else the import writes.
    let shard = series.root.join("sha256").join(
        digest_of(GARDEN_PLAN)
            .get(..2)
            .expect("a digest has a prefix"),
    );
    std::fs::write(&shard, b"in the way").expect("the obstacle is written");

    let failed = series.worker.process_pending_once().await;
    let state: String = sqlx::query_scalar("select state from claude_archive.import_runs")
        .fetch_one(series.pool())
        .await
        .expect("the run is readable");
    let reported: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.outbox_events
         where event_type = 'platform.operation.reported.v1'",
    )
    .fetch_one(series.pool())
    .await
    .expect("the reports are countable");

    std::fs::remove_file(&shard).expect("the obstacle is removed");
    let retried = series.worker.process_pending_once().await;
    let export_id: Uuid = sqlx::query_scalar("select export_id from claude_archive.exports")
        .fetch_one(series.pool())
        .await
        .expect("the export is readable");
    let result = recorded(&series, operation_id, export_id).await;
    series.finish().await;

    assert!(failed.is_err(), "the store fault is returned: {failed:?}");
    assert_eq!(state, "received", "the run stays where it was");
    assert_eq!(reported, 0, "nothing is reported for a retryable fault");
    assert_eq!(retried.expect("the next pass succeeds"), 1);
    assert_eq!(result.state, "completed");
    assert_eq!(result.operation.status, OperationStatus::Succeeded);
}

#[tokio::test]
async fn an_eraser_that_removed_a_knowledge_blob_before_registration_does_not_leave_a_dangling_row()
{
    // The account's only export is deleted: its rows are gone, its knowledge blob is listed for
    // erasure and nothing references it yet.
    let series = Series::open("knowledge-erase-race").await;
    let (export_id, _) = series.import(&export_with(vec![verified_file()])).await;
    let wide = Database::connect(
        &database_url(series.db.name()),
        6,
        std::time::Duration::from_secs(10),
    )
    .await
    .expect("a wider pool connects");
    let request = Uuid::now_v7();
    PrivacyDeletionPlanner::new(series.pool().clone())
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: format!("user:{}", cumulative::USER),
            account_id: series.account,
            request_id: request,
            request_key: "knowledge-erase".to_owned(),
            correlation_id: format!("privacy-delete:{request}"),
            export_id,
        })
        .await
        .expect("the export deletion is planned");
    let keys: Vec<String> = sqlx::query_scalar(
        "select blob_ref from claude_archive.privacy_deletion_items
         where request_id = $1 and action = 'erase_blob' and blob_ref is not null",
    )
    .bind(request)
    .fetch_all(series.pool())
    .await
    .expect("the erase items are readable");
    let knowledge_key = format!("sha256/{}", digest_of(GARDEN_PLAN));
    assert!(
        keys.contains(&knowledge_key),
        "the knowledge blob of the deleted project is an erase item"
    );
    let resolved: Vec<ResolvedDeletionBlob> = keys
        .into_iter()
        .map(|key| {
            let digest = key
                .strip_prefix("sha256/")
                .expect("an inventory key is a content address");
            ResolvedDeletionBlob {
                reference: series.store.locate(digest).expect("the blob is stored"),
                key,
            }
        })
        .collect();
    let executor = PrivacyDeletionExecutor::new(wide.pool().clone());
    let stopped = executor
        .finalize_with_blob_store(
            request,
            &series.store,
            &resolved,
            Some(FinalizationFault::AfterRowsCommitted),
        )
        .await;
    assert!(
        matches!(
            stopped,
            Err(PrivacyDeletionExecutionError::Injected(
                FinalizationFault::AfterRowsCommitted
            ))
        ),
        "the deletion stops after its rows are committed and before any byte is erased"
    );

    // The eraser has decided to remove the knowledge blob and holds its digest lock when another
    // account's identical file is ingested and its import is about to register it.
    let gate = EraseGate::for_digest(digest_of(GARDEN_PLAN));
    let eraser = {
        let (executor, store, resolved) = (
            executor.with_erase_gate(gate.clone()),
            series.store.clone(),
            resolved.clone(),
        );
        tokio::spawn(async move {
            executor
                .finalize_with_blob_store(request, &store, &resolved, None)
                .await
                .expect("the byte phase completes");
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.reached())
        .await
        .expect("the eraser reaches the knowledge blob and holds its digest lock");
    let other = cumulative::add_account(&series.db).await;
    let mut second = export_with(vec![verified_file()]);
    second["projects"][0]["description"] = serde_json::json!("another account");
    series.receive_as(other, &second).await;
    let pass = cumulative::worker_over(&series.db, &series.store, PlatformUsers::default());
    let import = tokio::spawn(async move { pass.process_pending_once().await });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let blocked = !import.is_finished();
    gate.release();
    eraser.await.expect("the eraser task ends");
    let passes = import.await.expect("the import task ends");

    let row: Option<(Option<String>,)> = sqlx::query_as(
        "select s.blob_ref from claude_archive.project_sources s
         join claude_archive.projects p on p.project_id = s.project_id
         where p.account_id = $1",
    )
    .bind(other)
    .fetch_optional(series.pool())
    .await
    .expect("the sources are readable");
    let resolves = series
        .store
        .locate(&digest_of(GARDEN_PLAN))
        .and_then(|blob| series.store.verify(&blob));
    wide.close().await;
    series.finish().await;

    assert!(
        blocked,
        "the import waits for the digest lock the eraser holds"
    );
    assert_eq!(passes.expect("the import survives the erasure"), 1);
    assert_eq!(
        row,
        Some((Some(knowledge_key),)),
        "the import committed its source row"
    );
    assert!(
        resolves.is_ok(),
        "the importer stored the blob again after the eraser removed it, so the row resolves: \
         {resolves:?}"
    );
}
