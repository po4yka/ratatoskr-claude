//! The cumulative series shared by the survivor-content tests: three exports of one account that
//! edit a project, a conversation and its parts, and the real deletion path over them.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionExecutor, PrivacyDeletionPlanner, RawExportDeletionPlanRequest,
    ResolvedDeletionBlob,
};
use sha2::Digest as _;
use uuid::Uuid;

use crate::cumulative::{self, Series};

/// The text only the second export holds.
pub(crate) const SECOND: &str = "E2EDIT";
/// The text only the third (newest) export holds.
pub(crate) const THIRD: &str = "E3SECRET";

/// The synthetic export as the `version`th (1 to 3) export of a cumulative series.
///
/// Export 2 edits the garden project description and instructions and a conversation title and
/// appends a part to an existing message; export 3 edits them again, appends another part, adds a
/// message no earlier export holds and a knowledge file only it presents.
pub(crate) fn export(version: u8) -> serde_json::Value {
    let edit = |base: &str| match version {
        1 => base.to_owned(),
        2 => format!("{base} {SECOND}"),
        _ => format!("{base} {THIRD}"),
    };
    let mut export = cumulative::base_export();
    let projects = export
        .get_mut("projects")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the base export lists projects");
    let garden = projects
        .first_mut()
        .and_then(serde_json::Value::as_object_mut)
        .expect("the garden project exists");
    garden.insert(
        "description".to_owned(),
        serde_json::json!(edit("Seasonal notes")),
    );
    garden.insert(
        "instructions".to_owned(),
        serde_json::json!(edit("Use metric units.")),
    );
    let mut files = vec![serde_json::json!({
        "id": "knowledge-garden-plan",
        "filename": "garden-plan.txt",
        "media_type": "text/plain",
        "sha256": "fe75f1cc18ee987f97d34faaded87de72b2ea0c96bca98e0b6c183b30f9c49fe",
        "bytes": b"garden plan",
    })];
    if version == 3 {
        let bytes = format!("{THIRD} knowledge plan").into_bytes();
        files.push(serde_json::json!({
            "id": "knowledge-newest-only",
            "filename": "newest-only.txt",
            "media_type": "text/plain",
            "sha256": format!("{:x}", sha2::Sha256::digest(&bytes)),
            "bytes": bytes,
        }));
    }
    garden.insert(
        "knowledge_files".to_owned(),
        serde_json::Value::Array(files),
    );
    let conversations = export
        .get_mut("conversations")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the base export lists conversations");
    let planting = conversations
        .first_mut()
        .and_then(serde_json::Value::as_object_mut)
        .expect("the planting conversation exists");
    planting.insert(
        "title".to_owned(),
        serde_json::json!(edit("Planting calendar")),
    );
    let messages = planting
        .get_mut("messages")
        .and_then(serde_json::Value::as_array_mut)
        .expect("the planting conversation has messages");
    let first_parts = messages
        .first_mut()
        .and_then(|message| message.get_mut("content"))
        .and_then(serde_json::Value::as_array_mut)
        .expect("the first message has parts");
    if version >= 2 {
        first_parts.push(serde_json::json!({"type": "text", "text": format!("{SECOND} part")}));
    }
    if version == 3 {
        first_parts.push(serde_json::json!({"type": "text", "text": format!("{THIRD} part")}));
        messages.push(serde_json::json!({
            "id": "message-assistant-2",
            "parent_id": "message-assistant-1",
            "role": "assistant",
            "content": [{"type": "text", "text": format!("{THIRD} reply")}]
        }));
    }
    conversations.push(serde_json::json!({
        "id": "conversation-pruning",
        "title": "Pruning",
        "created_at": "2026-08-02T08:00:00Z",
        "messages": [{
            "id": "message-user-2",
            "role": "user",
            "content": [{"type": "text", "text": "When should roses be pruned?"}]
        }]
    }));
    export
}

/// Three cumulative exports of one account, imported in order.
pub(crate) struct Cumulative {
    pub(crate) series: Series,
    /// The export ids in the order the exports were received.
    pub(crate) exports: [Uuid; 3],
}

pub(crate) async fn import_three(label: &str) -> Cumulative {
    import_series(label, [export(1), export(2), export(3)]).await
}

pub(crate) async fn import_series(label: &str, exports: [serde_json::Value; 3]) -> Cumulative {
    let series = Series::open(label).await;
    let mut ids = Vec::new();
    for export in &exports {
        ids.push(series.import(export).await.0);
    }
    let exports = <[Uuid; 3]>::try_from(ids).expect("three exports were imported");
    Cumulative { series, exports }
}

impl Cumulative {
    pub(crate) async fn finish(self) {
        self.series.finish().await;
    }

    pub(crate) fn tenant_ref() -> String {
        format!("user:{}", cumulative::USER)
    }

    /// Runs every phase of the request, byte erasure included.
    pub(crate) async fn execute(&self, request: Uuid) {
        let keys: Vec<String> = sqlx::query_scalar(
            "select blob_ref from claude_archive.privacy_deletion_items
             where request_id = $1 and action = 'erase_blob' and blob_ref is not null",
        )
        .bind(request)
        .fetch_all(self.series.pool())
        .await
        .expect("the erase items are readable");
        let resolved: Vec<ResolvedDeletionBlob> = keys
            .into_iter()
            .map(|key| {
                let digest = key
                    .strip_prefix("sha256/")
                    .expect("an inventory key is a content address");
                ResolvedDeletionBlob {
                    reference: self
                        .series
                        .store
                        .locate(digest)
                        .expect("the blob is stored"),
                    key,
                }
            })
            .collect();
        PrivacyDeletionExecutor::new(self.series.pool().clone())
            .finalize_with_blob_store(request, &self.series.store, &resolved, None)
            .await
            .expect("the deletion completes");
    }

    pub(crate) async fn delete_export(&self, export_id: Uuid) {
        let request = Uuid::now_v7();
        PrivacyDeletionPlanner::new(self.series.pool().clone())
            .plan_raw_export(&RawExportDeletionPlanRequest {
                tenant_ref: Self::tenant_ref(),
                account_id: self.series.account,
                request_id: request,
                request_key: format!("survivor-content-{request}"),
                correlation_id: format!("privacy-delete:{request}"),
                export_id,
            })
            .await
            .expect("the export deletion is planned");
        self.execute(request).await;
    }
}
