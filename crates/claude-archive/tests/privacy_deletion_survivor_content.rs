//! A privacy deletion leaves no content of the deleted export behind, and a record that a retained
//! export also holds is put back to a version a retained export presented (XR-021 CONTRACTS.md
//! R4-03).
//!
//! A project, conversation, message and content part exists once per account however many
//! cumulative exports contain it, so the newest export's text lives in the shared row. Every case
//! imports three cumulative exports through the real receipt and import path, deletes through the
//! real planner and executor, and asserts the content of the rows, never only their provenance.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod cumulative;

use cumulative::Series;
use sha2::Digest as _;
use uuid::Uuid;

/// The text only the second export holds.
const SECOND: &str = "E2EDIT";
/// The text only the third (newest) export holds.
const THIRD: &str = "E3SECRET";

/// The synthetic export as the `version`th (1 to 3) export of a cumulative series.
///
/// Export 2 edits the garden project description and instructions and a conversation title and
/// appends a part to an existing message; export 3 edits them again, appends another part, adds a
/// message no earlier export holds and a knowledge file only it presents.
fn export(version: u8) -> serde_json::Value {
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
struct Cumulative {
    series: Series,
    /// The export ids in the order the exports were received.
    exports: [Uuid; 3],
}

async fn import_three(label: &str) -> Cumulative {
    let series = Series::open(label).await;
    let mut exports = Vec::new();
    for version in 1..=3 {
        exports.push(series.import(&export(version)).await.0);
    }
    let exports = <[Uuid; 3]>::try_from(exports).expect("three exports were imported");
    Cumulative { series, exports }
}

impl Cumulative {
    async fn finish(self) {
        self.series.finish().await;
    }

    /// `(export, digest, text)` of every version of one record that an export presented, in
    /// export order, where `text` is the `field` of the version's snapshot.
    async fn presented(
        &self,
        kind: &str,
        external: &str,
        field: &str,
    ) -> Vec<(Uuid, String, String)> {
        let table = if kind == "project" {
            "projects p on p.project_id = o.subject_id and p.external_project_id"
        } else {
            "conversations p on p.conversation_id = o.subject_id and p.external_conversation_id"
        };
        sqlx::query_as(&format!(
            "select o.export_id, o.content_digest, v.snapshot ->> $3
             from claude_archive.export_observations o
             join claude_archive.{table} = $2
             join claude_archive.entity_versions v
               on v.entity_kind = o.subject_kind and v.entity_id = o.subject_id
              and v.content_digest = o.content_digest
             where o.subject_kind = $1 order by o.export_id"
        ))
        .bind(kind)
        .bind(external)
        .bind(field)
        .fetch_all(self.series.pool())
        .await
        .expect("the presented versions are readable")
    }

    /// How many messages of the planting conversation each export observed, in export order.
    async fn presented_messages(&self) -> Vec<(Uuid, i64)> {
        sqlx::query_as(
            "select o.export_id, count(*) from claude_archive.export_observations o
             join claude_archive.messages m on m.message_id = o.subject_id
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where o.subject_kind = 'message'
               and c.external_conversation_id = 'conversation-planting'
             group by o.export_id order by o.export_id",
        )
        .fetch_all(self.series.pool())
        .await
        .expect("the message observations are readable")
    }

    /// The garden description and the planting title the shared rows hold now.
    async fn shared_content(&self) -> (Option<String>, Option<String>) {
        sqlx::query_as(
            "select (select description from claude_archive.projects
                     where external_project_id = 'project-garden'),
                    (select title from claude_archive.conversations
                     where external_conversation_id = 'conversation-planting')",
        )
        .fetch_one(self.series.pool())
        .await
        .expect("the shared rows are readable")
    }

    /// How many snapshots one record keeps.
    async fn version_count(&self, kind: &str, external: &str) -> i64 {
        sqlx::query_scalar(
            "select count(*) from claude_archive.entity_versions v
             left join claude_archive.projects p
               on v.entity_kind = 'project' and p.project_id = v.entity_id
             left join claude_archive.conversations c
               on v.entity_kind = 'conversation' and c.conversation_id = v.entity_id
             where v.entity_kind = $1
               and coalesce(p.external_project_id, c.external_conversation_id) = $2",
        )
        .bind(kind)
        .bind(external)
        .fetch_one(self.series.pool())
        .await
        .expect("the snapshots are countable")
    }

    async fn count(&self, query: &str) -> i64 {
        sqlx::query_scalar(query)
            .fetch_one(self.series.pool())
            .await
            .expect("the rows are countable")
    }
}

#[tokio::test]
async fn a_cumulative_series_records_each_exports_own_version() {
    let series = import_three("survivor-versions").await;

    let descriptions = series
        .presented("project", "project-garden", "description")
        .await;
    let titles = series
        .presented("conversation", "conversation-planting", "title")
        .await;
    let project_versions = series.version_count("project", "project-garden").await;
    let conversation_versions = series
        .version_count("conversation", "conversation-planting")
        .await;
    let pruning_versions = series
        .version_count("conversation", "conversation-pruning")
        .await;
    let undigested = series
        .count(
            "select count(*) from claude_archive.export_observations
             where subject_kind in ('project', 'conversation') and content_digest is null",
        )
        .await;
    let stray_digests = series
        .count(
            "select count(*) from claude_archive.export_observations
             where subject_kind not in ('project', 'conversation')
               and content_digest is not null",
        )
        .await;
    let presented_messages = series.presented_messages().await;
    let (project_now, title_now) = series.shared_content().await;
    let exports = series.exports;
    series.finish().await;

    let texts = |rows: &[(Uuid, String, String)]| -> Vec<String> {
        rows.iter().map(|(_, _, text)| text.clone()).collect()
    };
    assert_eq!(
        texts(&descriptions),
        vec![
            "Seasonal notes".to_owned(),
            format!("Seasonal notes {SECOND}"),
            format!("Seasonal notes {THIRD}"),
        ],
        "each export's observation names the version of the project it presented"
    );
    assert_eq!(
        descriptions
            .iter()
            .map(|(export, _, _)| *export)
            .collect::<Vec<_>>(),
        exports.to_vec()
    );
    assert_eq!(
        texts(&titles),
        vec![
            "Planting calendar".to_owned(),
            format!("Planting calendar {SECOND}"),
            format!("Planting calendar {THIRD}"),
        ],
        "each export's observation names the version of the conversation it presented"
    );
    assert_eq!(
        project_versions, 3,
        "one snapshot per distinct project version"
    );
    assert_eq!(
        conversation_versions, 3,
        "one snapshot per distinct conversation version"
    );
    assert_eq!(
        pruning_versions, 1,
        "a conversation every export presents unchanged keeps one snapshot"
    );
    assert_eq!(
        undigested, 0,
        "every project and conversation observation names its version"
    );
    assert_eq!(
        stray_digests, 0,
        "only projects and conversations carry a version"
    );
    assert_eq!(
        presented_messages,
        vec![(exports[0], 2), (exports[1], 2), (exports[2], 3)],
        "an export observes the messages it presented and no others"
    );
    assert_eq!(
        project_now.as_deref(),
        Some(format!("Seasonal notes {THIRD}").as_str()),
        "the shared project row holds the newest content"
    );
    assert_eq!(
        title_now.as_deref(),
        Some(format!("Planting calendar {THIRD}").as_str()),
        "the shared conversation row holds the newest content"
    );
}
