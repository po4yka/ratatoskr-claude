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
mod survivor_series;

use ratatoskr_ai_archive_contracts::{AiConversationUpdated, AiProjectUpdated};
use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionPlanner, TenantDeletionPlanRequest,
};
use ratatoskr_event_envelope::EventEnvelope;
use sha2::Digest as _;
use survivor_series::{Cumulative, SECOND, THIRD, export, import_series, import_three};
use uuid::Uuid;

const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";

/// A copy of `version` that differs only by an unknown root field, so the archive has other bytes
/// and the same content.
fn export_repeating(version: u8, label: &str) -> serde_json::Value {
    let mut export = export(version);
    export
        .as_object_mut()
        .expect("the export is an object")
        .insert("exported_by".to_owned(), serde_json::json!(label));
    export
}

/// The content of the shared rows of the garden project and the planting conversation.
#[derive(Debug, PartialEq, Eq)]
struct Shared {
    description: Option<String>,
    instructions: Option<String>,
    title: Option<String>,
    /// `(part_index, body)` of the first message, in order.
    parts: Vec<(i32, String)>,
    messages: Vec<String>,
    /// The title each revision of the conversation recorded, in order.
    revision_titles: Vec<String>,
    /// The descriptions and titles the snapshots keep, sorted.
    snapshot_descriptions: Vec<String>,
    snapshot_titles: Vec<String>,
}

impl Cumulative {
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

    async fn delete_tenant(&self) {
        let request = Uuid::now_v7();
        PrivacyDeletionPlanner::new(self.series.pool().clone())
            .plan_tenant(&TenantDeletionPlanRequest {
                tenant_ref: Self::tenant_ref(),
                account_id: self.series.account,
                request_id: request,
                request_key: "survivor-content-tenant".to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
            })
            .await
            .expect("the tenant deletion is planned");
        self.execute(request).await;
    }

    /// The content of the shared rows, the part and revision history and the snapshots.
    async fn shared(&self) -> Shared {
        let pool = self.series.pool();
        let (description, instructions, title) = sqlx::query_as(
            "select (select description from claude_archive.projects
                     where external_project_id = 'project-garden'),
                    (select instructions from claude_archive.projects
                     where external_project_id = 'project-garden'),
                    (select title from claude_archive.conversations
                     where external_conversation_id = 'conversation-planting')",
        )
        .fetch_one(pool)
        .await
        .expect("the shared rows are readable");
        let parts = sqlx::query_as(
            "select p.part_index, p.body from claude_archive.content_parts p
             join claude_archive.messages m on m.message_id = p.message_id
             where m.external_message_id = 'message-user-1' order by p.part_index",
        )
        .fetch_all(pool)
        .await
        .expect("the parts are readable");
        let messages = sqlx::query_scalar(
            "select m.external_message_id from claude_archive.messages m
             join claude_archive.conversations c on c.conversation_id = m.conversation_id
             where c.external_conversation_id = 'conversation-planting'
             order by m.external_message_id",
        )
        .fetch_all(pool)
        .await
        .expect("the messages are readable");
        let revision_titles = sqlx::query_scalar(
            "select r.raw_record ->> 'title' from claude_archive.revisions r
             join claude_archive.conversations c on c.conversation_id = r.subject_id
             where r.subject_kind = 'conversation'
               and c.external_conversation_id = 'conversation-planting'
             order by r.revision_index",
        )
        .fetch_all(pool)
        .await
        .expect("the revisions are readable");
        let snapshot_descriptions = sqlx::query_scalar(
            "select v.snapshot ->> 'description' from claude_archive.entity_versions v
             join claude_archive.projects p on p.project_id = v.entity_id
             where v.entity_kind = 'project' and p.external_project_id = 'project-garden'
             order by 1",
        )
        .fetch_all(pool)
        .await
        .expect("the project snapshots are readable");
        let snapshot_titles = sqlx::query_scalar(
            "select v.snapshot ->> 'title' from claude_archive.entity_versions v
             join claude_archive.conversations c on c.conversation_id = v.entity_id
             where v.entity_kind = 'conversation'
               and c.external_conversation_id = 'conversation-planting'
             order by 1",
        )
        .fetch_all(pool)
        .await
        .expect("the conversation snapshots are readable");
        Shared {
            description,
            instructions,
            title,
            parts,
            messages,
            revision_titles,
            snapshot_descriptions,
            snapshot_titles,
        }
    }

    /// The tables of the schema that hold `needle` anywhere in a row.
    async fn tables_holding(&self, needle: &str) -> Vec<String> {
        let tables: Vec<String> = sqlx::query_scalar(
            "select table_name from information_schema.tables
             where table_schema = 'claude_archive' and table_type = 'BASE TABLE'
             order by table_name",
        )
        .fetch_all(self.series.pool())
        .await
        .expect("the tables are listed");
        let mut holding = Vec::new();
        for table in tables {
            let rows: i64 = sqlx::query_scalar(&format!(
                "select count(*) from claude_archive.\"{table}\" row where row::text like $1"
            ))
            .bind(format!("%{needle}%"))
            .fetch_one(self.series.pool())
            .await
            .expect("the rows are countable");
            if rows > 0 {
                holding.push(table);
            }
        }
        holding
    }

    /// Every archive fact in the order the pump publishes it.
    async fn facts(&self) -> Vec<EventEnvelope> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "select envelope from claude_archive.outbox_events
             where event_type like 'ai_archive.%' order by occurred_at, event_id",
        )
        .fetch_all(self.series.pool())
        .await
        .expect("the archive facts are readable");
        rows.into_iter()
            .map(|raw| serde_json::from_value(raw).expect("a complete envelope"))
            .collect()
    }

    /// The provider ids of the knowledge sources, sorted.
    async fn sources(&self) -> Vec<String> {
        sqlx::query_scalar(
            "select external_source_id from claude_archive.project_sources order by 1",
        )
        .fetch_all(self.series.pool())
        .await
        .expect("the sources are readable")
    }

    /// The archive id of an export.
    async fn archive_of(&self, export_id: Uuid) -> Uuid {
        sqlx::query_scalar("select ai_archive_id from claude_archive.exports where export_id = $1")
            .bind(export_id)
            .fetch_one(self.series.pool())
            .await
            .expect("the export is readable")
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

/// The blob store files whose bytes contain `needle`.
fn blob_files_holding(root: &std::path::Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("the directory is readable") {
            let path = entry.expect("the entry is readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if std::fs::read(&path)
                .expect("the blob is readable")
                .windows(needle.len())
                .any(|window| window == needle.as_bytes())
            {
                found.push(path);
            }
        }
    }
    found
}

/// The facts published after the last tombstone, which are the ones that follow the deletion.
fn after_the_tombstones(facts: &[EventEnvelope]) -> Vec<EventEnvelope> {
    let last = facts
        .iter()
        .rposition(|fact| fact.event_type.to_wire() == TOMBSTONED)
        .expect("the deletion enqueued a tombstone");
    facts
        .get(last + 1..)
        .expect("the tombstone is inside the list")
        .to_vec()
}

/// The part texts of the first message of the planting conversation in the second export.
fn parts_through(version: u8) -> Vec<(i32, String)> {
    let mut parts = vec![
        (0, "When should basil be planted?".to_owned()),
        (1, "## Climate\nWarm season".to_owned()),
    ];
    if version >= 2 {
        parts.push((2, format!("{SECOND} part")));
    }
    if version >= 3 {
        parts.push((3, format!("{THIRD} part")));
    }
    parts
}

#[tokio::test]
async fn deleting_the_newest_export_restores_the_intermediate_project_conversation_and_parts() {
    let series = import_three("survivor-newest").await;
    let before = series.shared().await;
    series.delete_export(series.exports[2]).await;

    let after = series.shared().await;
    let traces = series.tables_holding(THIRD).await;
    let files = blob_files_holding(&series.series.root, THIRD);
    series.finish().await;

    assert_eq!(
        before.description.as_deref(),
        Some(format!("Seasonal notes {THIRD}").as_str()),
        "before the deletion the shared row holds the newest content"
    );
    assert_eq!(
        after,
        Shared {
            description: Some(format!("Seasonal notes {SECOND}")),
            instructions: Some(format!("Use metric units. {SECOND}")),
            title: Some(format!("Planting calendar {SECOND}")),
            parts: parts_through(2),
            messages: vec![
                "message-assistant-1".to_owned(),
                "message-user-1".to_owned()
            ],
            revision_titles: vec![
                "Planting calendar".to_owned(),
                format!("Planting calendar {SECOND}")
            ],
            snapshot_descriptions: vec![
                "Seasonal notes".to_owned(),
                format!("Seasonal notes {SECOND}")
            ],
            snapshot_titles: vec![
                "Planting calendar".to_owned(),
                format!("Planting calendar {SECOND}")
            ],
        },
        "the project, the conversation title, the parts, the messages, the revision history and \
         the snapshots are those of the retained exports"
    );
    assert_eq!(
        traces,
        Vec::<String>::new(),
        "no table of the schema holds a byte of the deleted export's text"
    );
    assert_eq!(
        files,
        Vec::<std::path::PathBuf>::new(),
        "nor does the blob store"
    );
}

#[tokio::test]
async fn a_knowledge_file_only_the_deleted_export_presented_is_removed_with_its_blob() {
    let series = import_three("survivor-knowledge").await;
    let only_newest = format!(
        "{:x}",
        sha2::Sha256::digest(format!("{THIRD} knowledge plan"))
    );
    let kept = format!("{:x}", sha2::Sha256::digest(b"garden plan"));
    let sources_before: Vec<String> = series.sources().await;
    let present_before = series.series.store.locate(&only_newest).is_ok();
    series.delete_export(series.exports[2]).await;

    let sources = series.sources().await;
    let newest_blob = series.series.store.locate(&only_newest);
    let kept_blob = series
        .series
        .store
        .locate(&kept)
        .and_then(|blob| series.series.store.verify(&blob));
    let on_disk = blob_files_holding(&series.series.root, &format!("{THIRD} knowledge"));
    let status: String = sqlx::query_scalar(
        "select local_backup_status from claude_archive.projects
         where external_project_id = 'project-garden'",
    )
    .fetch_one(series.series.pool())
    .await
    .expect("the garden project exists");
    series.finish().await;

    assert_eq!(
        sources_before,
        vec!["knowledge-garden-plan", "knowledge-newest-only"],
        "the newest export added a source"
    );
    assert!(present_before, "its blob was stored");
    assert_eq!(
        sources,
        vec!["knowledge-garden-plan"],
        "the source no retained export observed is removed, the one they observed stays"
    );
    assert!(
        newest_blob.is_err(),
        "the removed source's blob is absent from the blob store"
    );
    assert_eq!(on_disk, Vec::<std::path::PathBuf>::new());
    assert!(
        kept_blob.is_ok(),
        "the blob of a source a retained export observed stays: {kept_blob:?}"
    );
    assert_eq!(status, "locally_backed_up");
}

#[tokio::test]
async fn deleting_the_middle_export_keeps_the_newest_content() {
    let series = import_three("survivor-middle").await;
    let before = series.shared().await;
    series.delete_export(series.exports[1]).await;

    let after = series.shared().await;
    let traces = series
        .tables_holding(&format!("Seasonal notes {SECOND}"))
        .await;
    let title_traces = series
        .tables_holding(&format!("Planting calendar {SECOND}"))
        .await;
    series.finish().await;

    assert_eq!(after.description, before.description);
    assert_eq!(after.instructions, before.instructions);
    assert_eq!(after.title, before.title);
    assert_eq!(
        after.parts,
        parts_through(3),
        "the parts the newest export also presents stay"
    );
    assert_eq!(
        after.snapshot_descriptions,
        vec![
            "Seasonal notes".to_owned(),
            format!("Seasonal notes {THIRD}")
        ],
        "the deleted export's version is not kept"
    );
    assert_eq!(
        after.revision_titles,
        vec![
            "Planting calendar".to_owned(),
            format!("Planting calendar {THIRD}")
        ]
    );
    assert_eq!(
        traces,
        Vec::<String>::new(),
        "no trace of the deleted export's project text"
    );
    assert_eq!(title_traces, Vec::<String>::new(), "nor of its title");
}

#[tokio::test]
async fn deleting_the_oldest_export_changes_no_content() {
    let series = import_three("survivor-oldest").await;
    let before = series.shared().await;
    series.delete_export(series.exports[0]).await;

    let after = series.shared().await;
    series.finish().await;

    assert_eq!(after.description, before.description);
    assert_eq!(after.instructions, before.instructions);
    assert_eq!(after.title, before.title);
    assert_eq!(after.parts, before.parts);
    assert_eq!(after.messages, before.messages);
    assert_eq!(
        after.snapshot_descriptions,
        vec![
            format!("Seasonal notes {SECOND}"),
            format!("Seasonal notes {THIRD}")
        ],
        "the oldest export's version is not kept"
    );
}

#[tokio::test]
async fn deleting_two_exports_in_turn_restores_the_oldest_content() {
    let series = import_three("survivor-in-turn").await;
    series.delete_export(series.exports[2]).await;
    series.delete_export(series.exports[1]).await;

    let after = series.shared().await;
    let sources = series.sources().await;
    let traces = series.tables_holding(SECOND).await;
    let file_traces = blob_files_holding(&series.series.root, SECOND);
    series.finish().await;

    assert_eq!(
        after,
        Shared {
            description: Some("Seasonal notes".to_owned()),
            instructions: Some("Use metric units.".to_owned()),
            title: Some("Planting calendar".to_owned()),
            parts: parts_through(1),
            messages: vec![
                "message-assistant-1".to_owned(),
                "message-user-1".to_owned()
            ],
            revision_titles: vec!["Planting calendar".to_owned()],
            snapshot_descriptions: vec!["Seasonal notes".to_owned()],
            snapshot_titles: vec!["Planting calendar".to_owned()],
        },
        "the first export's content is all that is left"
    );
    assert_eq!(sources, vec!["knowledge-garden-plan"]);
    assert_eq!(traces, Vec::<String>::new());
    assert_eq!(file_traces, Vec::<std::path::PathBuf>::new());
}

#[tokio::test]
async fn a_part_only_the_deleted_export_appended_is_removed_even_when_the_row_is_current() {
    // The newest export is not cumulative: it retitles the conversation and does not carry the
    // part the second export appended. The row digest is the newest export's, which stays, and the
    // part history still holds text only the deleted export wrote.
    let mut third = export(1);
    third["conversations"][0]["title"] = serde_json::json!(format!("Planting calendar {THIRD}"));
    let series = import_series("survivor-non-cumulative", [export(1), export(2), third]).await;
    let before = series.shared().await;
    series.delete_export(series.exports[1]).await;

    let after = series.shared().await;
    let traces = series.tables_holding(SECOND).await;
    let follow_up: Vec<String> = after_the_tombstones(&series.facts().await)
        .iter()
        .map(|fact| fact.event_type.to_wire())
        .collect();
    series.finish().await;

    assert_eq!(
        before.parts,
        parts_through(2),
        "the part history keeps what the second export appended"
    );
    assert!(
        follow_up.contains(&"ai_archive.conversation.updated.v1".to_owned()),
        "the retained export's fact carried the deleted part, so the conversation is announced \
         again from the rebuilt rows: {follow_up:?}"
    );
    assert_eq!(
        after.parts,
        parts_through(1),
        "the deleted export's part is gone though the row's own digest is a retained export's"
    );
    assert_eq!(
        after.title,
        Some(format!("Planting calendar {THIRD}")),
        "the newest retained export's title stays"
    );
    assert_eq!(
        traces,
        Vec::<String>::new(),
        "no trace of the deleted export's text"
    );
}

#[tokio::test]
async fn an_edit_repeated_by_a_retained_export_is_kept() {
    // The third export repeats everything the second one edited, so the content of the second
    // export survives its deletion.
    let series = import_series(
        "survivor-repeated",
        [export(1), export(2), export_repeating(2, "third")],
    )
    .await;
    series.delete_export(series.exports[1]).await;

    let after = series.shared().await;
    series.finish().await;

    assert_eq!(
        after.description,
        Some(format!("Seasonal notes {SECOND}")),
        "a retained export presents the edit"
    );
    assert_eq!(after.title, Some(format!("Planting calendar {SECOND}")));
    assert_eq!(after.parts, parts_through(2));
    assert_eq!(
        after.snapshot_descriptions,
        vec![
            "Seasonal notes".to_owned(),
            format!("Seasonal notes {SECOND}")
        ],
        "the version stays while a retained export names it"
    );
}

#[tokio::test]
async fn tenant_deletion_leaves_no_snapshot() {
    let series = import_three("survivor-tenant").await;
    let before: i64 = series
        .count("select count(*) from claude_archive.entity_versions")
        .await;
    series.delete_tenant().await;

    let snapshots = series
        .count("select count(*) from claude_archive.entity_versions")
        .await;
    let traces = series.tables_holding(THIRD).await;
    series.finish().await;

    assert!(before > 0, "the series recorded snapshots");
    assert_eq!(snapshots, 0, "no snapshot outlives the tenant");
    assert_eq!(traces, Vec::<String>::new());
}

#[tokio::test]
async fn the_survivor_fact_carries_the_restored_content_under_the_earliest_retained_export() {
    let series = import_three("survivor-fact").await;
    let second_archive = series.archive_of(series.exports[1]).await;
    series.delete_export(series.exports[2]).await;

    let facts = series.facts().await;
    let follow_up = after_the_tombstones(&facts);
    let everything = serde_json::to_string(&facts).expect("the facts serialize");
    series.finish().await;

    let types: Vec<String> = follow_up
        .iter()
        .map(|fact| fact.event_type.to_wire())
        .collect();
    assert_eq!(
        types,
        vec![
            "ai_archive.project.updated.v1",
            "ai_archive.conversation.updated.v1"
        ],
        "the garden project and the planting conversation were last announced by the deleted export"
    );
    let [project, conversation] =
        <[EventEnvelope; 2]>::try_from(follow_up).expect("two facts follow the tombstone");
    let project = project
        .payload_as::<AiProjectUpdated>()
        .expect("the payload decodes");
    project
        .validate()
        .expect("the project fact satisfies its invariants");
    assert_eq!(
        project
            .project
            .description
            .as_ref()
            .map(|text| text.as_str().to_owned()),
        Some(format!("Seasonal notes {SECOND}")),
        "the body is the restored content"
    );
    assert_eq!(
        project
            .project
            .instructions
            .as_ref()
            .map(|text| text.as_str().to_owned()),
        Some(format!("Use metric units. {SECOND}"))
    );
    assert_eq!(
        project.import_provenance.ai_archive_id.0, second_archive,
        "the earliest retained export that presented this content, not the earliest observer"
    );
    let conversation = conversation
        .payload_as::<AiConversationUpdated>()
        .expect("the payload decodes");
    conversation
        .validate()
        .expect("the conversation fact satisfies its invariants");
    assert_eq!(
        conversation
            .conversation
            .title
            .as_ref()
            .map(|title| title.as_str().to_owned()),
        Some(format!("Planting calendar {SECOND}"))
    );
    assert_eq!(
        conversation.conversation.messages.len(),
        2,
        "the newest export's message is gone"
    );
    assert_eq!(
        conversation.import_provenance.ai_archive_id.0,
        second_archive
    );
    assert!(
        !everything.contains(THIRD),
        "no fact left in the outbox carries a byte of the deleted export's text"
    );
}
