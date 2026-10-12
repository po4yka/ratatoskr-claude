//! The restore step of a privacy deletion touches only the accounts the request owns (XR-021
//! CONTRACTS.md R4-03).
//!
//! The scans that find a stale project or a version nobody names must not read or rewrite the rows
//! of another tenant, even when those rows are inconsistent, so the tenant boundary does not depend
//! on every import having committed its rows and observations together.
//!
//! Verified only against the documented synthetic export schema.

mod cumulative;
mod survivor_series;

use survivor_series::import_three;
use uuid::Uuid;

const FOREIGN_TITLE: &str = "FOREIGN-TENANT-ROW";

#[tokio::test]
async fn a_deletion_leaves_the_rows_of_another_account_alone() {
    let series = import_three("restore-scope").await;
    let pool = series.series.pool();
    let other = cumulative::add_account(&series.series.db).await;
    let project = Uuid::now_v7();
    sqlx::query(
        "insert into claude_archive.projects
         (project_id, account_id, external_project_id, title, upstream_state, content_digest)
         values ($1, $2, 'project-foreign', $3, 'present', 'foreign-digest')",
    )
    .bind(project)
    .bind(other)
    .bind(FOREIGN_TITLE)
    .execute(pool)
    .await
    .expect("the foreign project inserts");
    // The row is stale on purpose: an observation names another digest, and a snapshot exists for
    // it and for a digest nobody names.
    sqlx::query(
        "insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id, content_digest)
         values ($1, 'project', $2, 'other-digest')",
    )
    .bind(series.exports[0])
    .bind(project)
    .execute(pool)
    .await
    .expect("the foreign observation inserts");
    for (digest, title) in [("other-digest", "REWRITTEN"), ("unnamed-digest", "UNNAMED")] {
        sqlx::query(
            "insert into claude_archive.entity_versions
             (entity_kind, entity_id, content_digest, snapshot)
             values ('project', $1, $2,
                     jsonb_build_object('title', $3::text, 'description', null,
                                        'instructions', null, 'parser_name', 'p',
                                        'parser_version', '1'))",
        )
        .bind(project)
        .bind(digest)
        .bind(title)
        .execute(pool)
        .await
        .expect("the foreign snapshot inserts");
    }

    series.delete_export(series.exports[1]).await;

    let (title, digest): (Option<String>, Option<String>) = sqlx::query_as(
        "select title, content_digest from claude_archive.projects where project_id = $1",
    )
    .bind(project)
    .fetch_one(pool)
    .await
    .expect("the foreign project is readable");
    let versions: Vec<String> = sqlx::query_scalar(
        "select content_digest from claude_archive.entity_versions
         where entity_id = $1 order by content_digest",
    )
    .bind(project)
    .fetch_all(pool)
    .await
    .expect("the foreign snapshots are readable");
    series.finish().await;

    assert_eq!(
        title.as_deref(),
        Some(FOREIGN_TITLE),
        "the restore did not rewrite another account's project"
    );
    assert_eq!(digest.as_deref(), Some("foreign-digest"));
    assert_eq!(
        versions,
        vec!["other-digest".to_owned(), "unnamed-digest".to_owned()],
        "the restore did not prune another account's snapshots"
    );
}
