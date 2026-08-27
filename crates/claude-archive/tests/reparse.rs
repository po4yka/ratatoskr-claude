//! Reparse dry-run, idempotence, and omission contracts.

#![allow(clippy::expect_used, reason = "synthetic fixture construction")]

use std::io::Write as _;
use std::sync::Arc;

use ratatoskr_claude_archive::reparse::ReparseEngine;
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{
    AcquisitionMode, BlobStore, Conversation, Database, Limits, ParsedExport, ParserCapability,
    ParserDescriptor, ParserExecutionError, ParserExecutionInput, ParserExecutor, ParserIdentity,
    ParserRegistry, ParserStamp, ReparseChangeKind,
};
use uuid::Uuid;

#[derive(Debug)]
struct FixedParser {
    version: &'static str,
    omit: bool,
}

impl ParserExecutor for FixedParser {
    fn execute(
        &self,
        _input: ParserExecutionInput<'_>,
    ) -> Result<ParsedExport, ParserExecutionError> {
        let stamp = ParserStamp {
            schema_identifier: "claude-reparse-test".to_owned(),
            parser_identifier: "claude-test".to_owned(),
            parser_version: self.version.to_owned(),
        };
        Ok(ParsedExport {
            parser: stamp.clone(),
            conversations: if self.omit {
                Vec::new()
            } else {
                vec![Conversation {
                    external_id: "conversation-1".to_owned(),
                    project_external_id: None,
                    title: format!("shape-{}", self.version),
                    created_at: None,
                    messages: Vec::new(),
                    parser: stamp,
                    unknown_fields: Vec::new(),
                }]
            },
            ..ParsedExport::default()
        })
    }
}

fn limits() -> Limits {
    Limits {
        database_connections: 2,
        database_acquire_timeout_ms: 5_000,
        shutdown_timeout_ms: 5_000,
        max_archive_bytes: 1_048_576,
        max_archive_entries: 32,
        max_entry_bytes: 1_048_576,
        max_total_extracted_bytes: 2_097_152,
        max_compression_ratio: 100,
    }
}

fn zip_bytes() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            "conversations.json",
            zip::write::SimpleFileOptions::default(),
        )
        .expect("entry");
    writer.write_all(b"[]").expect("bytes");
    writer.finish().expect("zip").into_inner()
}

fn registry() -> Result<ParserRegistry, Box<dyn std::error::Error>> {
    let mut registry = ParserRegistry::default();
    for version in ["1.0", "2.0"] {
        registry.register_compiled(
            ParserDescriptor::new(
                "claude-test",
                version,
                vec![AcquisitionMode::ConsumerExport],
                vec!["claude-reparse-test".to_owned()],
                vec![ParserCapability::Conversations],
            ),
            Arc::new(FixedParser {
                version,
                omit: false,
            }),
        )?;
    }
    Ok(registry)
}

fn omitting_registry() -> Result<ParserRegistry, Box<dyn std::error::Error>> {
    let mut registry = ParserRegistry::default();
    for (version, omit) in [("1.0", false), ("2.0", true)] {
        registry.register_compiled(
            ParserDescriptor::new(
                "claude-test",
                version,
                vec![AcquisitionMode::ConsumerExport],
                vec!["claude-reparse-test".to_owned()],
                vec![ParserCapability::Conversations],
            ),
            Arc::new(FixedParser { version, omit }),
        )?;
    }
    Ok(registry)
}

async fn seed(
    db: &Database,
    blobs: &BlobStore,
) -> Result<(Uuid, Uuid, Uuid), Box<dyn std::error::Error>> {
    let tenant = Uuid::now_v7();
    let archive = Uuid::now_v7();
    let export = Uuid::now_v7();
    let conversation = Uuid::now_v7();
    let raw = blobs.store(
        ratatoskr_claude_archive::MediaType::parse("application/zip")?,
        &zip_bytes(),
    )?;
    sqlx::query(
        "insert into claude_archive.accounts (account_id, external_account_id) values ($1,$2)",
    )
    .bind(tenant)
    .bind(format!("reparse-{tenant}"))
    .execute(db.pool())
    .await?;
    sqlx::query("insert into claude_archive.exports (export_id,ai_archive_id,account_ref,acquisition,archive_hash,blob_ref,byte_size,detected_schema,parser_version,received_at) values ($1,$2,$3,'consumer_export',$4,$5,$6,'claude-reparse-test','1.0',now())")
        .bind(export).bind(archive).bind(tenant).bind(hex_bytes(&raw.digest_hex)).bind(format!("sha256/{}", raw.digest_hex)).bind(i64::try_from(raw.length_bytes)?).execute(db.pool()).await?;
    sqlx::query("insert into claude_archive.conversations (conversation_id,account_id,external_conversation_id,title,upstream_state) values ($1,$2,'conversation-1','shape-1.0','present')").bind(conversation).bind(tenant).execute(db.pool()).await?;
    sqlx::query("insert into claude_archive.export_observations (export_id,subject_kind,subject_id) values ($1,'conversation',$2)").bind(export).bind(conversation).execute(db.pool()).await?;
    Ok((tenant, archive, export))
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).expect("hex"), 16).expect("hex"))
        .collect()
}

async fn counts(db: &Database, export: Uuid) -> Result<(i64, i64, i64, i64), sqlx::Error> {
    sqlx::query_as("select (select count(*) from claude_archive.reparse_runs where export_id=$1),(select count(*) from claude_archive.revisions where export_id=$1),(select count(*) from claude_archive.extracted_artifacts where export_id=$1),(select count(*) from claude_archive.outbox_events where aggregate_id=$1::text)").bind(export).fetch_one(db.pool()).await
}

async fn evidence_counts(
    db: &Database,
    export: Uuid,
) -> Result<(i64, i64, i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "select
          (select count(*) from claude_archive.reparse_runs where export_id=$1),
          (select count(*) from claude_archive.revisions where export_id=$1),
          (select count(*) from claude_archive.extracted_artifacts where export_id=$1),
          (select count(*) from claude_archive.completeness_reports cr join claude_archive.import_runs ir on ir.run_id=cr.run_id where ir.export_id=$1),
          (select count(*) from claude_archive.outbox_events where aggregate_id=$1::text)",
    )
    .bind(export)
    .fetch_one(db.pool())
    .await
}

#[tokio::test]
async fn reparse_dry_run_matches_immediate_apply_without_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = TestDatabase::create().await?;
    let db = &fixture.database;
    let root = std::env::temp_dir().join(format!("claude-reparse-{}", Uuid::now_v7()));
    let blobs = BlobStore::open(&root)?;
    let (tenant, archive, export) = seed(db, &blobs).await?;
    let engine = ReparseEngine::new(db.pool().clone(), blobs, Arc::new(registry()?), limits());
    let before = counts(db, export).await?;
    let plan = engine
        .plan(tenant, archive, ParserIdentity::new("claude-test", "2.0"))
        .await?;
    assert_eq!(
        before,
        counts(db, export).await?,
        "dry-run must write no rows"
    );
    let applied = engine.apply(&plan).await?;
    assert_eq!(
        plan.report, applied,
        "dry-run and immediate apply report must match"
    );
    assert_eq!(
        applied
            .changes
            .iter()
            .filter(|c| c.kind == ReparseChangeKind::Changed)
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(root);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reparse_apply_is_idempotent_for_same_fingerprints()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = TestDatabase::create().await?;
    let db = &fixture.database;
    let root = std::env::temp_dir().join(format!("claude-reparse-{}", Uuid::now_v7()));
    let blobs = BlobStore::open(&root)?;
    let (tenant, archive, export) = seed(db, &blobs).await?;
    let engine = ReparseEngine::new(db.pool().clone(), blobs, Arc::new(registry()?), limits());
    let plan = engine
        .plan(tenant, archive, ParserIdentity::new("claude-test", "2.0"))
        .await?;
    let first = engine.apply(&plan).await?;
    let before_replay = evidence_counts(db, export).await?;
    assert_eq!(
        before_replay,
        (1, 1, 1, 1, 1),
        "one apply must persist each evidence class exactly once"
    );
    let replay = engine.apply(&plan).await?;
    assert_eq!(replay, first, "replay must return the original report");
    assert_eq!(
        evidence_counts(db, export).await?,
        before_replay,
        "replay must add no evidence"
    );
    let _ = std::fs::remove_dir_all(root);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reparse_omission_retains_existing_subject_with_warning()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = TestDatabase::create().await?;
    let db = &fixture.database;
    let root = std::env::temp_dir().join(format!("claude-reparse-{}", Uuid::now_v7()));
    let blobs = BlobStore::open(&root)?;
    let (tenant, archive, _export) = seed(db, &blobs).await?;
    let engine = ReparseEngine::new(
        db.pool().clone(),
        blobs,
        Arc::new(omitting_registry()?),
        limits(),
    );
    let plan = engine
        .plan(tenant, archive, ParserIdentity::new("claude-test", "2.0"))
        .await?;
    assert!(
        plan.report
            .changes
            .iter()
            .any(|change| change.subject_id == "conversation-1"
                && change.kind == ReparseChangeKind::ProposedRemoval)
    );
    assert!(
        plan.report
            .warnings
            .iter()
            .any(|warning| warning.code == "coverage_omission")
    );
    assert!(
        !plan
            .report
            .event_subjects
            .iter()
            .any(|subject| subject.contains("tombstone"))
    );
    engine.apply(&plan).await?;
    let retained: bool = sqlx::query_scalar("select exists(select 1 from claude_archive.conversations where account_id=$1 and external_conversation_id='conversation-1')").bind(tenant).fetch_one(db.pool()).await?;
    let tombstones: i64 = sqlx::query_scalar("select count(*) from claude_archive.outbox_events where tenant_ref=$1 and event_type='ai_archive.subject.tombstoned.v1'").bind(tenant.to_string()).fetch_one(db.pool()).await?;
    assert!(retained);
    assert_eq!(tombstones, 0);
    let _ = std::fs::remove_dir_all(root);
    fixture.cleanup().await?;
    Ok(())
}
