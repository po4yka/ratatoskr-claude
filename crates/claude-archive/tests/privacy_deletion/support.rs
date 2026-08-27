use std::collections::{BTreeMap, BTreeSet};

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::privacy_deletion::{
    ConversationDeletionPlanRequest, DeletionAction, DeletionCompletionReport, DeletionInventory,
    DeletionInventoryItem, DeletionItemKind, FinalizationFault, PrivacyDeletionError,
    PrivacyDeletionExecutionError, PrivacyDeletionExecutor, PrivacyDeletionPlanner,
    RawExportDeletionPlanRequest, ResolvedDeletionBlob, TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{BlobRef, BlobStore, MediaType, StoreError};
use uuid::Uuid;

const ACCOUNT_ID: &str = "10000000-0000-0000-0000-000000000001";
const REQUEST_ID: &str = "60000000-0000-0000-0000-000000000001";
const EXPORT_ID: &str = "20000000-0000-0000-0000-000000000001";
const IMPORT_RUN_ID: &str = "21000000-0000-0000-0000-000000000001";
const PROJECT_ID: &str = "30000000-0000-0000-0000-000000000001";
const SOURCE_ID: &str = "31000000-0000-0000-0000-000000000001";
const CONVERSATION_ID: &str = "40000000-0000-0000-0000-000000000001";
const MESSAGE_ONE_ID: &str = "41000000-0000-0000-0000-000000000001";
const MESSAGE_TWO_ID: &str = "41000000-0000-0000-0000-000000000002";
const RELATION_ID: &str = "42000000-0000-0000-0000-000000000001";
const CONTENT_PART_ID: &str = "43000000-0000-0000-0000-000000000001";
const ARTIFACT_ID: &str = "50000000-0000-0000-0000-000000000001";
const ARTIFACT_VERSION_ID: &str = "51000000-0000-0000-0000-000000000001";
const EXTERNAL_REFERENCE_ID: &str = "52000000-0000-0000-0000-000000000001";
const ASSET_ID: &str = "53000000-0000-0000-0000-000000000001";
const REVISION_ID: &str = "54000000-0000-0000-0000-000000000001";
const COMPLETENESS_REPORT_ID: &str = "55000000-0000-0000-0000-000000000001";
const EXTRACTED_ARTIFACT_ID: &str = "56000000-0000-0000-0000-000000000001";
const COMPLETION_EVENT_ID: &str = "57000000-0000-0000-0000-000000000001";
const OUTBOX_EVENT_ID: &str = "58000000-0000-0000-0000-000000000001";
const RAW_BLOB: &str = "owned/sha256/raw-archive";
const EXTRACTED_BLOB: &str = "owned/sha256/extracted-artifact";
const SHARED_BLOB: &str = "owned/sha256/shared-evidence";

const SEED_SQL: &str = r#"
insert into claude_archive.accounts (account_id, external_account_id)
values
 ('10000000-0000-0000-0000-000000000001', 'selected-account'),
 ('10000000-0000-0000-0000-000000000002', 'retained-account');

insert into claude_archive.exports
 (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref, byte_size,
  detected_schema, parser_version, received_at)
values
 ('20000000-0000-0000-0000-000000000001', '11000000-0000-0000-0000-000000000001',
  '10000000-0000-0000-0000-000000000001', 'consumer_export', decode(repeat('11', 32), 'hex'),
  'owned/sha256/raw-archive', 11, 'consumer-export', 'claude-consumer@1',
  '2026-01-01T00:00:00Z');

insert into claude_archive.import_runs (run_id, export_id, state)
values ('21000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000001', 'completed');

insert into claude_archive.projects
 (project_id, account_id, external_project_id, upstream_state, local_backup_status)
values
 ('30000000-0000-0000-0000-000000000001',
  '10000000-0000-0000-0000-000000000001', 'selected-project', 'present', 'locally_backed_up'),
 ('30000000-0000-0000-0000-000000000002',
  '10000000-0000-0000-0000-000000000002', 'retained-project', 'present', 'locally_backed_up');

insert into claude_archive.project_sources
 (source_id, project_id, external_source_id, source_kind, byte_size, content_hash, blob_ref,
  locally_backed_up)
values
 ('31000000-0000-0000-0000-000000000001',
  '30000000-0000-0000-0000-000000000001', 'selected-source', 'file', 7,
  decode(repeat('33', 32), 'hex'), 'owned/sha256/shared-evidence', true),
 ('31000000-0000-0000-0000-000000000002',
  '30000000-0000-0000-0000-000000000002', 'retained-source', 'file', 7,
  decode(repeat('33', 32), 'hex'), 'owned/sha256/shared-evidence', true);

insert into claude_archive.conversations
 (conversation_id, account_id, external_conversation_id, upstream_state, local_backup_status)
values ('40000000-0000-0000-0000-000000000001',
        '10000000-0000-0000-0000-000000000001', 'selected-conversation',
        'present', 'locally_backed_up');

insert into claude_archive.messages
 (message_id, conversation_id, external_message_id, parent_message_id, role)
values
 ('41000000-0000-0000-0000-000000000001',
  '40000000-0000-0000-0000-000000000001', 'message-one', null, 'user'),
 ('41000000-0000-0000-0000-000000000002',
  '40000000-0000-0000-0000-000000000001', 'message-two',
  '41000000-0000-0000-0000-000000000001', 'assistant');

insert into claude_archive.message_relations
 (relation_id, parent_message_id, child_message_id, relation_kind)
values ('42000000-0000-0000-0000-000000000001',
        '41000000-0000-0000-0000-000000000001',
        '41000000-0000-0000-0000-000000000002', 'branch_continuation');

-- An unknown typed part is both the retained unknown-provider record and its normalized part.
insert into claude_archive.content_parts
 (content_part_id, message_id, part_index, part_kind, payload)
values ('43000000-0000-0000-0000-000000000001',
        '41000000-0000-0000-0000-000000000001', 0, 'unknown',
        '{"provider_kind":"future_variant"}');

insert into claude_archive.artifacts
 (artifact_id, project_id, external_artifact_id, upstream_state, local_backup_status)
values ('50000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001', 'selected-artifact',
        'present', 'locally_backed_up');

insert into claude_archive.artifact_versions
 (version_id, artifact_id, external_version_id, version_index, raw_record)
values ('51000000-0000-0000-0000-000000000001',
        '50000000-0000-0000-0000-000000000001', 'version-one', 0, '{}');

insert into claude_archive.external_references (reference_id, project_id, provider_reference)
values ('52000000-0000-0000-0000-000000000001',
        '30000000-0000-0000-0000-000000000001', 'opaque-provider-reference');

insert into claude_archive.assets
 (asset_id, asset_kind, external_asset_id, byte_size, content_hash, blob_ref, source_id)
values ('53000000-0000-0000-0000-000000000001', 'knowledge_file', 'selected-asset', 7,
        decode(repeat('33', 32), 'hex'), 'owned/sha256/shared-evidence',
        '31000000-0000-0000-0000-000000000001');

insert into claude_archive.revisions
 (revision_id, subject_kind, subject_id, export_id, revision_index, raw_record)
values ('54000000-0000-0000-0000-000000000001', 'conversation',
        '40000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000001', 0, '{}');

insert into claude_archive.completeness_reports (report_id, run_id, status)
values ('55000000-0000-0000-0000-000000000001',
        '21000000-0000-0000-0000-000000000001', 'complete');

insert into claude_archive.extracted_artifacts
 (extracted_artifact_id, export_id, artifact_index, artifact_kind, blob_ref, content_hash, byte_size)
values ('56000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000001', 0, 'entry',
        'owned/sha256/extracted-artifact', decode(repeat('22', 32), 'hex'), 13);

insert into claude_archive.export_observations (export_id, subject_kind, subject_id)
values
 ('20000000-0000-0000-0000-000000000001', 'project',
  '30000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'project_source',
  '31000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'conversation',
  '40000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'message',
  '41000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'message',
  '41000000-0000-0000-0000-000000000002'),
 ('20000000-0000-0000-0000-000000000001', 'content_part',
  '43000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'artifact',
  '50000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'artifact_version',
  '51000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'asset',
  '53000000-0000-0000-0000-000000000001'),
 ('20000000-0000-0000-0000-000000000001', 'external_reference',
  '52000000-0000-0000-0000-000000000001');

insert into claude_archive.knowledge_analysis_links
 (completion_event_id, ai_archive_id, subject_kind, subject_id, content_digest_hex, completed_at)
values ('57000000-0000-0000-0000-000000000001',
        '11000000-0000-0000-0000-000000000001', 'conversation',
        '40000000-0000-0000-0000-000000000001', repeat('44', 32),
        '2026-01-02T00:00:00Z');

insert into claude_archive.inbox_events
 (consumer_name, event_id, consumed_at, handler_outcome)
values ('knowledge-analysis-completed', '57000000-0000-0000-0000-000000000001',
        '2026-01-02T00:00:00Z', 'processed');

insert into claude_archive.outbox_events
 (event_id, event_type, aggregate_type, aggregate_id, envelope, payload_digest,
  correlation_id, tenant_ref, occurred_at)
values ('58000000-0000-0000-0000-000000000001', 'claude.export.ingested.v1', 'export',
        '20000000-0000-0000-0000-000000000001', '{}', decode(repeat('55', 32), 'hex'),
        'seed-correlation', 'tenant:selected', '2026-01-02T00:00:00Z');
"#;

fn item(
    kind: DeletionItemKind,
    subject_id: &str,
    action: DeletionAction,
    blob_ref: Option<&str>,
) -> DeletionInventoryItem {
    DeletionInventoryItem {
        kind,
        subject_id: subject_id.to_owned(),
        action,
        blob_ref: blob_ref.map(ToOwned::to_owned),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the complete closure stays visible as one reviewed golden inventory"
)]
fn expected_inventory(request_id: Uuid) -> DeletionInventory {
    let items = vec![
        item(
            DeletionItemKind::RawArchive,
            EXPORT_ID,
            DeletionAction::EraseBlob,
            Some(RAW_BLOB),
        ),
        item(
            DeletionItemKind::ExtractedArtifact,
            EXTRACTED_ARTIFACT_ID,
            DeletionAction::EraseBlob,
            Some(EXTRACTED_BLOB),
        ),
        item(
            DeletionItemKind::ImportRun,
            IMPORT_RUN_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::CompletenessReport,
            COMPLETENESS_REPORT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::UnknownRecord,
            CONTENT_PART_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Project,
            PROJECT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::ProjectSource,
            SOURCE_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Conversation,
            CONVERSATION_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Message,
            MESSAGE_ONE_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Message,
            MESSAGE_TWO_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::MessageRelation,
            RELATION_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::ContentPart,
            CONTENT_PART_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Artifact,
            ARTIFACT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::ArtifactVersion,
            ARTIFACT_VERSION_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Asset,
            ASSET_ID,
            DeletionAction::RemoveRecord,
            Some(SHARED_BLOB),
        ),
        item(
            DeletionItemKind::ExternalReference,
            EXTERNAL_REFERENCE_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Revision,
            REVISION_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::AnalysisLink,
            COMPLETION_EVENT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Inbox,
            COMPLETION_EVENT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::Outbox,
            OUTBOX_EVENT_ID,
            DeletionAction::RemoveRecord,
            None,
        ),
        item(
            DeletionItemKind::DownstreamTombstone,
            CONVERSATION_ID,
            DeletionAction::EmitTombstone,
            None,
        ),
        item(
            DeletionItemKind::SharedBlob,
            SHARED_BLOB,
            DeletionAction::RetainShared,
            Some(SHARED_BLOB),
        ),
    ];
    let category_totals = items
        .iter()
        .fold(BTreeMap::new(), |mut totals, inventory_item| {
            *totals.entry(inventory_item.kind).or_insert(0) += 1;
            totals
        });
    DeletionInventory {
        request_id,
        items,
        category_totals,
    }
}

#[tokio::test]
async fn deletion_inventory_enumerates_complete_scope() {
    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(SEED_SQL)
        .execute(db.database.pool())
        .await
        .expect("the complete tenant and shared-evidence fixture seeds");

    let request_id = Uuid::parse_str(REQUEST_ID).expect("request fixture UUID is valid");
    let planner = PrivacyDeletionPlanner::new(db.database.pool().clone());
    let actual = planner
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: "tenant:selected".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT_ID).expect("account fixture UUID is valid"),
            request_id,
            request_key: "complete-closure".to_owned(),
            correlation_id: "privacy-red-5-1".to_owned(),
        })
        .await
        .expect("inventory planning reaches the behavioral assertion");
    let expected = expected_inventory(request_id);

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    assert_eq!(
        actual, expected,
        "inventory must enumerate every seeded item and derive totals from those items"
    );
}

#[derive(Debug, PartialEq, Eq)]
enum PublicPlanResult {
    Accepted,
    NotFound,
    Unexpected(String),
}

fn public_result(result: Result<DeletionInventory, PrivacyDeletionError>) -> PublicPlanResult {
    match result {
        Ok(_) => PublicPlanResult::Accepted,
        Err(PrivacyDeletionError::NotFound) => PublicPlanResult::NotFound,
        Err(error) => PublicPlanResult::Unexpected(error.to_string()),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ScopeObservation {
    foreign_result: PublicPlanResult,
    unknown_result: PublicPlanResult,
    privacy_requests: i64,
    privacy_items: i64,
    privacy_audits: i64,
    privacy_tombstones: i64,
    foreign_exports: i64,
}

#[tokio::test]
async fn deletion_scope_does_not_disclose_cross_tenant_subjects() {
    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r"
        insert into claude_archive.accounts (account_id, external_account_id)
        values
         ('71000000-0000-0000-0000-000000000001', 'tenant-a-account'),
         ('71000000-0000-0000-0000-000000000002', 'tenant-b-account');

        insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, received_at)
        values
         ('72000000-0000-0000-0000-000000000002',
          '73000000-0000-0000-0000-000000000002',
          '71000000-0000-0000-0000-000000000002', 'consumer_export',
          decode(repeat('72', 32), 'hex'), 'owned/sha256/tenant-b-raw', 12,
          '2026-01-03T00:00:00Z');
        ",
    )
    .execute(db.database.pool())
    .await
    .expect("two tenants and tenant B's raw export seed");

    let planner = PrivacyDeletionPlanner::new(db.database.pool().clone());
    let account_a = Uuid::parse_str("71000000-0000-0000-0000-000000000001")
        .expect("tenant A fixture UUID is valid");
    let foreign = planner
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: "tenant:a".to_owned(),
            account_id: account_a,
            request_id: Uuid::parse_str("74000000-0000-0000-0000-000000000001")
                .expect("foreign request UUID is valid"),
            request_key: "foreign-export".to_owned(),
            correlation_id: "privacy-red-5-3-foreign".to_owned(),
            export_id: Uuid::parse_str("72000000-0000-0000-0000-000000000002")
                .expect("tenant B export UUID is valid"),
        })
        .await;
    let unknown = planner
        .plan_raw_export(&RawExportDeletionPlanRequest {
            tenant_ref: "tenant:a".to_owned(),
            account_id: account_a,
            request_id: Uuid::parse_str("74000000-0000-0000-0000-000000000002")
                .expect("unknown request UUID is valid"),
            request_key: "unknown-export".to_owned(),
            correlation_id: "privacy-red-5-3-unknown".to_owned(),
            export_id: Uuid::parse_str("72000000-0000-0000-0000-000000000099")
                .expect("unknown export UUID is valid"),
        })
        .await;

    let mutation_counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "select
           (select count(*) from claude_archive.privacy_deletion_requests),
           (select count(*) from claude_archive.privacy_deletion_items),
           (select count(*) from claude_archive.privacy_deletion_audits),
           (select count(*) from claude_archive.outbox_events
             where event_type = 'ai_archive.subject.tombstoned.v1'),
           (select count(*) from claude_archive.exports
             where export_id = '72000000-0000-0000-0000-000000000002')",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("privacy and foreign-evidence counts remain queryable");
    let actual = ScopeObservation {
        foreign_result: public_result(foreign),
        unknown_result: public_result(unknown),
        privacy_requests: mutation_counts.0,
        privacy_items: mutation_counts.1,
        privacy_audits: mutation_counts.2,
        privacy_tombstones: mutation_counts.3,
        foreign_exports: mutation_counts.4,
    };
    let expected = ScopeObservation {
        foreign_result: PublicPlanResult::NotFound,
        unknown_result: PublicPlanResult::NotFound,
        privacy_requests: 0,
        privacy_items: 0,
        privacy_audits: 0,
        privacy_tombstones: 0,
        foreign_exports: 1,
    };

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    assert_eq!(
        actual, expected,
        "foreign and unknown raw-export scopes must be indistinguishable and mutation-free"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct ConversationClosureObservation {
    raw_archive_ids: Vec<String>,
    conversation_actions: BTreeMap<String, DeletionAction>,
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the complete multi-export provenance fixture is reviewed as one behavior"
)]
async fn conversation_plan_includes_containing_archives_and_only_unprovenanced_collateral() {
    const ACCOUNT: &str = "81000000-0000-0000-0000-000000000001";
    const CONTAINING_ONE: &str = "82000000-0000-0000-0000-000000000001";
    const CONTAINING_TWO: &str = "82000000-0000-0000-0000-000000000002";
    const RETAINED_EXPORT: &str = "82000000-0000-0000-0000-000000000003";
    const TARGET: &str = "83000000-0000-0000-0000-000000000001";
    const RETAINED_SIBLING: &str = "83000000-0000-0000-0000-000000000002";
    const COLLATERAL_SIBLING: &str = "83000000-0000-0000-0000-000000000003";

    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r"
        insert into claude_archive.accounts (account_id, external_account_id)
        values ('81000000-0000-0000-0000-000000000001', 'conversation-owner');

        insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, received_at)
        values
         ('82000000-0000-0000-0000-000000000001',
          '82100000-0000-0000-0000-000000000001',
          '81000000-0000-0000-0000-000000000001', 'consumer_export',
          decode(repeat('81', 32), 'hex'), 'owned/sha256/containing-one', 11,
          '2026-02-01T00:00:00Z'),
         ('82000000-0000-0000-0000-000000000002',
          '82100000-0000-0000-0000-000000000002',
          '81000000-0000-0000-0000-000000000001', 'consumer_export',
          decode(repeat('82', 32), 'hex'), 'owned/sha256/containing-two', 12,
          '2026-02-02T00:00:00Z'),
         ('82000000-0000-0000-0000-000000000003',
          '82100000-0000-0000-0000-000000000003',
          '81000000-0000-0000-0000-000000000001', 'consumer_export',
          decode(repeat('83', 32), 'hex'), 'owned/sha256/retained-third', 13,
          '2026-02-03T00:00:00Z');

        insert into claude_archive.conversations
         (conversation_id, account_id, external_conversation_id, upstream_state)
        values
         ('83000000-0000-0000-0000-000000000001',
          '81000000-0000-0000-0000-000000000001', 'target', 'present'),
         ('83000000-0000-0000-0000-000000000002',
          '81000000-0000-0000-0000-000000000001', 'retained-sibling', 'present'),
         ('83000000-0000-0000-0000-000000000003',
          '81000000-0000-0000-0000-000000000001', 'collateral-sibling', 'present');

        insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id)
        values
         ('82000000-0000-0000-0000-000000000001', 'conversation',
          '83000000-0000-0000-0000-000000000001'),
         ('82000000-0000-0000-0000-000000000001', 'conversation',
          '83000000-0000-0000-0000-000000000002'),
         ('82000000-0000-0000-0000-000000000002', 'conversation',
          '83000000-0000-0000-0000-000000000001'),
         ('82000000-0000-0000-0000-000000000002', 'conversation',
          '83000000-0000-0000-0000-000000000003'),
         ('82000000-0000-0000-0000-000000000003', 'conversation',
          '83000000-0000-0000-0000-000000000002');
        ",
    )
    .execute(db.database.pool())
    .await
    .expect("multi-export conversation provenance fixture seeds");

    let planner = PrivacyDeletionPlanner::new(db.database.pool().clone());
    let inventory = planner
        .plan_conversation(&ConversationDeletionPlanRequest {
            tenant_ref: "tenant:conversation-owner".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account fixture UUID is valid"),
            request_id: Uuid::parse_str("84000000-0000-0000-0000-000000000001")
                .expect("request fixture UUID is valid"),
            request_key: "conversation-closure".to_owned(),
            correlation_id: "privacy-red-5-5".to_owned(),
            conversation_id: Uuid::parse_str(TARGET)
                .expect("target conversation fixture UUID is valid"),
        })
        .await
        .expect("conversation planning reaches the behavioral assertion");

    let raw_archive_ids = inventory
        .items
        .iter()
        .filter(|item| item.kind == DeletionItemKind::RawArchive)
        .map(|item| item.subject_id.clone())
        .collect();
    let conversation_actions = inventory
        .items
        .iter()
        .filter(|item| item.kind == DeletionItemKind::Conversation)
        .map(|item| (item.subject_id.clone(), item.action))
        .collect();
    let actual = ConversationClosureObservation {
        raw_archive_ids,
        conversation_actions,
    };
    let expected = ConversationClosureObservation {
        raw_archive_ids: vec![CONTAINING_ONE.to_owned(), CONTAINING_TWO.to_owned()],
        conversation_actions: BTreeMap::from([
            (TARGET.to_owned(), DeletionAction::RemoveRecord),
            (RETAINED_SIBLING.to_owned(), DeletionAction::RetainEvidenced),
            (COLLATERAL_SIBLING.to_owned(), DeletionAction::RemoveRecord),
        ]),
    };

    let retained_export_count: i64 =
        sqlx::query_scalar("select count(*) from claude_archive.exports where export_id = $1")
            .bind(Uuid::parse_str(RETAINED_EXPORT).expect("retained export fixture UUID is valid"))
            .fetch_one(db.database.pool())
            .await
            .expect("retained raw evidence remains queryable");
    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");

    assert_eq!(
        retained_export_count, 1,
        "planning must not mutate retained raw evidence"
    );
    assert_eq!(
        actual, expected,
        "conversation scope must remove all containing raw evidence and only collateral without retained provenance"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct AtomicFinalizationObservation {
    fault_was_injected: bool,
    normalized_rows: i64,
    provenance_rows: i64,
    terminal_audits: i64,
    user_requested_tombstones: i64,
    request_state: String,
}

#[path = "execution_cases.rs"]
mod execution_cases;

#[path = "erasure_cases.rs"]
mod erasure_cases;
