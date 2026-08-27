use super::*;

fn canonical_blob_key(reference: &BlobRef) -> String {
    format!("sha256/{}", reference.digest_hex)
}

#[derive(Debug, PartialEq, Eq)]
struct SharedBlobDeletionObservation {
    execution_status: Option<String>,
    shared_action: Option<DeletionAction>,
    shared_bytes: Option<Vec<u8>>,
    raw_blob_absent: bool,
    extracted_blob_absent: bool,
    surviving_source_rows: i64,
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "exclusive and cross-tenant shared bytes form one reachability proof"
)]
async fn tenant_deletion_retains_blob_referenced_by_another_tenant() {
    const SELECTED_ACCOUNT: &str = "b1000000-0000-0000-0000-000000000001";
    const SELECTED_EXPORT: &str = "b2000000-0000-0000-0000-000000000001";
    const REQUEST: &str = "b9000000-0000-0000-0000-000000000001";
    const SHARED_BYTES: &[u8] = b"byte-identical tenant evidence";

    let root = temp_root("privacy-shared-blob");
    let blob_store = BlobStore::open(&root).expect("the canonical BlobStore fixture opens");
    let binary_media =
        MediaType::parse("application/octet-stream").expect("fixture media type is canonical");
    let raw_ref = blob_store
        .store(
            MediaType::parse("application/zip").expect("raw media type is canonical"),
            b"selected exclusive raw archive",
        )
        .expect("selected raw bytes are stored");
    let extracted_ref = blob_store
        .store(binary_media.clone(), b"selected exclusive extracted bytes")
        .expect("selected extracted bytes are stored");
    let shared_ref = blob_store
        .store(binary_media, SHARED_BYTES)
        .expect("shared bytes are stored once by content address");
    let raw_key = canonical_blob_key(&raw_ref);
    let extracted_key = canonical_blob_key(&extracted_ref);
    let shared_key = canonical_blob_key(&shared_ref);

    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r"
        insert into claude_archive.accounts (account_id, external_account_id)
        values
         ('b1000000-0000-0000-0000-000000000001', 'selected-blob-owner'),
         ('b1000000-0000-0000-0000-000000000002', 'surviving-blob-owner');

        insert into claude_archive.projects
         (project_id, account_id, external_project_id, upstream_state, local_backup_status)
        values
         ('b4000000-0000-0000-0000-000000000001',
          'b1000000-0000-0000-0000-000000000001', 'selected-project',
          'present', 'locally_backed_up'),
         ('b4000000-0000-0000-0000-000000000002',
          'b1000000-0000-0000-0000-000000000002', 'surviving-project',
          'present', 'locally_backed_up');
        ",
    )
    .execute(db.database.pool())
    .await
    .expect("selected and surviving tenant roots seed");
    sqlx::query(
        "insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, received_at)
         values ($1, $2, $3, 'consumer_export', decode($4, 'hex'), $5, $6, now())",
    )
    .bind(Uuid::parse_str(SELECTED_EXPORT).expect("selected export UUID is valid"))
    .bind(
        Uuid::parse_str("b2100000-0000-0000-0000-000000000001")
            .expect("archive identity UUID is valid"),
    )
    .bind(Uuid::parse_str(SELECTED_ACCOUNT).expect("selected account UUID is valid"))
    .bind(&raw_ref.digest_hex)
    .bind(&raw_key)
    .bind(i64::try_from(raw_ref.length_bytes).expect("fixture raw length fits"))
    .execute(db.database.pool())
    .await
    .expect("exclusive raw archive reference seeds");
    sqlx::query(
        "insert into claude_archive.extracted_artifacts
         (extracted_artifact_id, export_id, artifact_index, artifact_kind,
          blob_ref, content_hash, byte_size)
         values ($1, $2, 0, 'asset', $3, decode($4, 'hex'), $5)",
    )
    .bind(
        Uuid::parse_str("b3000000-0000-0000-0000-000000000001")
            .expect("extracted artifact UUID is valid"),
    )
    .bind(Uuid::parse_str(SELECTED_EXPORT).expect("selected export UUID is valid"))
    .bind(&extracted_key)
    .bind(&extracted_ref.digest_hex)
    .bind(i64::try_from(extracted_ref.length_bytes).expect("fixture extracted length fits"))
    .execute(db.database.pool())
    .await
    .expect("exclusive extracted reference seeds");
    sqlx::query(
        "insert into claude_archive.project_sources
         (source_id, project_id, external_source_id, source_kind, byte_size,
          content_hash, blob_ref, locally_backed_up)
         values
          ('b5000000-0000-0000-0000-000000000001',
           'b4000000-0000-0000-0000-000000000001', 'selected-shared', 'file',
           $1, decode($2, 'hex'), $3, true),
          ('b5000000-0000-0000-0000-000000000002',
           'b4000000-0000-0000-0000-000000000002', 'surviving-shared', 'file',
           $1, decode($2, 'hex'), $3, true)",
    )
    .bind(i64::try_from(shared_ref.length_bytes).expect("fixture shared length fits"))
    .bind(&shared_ref.digest_hex)
    .bind(&shared_key)
    .execute(db.database.pool())
    .await
    .expect("both tenants reference the byte-identical canonical blob key");

    let request_id = Uuid::parse_str(REQUEST).expect("request fixture UUID is valid");
    let inventory = PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: "tenant:selected-blob-owner".to_owned(),
            account_id: Uuid::parse_str(SELECTED_ACCOUNT)
                .expect("selected account fixture UUID is valid"),
            request_id,
            request_key: "shared-blob-reachability".to_owned(),
            correlation_id: "privacy-red-6-5".to_owned(),
        })
        .await
        .expect("tenant deletion inventory is durably planned");
    let shared_action = inventory
        .items
        .iter()
        .find(|item| item.kind == DeletionItemKind::SharedBlob && item.subject_id == shared_key)
        .map(|item| item.action);
    let resolved_blobs = vec![
        ResolvedDeletionBlob {
            key: raw_key,
            reference: raw_ref.clone(),
        },
        ResolvedDeletionBlob {
            key: extracted_key,
            reference: extracted_ref.clone(),
        },
        ResolvedDeletionBlob {
            key: shared_key,
            reference: shared_ref.clone(),
        },
    ];
    let execution = PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(request_id, &blob_store, &resolved_blobs)
        .await;
    let surviving_source_rows: i64 = sqlx::query_scalar(
        "select count(*) from claude_archive.project_sources
         where source_id = 'b5000000-0000-0000-0000-000000000002'",
    )
    .fetch_one(db.database.pool())
    .await
    .expect("surviving tenant evidence remains queryable");
    let actual = SharedBlobDeletionObservation {
        execution_status: execution.ok().map(|report| report.status),
        shared_action,
        shared_bytes: blob_store.read(&shared_ref).ok(),
        raw_blob_absent: matches!(blob_store.read(&raw_ref), Err(StoreError::Missing { .. })),
        extracted_blob_absent: matches!(
            blob_store.read(&extracted_ref),
            Err(StoreError::Missing { .. })
        ),
        surviving_source_rows,
    };
    let expected = SharedBlobDeletionObservation {
        execution_status: Some("completed".to_owned()),
        shared_action: Some(DeletionAction::RetainShared),
        shared_bytes: Some(SHARED_BYTES.to_vec()),
        raw_blob_absent: true,
        extracted_blob_absent: true,
        surviving_source_rows: 1,
    };

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    remove(&root);
    assert_eq!(
        actual, expected,
        "tenant deletion must erase only exclusive blobs and retain shared bytes for surviving evidence"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct PrivacyResidueSnapshot {
    nonempty_tables: BTreeSet<String>,
    forbidden_hits: Vec<String>,
    raw_blob_absent: bool,
    extracted_blob_absent: bool,
}

async fn privacy_residue_snapshot(
    pool: &sqlx::PgPool,
    forbidden: &[(&str, String)],
    raw_blob_absent: bool,
    extracted_blob_absent: bool,
) -> Result<PrivacyResidueSnapshot, sqlx::Error> {
    let tables: Vec<String> = sqlx::query_scalar(
        "select table_name from information_schema.tables
         where table_schema = 'claude_archive' and table_type = 'BASE TABLE'
         order by table_name",
    )
    .fetch_all(pool)
    .await?;
    let mut nonempty_tables = BTreeSet::new();
    let mut forbidden_hits = Vec::new();
    for table in tables {
        let query =
            format!("select to_jsonb(row_value)::text from claude_archive.\"{table}\" row_value");
        let rows: Vec<String> = sqlx::query_scalar(&query).fetch_all(pool).await?;
        if !rows.is_empty() {
            nonempty_tables.insert(table.clone());
        }
        for row in rows {
            for (label, marker) in forbidden {
                if row.contains(marker) {
                    forbidden_hits.push(format!("{table}:{label}"));
                }
            }
        }
    }
    forbidden_hits.sort();
    forbidden_hits.dedup();
    Ok(PrivacyResidueSnapshot {
        nonempty_tables,
        forbidden_hits,
        raw_blob_absent,
        extracted_blob_absent,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "the privacy residue fixture keeps every forbidden evidence class visible"
)]
#[tokio::test]
async fn tenant_deletion_retains_only_content_free_audit() {
    const ACCOUNT: &str = "c1000000-0000-0000-0000-000000000001";
    const EXPORT: &str = "c2000000-0000-0000-0000-000000000001";
    const REQUEST: &str = "c9000000-0000-0000-0000-000000000001";
    const SOURCE_KEY: &str =
        "sha256/c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
    const ARTIFACT_KEY: &str =
        "sha256/c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2";
    const ASSET_KEY: &str =
        "sha256/c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";

    let root = temp_root("privacy-content-free-residue");
    let blob_store = BlobStore::open(&root).expect("the canonical BlobStore fixture opens");
    let raw_ref = blob_store
        .store(
            MediaType::parse("application/zip").expect("raw media type is canonical"),
            b"private raw archive bytes",
        )
        .expect("private raw bytes are stored");
    let extracted_ref = blob_store
        .store(
            MediaType::parse("application/octet-stream")
                .expect("extracted media type is canonical"),
            b"private extracted bytes",
        )
        .expect("private extracted bytes are stored");
    let raw_key = canonical_blob_key(&raw_ref);
    let extracted_key = canonical_blob_key(&extracted_ref);

    let db = TestDatabase::create()
        .await
        .expect("the current schema provisions a disposable PostgreSQL database");
    sqlx::raw_sql(
        r#"
        insert into claude_archive.accounts
         (account_id, external_account_id, display_name)
        values ('c1000000-0000-0000-0000-000000000001',
                'private-external-account', 'private-account-display');

        insert into claude_archive.exports
         (export_id, ai_archive_id, account_ref, acquisition, archive_hash, blob_ref,
          byte_size, detected_schema, parser_version, received_at)
        values ('c2000000-0000-0000-0000-000000000001',
                'c2100000-0000-0000-0000-000000000001',
                'c1000000-0000-0000-0000-000000000001', 'consumer_export',
                decode(repeat('c0', 32), 'hex'), 'raw-placeholder', 1,
                'private-detected-schema', 'private-parser-version', now());

        insert into claude_archive.import_runs (run_id, export_id, state, warnings)
        values ('c2200000-0000-0000-0000-000000000001',
                'c2000000-0000-0000-0000-000000000001', 'completed',
                '{"private":"private-import-warning"}');
        insert into claude_archive.completeness_reports
         (report_id, run_id, status, warnings)
        values ('c2300000-0000-0000-0000-000000000001',
                'c2200000-0000-0000-0000-000000000001', 'complete',
                '{"private":"private-completeness-warning"}');

        insert into claude_archive.projects
         (project_id, account_id, external_project_id, title, instructions_blob_ref,
          upstream_state, local_backup_status)
        values ('c3000000-0000-0000-0000-000000000001',
                'c1000000-0000-0000-0000-000000000001', 'private-external-project',
                'private-project-title', 'sha256/private-instructions-key',
                'present', 'locally_backed_up');
        insert into claude_archive.project_sources
         (source_id, project_id, external_source_id, source_kind, title, byte_size,
          content_hash, blob_ref, provider_reference, locally_backed_up)
        values ('c3100000-0000-0000-0000-000000000001',
                'c3000000-0000-0000-0000-000000000001', 'private-external-source',
                'file', 'private-source-title', 32, decode(repeat('c1', 32), 'hex'),
                'sha256/c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1',
                'private-provider-reference', true);

        insert into claude_archive.conversations
         (conversation_id, account_id, external_conversation_id, title, upstream_state)
        values ('c4000000-0000-0000-0000-000000000001',
                'c1000000-0000-0000-0000-000000000001',
                'private-external-conversation', 'private-conversation-title', 'present');
        insert into claude_archive.messages
         (message_id, conversation_id, external_message_id, role)
        values ('c4100000-0000-0000-0000-000000000001',
                'c4000000-0000-0000-0000-000000000001',
                'private-external-message', 'user');
        insert into claude_archive.content_parts
         (content_part_id, message_id, part_index, part_kind, body)
        values ('c4200000-0000-0000-0000-000000000001',
                'c4100000-0000-0000-0000-000000000001', 0, 'text',
                'private-message-body');
        insert into claude_archive.content_parts
         (content_part_id, message_id, part_index, part_kind, payload)
        values ('c4200000-0000-0000-0000-000000000002',
                'c4100000-0000-0000-0000-000000000001', 1, 'unknown',
                '{"private":"private-provider-payload"}');

        insert into claude_archive.artifacts
         (artifact_id, project_id, external_artifact_id, title, upstream_state,
          local_backup_status)
        values ('c5000000-0000-0000-0000-000000000001',
                'c3000000-0000-0000-0000-000000000001', 'private-external-artifact',
                'private-artifact-title', 'present', 'locally_backed_up');
        insert into claude_archive.artifact_versions
         (version_id, artifact_id, external_version_id, version_index, blob_ref,
          content_hash, byte_size, raw_record)
        values ('c5100000-0000-0000-0000-000000000001',
                'c5000000-0000-0000-0000-000000000001', 'private-artifact-version', 0,
                'sha256/c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2',
                decode(repeat('c2', 32), 'hex'), 32,
                '{"private":"private-artifact-raw-record"}');
        insert into claude_archive.assets
         (asset_id, asset_kind, external_asset_id, filename, byte_size, content_hash,
          blob_ref, source_id)
        values ('c5200000-0000-0000-0000-000000000001', 'knowledge_file',
                'private-external-asset', 'private-asset-filename.txt', 32,
                decode(repeat('c3', 32), 'hex'),
                'sha256/c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3',
                'c3100000-0000-0000-0000-000000000001');
        insert into claude_archive.external_references
         (reference_id, project_id, provider_reference)
        values ('c5300000-0000-0000-0000-000000000001',
                'c3000000-0000-0000-0000-000000000001',
                'private-external-provider-reference');
        insert into claude_archive.revisions
         (revision_id, subject_kind, subject_id, export_id, revision_index, raw_record)
        values ('c5400000-0000-0000-0000-000000000001', 'conversation',
                'c4000000-0000-0000-0000-000000000001',
                'c2000000-0000-0000-0000-000000000001', 0,
                '{"private":"private-revision-record"}');

        insert into claude_archive.extracted_artifacts
         (extracted_artifact_id, export_id, artifact_index, artifact_kind, blob_ref,
          content_hash, byte_size)
        values ('c5500000-0000-0000-0000-000000000001',
                'c2000000-0000-0000-0000-000000000001', 0, 'asset',
                'extracted-placeholder', decode(repeat('c5', 32), 'hex'), 1);

        insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id)
        values
         ('c2000000-0000-0000-0000-000000000001', 'project',
          'c3000000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'project_source',
          'c3100000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'conversation',
          'c4000000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'message',
          'c4100000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'content_part',
          'c4200000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'content_part',
          'c4200000-0000-0000-0000-000000000002'),
         ('c2000000-0000-0000-0000-000000000001', 'artifact',
          'c5000000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'artifact_version',
          'c5100000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'asset',
          'c5200000-0000-0000-0000-000000000001'),
         ('c2000000-0000-0000-0000-000000000001', 'external_reference',
          'c5300000-0000-0000-0000-000000000001');

        insert into claude_archive.knowledge_analysis_links
         (completion_event_id, ai_archive_id, subject_kind, subject_id,
          content_digest_hex, completed_at)
        values ('c6000000-0000-0000-0000-000000000001',
                'c2100000-0000-0000-0000-000000000001', 'conversation',
                'c4000000-0000-0000-0000-000000000001', repeat('d1', 32), now());
        insert into claude_archive.inbox_events
         (consumer_name, event_id, consumed_at, handler_outcome)
        values ('private-inbox-consumer', 'c6000000-0000-0000-0000-000000000001',
                now(), 'processed');
        insert into claude_archive.outbox_events
         (event_id, event_type, aggregate_type, aggregate_id, envelope,
          payload_digest, correlation_id, tenant_ref, occurred_at)
        values ('c6100000-0000-0000-0000-000000000001',
                'claude.private.seed.v1', 'export',
                'c2000000-0000-0000-0000-000000000001',
                '{"private":"private-outbox-payload"}', decode(repeat('d2', 32), 'hex'),
                'private-seed-correlation', 'tenant:private-owner', now());

        insert into claude_archive.portable_exports
         (portable_export_id, tenant_ref, state, filters, output_blob_ref,
          correlation_id)
        values ('c7000000-0000-0000-0000-000000000001', 'tenant:private-owner',
                'completed', '{"private":"private-lifecycle-filter"}',
                'sha256/private-portable-output', 'private-portable-correlation');
        insert into claude_archive.parser_migration_reports
         (migration_report_id, tenant_ref, operation_key, parser_name, parser_version,
          plan_fingerprint, state, report, correlation_id)
        values ('c7100000-0000-0000-0000-000000000001', 'tenant:private-owner',
                'private-migration-operation', 'private-parser-name', 'private-parser-target',
                decode(repeat('d3', 32), 'hex'), 'completed',
                '{"private":"private-migration-report"}', 'private-migration-correlation');
        "#,
    )
    .execute(db.database.pool())
    .await
    .expect("private evidence spans normalized, raw, event, and lifecycle relations");
    sqlx::query(
        "update claude_archive.exports
         set archive_hash = decode($1, 'hex'), blob_ref = $2, byte_size = $3
         where export_id = $4",
    )
    .bind(&raw_ref.digest_hex)
    .bind(&raw_key)
    .bind(i64::try_from(raw_ref.length_bytes).expect("fixture raw length fits"))
    .bind(Uuid::parse_str(EXPORT).expect("export fixture UUID is valid"))
    .execute(db.database.pool())
    .await
    .expect("raw row names the real canonical BlobStore object");
    sqlx::query(
        "update claude_archive.extracted_artifacts
         set blob_ref = $1, content_hash = decode($2, 'hex'), byte_size = $3
         where extracted_artifact_id = 'c5500000-0000-0000-0000-000000000001'",
    )
    .bind(&extracted_key)
    .bind(&extracted_ref.digest_hex)
    .bind(i64::try_from(extracted_ref.length_bytes).expect("fixture extracted length fits"))
    .execute(db.database.pool())
    .await
    .expect("extracted row names the real canonical BlobStore object");

    let request_id = Uuid::parse_str(REQUEST).expect("request fixture UUID is valid");
    PrivacyDeletionPlanner::new(db.database.pool().clone())
        .plan_tenant(&TenantDeletionPlanRequest {
            tenant_ref: "tenant:private-owner".to_owned(),
            account_id: Uuid::parse_str(ACCOUNT).expect("account fixture UUID is valid"),
            request_id,
            request_key: "content-free-residue".to_owned(),
            correlation_id: "privacy-red-6-7".to_owned(),
        })
        .await
        .expect("private tenant deletion inventory is durably planned");
    PrivacyDeletionExecutor::new(db.database.pool().clone())
        .finalize_with_blob_store(
            request_id,
            &blob_store,
            &[
                ResolvedDeletionBlob {
                    key: raw_key.clone(),
                    reference: raw_ref.clone(),
                },
                ResolvedDeletionBlob {
                    key: extracted_key.clone(),
                    reference: extracted_ref.clone(),
                },
            ],
        )
        .await
        .expect("the tenant deletion reaches terminal residue inspection");

    let forbidden = vec![
        ("external_account", "private-external-account".to_owned()),
        ("account_display", "private-account-display".to_owned()),
        ("raw_digest", raw_ref.digest_hex.clone()),
        ("raw_blob_key", raw_key.clone()),
        ("extracted_digest", extracted_ref.digest_hex.clone()),
        ("extracted_blob_key", extracted_key.clone()),
        ("project_title", "private-project-title".to_owned()),
        ("instructions", "sha256/private-instructions-key".to_owned()),
        ("source_title", "private-source-title".to_owned()),
        (
            "provider_reference",
            "private-provider-reference".to_owned(),
        ),
        ("source_blob", SOURCE_KEY.to_owned()),
        (
            "conversation_title",
            "private-conversation-title".to_owned(),
        ),
        ("message_body", "private-message-body".to_owned()),
        ("provider_payload", "private-provider-payload".to_owned()),
        ("artifact_title", "private-artifact-title".to_owned()),
        ("artifact_blob", ARTIFACT_KEY.to_owned()),
        ("asset_filename", "private-asset-filename.txt".to_owned()),
        ("asset_blob", ASSET_KEY.to_owned()),
        ("revision", "private-revision-record".to_owned()),
        ("outbox_payload", "private-outbox-payload".to_owned()),
        ("lifecycle_filter", "private-lifecycle-filter".to_owned()),
        ("migration_report", "private-migration-report".to_owned()),
    ];
    let actual = privacy_residue_snapshot(
        db.database.pool(),
        &forbidden,
        matches!(blob_store.read(&raw_ref), Err(StoreError::Missing { .. })),
        matches!(
            blob_store.read(&extracted_ref),
            Err(StoreError::Missing { .. })
        ),
    )
    .await
    .expect("every owned table remains queryable for terminal residue inspection");
    let expected = PrivacyResidueSnapshot {
        nonempty_tables: BTreeSet::from([
            "outbox_events".to_owned(),
            "privacy_deletion_audits".to_owned(),
            "privacy_deletion_requests".to_owned(),
        ]),
        forbidden_hits: Vec::new(),
        raw_blob_absent: true,
        extracted_blob_absent: true,
    };

    db.cleanup()
        .await
        .expect("cleanup succeeds before assertion");
    remove(&root);
    assert_eq!(
        actual, expected,
        "terminal tenant deletion may retain only content-free operational evidence"
    );
}
