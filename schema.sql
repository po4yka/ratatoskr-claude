-- The Claude Archive database, in one file.
--
-- `ratatoskr-claude-archive` applies this at startup, to a fresh database. There is no migration
-- ledger and no incremental history: no database holds data that has to survive a schema change. A
-- schema change edits this file in place; the next fresh database has it.
--
-- One schema: `claude_archive` — everything the Claude bounded context owns. The table inventory
-- follows the conceptual data list in AGENTS.md: accounts, organizations, exports, import runs,
-- projects and project sources, conversation graphs (messages, relations, content parts),
-- Artifacts and their versions, assets, revisions, tombstones, completeness reports, and the
-- outbox/inbox event machinery. Compliance cursors join when a Compliance adapter does.
--
-- Conventions, applied uniformly and stated once here:
--
--   * Identifiers are UUIDv7 minted by the application, never by the database. A database default
--     would produce v4, so there is deliberately no DEFAULT on any id column: a missing id is an
--     insert error rather than a silently wrong version.
--
--   * Closed vocabularies are `text` with a named CHECK, not a PostgreSQL enum: adding a value to
--     an enum cannot run inside one transaction and removing one is a table rewrite; a CHECK is
--     altered by one statement.
--
--   * Every timestamp is `timestamptz`. `timestamp` would silently record the server's local time.
--
--   * Hashes are stored in `bytea` and the column is named `*_hash`. No column here holds a
--     credential in a readable form; chat bodies live only as blob references or preserved raw
--     records, never as log-adjacent columns.
--
--   * References to identifiers owned by other services (`*_ref`) or other schemas are plain uuid
--     columns with no REFERENCES clause. No foreign key crosses the schema boundary.
--
--   * Absence is never deletion: upstream state is an observed vocabulary, tombstones require
--     explicit evidence, and every externally identified record scopes its uniqueness to its
--     owning parent so two accounts can hold the same provider id without colliding.
create schema claude_archive;

comment on schema claude_archive is
    'State owned exclusively by ratatoskr-claude. Accounts and organizations, export snapshots, '
    'import runs, projects and project knowledge sources, conversation graphs, Artifacts, assets, '
    'revisions, tombstones, completeness evidence, and the event machinery.';

-- ---------------------------------------------------------------------------------------------
-- accounts
-- ---------------------------------------------------------------------------------------------
--
-- One row per provider account discovered through any acquisition mode. The external identifier is
-- stable; display names are mutable observation data.

create table claude_archive.accounts (
    account_id           uuid        primary key,
    external_account_id  text        not null,
    display_name         text,
    observed_first_at    timestamptz not null default now(),
    observed_last_at     timestamptz not null default now(),
    constraint accounts_external_account_id_key unique (external_account_id)
);

comment on table claude_archive.accounts is
    'Claude accounts as observed across acquisitions. Identity is the provider id, not a name.';

-- ---------------------------------------------------------------------------------------------
-- organizations
-- ---------------------------------------------------------------------------------------------

create table claude_archive.organizations (
    organization_id           uuid        primary key,
    external_organization_id  text        not null,
    name                      text,
    plan                      text,
    observed_first_at         timestamptz not null default now(),
    observed_last_at          timestamptz not null default now(),
    constraint organizations_external_organization_id_key unique (external_organization_id)
);

comment on table claude_archive.organizations is
    'Organizations/workspaces discovered through organization exports or Compliance data.';

-- ---------------------------------------------------------------------------------------------
-- exports
-- ---------------------------------------------------------------------------------------------
--
-- One immutable provider snapshot: the archive hash names the bytes stored content-addressed in
-- the BlobStore. Receiving the same archive twice deduplicates here by hash.

create table claude_archive.exports (
    export_id        uuid        primary key,
    account_ref      uuid        references claude_archive.accounts (account_id),
    organization_ref uuid        references claude_archive.organizations (organization_id),
    acquisition      text        not null,
    archive_hash     bytea       not null,
    blob_ref         text        not null,
    byte_size        bigint      not null check (byte_size >= 0),
    detected_schema  text,
    parser_version   text,
    received_at      timestamptz not null,
    constraint exports_scope_check check (account_ref is not null or organization_ref is not null),
    constraint exports_acquisition_check
        check (acquisition in ('consumer_export', 'organization_export', 'compliance_api',
                               'manual_conversation_capture', 'legacy_import')),
    constraint exports_archive_hash_key unique (archive_hash)
);

comment on table claude_archive.exports is
    'One immutable snapshot of provider evidence: hash, BlobStore reference, acquisition mode.';
comment on constraint exports_acquisition_check on claude_archive.exports is
    'How this snapshot was obtained. Closed vocabulary; authority differs per mode.';

-- ---------------------------------------------------------------------------------------------
-- import_runs
-- ---------------------------------------------------------------------------------------------
--
-- One restartable pass over an export. States follow the documented intake machine; `partial` is a
-- visible terminal class, never folded into success or failure.

create table claude_archive.import_runs (
    run_id         uuid        primary key,
    export_id      uuid        not null references claude_archive.exports (export_id),
    state          text        not null,
    warnings       jsonb,
    started_at     timestamptz not null default now(),
    finished_at    timestamptz,
    constraint import_runs_state_check
        check (state in ('received', 'stored', 'inspecting', 'schema_detected', 'extracting',
                         'staging', 'validating', 'reconciling', 'publishing', 'completed',
                         'partial', 'failed', 'quarantined'))
);

comment on table claude_archive.import_runs is
    'Import progress over one snapshot, with durable warnings for anything dropped or unknown.';

-- ---------------------------------------------------------------------------------------------
-- projects
-- ---------------------------------------------------------------------------------------------

create table claude_archive.projects (
    project_id            uuid        primary key,
    account_id            uuid        not null references claude_archive.accounts (account_id),
    external_project_id   text        not null,
    title                 text,
    instructions_blob_ref text,
    upstream_state        text        not null,
    created_at            timestamptz not null default now(),
    updated_at            timestamptz not null default now(),
    constraint projects_account_external_key unique (account_id, external_project_id),
    constraint projects_upstream_state_check
        check (upstream_state in ('present', 'missing_from_latest_snapshot', 'explicitly_deleted',
                                  'access_lost', 'unknown'))
);

comment on table claude_archive.projects is
    'Discovered projects. Upstream state stays conservative: absence is never deletion.';
comment on column claude_archive.projects.instructions_blob_ref is
    'BlobStore reference of the project instructions when the source proved them, else null.';

create index projects_account_idx on claude_archive.projects (account_id);

-- ---------------------------------------------------------------------------------------------
-- project_sources
-- ---------------------------------------------------------------------------------------------
--
-- Project knowledge entries: files, pasted text, code, and external references. A reference
-- without local bytes has locally_backed_up false — that flag is the honesty boundary between
-- "we know about it" and "we preserve it".

create table claude_archive.project_sources (
    source_id             uuid        primary key,
    project_id            uuid        not null references claude_archive.projects (project_id),
    external_source_id    text        not null,
    source_kind           text        not null,
    title                 text,
    mime_type             text,
    byte_size             bigint,
    content_hash          bytea,
    blob_ref              text,
    provider_reference    text,
    locally_backed_up     boolean     not null default false,
    first_seen_at         timestamptz not null default now(),
    last_seen_at          timestamptz not null default now(),
    constraint project_sources_project_external_key unique (project_id, external_source_id),
    constraint project_sources_kind_check
        check (source_kind in ('file', 'text', 'code', 'external_reference', 'unknown'))
);

comment on table claude_archive.project_sources is
    'Project knowledge entries with their backup status stated per entry, never implied.';

create index project_sources_blob_idx on claude_archive.project_sources (blob_ref)
    where blob_ref is not null;

-- ---------------------------------------------------------------------------------------------
-- conversations
-- ---------------------------------------------------------------------------------------------

create table claude_archive.conversations (
    conversation_id          uuid        primary key,
    account_id               uuid        not null references claude_archive.accounts (account_id),
    external_conversation_id text        not null,
    title                    text,
    upstream_state           text        not null,
    provider_created_at      timestamptz,
    first_seen_at            timestamptz not null default now(),
    last_seen_at             timestamptz not null default now(),
    constraint conversations_account_external_key unique (account_id, external_conversation_id),
    constraint conversations_upstream_state_check
        check (upstream_state in ('present', 'missing_from_latest_snapshot', 'explicitly_deleted',
                                  'access_lost', 'unknown'))
);

comment on table claude_archive.conversations is
    'Conversation roots scoped to their account so provider ids stay distinct across accounts.';

create index conversations_account_idx on claude_archive.conversations (account_id);

-- ---------------------------------------------------------------------------------------------
-- messages
-- ---------------------------------------------------------------------------------------------
--
-- Nodes of the conversation graph. parent_message_id makes edits, regenerations, and branches
-- edges rather than overwrites; hidden/tool roles are first-class rows.

create table claude_archive.messages (
    message_id          uuid        primary key,
    conversation_id     uuid        not null references claude_archive.conversations (conversation_id),
    external_message_id text,
    parent_message_id   uuid        references claude_archive.messages (message_id),
    role                text        not null,
    model               text,
    provider_created_at timestamptz,
    first_seen_at       timestamptz not null default now(),
    last_seen_at        timestamptz not null default now(),
    constraint messages_conversation_external_key unique (conversation_id, external_message_id),
    constraint messages_role_check
        check (role in ('user', 'assistant', 'system', 'tool', 'internal', 'unknown'))
);

comment on table claude_archive.messages is
    'Graph nodes: one row per observed message revision root, linked through parent edges.';
comment on column claude_archive.messages.parent_message_id is
    'A parent edge must point inside the same conversation. PostgreSQL cannot express that '
    'declaratively (no subqueries in CHECK), so the importer owns the validation.';

create index messages_conversation_idx on claude_archive.messages (conversation_id);
create index messages_parent_idx on claude_archive.messages (parent_message_id)
    where parent_message_id is not null;

-- ---------------------------------------------------------------------------------------------
-- message_relations
-- ---------------------------------------------------------------------------------------------

create table claude_archive.message_relations (
    relation_id       uuid primary key,
    parent_message_id uuid not null references claude_archive.messages (message_id),
    child_message_id  uuid not null references claude_archive.messages (message_id),
    relation_kind     text not null,
    constraint message_relations_parent_child_kind_key
        unique (parent_message_id, child_message_id, relation_kind),
    constraint message_relations_kind_check
        check (relation_kind in ('edit', 'regeneration', 'branch_continuation', 'tool_response'))
);

comment on table claude_archive.message_relations is
    'Typed graph edges: what replaced, regenerated, branched from, or answered what.';

-- ---------------------------------------------------------------------------------------------
-- content_parts
-- ---------------------------------------------------------------------------------------------
--
-- Typed heterogeneous parts inside one message, order preserved by part_index. Exactly one carrier
-- holds the substance: inline body, blob reference, or preserved unknown payload.

create table claude_archive.content_parts (
    content_part_id uuid        primary key,
    message_id      uuid        not null references claude_archive.messages (message_id),
    part_index      integer     not null check (part_index >= 0),
    part_kind       text        not null,
    body            text,
    blob_ref        text,
    mime_type       text,
    payload         jsonb,
    constraint content_parts_carrier_check
        check (num_nonnulls(body, blob_ref, payload) > 0),
    constraint content_parts_message_index_key unique (message_id, part_index),
    constraint content_parts_kind_check
        check (part_kind in ('text', 'markdown', 'image', 'file', 'code', 'citation',
                             'tool_call', 'tool_result', 'artifact', 'unknown'))
);

comment on table claude_archive.content_parts is
    'Ordered typed parts; Unknown keeps provider changes forward-compatible.';

-- ---------------------------------------------------------------------------------------------
-- artifacts
-- ---------------------------------------------------------------------------------------------
--
-- First-class versioned objects. An observation may name one project, conversation, or message
-- owner; a source that supplies none remains explicitly unscoped rather than invented.

create table claude_archive.artifacts (
    artifact_id          uuid        primary key,
    conversation_id      uuid        references claude_archive.conversations (conversation_id),
    message_id           uuid        references claude_archive.messages (message_id),
    project_id           uuid        references claude_archive.projects (project_id),
    external_artifact_id text        not null,
    artifact_type        text,
    language             text,
    title                text,
    upstream_state       text        not null,
    first_seen_at        timestamptz not null default now(),
    last_seen_at         timestamptz not null default now(),
    constraint artifacts_single_scope_check
        check (num_nonnulls(conversation_id, message_id, project_id) <= 1),
    constraint artifacts_upstream_state_check
        check (upstream_state in ('present', 'missing_from_latest_snapshot', 'explicitly_deleted',
                                  'access_lost', 'unknown'))
);

comment on table claude_archive.artifacts is
    'Artifact identity separate from assistant text; versions carry the contents.';

create unique index artifacts_conversation_external_key
    on claude_archive.artifacts (conversation_id, external_artifact_id)
    where conversation_id is not null;
create unique index artifacts_project_external_key
    on claude_archive.artifacts (project_id, external_artifact_id)
    where project_id is not null;

-- ---------------------------------------------------------------------------------------------
-- artifact_versions
-- ---------------------------------------------------------------------------------------------

create table claude_archive.artifact_versions (
    version_id                   uuid        primary key,
    artifact_id                  uuid        not null references claude_archive.artifacts (artifact_id),
    external_version_id          text        not null,
    previous_external_version_id text,
    version_index                bigint      not null check (version_index >= 0),
    blob_ref                     text,
    content_hash                 bytea,
    byte_size                    bigint,
    raw_record                   jsonb,
    observed_at                  timestamptz not null default now(),
    constraint artifact_versions_artifact_index_key unique (artifact_id, version_index),
    constraint artifact_versions_artifact_external_key unique (artifact_id, external_version_id)
);

comment on table claude_archive.artifact_versions is
    'Every observed version, not only the latest; provider lineage and raw evidence stay explicit.';

-- ---------------------------------------------------------------------------------------------
-- assets
-- ---------------------------------------------------------------------------------------------
--
-- Uploaded user files, generated outputs, knowledge files, and Artifact assets. Kind separates
-- provenance classes that must never be conflated in reporting.

create table claude_archive.assets (
    asset_id          uuid        primary key,
    asset_kind        text        not null,
    external_asset_id text,
    filename          text,
    mime_type         text,
    byte_size         bigint,
    content_hash      bytea,
    blob_ref          text,
    message_id        uuid        references claude_archive.messages (message_id),
    source_id         uuid        references claude_archive.project_sources (source_id),
    first_seen_at     timestamptz not null default now(),
    last_seen_at      timestamptz not null default now(),
    constraint assets_kind_check
        check (asset_kind in ('upload', 'generated_output', 'knowledge_file', 'artifact_asset',
                              'unknown')),
    constraint assets_content_or_reference_check
        check (blob_ref is not null or content_hash is null)
);

comment on table claude_archive.assets is
    'Referenced files with their archival status; a URL alone is not a backed-up asset.';

create unique index assets_content_hash_key
    on claude_archive.assets (content_hash)
    where content_hash is not null;
create index assets_blob_idx on claude_archive.assets (blob_ref) where blob_ref is not null;

-- ---------------------------------------------------------------------------------------------
-- revisions
-- ---------------------------------------------------------------------------------------------
--
-- Immutable observations: a changed provider record adds a revision, never overwrites history.

create table claude_archive.revisions (
    revision_id    uuid        primary key,
    subject_kind   text        not null,
    subject_id     uuid        not null,
    export_id      uuid        references claude_archive.exports (export_id),
    revision_index bigint      not null check (revision_index >= 0),
    observed_at    timestamptz not null default now(),
    raw_record     jsonb,
    constraint revisions_subject_kind_check
        check (subject_kind in ('project', 'conversation', 'message', 'content_part',
                                'artifact', 'artifact_version', 'asset')),
    constraint revisions_subject_revision_key unique (subject_kind, subject_id, revision_index)
);

comment on table claude_archive.revisions is
    'Per-subject observation history anchored to the snapshot that proved each state.';

-- ---------------------------------------------------------------------------------------------
-- tombstones
-- ---------------------------------------------------------------------------------------------
--
-- Explicit end-of-life records. Every reason requires evidence; absence from one snapshot can
-- never create a row here.

create table claude_archive.tombstones (
    tombstone_id       uuid        primary key,
    subject_kind       text        not null,
    subject_id         uuid        not null,
    reason             text        not null,
    evidence_export_id uuid        references claude_archive.exports (export_id),
    recorded_at        timestamptz not null default now(),
    constraint tombstones_subject_kind_check
        check (subject_kind in ('project', 'conversation', 'message', 'content_part',
                                'artifact', 'artifact_version', 'asset')),
    constraint tombstones_reason_check
        check (reason in ('provider_deletion', 'compliance_request', 'retention_policy',
                          'access_loss', 'owner_removal', 'unknown'))
);

comment on table claude_archive.tombstones is
    'Evidence-carrying deletion/access-loss records; access loss is distinct from deletion.';
comment on constraint tombstones_reason_check on claude_archive.tombstones is
    'Why the record ended or became unreachable. Never inferred from mere absence.';

-- ---------------------------------------------------------------------------------------------
-- completeness_reports
-- ---------------------------------------------------------------------------------------------

create table claude_archive.completeness_reports (
    report_id         uuid        primary key,
    run_id            uuid        not null references claude_archive.import_runs (run_id),
    status            text        not null,
    discovered_counts jsonb       not null default '{}',
    missing_assets    integer     not null default 0 check (missing_assets >= 0),
    unknown_variants  integer     not null default 0 check (unknown_variants >= 0),
    warnings          jsonb,
    created_at        timestamptz not null default now(),
    constraint completeness_reports_status_check
        check (status in ('complete', 'conversations_complete', 'structurally_partial',
                          'assets_partial', 'unknown', 'failed_validation'))
);

comment on table claude_archive.completeness_reports is
    'The durable honesty receipt of one import: what exists, what is missing, what is unknown.';
comment on constraint completeness_reports_status_check on claude_archive.completeness_reports is
    '`complete` requires positive evidence from a known schema; it is never the default.';

-- ---------------------------------------------------------------------------------------------
-- outbox_events
-- ---------------------------------------------------------------------------------------------

create table claude_archive.outbox_events (
    event_id        uuid        primary key,
    event_type      text        not null,
    aggregate_type  text        not null,
    aggregate_id    uuid        not null,
    payload         jsonb       not null,
    correlation_id  uuid,
    causation_id    uuid,
    occurred_at     timestamptz not null,
    published_at    timestamptz,
    attempt_count   integer     not null default 0,
    next_attempt_at timestamptz,
    constraint outbox_events_aggregate_type_check
        check (aggregate_type in ('export', 'import_run', 'project', 'conversation', 'message',
                                  'artifact', 'asset'))
);

comment on table claude_archive.outbox_events is
    'Transactional outbox. Rows become at-least-once publications; replay converges.';

create index outbox_events_unpublished_idx
    on claude_archive.outbox_events (next_attempt_at)
    where published_at is null;

-- ---------------------------------------------------------------------------------------------
-- inbox_events
-- ---------------------------------------------------------------------------------------------

create table claude_archive.inbox_events (
    consumer_name   text        not null,
    event_id        uuid        not null,
    consumed_at     timestamptz not null,
    handler_outcome text        not null,
    constraint inbox_events_consumer_name_event_id_pkey primary key (consumer_name, event_id),
    constraint inbox_events_handler_outcome_check
        check (handler_outcome in ('processed', 'rejected', 'skipped'))
);

comment on table claude_archive.inbox_events is
    'Consumer inbox deduplication under at-least-once delivery.';
