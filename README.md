# Ratatoskr Claude

`ratatoskr-claude` is the Claude archive bounded context for Ratatoskr. It preserves official Claude exports and supported Compliance data as immutable evidence, normalizes projects, project knowledge, conversation graphs, files, and Artifacts, and publishes searchable local projections without relying on a live Claude browser session.

> **Status:** implementation plan items 1–6 are complete: a Rust service runs locally against PostgreSQL with typed strict configuration, structured telemetry, operator health routes (`/health/live`, `/health/ready`, `/metrics`, `/version`), typed errors, a content-addressed BlobStore adapter with capped streaming ingest, and the first-version `claude_archive` schema applied at startup. Authenticated tenant-scoped archive receipt exists: claims are verified before storage, archives are hashed and counted while streaming under a configured byte cap, raw bytes land write-once behind fleet-shaped `BlobRef`s, re-delivered digests report an explicit duplicate outcome, and every accepted receipt starts a durable, crash-resumable import run. Plan item 3 adds bounded, non-executing ZIP inspection, bounded direct extraction to content-addressed BlobRefs with raw-digest provenance, active-media quarantine, and a versioned exact-match parser-declaration registry. Plan item 4 adds one exact consumer-export parser for the documented synthetic schema: it maps projects, conversations, messages, ordered text/Markdown parts, and Artifacts; stamps records with parser provenance; retains unknown fields and content variants as inert JSON evidence; and has committed deterministic golden tests. Plan item 5 promotes project instructions to first-class records, maps synthetic Project Knowledge references, classifies available bytes as verified or quarantined BlobRef evidence, and emits deterministic per-archive and cumulative completeness reports. Plan item 6 reconciles immutable Artifact-version chains across repeated exports, retains verified version payloads as BlobRefs, and produces a deterministic safe Artifact representation. Compatibility with a real Claude export remains unverified until the owner supplies a minimized protected fixture. Compliance adapters, events, and complete project-export writing described below are still planned.

> [!IMPORTANT]
> **Ratatoskr is in development.** No database holds data that has to survive a schema change.
> While this status holds, these two rules replace what the documents below plan:
>
> - the API and the database keep their first version. There is no `v2` and no later major
>   version.
> - the database has no migrations. No schema exists yet. The first persistence change creates one
>   schema definition, and later schema changes edit it in place.
>
> Only the repository owner changes this status.

## Product boundary

This repository archives data from the Claude product. It is not an Anthropic Messages API inference client and does not treat API requests made by unrelated applications as the user's claude.ai history.

It owns:

- Claude account and workspace archive identity;
- immutable official export snapshots;
- projects and discovered project instructions;
- project knowledge and source references;
- conversations and message graphs;
- uploaded files;
- Artifacts and discovered versions;
- collaboration and visibility metadata when present;
- provider-specific raw records and unknown variants;
- completeness reports;
- optional Enterprise Compliance ingestion;
- portable local Markdown/JSON project exports.

It does not call Claude models, store Claude passwords or cookies, automate the consumer web interface, or own semantic search.

## Acquisition modes

```rust
pub enum ClaudeAcquisition {
    ConsumerExport,
    OrganizationExport,
    ComplianceApi,
    ManualConversationCapture,
    LegacyImport,
}
```

### Personal account export

For Claude Free, Pro, or Max the supported workflow is periodic and snapshot-based:

```text
Claude Settings > Privacy > Export data
  -> download official archive
  -> ratatoskr-export-agent
  -> immutable BlobStore
  -> ratatoskr-claude importer
```

The service preserves what the export contains and reports what it cannot prove. It does not claim a continuous replica of the user's account.

### Team and Enterprise

Organization exports and a configured Enterprise Compliance API may provide broader coverage, including chats, uploaded files, projects, and activity data. The connector records actual granted capabilities and observed fields rather than assuming all workspace plans expose the same surface.

Continuous Compliance ingestion and periodic full exports are complementary: incremental events reduce archive latency, while snapshots reconcile structure and assets.

## Raw-first archive model

Every provider archive is stored before normalization:

```text
/raw/claude/YYYY/MM/<sha256>.zip
```

The immutable archive, acquisition method, detected schema, parser version, warnings, and normalized record set form one provider snapshot. Parser upgrades replay existing raw evidence instead of requiring another export.

## Planned data model

The service will own a `claude_archive.*` PostgreSQL schema when persistence is implemented:

```text
claude_accounts
claude_workspaces
claude_exports
claude_import_runs
claude_projects
claude_project_revisions
claude_project_sources
claude_project_knowledge
claude_conversations
claude_conversation_revisions
claude_messages
claude_message_revisions
claude_content_parts
claude_attachments
claude_artifacts
claude_artifact_revisions
claude_memberships
claude_raw_records
claude_completeness_reports
claude_compliance_cursors
claude_tombstones
outbox_events
inbox_events
```

Archives, files, Artifact payloads, raw JSON, project knowledge, and portable exports live in the content-addressed BlobStore.

## Projects and project knowledge

A Claude project may include:

- title and description;
- project instructions;
- isolated conversation history;
- uploaded documents, text, or code;
- project knowledge entries;
- external GitHub or Google Drive references;
- Artifacts;
- membership, visibility, and collaboration metadata;
- first and last observed snapshots.

Consumer export documentation does not guarantee that every project knowledge file or setting is reconstructable. The importer therefore records discovered coverage and emits a completeness report instead of assuming that parsed conversations equal a complete project backup.

External project sources are modeled as references:

```text
provider_reference
original_url
original_provider
observed_title
observed_version
locally_backed_up
local_blob_ref
```

A GitHub repository referenced by a Claude project is fully preserved only when `ratatoskr-github` and `ratatoskr-vault` have archived it. A linked cloud document likewise requires its own connector before `locally_backed_up` can be true.

## Conversation graph

Claude conversations are normalized as graphs and revision histories rather than flat arrays. The model supports:

- message edits or regenerated responses when represented;
- branches;
- tool calls and results;
- citations;
- attachments;
- Artifacts;
- interrupted responses;
- hidden or provider-specific content parts;
- changes observed across exports.

```rust
pub struct ArchivedMessage {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub external_id: Option<String>,
    pub parent_message_id: Option<Uuid>,
    pub role: MessageRole,
    pub content: Vec<ContentPart>,
    pub model: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub provider_metadata: serde_json::Value,
}
```

Normalized revisions are immutable. A changed provider record creates a new observation and revision rather than destroying previous evidence.

## Artifacts

Artifacts are first-class archived records, separate from plain assistant text. The archive model may preserve:

- Artifact identity and title;
- type or language;
- source conversation and message;
- content or blob reference;
- provider metadata;
- observed version lineage;
- creation and update timestamps;
- related downloaded files.

Unknown Artifact forms are preserved as raw records until a parser version understands them.

The current safe portable representation is an in-memory deterministic Artifact subtree: it
contains normalized JSON for each Artifact and version, including provenance, original inert
provider records, BlobRef availability, unknown fields, predecessor evidence, and rendering status. Only verified `text/plain` and
`text/markdown` Artifact payloads get inert `.txt` or `.md` derivatives. HTML, code, canvas,
images, malformed bytes, unverified payloads, and unknown Artifact types are not rendered; their
normalized JSON reports an explicit `unrenderable` reason instead. The full atomic project-export
writer remains plan item 9.

## Content parts

```rust
pub enum ContentPart {
    Text(String),
    Markdown(String),
    Image(BlobRef),
    File(AttachmentRef),
    Code(CodeBlock),
    Citation(Citation),
    ToolCall(ToolCall),
    ToolResult(ToolResult),
    Artifact(ArtifactRef),
    Unknown(serde_json::Value),
}
```

`Unknown` is mandatory to keep provider changes forward-compatible.

## Safe import pipeline

```text
1. Receive archive
2. Compute SHA-256
3. Store the original archive immutably
4. Enforce path, count, size, and compression limits
5. Detect Claude export schema
6. Stream accepted entries directly into quarantined or candidate BlobRefs under cumulative limits
7. Preserve manifests and raw records
8. Parse through staging tables
9. Validate project, conversation, file, and Artifact relationships
10. Store attachments by content hash
11. Reconcile immutable revisions
12. Produce completeness report
13. Publish archive events
14. Queue optional Knowledge indexing
```

The importer rejects path traversal and absolute paths, bounds decompressed size, detects suspicious compression, sniffs MIME types, never executes archive files, and never renders active HTML during import.

Malformed or unknown records become warnings and preserved raw blobs whenever safe, rather than disappearing silently.

## Snapshot and upstream-state semantics

Every provider export remains independently addressable:

```text
snapshot-2026-08
snapshot-2026-09
snapshot-2026-10
```

Conservative upstream states:

```rust
pub enum UpstreamState {
    Present,
    MissingFromLatestSnapshot,
    ExplicitlyDeleted,
    AccessLost,
    Unknown,
}
```

Absence from one snapshot is not sufficient evidence for deletion. A shared project may disappear because access changed, the owner removed the user, export scope differed, or the parser could not associate a record.

Local hard deletion is controlled by explicit Ratatoskr retention policy, not by one provider snapshot.

## Completeness reporting

An import reports both discovered content and gaps:

```text
Projects discovered:              12
Conversations discovered:        418
Messages discovered:           6,904
Project knowledge items:          73
Attachments referenced:          161
Attachments archived:            158
Artifacts discovered:             96
Missing assets:                     3
Unknown record variants:            5
Completeness: ASSETS_PARTIAL
```

Planned statuses:

```rust
pub enum ExportCompleteness {
    Complete,
    ConversationsComplete,
    StructurallyPartial,
    AssetsPartial,
    Unknown,
    FailedValidation,
}
```

`Complete` requires positive evidence from a known schema and validation rules. It is not the default for personal exports.

## Compliance API mode

A configured Enterprise adapter may:

- maintain durable cursors;
- fetch chats, projects, files, and activity records available to the organization;
- preserve raw API responses;
- emit immutable revisions;
- detect cursor age and retention risk;
- reconcile against periodic organization exports;
- report capability and permission gaps.

Compliance credentials remain inside this service. Incremental ingestion never deletes data merely because one API page or capability is unavailable.

## Portable local representation

A normalized project can be exported independently of Ratatoskr:

```text
project-name/
├── project.json
├── README.md
├── instructions.md
├── conversations/
│   ├── chat-001.md
│   ├── chat-001.json
│   └── ...
├── knowledge/
├── artifacts/
├── attachments/
└── manifest.json
```

This representation supplements, but never replaces, the immutable provider archive.

## Search and analysis integration

`ratatoskr-knowledge` consumes normalized project and conversation events to provide:

- full-text and semantic search;
- cross-provider topic clustering;
- project digests;
- decision and action-item extraction;
- linking to repositories, articles, and other conversations;
- duplicate and related-discussion detection.

Knowledge cannot mutate the source archive and never receives Claude credentials.

## Commands and events

Expected contracts include:

```text
claude.export.ingest_requested.v1
claude.export.ingested.v1
claude.export.partial.v1
claude.project.upserted.v1
claude.conversation.upserted.v1
claude.artifact.upserted.v1
claude.asset.stored.v1
claude.snapshot.completed.v1
claude.compliance.sync_requested.v1
claude.compliance.cursor_advanced.v1
ai_archive.project.changed.v1
ai_archive.conversation.changed.v1
```

Archive hashes and provider identities make import replay idempotent. Compliance events converge on the same immutable source and revision records.

## Security and privacy invariants

1. Raw provider exports are immutable and hash-addressed.
2. No Claude password, session cookie, or undocumented browser token is collected.
3. Archives and files are treated as untrusted input.
4. Unknown records are preserved losslessly where safe.
5. Tenant and workspace ownership is checked on every record.
6. Conversation content and files never enter logs or traces.
7. External analysis follows explicit Knowledge provider and redaction policy.
8. Shared-project access loss is not treated as user-requested local deletion.
9. Incognito content is not claimed as backed up unless present in an authoritative organization export or Compliance source.
10. Missing records do not trigger destructive retention actions.

## Observability

Core metrics include:

```text
claude_export_import_duration
claude_export_bytes
claude_projects_imported
claude_conversations_imported
claude_messages_imported
claude_knowledge_items_imported
claude_artifacts_imported
claude_missing_assets
claude_unknown_record_variants
claude_completeness_status
claude_compliance_cursor_age
claude_snapshot_age
```

Every import records acquisition method, archive hash, detected schema, parser version, counts, warnings, completeness, and operation correlation.

## Non-goals

- Anthropic model inference or Messages API gateway behavior.
- Browser automation of claude.ai.
- Storing passwords, cookies, or MFA secrets.
- Claiming a live personal-account replica from periodic exports.
- Assuming all project knowledge is present without evidence.
- Owning search or generated analyses.
- Replacing raw evidence with portable Markdown.
- Treating API traffic from unrelated applications as Claude product history.

## Initial milestones

1. Define export, project, knowledge, conversation-graph, Artifact, asset, and completeness schemas.
2. Implement safe immutable archive ingestion.
3. Discover and version the first real personal Claude export schema.
4. Import conversations and revisions.
5. Import projects, knowledge, files, Artifacts, and unknown records where present.
6. Produce completeness reports and portable exports.
7. Integrate the macOS export agent.
8. Publish normalized events for Knowledge indexing.
9. Add Enterprise organization-export and Compliance adapters.
10. Add replay tests across multiple sanitized historical fixtures.

## Workspace integration

`ratatoskr-workspace` pins this repository as `repos/ai-archive/claude` beside the rest of the fleet and checks the pinned snapshot with its harness: the receipt and operator ports of `deploy/systemd/claude.conf.example` (8097 and 9086) against the Edge upstream table, and the `CLAUDE` stanza of Platform's `deploy/nats/ratatoskr.conf` against the identity table. `deploy/nats/identity.conf` is this repository's byte-equal copy of that stanza, which its own authorized-broker tests run against. The `export-agent-product` integration profile builds this service behind Platform's Edge and uploads a synthetic Claude archive to it under the deployed NATS ACL. A native smoke of the whole receipt flow across services has not been run against this revision. Real exports remain private test data; public CI uses synthetic archives and redacted structural fixtures.

## Project status

Implemented, and verified against the synthetic export schema `claude-export-2026-08-synthetic` only: configuration, telemetry, operator health, the content-addressed blob store and the owned `claude_archive` schema (`schema.sql` is the single editable definition of that first version); the Platform archive receipt with capped streaming hashing, write-once raw storage and explicit duplicate outcomes; the durable import-run state machine; bounded non-executing ZIP inspection and the exact-match parser registry; the consumer-export parser for projects, conversations, messages, Project Knowledge references and Artifacts with version chains; completeness reports; the `ai_archive.*` facts and operation reports, published from a transactional outbox by one JetStream pump; reparse and parser migration; privacy deletion with tombstones; and a deterministic portable export. The operator commands are `check-config`, `reparse`, `parser-migrate`, `portable-export` and `privacy-delete`. Not implemented: validation against a real Claude export (a protected fixture is a required follow-up, so no compatibility with a current provider export is claimed), schema detection beyond the declared synthetic identifier, the Enterprise organization-export and Compliance API adapters (the acquisition modes exist, no adapter does), and a complete project-export writer. `DEVELOPMENT.md` records the exact local and CI gate commands.

### Platform receipt, archive facts and privacy deletion

The service answers the Platform archive receipt on `POST /v1/ai-archives/receipt` and advertises it on the unauthenticated `GET /v1/capabilities`; every refusal is a JSON `ErrorEnvelope`, because Edge discards any other body. A Platform user outside the configured mapping is answered `403 claude.account.unmapped` (the device credential is not at fault), and the first receipt from a mapped user creates the archive account row itself, so an operator inserts nothing by hand. The receipt is served on its own loopback listener, which defaults to `127.0.0.1:8097`, and the operator plane on `127.0.0.1:9086`; neither serves the other's routes.

Configuration for the receipt flow is `RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS`, `RATATOSKR__RECEIPT__EVENT_BUS_URL` and `RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH`, and `RATATOSKR__RECEIPT__MAX_EVENT_BYTES` (default 8000000, accepted 65536 to 8388608) for the size of one published fact; `deploy/systemd/claude.conf.example` sets them all and is loaded by a test. A fact over the size limit is not enqueued and the import is reported as `partially_succeeded` with the warning `ai_archive.facts.too_large`; an outbox row larger than the broker accepts is marked undeliverable and skipped instead of wedging the pump. Deletion erases bytes under a per-digest lock after re-checking references, and imports take the same lock before they register a reference, so an import of identical bytes can never leave a row that points at an erased blob.

A completed import or an applied reparse enqueues, in the transaction that persists it, an `ai_archive.archive.imported.v1` fact and the project, conversation and Artifact facts it changed, owned by the Platform user that `RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS` maps to the archive account. One pump publishes every archive event type, in order, and marks a row published only after the broker acknowledged it. An import that is not complete is reported as `partially_succeeded` with the `ai_archive.import.incomplete` warning.

Privacy deletion removes the scope rows and every dependent in one transaction, then erases the bytes no remaining row references, then completes the request. It enqueues an `ai_archive.subject.tombstoned.v1` envelope with reason `user_requested` for each deleted archive, each deleted conversation and project, and each Knowledge-linked Artifact. A conversation, project or Artifact that a retained export also observed is kept and announced again as an update under the earliest such export, after the tombstone, so Knowledge does not drop what the deletion kept. These behaviours are verified against the synthetic export schema only; no claim is made about a real Claude export.

### Deployment

The service connects as the PostgreSQL role `ratatoskr_claude` to the shared `ratatoskr` database on port 5432 and applies `schema.sql` itself at startup, which creates the `claude_archive` schema. Provision in this order: Platform's `deploy/postgres/01-database-and-roles.sql`, the first Edge start, Platform's `02-grants.sql`, then `deploy/postgres/role.sql` of this repository before the first start of this service, run by a PostgreSQL administrator, followed by `\password ratatoskr_claude` and the same password in `RATATOSKR__STORAGE__DATABASE_URL`. The role has no privilege beyond `connect` and `create` on the database; nothing is seeded by hand, because the first receipt from a mapped Platform user creates the archive account. The event bus identity is the `CLAUDE` stanza of Platform's `deploy/nats/ratatoskr.conf`; its seed is installed at `/etc/ratatoskr/claude.nkey` and is read when the service opens a broker connection: a rotated seed takes effect after the next failed publish pass, or at the next restart, whichever comes first. The stanza lets the identity subscribe only to `_INBOX_claude.>`, so reload NATS and deploy the services of one snapshot together.
