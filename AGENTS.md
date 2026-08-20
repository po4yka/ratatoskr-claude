# Ratatoskr Claude Archive Agent Instructions

## Scope

These instructions apply to the `ratatoskr-claude` repository.

This repository owns archival ingestion and local preservation of **Claude product data**: exports, organizations/workspaces, projects, project knowledge, conversations, files, Artifacts, and provider-specific records.

## Product boundary

Claude product archives are not the same system as Anthropic Messages/inference API traffic.

This repository must not:

- act as an Anthropic inference gateway;
- represent Messages API requests as claude.ai chat history;
- call Claude models as part of archive ownership;
- automate a consumer Claude login/session to crawl history;
- store user passwords, MFA secrets, or browser cookies.

Analysis and embeddings belong to `ratatoskr-knowledge`; this service preserves and normalizes provider archive evidence.

## Repository mission

The service must:

- accept official personal, organization, and supported Compliance API data;
- store original provider exports/events immutably before parsing;
- detect and version provider schemas;
- preserve projects, project instructions, project knowledge, conversations, message graphs, files, and Artifacts;
- retain unknown provider records for forward-compatible reprocessing;
- reconcile repeated snapshots conservatively;
- report completeness and missing categories/assets explicitly;
- produce portable local exports;
- publish stable archive contracts for Knowledge and clients.

## Current phase

The repository is in architecture bootstrap. Do not assume Rust crates, parsers, migrations, Compliance adapters, fixtures, or CI commands exist unless they are present in the checkout.

When creating initial implementation:

- implement raw immutable storage and hostile archive inspection before rich projections;
- version every parser and normalized interpretation;
- preserve unknown records;
- keep personal export, organization export, and Compliance ingestion as distinct adapters;
- do not code against an unofficial sample as if it were a stable permanent schema.

### Development status

Ratatoskr is in development. No database holds data that has to survive a schema change. While this
status holds, these rules are binding, and they override anything else in this repository that
plans otherwise, including the rest of this file:

- **One version only.** The API, the database, and the contracts keep their first version. Do not
  add a `v2` or a later major version, and do not add version negotiation, deprecation windows, or
  parallel-major routing.
- **No database migrations.** Do not add a migration file, and do not add migration tooling. A
  schema change edits the current schema definition in place, and a test database is created from
  that definition.
- **The product is `Ratatoskr`.** It is not "Ratatoskr Next". Do not write that name in code,
  documentation, identifiers, comments, or commit messages.

Only the repository owner changes this status. Ask before you write anything these rules forbid.

## How a change starts

Every non-trivial change begins as an OpenSpec change rather than as an edit. In your assistant that
is `/opsx:propose <what you want to build>`, or `/opsx:explore` first when the shape is not clear
yet. The command writes `openspec/changes/<id>/` holding a proposal, the spec deltas, a design and a
task list, and you read that plan before any code is written. `/opsx:apply` builds it and
`/opsx:archive` folds the deltas into `openspec/specs/`.

`openspec/specs/` holds the behaviour that is true today, and it starts empty on purpose. A spec here
grows from a change that needed it. Do NOT convert `docs/REQUIREMENTS.md`, `docs/INTERFACES.md`,
`docs/DOMAIN.md` or `docs/DATA_MODEL.md` into specs in bulk. Those documents stay where they are, as
material an exploration reads. A spec set produced by bulk conversion is large, stale on the day it
lands, and trusted by nobody.

Behaviour that more than one repository can see — the shape of a contract, the meaning of a field, the
order in which repositories must receive a change — belongs in the `ratatoskr-workspace` store, not
here. `openspec/config.yaml` references it, so `openspec instructions` in this repository lists the
store's specs with the exact command that fetches one. Cite that spec from a local proposal instead
of restating it.

### Tests come first

The task list carries one pair per behaviour. The first task adds a test that fails. The second makes
it pass. Never one task that does both.

- Run the new test before you write the implementation, and confirm it fails for the reason the task
  states — not for a compile error or a typo.
- A refactor task comes after the tests are green. It adds no test and changes no behaviour.
- A task that cannot start from a failing test says why in one line. Configuration, documentation and
  generated files are the usual reasons.
- Do not tick a task whose test has not been run.

Nothing can check the order in which the two were written. What CI does check is
`openspec validate --archived`, which fails when a change was archived with a task left unticked, and
the step in `fleet.yml` that fails when a repository holds a manifest and a `ci.yml` that never runs
a test. `ratatoskr-workspace/docs/QUALITY_GATES.md` states that limit rather than implying it is
covered.

## Sources of truth

Use this order:

1. active task/changeset and accepted ADRs;
2. `README.md`;
3. AI archive/event contracts from `ratatoskr-contracts`;
4. immutable raw provider export or Compliance event;
5. parser-version documentation and completeness report;
6. normalized projections;
7. implementation details.

If normalized state conflicts with the raw provider data, preserve the raw evidence and correct/version the parser or projection.

## Acquisition modes

Represent acquisition explicitly, for example:

```text
ConsumerExport
OrganizationExport
ComplianceApi
ManualConversationCapture
LegacyImport
```

Rules:

- each mode has separate authority and completeness;
- record account/organization/workspace identity where available;
- record request, receive, import, and provider-event timestamps separately;
- record archive hash or Compliance cursor/event ID;
- do not claim a consumer export is a live replica;
- do not claim full project-knowledge coverage unless the actual export/API proves it;
- manual capture is partial and never has absence-based deletion authority;
- organization data must remain scoped to the owning organization and authorization policy.

## Hard bounded-context rules

### Claude Archive owns

- Claude account/organization/workspace archive identity;
- immutable raw exports and Compliance records;
- import runs, parsers, warnings, and completeness;
- projects, project instructions, and observed project metadata;
- project knowledge/source records and available bytes/references;
- conversations and message graphs;
- uploaded/generated files;
- Artifacts and Artifact versions;
- provider-specific unknown records;
- snapshot/revision/tombstone/access-loss state;
- portable archive exports;
- Claude-specific outbox/inbox records;
- references to Knowledge indexing/analysis.

### Claude Archive does not own

- Anthropic inference API requests;
- general summaries, entities, embeddings, or search ranking;
- Platform sessions/devices;
- Ratatoskr collections/tags;
- browser automation or consumer Claude credentials;
- ChatGPT archive data;
- external GitHub/Google Drive content merely linked from a project;
- BlobStore implementation outside the approved interface.

## Raw-first immutable storage

This is non-negotiable:

> Store original Claude archives or Compliance records before destructive parsing or normalization.

Rules:

- compute SHA-256 while receiving/streaming where practical;
- use content-addressed or collision-safe BlobStore keys;
- record size, acquisition mode, account/workspace metadata, and receive time;
- never rewrite the original ZIP/JSON/event payload;
- verify stored bytes match the expected hash;
- deduplicate identical archives by hash while preserving necessary import/audit records;
- keep raw data access-controlled and absent from ordinary logs;
- never delete the only raw snapshot because normalized import succeeded.

A normalized projection is not sufficient archival evidence.

## Safe archive intake

Treat every archive as hostile input.

Mandatory controls:

- reject absolute paths and path traversal;
- normalize/validate every archive entry;
- cap archive bytes, entry count, nesting, per-entry size, and total decompressed size;
- detect suspicious compression ratios and zip bombs;
- reject or safely handle symlinks, hard links, devices, and special files;
- never execute archive contents;
- never render active HTML during import;
- sniff MIME safely;
- use isolated temporary directories with restrictive permissions;
- clean only owned temporary paths;
- treat filenames, URLs, Markdown, code, and Artifact contents as untrusted data.

A parse failure must retain the raw archive and a durable failed/quarantined import run.

## Schema detection and parser versioning

- Detect schema/export version from actual structure, not filename alone.
- Assign stable detected-schema and parser-version identifiers.
- Keep parser modules versioned and covered by fixtures.
- Preserve unknown files, sections, record types, and content variants as raw references where safe.
- Never report full success after silently dropping unknown records.
- Parser upgrades create a new reprocessing/projection revision.
- Record warnings, missing relationships, unknown fields, and asset mismatches.
- Keep organization/personal export variants explicit.

Avoid a permissive parser that converts all data to generic JSON without validating relationships.

## Import state machine

Use explicit durable state, for example:

```text
received
stored
inspecting
schema_detected
extracting
staging
validating
reconciling
publishing
completed
partial
failed
quarantined
```

Rules:

- transitions are idempotent;
- retries resume/restart only safe documented phases;
- relationship and asset validation are part of completion;
- `partial` remains a visible terminal class with warnings;
- failed normalization never invalidates raw evidence;
- replayed/out-of-order commands cannot regress terminal state;
- every run retains acquisition, archive/event identity, parser version, and correlation IDs.

## Project model

A Claude Project may include:

- external project ID;
- title/description;
- project instructions;
- conversation history;
- project knowledge base;
- uploaded documents/text/code;
- Artifacts;
- collaborators/visibility;
- external GitHub/Google Drive references;
- created/updated/archived/deleted/access observations.

Rules:

- absence of project knowledge or instructions from one export does not prove they never existed;
- preserve each source's kind, external reference, observed metadata, available bytes, and local-backup status;
- external GitHub references are backed up by `ratatoskr-github`/`ratatoskr-vault`, not implicitly by Claude Archive;
- external Drive references are not local backups unless bytes are present or another service verifies them;
- do not fetch external sources using Claude credentials;
- project completeness is component-specific;
- shared-project access loss is distinct from explicit deletion.

## Project knowledge

Project knowledge entries may include files, pasted text, code, and external references.

For every entry preserve where available:

- provider source ID;
- source kind;
- title/filename;
- MIME/size/hash evidence;
- provider reference/URL;
- local blob reference if bytes are archived;
- first/last seen snapshots;
- parse/availability status;
- relationship to project and conversations.

Rules:

- a reference without bytes is not a complete backup;
- do not execute code or render active content;
- keep source version/revision when observed;
- report missing knowledge assets in completeness output;
- avoid duplicate storage through content hashes while retaining provider relationship records.

## Conversations are graphs

Do not assume every Claude conversation is strictly linear.

Support, when observed:

- conversation and message external IDs;
- parent/revision relationships;
- edited/regenerated responses;
- branches or alternate continuations;
- system/tool/internal roles;
- citations/tool calls/results;
- interrupted/incomplete responses;
- file and Artifact relationships;
- model metadata and provider-specific fields.

Rules:

- preserve graph relationships and alternate paths;
- derived Markdown transcript is a projection, not canonical graph storage;
- validate cycles, orphans, duplicate IDs, and missing parents;
- preserve unknown message/content variants;
- do not discard hidden/tool records merely because a standard UI transcript omits them.

## Content parts

Use typed heterogeneous content parts, for example:

```text
Text
Markdown
Image
File
Code
Citation
ToolCall
ToolResult
Artifact
Unknown
```

Rules:

- preserve ordering;
- distinguish references from locally available bytes;
- validate size, MIME, hashes, and ownership;
- preserve unknown variants safely;
- never execute code/tool calls or unsafe HTML;
- do not infer model/tool activity beyond provider evidence.

## Artifacts

Artifacts are first-class versioned archive objects, not flattened message text.

Preserve where available:

- Artifact external ID;
- type/title/language;
- owning project/conversation/message;
- version/revision graph;
- content and content hash;
- creation/update timestamps;
- provider metadata;
- available assets/files;
- raw record provenance.

Rules:

- preserve all observed versions, not only the latest;
- do not execute/render active code without sandboxed client policy;
- a generated Markdown export is a derivative, not canonical Artifact storage;
- report missing content/assets;
- handle filename/path collisions safely in portable exports.

## Files and assets

- Store available bytes content-addressably with cryptographic hashes.
- Record provider filename, MIME evidence, size, external ID, and relationships.
- Missing referenced assets create explicit completeness warnings.
- A URL/reference alone is not an archived file.
- Distinguish uploaded user files, generated outputs, project knowledge, and Artifact assets.
- Sanitize filenames for display and portable export.
- Never use untrusted names as storage paths.
- Apply malware/active-content policy where configured.

## Snapshot and revision semantics

Every official export remains an immutable snapshot.

Rules:

- later snapshots do not replace prior raw archives;
- changed projects/conversations/messages/knowledge/Artifacts create new observations or revisions;
- current projections reference first/last seen snapshots;
- absence from one snapshot does not prove deletion;
- explicit provider deletion/compliance evidence or policy-defined reconciliation creates tombstones;
- local hard delete follows separate retention/user policy;
- access lost, organization removal, and owner deletion remain distinct states;
- incognito chats may be absent from personal exports and completeness must not imply otherwise.

Representative states:

```text
Present
MissingFromLatestSnapshot
ExplicitlyDeleted
AccessLost
Unknown
```

Do not reduce them to a single boolean.

## Completeness reporting

Every import produces a durable report including where available:

- archive/schema/parser identity;
- accounts/organizations/workspaces discovered;
- projects and instructions discovered;
- project knowledge referenced, archived, missing, or unknown;
- conversations/messages and graph warnings;
- files and assets referenced/archived/missing;
- Artifacts and versions discovered;
- collaborators/visibility metadata found;
- unknown record variants/categories;
- acquisition-specific limitations.

Representative status classes:

```text
Complete
ConversationsComplete
StructurallyPartial
AssetsPartial
Unknown
FailedValidation
```

Use `Complete` only when the actual export/API and expected categories justify it.

## Compliance API rules

If a Claude Enterprise Compliance adapter is implemented:

- use official supported authentication with least privilege;
- isolate organization credentials and ownership;
- persist durable cursors/checkpoints only after raw records/files are stored;
- deduplicate events and file content;
- handle provider retention windows and cursor lag observably;
- classify scope/revocation/rate/schema failures explicitly;
- reconcile chats/files/projects with periodic organization exports when available;
- preserve activity/audit data separately from content projections;
- do not broaden collection beyond the authorized organization/workspace;
- keep raw Compliance events append-only/immutable according to policy.

Compliance credentials never enter clients, exported archives, events, or logs.

## Portable local export

Portable export should remain readable without Ratatoskr, for example:

```text
project/
  project.json
  README.md
  instructions.md
  conversations/*.json
  conversations/*.md
  knowledge/
  artifacts/
  files/
  manifest.json
```

Rules:

- include source snapshot IDs, hashes, completeness, missing references, and exporter version;
- preserve graph JSON even when Markdown provides a preferred linear view;
- preserve Artifact version metadata;
- sanitize paths and resolve collisions deterministically;
- do not include secrets or unrelated organization data;
- distinguish provider-original bytes from generated projections;
- write atomically and verify output manifest/hash where practical.

Portable export is not a claim that it can be imported back into Claude.

## Knowledge integration

Publish stable archive references/events containing:

- provider/account/organization/project/conversation/message/Artifact IDs;
- snapshot/revision IDs;
- content hashes;
- normalized typed content;
- raw export provenance;
- file/blob references;
- completeness/availability state;
- operation/correlation IDs.

`ratatoskr-knowledge` owns summaries, embeddings, decision extraction, entities, and semantic retrieval. Knowledge output never replaces archive evidence.

## Persistence and migrations

Claude Archive writes only its owned schema.

Conceptual data includes:

```text
claude_accounts
claude_organizations
claude_exports
claude_import_runs
claude_projects
claude_project_sources
claude_conversations
claude_messages
claude_message_relations
claude_content_parts
claude_artifacts
claude_artifact_versions
claude_assets
claude_revisions
claude_tombstones
claude_outbox
claude_inbox
```

Rules:

- no cross-schema writes or foreign keys;
- raw provider records remain separate from normalized projections;
- stable provider IDs and snapshot/revision uniqueness are constrained;
- migrations preserve raw links, graph/source/Artifact relationships, and completeness history;
- destructive cleanup cannot remove the sole evidence for normalized state;
- large bytes use protected BlobStore references.

## Commands and events

Representative messages include:

```text
claude.export.received.v1
claude.export.ingested.v1
claude.project.upserted.v1
claude.conversation.upserted.v1
claude.artifact.upserted.v1
ai_archive.asset.stored.v1
ai_archive.snapshot.completed.v1
ai_archive.snapshot.partial.v1
```

Use canonical contracts, transactional outbox, inbox deduplication, correlation/causation IDs, and at-least-once-safe handlers.

Do not put full private archives/messages in broadly distributed events.

## Prohibited implementation approaches

Do not add:

- automated claude.ai web login;
- browser cookie/session storage or replay;
- undocumented consumer history endpoints as the supported path;
- password/MFA collection;
- browser scraping of all chats/projects;
- Anthropic inference messages represented as claude.ai history;
- automatic model analysis inside archive import;
- deletion based solely on absence from one export;
- fetching GitHub/Drive project sources with Claude credentials.

## Security and privacy

- Treat exports, chats, project knowledge, files, prompts, and Artifacts as highly sensitive.
- Enforce account/organization/project ownership at every endpoint.
- Encrypt credentials and sensitive storage where configured.
- Do not log chat bodies, raw JSON, archive paths, tokens, titles, filenames, or Artifact contents by default.
- Limit and audit raw archive/Compliance access.
- Redact errors before user display.
- Never execute code, tool calls, HTML, macros, or files from exports.
- Keep portable exports private by default and write atomically.
- Audit imports, exports, raw access, retention, and hard deletion.
- Use least-privilege database, network, and BlobStore roles.

## Observability

Required telemetry should cover:

- archives/events received, deduplicated, and stored;
- schema/parser detection;
- import phase latency/status;
- organization/project/conversation/message/source/Artifact/asset counts;
- graph/source/Artifact validation warnings;
- unknown variants/categories;
- completeness classes;
- Compliance cursor lag, rate limit, and reauth state;
- reprocessing/portable export operations;
- outbox/inbox lag and duplicates;
- correlation, archive, import-run, project, conversation, and Artifact IDs in non-sensitive form.

Never put titles, message content, filenames, organization names, or emails in normal metric labels.

## Testing expectations

When implementation exists, include applicable tests for:

- archive hashing/deduplication;
- path traversal, symlink, count, size, and zip-bomb defenses;
- personal/organization schema detection and parser selection;
- unknown record preservation;
- import state-machine idempotency/recovery;
- project instructions and project source relationships;
- external reference versus locally backed-up status;
- conversation graph branches/edits/cycles/orphans/duplicates;
- heterogeneous content parts;
- Artifact identity/version graph/content hashing;
- asset hashing/missing references/path safety;
- completeness classification;
- absence-without-deletion, access loss, and explicit tombstones;
- Compliance cursors/deduplication/organization isolation;
- portable export manifest/determinism/collisions;
- outbox/inbox replay and migrations.

Use synthetic or aggressively minimized/redacted fixtures. Never commit personal Claude exports, organization data, private chats, files, or Artifacts to a public repository.

## Cross-repository change rules

Use a workspace changeset when changing:

- AI archive/event contracts;
- upload APIs used by Platform/export-agent/web/mobile;
- normalized content consumed by Knowledge;
- BlobStore/asset references;
- completeness/status semantics shown by clients;
- Compliance authentication/deployment;
- GitHub/Drive reference integration;
- migration/backfill/portable export formats.

List producer/consumer compatibility, rollout, rollback, reprocessing/reindexing, storage, privacy, and user-visible completeness impact.

## Git and PR workflow

- State acquisition mode and affected schema/parser versions.
- Keep parser semantic changes separate from unrelated infrastructure refactors.
- Include safe fixtures and graph/source/Artifact/completeness evidence.
- Document raw storage, reprocessing, retention, external reference, and asset impact.
- Do not add browser login/session automation.
- Do not commit credentials, personal/organization exports, raw chats, files, titles, or Artifacts.
- Do not claim completeness without explicit evidence.
- Update README/ADRs when provider format, API capability, or archive semantics change.

## Completion criteria

A task is complete only when:

- responsibility belongs to the Claude Archive context;
- raw provider evidence is immutable and verified;
- archive intake is hostile-input safe;
- parser/schema versions and unknown records are preserved;
- projects, knowledge sources, graph conversations, content parts, files, and Artifacts reconcile idempotently;
- completeness is explicit and conservative;
- external references are not misrepresented as local backups;
- absence from one snapshot does not cause deletion;
- no browser-session automation or inference responsibility is introduced;
- portable output and downstream events preserve provenance;
- relevant security/import/graph/Artifact/export tests pass;
- contracts, migrations, telemetry, privacy, and cross-repository rollout are documented.
