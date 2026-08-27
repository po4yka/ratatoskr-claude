## Context

See `proposal.md` and the nine capability deltas. The repository already has immutable raw receipts, hostile ZIP inspection, an exact parser registry, synthetic consumer-export projection parsing, normalized Claude graph/Project Knowledge/Artifact models, completeness reporting, content-addressed blobs, and transactional Knowledge outbox publication. It does not yet persist all export-to-entity observation edges needed for exhaustive deletion and reparse comparison, nor expose a portable archive, deletion, reparse, or parser-migration operator surface.

The development-status rules are binding: edit the one current `schema.sql` in place, keep one API/contract version, and add no database migration files or tooling. The first production parser is still based on synthetic evidence, so generic reparse machinery must not invent a second production parser or claim verified real-export compatibility.

## Goals / Non-Goals

**Goals:**

- Produce a tenant-scoped ZIP whose bytes, member paths, JSON, Markdown, and manifest are deterministic for identical selected evidence.
- Make privacy deletion inventory-complete, resumable, fail-closed across BlobStore and PostgreSQL, and atomic for normalized erasure, content-free audit, and Knowledge removal facts.
- Re-run an exact compatible newer parser over verified raw archives through one immutable plan shared by dry-run and apply.
- Orchestrate parser upgrades deterministically over reparse and report partial outcomes honestly.
- Give the owner a documented, testable private-to-golden workflow for newly observed real Claude export shapes.

**Non-Goals:**

- Importing portable ZIPs back into Claude, browser automation, active content rendering, fetching external GitHub/Drive references, provider login/session handling, or inference.
- Database migrations, schema-version negotiation, a second major API/contract, or compatibility shims.
- Treating parser omission, snapshot absence, a synthetic fixture, or an external reference without bytes as proof of deletion or completeness.
- Committing an owner export, private digest, real title, filename, identifier, Project Knowledge, Artifact content, conversation content, or asset.

## Decisions

### 1. One canonical member model produces the portable ZIP

The exporter asks a tenant-scoped repository for a fully selected portable state: raw-export/parser/snapshot provenance; completeness; projects and instructions; Project Knowledge sources; graph conversations and typed content parts; Artifacts and versions; asset availability and blob references. It renders canonical JSON and readable Markdown into an in-memory member model, generates safe paths from stable opaque identities plus digest suffixes, sorts paths by UTF-8 bytes, and appends canonical `manifest.json` last.

ZIP entries use the stored method, a fixed DOS timestamp, fixed permissions, no filesystem metadata, and one fixed member order. JSON objects are recursively canonicalized; record arrays sort by stable identity and graph ordinal. Markdown begins with inert provenance comments and never renders active HTML. Verified assets enter only after `BlobStore` integrity verification. The output is written to an owned temporary sibling and atomically renamed; every failure removes only that owned temporary path.

A clock-stamped compressed ZIP was rejected because compressor and metadata drift break byte determinism. Provider filenames as member paths were rejected because traversal and collisions would remain attacker-controlled.

### 2. Current-schema observation edges are the deletion and reparse authority

Edit `schema.sql` in place to persist export-to-normalized-subject observations and extracted-artifact references, then add portable-export operations, privacy deletion requests/items/audit, reparse runs, and parser-migration reports. Every live relation is tenant-owned and every foreign key remains inside `claude_archive`. A content-free terminal deletion audit survives tenant erasure without a foreign key or external tenant/account identifier; it uses the opaque request identity, scope class, counts, timestamps, correlation ID, outcome, and non-sensitive evidence reference only.

The alternative of reconstructing provenance from current projection rows was rejected: it cannot prove which retained raw archives contain a conversation and therefore cannot safely enumerate deletion closure or compare reparse output.

### 3. Privacy deletion is inventory-first and resumable

The transaction that creates a deletion inventory takes a tenant-scoped PostgreSQL advisory transaction lock, verifies authorization without existence disclosure, inserts one idempotent active request, and enumerates immutable items in stable category/identity order. Raw-export scope follows the selected export's observation edges. Conversation scope first finds every containing export, because deleting a normalized row while retaining the raw copy would not satisfy privacy erasure. Tenant scope enumerates every tenant-owned row and reference.

Inventory actions distinguish database erasure, exclusive blob erasure, retained shared evidence, downstream tombstone, and content-free audit. Planning must account for every owned table and blob-reference column; category totals are derived from the item rows, never maintained independently.

Execution takes the same tenant lock, resumes unfinished items, and deletes exact locally owned blobs only after a fresh reachability query shows no retained reference. Physical blob work occurs outside a database transaction and is idempotent. The final database transaction locks the request and inventory, rechecks retained references, deletes the selected provenance/normalized closure, inserts replay-safe `user_requested` tombstone outbox facts, and appends the content-free audit. Any transaction failure rolls all three database effects back; the request remains resumable even if exclusive blobs are already absent.

Trying to put filesystem deletion inside PostgreSQL atomicity was rejected because rollback cannot restore erased bytes. Deleting database rows first was rejected because a crash could orphan private bytes without a durable inventory.

### 4. Blob erasure resolves only an exact owned content address

Add idempotent erase beside verified read. It validates owner, algorithm, lowercase digest shape, expected content-addressed path, and containment beneath the configured object root; it removes exactly that file and tolerates already absent bytes. The deletion service, not BlobStore, owns reachability policy and supplies only inventory references that a fresh database query classified exclusive.

### 5. Reparse uses one immutable fingerprinted plan

Extend the registry with exact `name@version` resolution and deterministically ordered compatible identities while leaving ordinary intake ambiguity-safe. `ReparseEngine` loads the tenant-owned export and current projection fingerprint, verifies raw bytes, reinspects/extracts under current limits, runs the exact compatible newer parser, validates and reconciles into an in-memory candidate, then compares by stable subject identity and content digest.

The plan binds raw digest, parser-registry fingerprint, target parser identity, current projection fingerprint, canonical change list, completeness/warnings, candidate extracted artifacts, and prospective outbox subjects. `--dry-run` serializes this plan without database or blob writes. Apply rejects stale fingerprints, persists newly required artifacts, and commits the parser-stamped run/revisions/completeness/outbox in one transaction. A unique execution identity returns the prior report on replay. Missing candidate identities become `proposed_removal` warnings and retain current evidence.

Separate dry-run logic was rejected because it would inevitably drift from apply. Automatically choosing the highest parser was rejected because version ordering does not prove semantic compatibility and ordinary intake must remain ambiguity-safe.

### 6. Parser migration is deterministic orchestration over reparse

The migration planner selects one tenant's exports, sorts by stable internal export identity, and classifies each exactly once as eligible, already current, unsupported, raw missing, privacy blocked, or failed inspection. Totals are reduced from the sorted entries. Apply invokes the same reparse engine only for eligible entries, continues after archive-local failure, preserves every non-eligible classification, and returns `completed`, `partial`, or `failed` without rolling back successful independent archives.

Operator commands are:

- `portable-export --tenant TENANT --output PATH [--project ID] [--observed-from RFC3339] [--observed-to RFC3339]`;
- `privacy-delete --tenant TENANT --request-key KEY --scope tenant|export|conversation [--target ID] [--dry-run]`;
- `reparse --tenant TENANT --archive UUID --parser NAME@VERSION [--dry-run]`;
- `parser-migrate --tenant TENANT --parser NAME@VERSION [--dry-run]`.

Each emits one stable JSON report to stdout; diagnostics go to stderr without content. Exit `0` means completed plan/apply, `1` means operational failure or partial migration, and `2` means invalid invocation. All report arrays are sorted before serialization.

### 7. Owner fixture discovery separates private evidence from committed goldens

Document the workflow in `docs/testing/OWNER_FIXTURE_DISCOVERY.md`: obtain explicit owner authorization; place the original in an access-controlled location outside Git; record a private digest/receipt; run production inspection; derive the smallest structurally faithful case with synthetic identifiers and content; compare schema signals, parser selection, variant inventory, relationships, unknown preservation, and completeness between private source and candidate; run secret/PII/path scans; record consent/license/reviewer/owner approvals in a non-sensitive admission manifest; and deliberately bless/review the golden.

A fixture-admission validator reads a strict human-authored manifest and fails closed on missing gates, raw archive extensions, suspicious private values, unsafe paths, or unlisted files. CI sees only the synthetic fixture, its non-sensitive admission record, and deterministic output. The private digest and source stay outside Git and ordinary logs.

## Risks / Trade-offs

- [Portable ZIP assembly can use memory proportional to selected evidence] → enforce existing archive/output limits, select before rendering, and fail before publication; future streaming must preserve the same member-order contract.
- [Deletion closure misses a newly added table or blob column] → centralize inventory categories, add a schema inventory test that enumerates every owned relation/reference, and require the completeness-count regression test to fail when a category is unhandled.
- [Filesystem erasure succeeds before database finalization fails] → persist immutable inventory first, make erasure idempotent, retain a resumable request, and make the final database effects atomic.
- [Shared content is erased because deduplication crosses tenants] → decide erasure only after a fresh locked database reachability query; classify any retained reference as shared.
- [Dry-run output becomes stale before apply] → bind plans to raw, registry, and projection fingerprints and refuse stale application.
- [Generic reparse exists before a second production parser] → use hand-written test parsers only in tests; production reports `already_current` or `unsupported` rather than adding a fake version.
- [Owner fixture redaction preserves a secret or changes schema meaning] → require independent scan/review gates and structural comparison before admission; unsupported remains the truthful default.

## Migration Plan

1. Extend the single current schema definition and verify it applies repeatably to fresh PostgreSQL 17 databases; create no migration artifact.
2. Land export, deletion, reparse, migration, and fixture-admission behavior behind explicit operator commands, with no background execution or automatic parser upgrade.
3. Recreate development/test databases from `schema.sql`; no production data preservation or backfill is promised under development status.
4. Rollback removes command routing and new runtime modules while retained raw evidence remains readable by the prior importer. Recreate databases from the reverted current schema; portable ZIPs already delivered to owners remain independent local files.
