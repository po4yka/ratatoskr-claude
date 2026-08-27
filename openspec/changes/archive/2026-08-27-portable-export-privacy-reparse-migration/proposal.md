## Why

Claude Archive can preserve and publish normalized evidence, but plan item 9 remains incomplete: an owner cannot yet produce an independently verifiable local copy, erase tenant-owned evidence with downstream removal guarantees, or safely rebuild projections from retained raw archives as parsers evolve. The repository also lacks a controlled path for turning an owner-authorized real export into minimized golden compatibility evidence without committing private source data.

## What Changes

- Add a tenant-scoped deterministic portable ZIP export containing canonical JSON, readable Markdown, verified asset bytes, and a manifest with member digests, completeness, source-snapshot, raw-export, and parser provenance.
- Add inventory-first privacy deletion for tenant, raw-export, and conversation scopes. Its closure includes affected projects, Project Knowledge, messages, Artifacts, assets, and provenance. Apply database erasure, explicit tombstones, Knowledge-removal outbox facts, and content-free audit evidence atomically; erase an exact locally owned blob only after committed reachability proves it is no longer referenced.
- Add an explicit `reparse` operator command over verified preserved raw archives. Dry-run and apply share one immutable comparison plan, application is idempotent, and parser omissions remain warnings rather than inferred deletions.
- Add deterministic parser-version migration orchestration and JSON reports over the reparse engine. This is projection/parser migration tooling only; it does not add database migrations or schema-version negotiation.
- Document the consent, private handling, minimization, review, and golden-admission process for new owner-provided real fixtures. No owner archive or private content is committed.
- Add test-first coverage for portable byte determinism, deletion completeness enumeration and atomic propagation, reparse idempotence and dry-run fidelity, deterministic migration reports, and fixture-admission validation.

## Capabilities

### New Capabilities

- `portable-archive-export`: Produce a tenant-scoped, deterministic, verifiable local archive from normalized Claude evidence and verified assets.
- `privacy-deletion`: Authorize, enumerate, atomically erase, audit, and propagate explicit removal of tenant-owned Claude archive evidence.
- `archive-reparse`: Re-run an exact compatible parser over retained raw evidence with side-effect-free dry runs, idempotent apply, and conservative comparison.
- `parser-version-migration`: Plan and apply deterministic tenant-scoped parser upgrades as orchestration over archive reparse, with stable reports and partial outcomes.
- `owner-fixture-discovery`: Admit minimized owner-authorized real-export structures as reviewed golden compatibility fixtures without retaining private source data.

### Modified Capabilities

- `claude-archive-schema`: Persist export-to-entity provenance, deletion inventories/audits, reparse runs, and parser-migration reports in the single current schema definition.
- `blob-store`: Add exact, idempotent erasure for unreferenced locally owned blobs while preserving immutable storage and fail-closed integrity behavior.
- `parser-registry`: Add exact parser identity lookup and deterministic compatible-version discovery without ambiguous automatic selection.
- `claude-knowledge-events`: Publish replay-safe explicit removal facts in the same transaction that commits archive privacy deletion.

## Impact

- Affects the Rust archive domain crate, PostgreSQL schema definition, BlobStore boundary, parser registry, operator command surface, documentation, and synthetic/golden tests.
- Uses the existing workspace AI-archive lifecycle and BlobRef contracts; no new cross-repository contract or production dependency is planned.
- Changes the current schema definition in place and creates fresh test databases from it. No migration file, migration framework, second API/schema major, or compatibility route is introduced.
- Raw archives remain immutable for ordinary reparse, but an explicitly authorized privacy-deletion scope may erase retained raw evidence; reparse and portable export then report that evidence as unavailable rather than fabricating success.
