# Archive receipt and import state

## Why

Plan item 2 of `docs/IMPLEMENTATION_PLAN.md` calls for authenticated receipt, streaming hash, immutable raw storage, and import state: the first vertical slice where a provider archive actually enters this repository's custody. Item 1 built the container (config, telemetry, health, BlobStore, schema) but nothing can yet accept an export, prove what arrived, or record durable progress. Raw-first intake is non-negotiable in AGENTS.md, and every later parser item consumes exactly the evidence this slice produces.

## What Changes

- Add an authenticated, tenant-scoped archive receipt: a caller presents a tenant principal naming exactly one known account or organization; receipt refuses principals that resolve to nothing or to more than one scope before any byte is stored.
- Add streaming SHA-256 ingestion: archives are hashed and counted while received in bounded chunks, never fully buffered; the configured maximum archive byte size is enforced mid-stream and an exceeded upload is refused without leaving durable state behind.
- Extend the BlobStore with a capped streaming ingest path that preserves the existing write-once, verify-on-read semantics and returns the same fleet-shaped `BlobRef` (per the workspace `blob-references` spec).
- Add durable import-run state: every accepted receipt creates an import run whose state transitions are guarded, idempotent database updates; a run interrupted at any point resumes from its recorded state after restart, and terminal states refuse regression.
- Add duplicate detection by archive digest with explicit outcomes: receiving bytes whose digest already exists returns the existing export as a distinct outcome instead of re-storing bytes or silently creating a second snapshot record.
- Add a configurable maximum-archive-bytes limit to the typed process configuration with the same strict validation style as the existing limits.
- Out of scope: HTTP wire endpoints for Platform/export-agent integration (cross-repository contract work), safe container inspection/extraction, schema detection, parsers, completeness, events (plan items 3+).

## Capabilities

### New Capabilities

- `archive-receipt`: Authenticated tenant-scoped acceptance of provider archive uploads: principal verification before storage, streaming hash and size accounting under a configured cap, write-once raw placement, and explicit stored/duplicate outcomes keyed by archive digest.
- `import-state`: The durable, resumable state machine for one import pass over a received archive: guarded idempotent transitions, crash recovery from the last recorded state, and terminal states that cannot regress.

### Modified Capabilities

- `blob-store`: Adds the requirement that bytes may arrive as an unbounded stream: the store SHALL consume a stream in bounded chunks while hashing, SHALL enforce a caller-supplied byte cap by refusing mid-stream, and SHALL leave either nothing or one complete immutable object behind.

## Impact

- Code: `crates/claude-archive` gains `receipt` and `import_state` modules; `blob_store` gains a streaming ingest path alongside the buffered one; `config` gains one limit; `lib.rs` re-exports. The service binary wires nothing new yet (no HTTP surface in scope).
- Database: no schema change. The existing first-version `claude_archive.exports` and `claude_archive.import_runs` tables already carry digest uniqueness, blob references, byte sizes, acquisition vocabulary, and the import-state CHECK vocabulary this slice needs.
- Configuration: new optional-with-default `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES` key; unknown-key strictness unchanged.
- Dependencies: none added.
- Documentation: README status paragraph and DEVELOPMENT.md workflow claims move from "receipt planned" to "receipt and import state exist".
