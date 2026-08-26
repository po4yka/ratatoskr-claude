# Design - archive receipt and import state

## Context

Item 1 landed the container: typed strict config, telemetry, health plane, a buffered content-addressed `BlobStore` (`crates/claude-archive/src/blob_store.rs`), the `claude_archive` schema applied at startup, and a disposable-database test harness. The schema's `exports` table already carries `archive_hash` (unique), `blob_ref`, `byte_size`, `acquisition`, `account_ref`/`organization_ref` under a scope CHECK, and `received_at`; `import_runs` carries the thirteen-state CHECK vocabulary from AGENTS.md. Binding constraints: development status forbids migrations (schema edits happen in place; here no edit is needed), behaviour visible to several repositories belongs to the workspace store (the fleet `blob-references` spec governs every produced reference), and clippy.toml enforces functions <= 100 lines, <= 7 arguments, nesting depth <= 5.

## Goals / Non-Goals

**Goals:**

- One domain-level receipt operation that takes authenticated tenant identity, an acquisition mode, and an unbounded byte stream, and produces either a stored snapshot or an explicit duplicate verdict.
- Hash-and-size accounting that never materializes the whole archive in memory, with the byte cap enforced mid-stream.
- An import-run state machine whose transitions are single guarded SQL updates: crash-safe, replay-safe, regression-proof.
- Every spec scenario realized by a named test written before its implementation.

**Non-Goals:**

- HTTP/wire endpoints for Platform or the export agent (cross-repository contract work; the receipt function is the seam they will call).
- Container inspection, extraction, schema detection, parsers, completeness, events (plan items 3+).
- Tenant provisioning: receipt verifies tenants exist but does not create them; later slices create accounts/organizations from parsed evidence.
- A second BlobStore backend abstraction.

## Decisions

### D1: Receipt is a library seam, not an HTTP route

`receipt::receive_archive(...)` lives in the domain crate and is exercised by integration tests directly. The Platform-facing upload protocol (encoding, auth transport, fault envelopes) is a cross-repository contract change per AGENTS.md; landing it now would freeze a wire format no consumer has agreed to. Alternative considered: add an axum route now - rejected because INTERFACES.md inbound surface belongs to the workspace contracts and no upload contract exists in the store yet.

### D2: Authentication is principal verification against owned tenant rows

A `TenantPrincipal` carries either `Account(Uuid)` or `Organization(Uuid)` plus the acquisition mode, and is constructed only through `authenticate_principal` which resolves the claim against `claude_archive.accounts`/`.organizations`. Resolution rules: exactly one row must match, and exactly one scope variant may be claimed; a claim matching nothing, or claiming both scopes, or an organization-scoped consumer-export attempt is refused with a typed error before the first byte is read. Alternatives: bearer tokens or an external identity provider - rejected, no fleet-wide identity service exists; the principal constructor is the seam where Platform authentication will slot in. The `exports_scope_check` constraint stays the last-line guarantee that a stored snapshot always names a tenant.

### D3: Streaming ingest inside the existing BlobStore

Add `store_stream(media_type, reader, max_bytes)` alongside the buffered `store`: consume `Read` in 64 KiB chunks, fold each chunk into one incremental SHA-256 and into the staging file (reusing the existing stage/sync/hard-link publish machinery), refuse an empty stream, and enforce `max_bytes` by refusing mid-stream with the staged file removed. On completion the digest is fully determined, so the existing exists-verify/hard-link race handling applies unchanged. `store` becomes a thin wrapper over a cursor, keeping one code path for placement. Alternatives: hash to a temp file then copy - doubles I/O; hash-then-write two passes over the reader - impossible for a non-replayable stream. The 64 KiB chunk size trades syscall count against memory boundedness and matches the existing 8 KiB staging granularity in spirit while keeping the function well under the 100-line limit.

### D4: Duplicate detection relies on the unique constraint, not a pre-check

Order of operations: stream into the BlobStore (idempotent by content addressing), then in one transaction insert `exports` and the initial `import_runs` row. A conflicting `archive_hash` insert fails; the transaction loads the existing export and receipt returns `Duplicate { existing_export_id }`. A pre-INSERT SELECT would only narrow the window, still need the constraint, and add a round trip; the constraint alone is race-free. Orphan blobs from crashes between placement and commit are inert: the same digest redelivered dedupes onto the same object and the row is then written; no GC is added in this slice.

### D5: Guarded-transition state machine over `import_runs`

The Rust `ImportState` enum mirrors the schema CHECK vocabulary exactly (`received, stored, inspecting, schema_detected, extracting, staging, validating, reconciling, publishing, completed, partial, failed, quarantined`). A static transition table admits only the documented successors: the linear pipeline `received -> stored -> inspecting -> schema_detected -> extracting -> staging -> validating -> reconciling -> publishing -> completed`, with `partial`, `failed`, and `quarantined` reachable from every non-terminal pipeline state. Advancing runs `UPDATE import_runs SET state = $to WHERE run_id = $run AND state = $from`: zero rows updated means either a replay (current already equals `to`; reported as already-applied, not an error) or a divergence (typed conflict naming nothing but the states). Terminal states have no outgoing edges, so no replay can regress them. Resume is simply reading the current state and continuing; a crashed process holds no locks. ARCHITECTURE.md sketches friendlier state names (`raw_stored`, `parsing`); the database CHECK from item 1 follows AGENTS.md, and AGENTS.md is authoritative, so the enum matches the database rather than renaming anything.

### D6: The archive byte cap joins the typed limits

`Limits.max_archive_bytes: u64`, loaded from `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES`, default 10 GiB, validated strictly positive like its siblings. Tests exercise refusals with tiny caps; production sizing is an operator decision, not a code constant. Alternative: a separate intake-limits section - rejected, one flat `limits` section already exists and a second home for thresholds invites drift.

### D7: Acquisition modes become a typed closed vocabulary

`AcquisitionMode` enum with the five schema values and lossless text mappings; the compiler makes an unknown mode unrepresentable at the type level while `FromStr` keeps the database round-trip honest. This replaces ad hoc strings at the receipt boundary without touching the schema.

### D8: Crash-window posture

Three windows exist and all are safe by construction: (a) crash mid-stream - staging file removed on the failure path, or left inert if SIGKILL'd; a UUID-named staging entry is never mistaken for content; (b) crash after blob placement, before commit - orphan object dedupes on redelivery (D4); (c) crash after commit, before the first transition - resume advances `received -> stored`. Each window gets an explicit test or an explicit design note referenced by a test comment.

## Risks / Trade-offs

- [Disk exhaustion from concurrent large uploads] -> the byte cap bounds each upload and the operator sizes the volume; per-tenant concurrency limiting arrives with the API plane.
- [Orphaned staging/object files accumulate after crashes] -> inert by naming and location; a maintenance sweep is deferred until there is an operational story, noted in code as `TODO(po4yka)` follow-up territory, not silently ignored.
- [SHA-256 collision between two different archives] -> cryptographically negligible; the store's verify-resident check turns a collision into a loud `Collision` instead of silent data loss.
- [Guarded-update state machine serializes through the database] -> import runs are low-frequency, human-scale operations; contention is not a realistic load profile.

## Migration Plan

No database migration (development status). Deployment is: merge, restart; new code writes rows the existing schema already accepts. Rollback is restarting the previous binary; rows written by this slice are plain inserts into pre-existing tables.

## Open Questions

None. The wire-protocol question is deliberately deferred to the cross-repository contract change and does not affect these specs.
