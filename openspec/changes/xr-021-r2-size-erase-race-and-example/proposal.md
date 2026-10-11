## Why

Round 2 of changeset XR-021 fixes the defects that independent verifiers found by tracing each flow through the pushed code. Three of them land in the Claude archive service. The binding text is XR-021 CONTRACTS.md sections R2-07, R2-04 and R2-10; this proposal cites them and does not restate them.

- Blob erasure never re-checks references. `erase_bytes` erases each `erase_blob` item without asking whether a surviving row references the digest; the only check ran in the earlier row-removal transaction. An import of identical bytes that lands between the two commits can leave a committed row that references an erased blob (R2-07).
- A conversation or Artifact fact larger than the broker's payload limit is enqueued, refused on every attempt, keeps the pump failing and keeps readiness false forever, while the import is reported as a plain success (R2-04 rules 2 and 4).
- No operator example is shipped, so the receipt accounts and the event bus are silently disabled unless the operator guesses the variable names; the docs do not list them (R2-10).

## What Changes

- One blob reference protocol for registration and erasure: `blob_lock` is `pg_advisory_xact_lock(hashtextextended('blob:' || digest_hex, 0))`. The eraser takes, per item and in one transaction, the tenant lock, the digest lock, recomputes the references with the planner's function, then retains or erases and marks the item. The registrars (the receipt and the import paths) take the digest locks, ascending, before the first referencing insert and verify the blob is present; a missing blob is stored again from the bytes the registrar still holds, and otherwise the registration fails with a retryable class and writes nothing.
- A conversation or Artifact fact whose serialized envelope exceeds `RATATOSKR__RECEIPT__MAX_EVENT_BYTES` (default 8000000, validated 65536..=8388608) is not enqueued; the import's terminal operation report is `partially_succeeded` with the warning `ai_archive.facts.too_large`, and at most one `ai_archive.import.incomplete`.
- `claude_archive.outbox_events.undeliverable_at` marks a row whose body is larger than the connected server's `max_payload` (minus 1 KiB); the pump skips it, records `last_error = payload_too_large`, counts `claude_archive_outbox_undeliverable_total{class}`, and an undeliverable row never makes readiness false. A transient refusal never marks a row undeliverable.
- `deploy/systemd/claude.conf.example` ships and is loaded by a boot test through `Config::from_environment`; `README.md` and `DEVELOPMENT.md` list the bus, accounts and size-guard variables.

Breaking, with no shims: `schema.sql` gains `outbox_events.undeliverable_at` in place; `record_verified_platform_archive` takes the `BlobStore` so it can hold the digest lock and check presence; `PrivacyDeletionExecutor` gains a test seam for the erase step. No migration file is added.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `privacy-deletion`: the blob reference protocol shared by erasure and registration.
- `claude-knowledge-events`: the enqueue-time size guard, the honest report and the undeliverable row.
- `service-runtime`: the shipped operator example.

## Impact

Touches `privacy_deletion/execution.rs` and `rows.rs`, the receipt, initial-import, reparse and Artifact persistence paths, `contract_projection.rs`, `operation_outbox.rs`, `events.rs`, `config.rs`, `schema.sql`, the telemetry registry, `deploy/systemd/claude.conf.example`, `README.md` and `DEVELOPMENT.md`. The receipt listener and its error vocabulary, tombstone owner mapping for non-user references, account erasure transport and the workspace profiles are out of scope.
