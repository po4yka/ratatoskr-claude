# Tasks - archive receipt and import state

## 1. Archive byte cap in configuration

- [x] 1.1 Add failing tests in `crates/claude-archive/tests/config.rs`: `max_archive_bytes_defaults_to_ten_gibibytes` (default `Limits` carries 10_737_418_240), `max_archive_bytes_accepts_configured_value` (`RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES=1024` parses through `from_environment`), `max_archive_bytes_rejects_non_positive_value` (`0` yields a violation naming `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES`). Extend `Limits` first with `max_archive_bytes: u64` defaulting to 0 and no environment handling, so all three fail on their value assertions, never on compilation. Run: `cargo test -p ratatoskr-claude-archive --test config --locked`; confirm all three fail for the stated reason.

- [x] 1.2 Implement the `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES` entry in `crates/claude-archive/src/config.rs` (strict positive parse, default 10 GiB in `Default for Config`). Verification: the three tests from 1.1 pass.

## 2. Capped streaming ingest in the blob store

- [x] 2.1 Add failing tests in `crates/claude-archive/tests/blob_store.rs`: `streamed_ingest_matches_buffered_reference_across_chunk_boundaries` (payload spanning several 64 KiB chunks plus ragged remainder; streamed reference equals the buffered reference for the same bytes), `streamed_ingest_refuses_oversized_stream_and_leaves_nothing_complete` (payload exceeding its cap; expect the limit error; reading back at the independently computed digest path fails `Missing`; staging directory holds no leftover files), `streamed_ingest_refuses_empty_stream` (expect the empty-input error), `streamed_ingest_preserves_write_once_deduplication` (the same bytes streamed twice yield equal references and exactly one object beneath the store root). Stub `BlobStore::store_stream` to return `Err(StoreError::Unavailable)` so the four fail on assertions. Run: `cargo test -p ratatoskr-claude-archive --test blob_store --locked`; confirm failures.

- [x] 2.2 Implement `BlobStore::store_stream(media_type, reader, max_bytes)` in `crates/claude-archive/src/blob_store.rs`: bounded-chunk consumption folding an incremental SHA-256 while staging, mid-stream cap refusal removing the staged file, empty-input refusal, then the existing verify-resident/hard-link publish. Verification: the four tests from 2.1 pass.

- [x] 2.3 Refactor the buffered `store` to delegate placement to the streaming path over an in-memory cursor. Adds no test and changes no behaviour; the full suite from 2.1 and item 1 stays green as verification.

## 3. Durable import-run state machine

- [x] 3.1 Add failing tests in `crates/claude-archive/tests/import_state.rs` against `TestDatabase`: `fresh_run_starts_at_received_and_reads_back`; `pipeline_advances_through_documented_successors_persistently` (walk received through completed, asserting each persisted state via an independent fresh query); `transition_from_unexpected_origin_is_refused_without_change` (expected origin `received` while the run sits at `stored` conflicts and leaves `stored`); `replaying_applied_transition_reports_already_applied`; `run_resumes_after_simulated_crash_mid_pipeline` (advance to `extracting`, close that database handle entirely, open a brand-new handle, read `extracting`, finish to `completed`); `terminal_states_accept_no_further_transitions` (seed each of completed/partial/failed/quarantined, attempt one advance each, expect conflict and unchanged state). Stub every `ImportRunStore` method to return a typed unimplemented error so tests fail on assertions. Requires the compose database. Run: `cargo test -p ratatoskr-claude-archive --test import_state --locked`; confirm failures.

- [x] 3.2 Implement `crates/claude-archive/src/import_state.rs`: `ImportState` enum mirroring the schema CHECK vocabulary, static successor table (linear pipeline plus partial/failed/quarantined reachable from non-terminal states), `ImportRunStore::create_initial`, guarded `advance` as one atomic `UPDATE ... WHERE run_id AND state = expected_origin` distinguishing already-applied replay from genuine conflict, and a resume read. Verification: all 3.1 tests pass.

## 4. Authenticated tenant-scoped receipt

- [x] 4.1 Add failing tests in `crates/claude-archive/tests/receipt.rs` against `TestDatabase` and a scratch blob root: `authenticated_stored_receipt_records_export_run_and_readable_bytes` (seeded account, chunk-fed archive larger than one internal chunk; stored outcome; export row carrying acquisition, digest equal to an independently computed SHA-256, exact byte size, resolvable blob reference whose read-back equals the upload; exactly one import run for the export in the initial state); `duplicate_receipt_names_existing_export_without_second_row_or_object` (identical bytes delivered twice; second outcome names the first export id; exactly one export row and one stored object); `distinct_archives_yield_distinct_exports`; `unknown_tenant_principal_is_refused_before_any_storage` (unresolvable identifier; refused outcome; zero export rows; zero objects under the store's digest tree); `ambiguous_scope_principal_is_refused` (claiming account and organization together); `oversized_stream_is_refused_with_cap_error_and_no_durable_state` (tiny cap override; limit error; no rows, no object); `empty_stream_is_refused`. Stub `receive_archive` to return a typed unimplemented error so every expectation fails on assertions. Requires the compose database. Run: `cargo test -p ratatoskr-claude-archive --test receipt --locked`; confirm failures.

- [x] 4.2 Implement `crates/claude-archive/src/receipt.rs`: `AcquisitionMode` closed vocabulary with lossless text mappings, `TenantPrincipal` construction resolving exactly one known tenant and refusing none/both/inconsistent scopes before any byte is read, `ReceiptOutcome::{Stored, Duplicate}` and `receive_archive` orchestrating capped streaming ingest, one transaction inserting `exports` plus the initial `import_runs` row, constraint-conflict mapping to the duplicate outcome naming the existing export, and re-export through `lib.rs`. Verification: all 4.1 tests pass.

## 5. Documentation, gates, archive

- [x] 5.1 Move README.md status and DEVELOPMENT.md workflow claims from "receipt planned" to "receipt and import state exist; inspection/parsing remain planned", keeping the boundary honest about what does not exist yet. This task cannot start from a failing test: it is documentation alignment.

- [x] 5.2 Run the docs-only gate (`git diff --check`, `openspec validate --all --strict`) and the full product gate from DEVELOPMENT.md (fetch/deny/fmt/clippy/build/test/doc-test/release build) until both exit 0.

- [x] 5.3 Tick every task, archive the change so the deltas fold into `openspec/specs/`, then run `openspec validate --archived` as part of the final gate.
