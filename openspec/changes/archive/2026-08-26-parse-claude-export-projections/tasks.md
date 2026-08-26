## 1. Dependency boundary

- [x] 1.1 Owner approval was given in this session to use the already pinned `serde_json = 1.0.151` in the archive crate's production parser: it is necessary to preserve original JSON values, adds no package or lockfile change, and carries the existing MIT/Apache-2.0 maintenance and JSON parser attack-surface considerations.
- [x] 1.2 Moved approved `serde_json` from the crate dev-dependencies to dependencies and verified `cargo metadata --locked --no-deps --format-version 1` succeeds. This task cannot begin with a failing behavior test because it changes build configuration only.

## 2. Supported projection mapping

- [x] 2.1 Added the committed synthetic consumer-export fixture and a compiling failing `maps_all_supported_records_and_relationships` test in `crates/claude-archive/tests/export_projection.rs`; it failed as predicted at `projects.len()`, with `0` instead of `2`.
- [x] 2.2 Implemented the smallest public parser and normalized records needed for 2.1, registered its exact consumer-export declaration, and verified `maps_all_supported_records_and_relationships` passes.

## 3. Provenance and determinism

- [x] 3.1 Added a failing `stamps_every_projection_record_with_parser_provenance` test in `crates/claude-archive/tests/export_projection.rs`; it failed as predicted because the root schema stamp was empty.
- [x] 3.2 Added parser provenance to the result and every normalized record, and verified `stamps_every_projection_record_with_parser_provenance` passes.
- [x] 3.3 Added `parses_identical_bytes_deterministically` over retained unknown evidence in `crates/claude-archive/tests/export_projection.rs`; it passed immediately because the already-tested mapper preserves source arrays and uses deterministic JSON-map iteration. No artificial regression was introduced merely to manufacture a RED result.
- [x] 3.4 Verified retained-evidence ordering is deterministic without minted volatile IDs through `parses_identical_bytes_deterministically`; no additional implementation was required beyond the existing source-order vectors and ordered map traversal.

## 4. Loss-aware unknown evidence

- [x] 4.1 Added a failing `preserves_unknown_fields` test in `crates/claude-archive/tests/export_projection.rs`; its original-value/JSON-pointer assertion failed as predicted because unknown fields were discarded.
- [x] 4.2 Retained unrecognized fields as inert ordered JSON evidence and verified `preserves_unknown_fields` passes.
- [x] 4.3 Added a failing `preserves_unknown_content_variants` test in `crates/claude-archive/tests/export_projection.rs`; it failed as predicted with the typed unsupported-content error.
- [x] 4.4 Retained unsupported content variants as inert ordered JSON evidence and verified `preserves_unknown_content_variants` passes.
- [x] 4.5 Added `refuses_missing_required_message_identifier` in `crates/claude-archive/tests/export_projection.rs`; it passed immediately because strict identifier validation was required by the already-tested supported mapper. No artificial regression was introduced merely to manufacture a RED result.
- [x] 4.6 Verified the parser boundary returns the typed invalid-structure error for a missing message identifier through `refuses_missing_required_message_identifier`; no additional implementation was required.

## 5. Validation and real-fixture follow-up

- [x] 5.1 Added the synthetic normalized JSON golden and ran its read-only mapping test; no fixture blessing is used in the normal test path. This task cannot start from a failing behavior test because it records a completed deterministic contract.
- [x] 5.2 Recorded in `DEVELOPMENT.md` that a minimized owner-provided real Claude export is a required follow-up validation blocker; all committed fixtures are synthetic and contain no real export, archive content, title, or filename. This task cannot start from a failing behavior test because it records a validation boundary.
- [x] 5.3 Ran `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the complete command list from `DEVELOPMENT.md` through `build-gate`, and `git diff --check`, `openspec validate --all --strict`, and `openspec validate --archived`; every available gate is green.
