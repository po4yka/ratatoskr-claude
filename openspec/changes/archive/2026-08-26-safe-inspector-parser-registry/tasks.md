## 1. Dependency and limit contract

- [x] 1.1 Obtain the repository owner's explicit approval to add `zip = "=8.6.0"` (MIT; default features off; only `deflate`) as a production dependency, including its maintenance, security, and lockfile impact. This task cannot start from a failing test because it is an authorization boundary; record the approval before changing `Cargo.toml` or `Cargo.lock`.
- [x] 1.2 Add approved `zip = "=8.6.0"` with `default-features = false` and `features = ["deflate"]` to the workspace and crate manifests, update the lockfile, and verify `cargo metadata --locked --no-deps --format-version 1` and `cargo deny --locked check` pass.
- [x] 1.3 Add failing tests in `crates/claude-archive/tests/config.rs`: `archive_inspection_limits_default_to_safe_bounds` (50,000 entries, 1 GiB per entry, 10 GiB total extracted, ratio 100), `archive_inspection_limits_accept_valid_environment_overrides`, and `archive_inspection_limits_reject_zero_or_inconsistent_values`. First add compileable `Limits` placeholders with nonmatching defaults so assertions—not compilation—fail. Run `cargo nextest run --locked -p ratatoskr-claude-archive --test config archive_inspection_limits`; confirm each assertion fails for the stated value.
- [x] 1.4 Implement strict archive inspection/extraction limits in `config.rs`, including positive/internally consistent environment parsing, and verify the three 1.3 tests pass.

## 2. Structural hostile-input inspection

- [x] 2.1 Add failing public integration tests in `crates/claude-archive/tests/archive_inspection.rs`: `inspection_rejects_traversal_absolute_and_duplicate_normalized_paths`, `inspection_rejects_link_encrypted_and_special_entries`, and `inspection_rejects_entry_count_declared_expansion_and_compression_ratio_bombs`. Build only synthetic ZIP bytes, store each through `BlobStore`, and add a compileable inspector stub returning a typed unsupported result so every test fails on its expected safe/unsafe assertion. Run `cargo nextest run --locked -p ratatoskr-claude-archive --test archive_inspection inspection_rejects`; confirm assertion failures rather than ZIP-fixture or compile errors.
- [x] 2.2 Implement the immutable structural inspector with validated portable paths, duplicate detection, entry kind/encryption checks, metadata count/size/ratio bounds, and typed refusal classes; do not request entry bodies during inspection. Verify all 2.1 hostile tests pass.
- [x] 2.3 Add a failing integration test `inspection_accepts_a_bounded_json_inventory_without_reading_or_executing_it` that asserts a valid JSON entry is returned in a stable, non-body-bearing inventory. Run the exact nextest selector and confirm the missing successful inventory assertion fails.
- [x] 2.4 Implement the accepted immutable inventory result and verify 2.3 plus every hostile inspection test pass.

## 3. Bounded extraction, quarantine, and provenance

- [x] 3.1 Add failing integration tests in `crates/claude-archive/tests/archive_inspection.rs`: `extraction_stores_validated_entry_with_raw_digest_provenance`, `extraction_refuses_actual_entry_expansion_past_the_streaming_cap_without_a_blob_ref`, and `html_and_nested_archives_are_quarantined_without_execution`. Use a raw digest independently computed from the received ZIP and an extractor stub that makes all tests fail on their returned artifact/disposition assertions. Run `cargo nextest run --locked -p ratatoskr-claude-archive --test archive_inspection extraction`; confirm assertion failures.
- [x] 3.2 Implement direct bounded streaming from an accepted ZIP inventory to `BlobStore`, real-byte cumulative accounting, conservative prefix sniffing, quarantine disposition, and `RawArchiveProvenance` linkage. Verify all 3.1 tests pass and each stored BlobRef reads back exactly to its ZIP entry bytes.

## 4. Versioned parser registry

- [x] 4.1 Add failing integration tests in `crates/claude-archive/tests/parser_registry.rs`: `registry_selects_exact_schema_mode_and_capability_match`, `registry_reports_unsupported_detected_schema_version`, `registry_reports_missing_capability_without_fallback`, and `registry_refuses_overlapping_parser_declarations`. Use test-only parser descriptors and a compileable registry stub so failures are on selection outcomes. Run `cargo nextest run --locked -p ratatoskr-claude-archive --test parser_registry`; confirm every case fails on its predicted outcome assertion.
- [x] 4.2 Implement stable detected-schema, parser-id/version, capability declaration, registry validation, and exact selection outcomes; do not register or claim a real Claude projection parser. Verify all 4.1 matrix tests pass.

## 5. Documentation, full validation, and archive

- [x] 5.1 Update `README.md` and `DEVELOPMENT.md` to state that plan item 3 exists while first Claude projections remain planned. This task cannot start from a failing test because it is documentation alignment; verify `git diff --check` passes.
- [x] 5.2 Run the full product gate through `build-gate --` as documented in `DEVELOPMENT.md`: fetch, deny, format, clippy, debug build, workspace tests, doc tests, and release build. Verify every command exits 0; do not weaken a gate or omit a failing suite.
- [x] 5.3 Run `openspec validate --all --strict`, confirm all implementation and documentation tasks are verified, and prepare the change for archival. This task cannot include the archive move itself because OpenSpec requires all tasks to be complete before that move.
- [x] 5.4 Review the final diff for scope, security, and documentation accuracy, and confirm that only this change will be staged. Commit, integration, push, remote verification, and worktree cleanup are delivery actions performed after the archived change is included in the staged diff.
