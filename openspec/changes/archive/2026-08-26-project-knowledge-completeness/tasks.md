## 1. Project evidence projection

- [x] 1.1 Add `preserves_first_class_project_instructions_and_knowledge_references` to `crates/claude-archive/tests/export_projection.rs`, with the expanded synthetic fixture; after adding only the compiling empty projection surface, run it and confirm its instruction-count assertion fails with `0` rather than `1`.
- [x] 1.2 Implement the exact synthetic `knowledge_files` mapping and separate `ProjectInstruction` and `ProjectKnowledgeFile` records in `export_projection.rs`; rerun `preserves_first_class_project_instructions_and_knowledge_references` and verify it passes with provider/project relationships and parser stamps intact.

## 2. Verified local knowledge files

- [x] 2.1 Add `stores_matching_knowledge_bytes_as_a_verified_blob` to a new `crates/claude-archive/tests/project_knowledge.rs`; run it against the compiling no-op ingest boundary and confirm the expected verified result assertion fails.
- [x] 2.2 Implement the bounded `ProjectKnowledgeIngestor` using the existing BlobStore and independent SHA-256 comparison; rerun `stores_matching_knowledge_bytes_as_a_verified_blob` and verify its BlobRef digest, length, and verified status pass.

## 3. Quarantine semantics

- [x] 3.1 Add `quarantines_digest_mismatch_without_publishing_a_backup` to `crates/claude-archive/tests/project_knowledge.rs`; run it with the initial ingest behavior and confirm the quarantine-status assertion fails rather than a setup or compile error.
- [x] 3.2 Implement digest-anomaly classification with retained inert evidence and content-free warning text; rerun `quarantines_digest_mismatch_without_publishing_a_backup` and verify the blob is not exposed as a verified backup.

## 4. Completeness reporting

- [x] 4.1 Add `reports_fixture_counts_gaps_and_cumulative_math` to `crates/claude-archive/tests/completeness.rs`; run it with the compiling empty report surface and confirm its per-archive verified/missing/quarantined count assertion fails.
- [x] 4.2 Implement per-archive and cumulative deterministic completeness calculation, status precedence, and source-order warnings; rerun `reports_fixture_counts_gaps_and_cumulative_math` and verify fixture and cumulative counts, warnings, and `AssetsPartial` classification pass.

## 5. Contract and full validation

- [x] 5.1 Update the synthetic golden projection and `DEVELOPMENT.md` scope status after the behavior tests are green; verify the golden mapping test and documentation accurately name the synthetic-only validation boundary. This task cannot start from a failing behavior test because it records completed behavior and validation scope.
- [x] 5.2 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, every command in the `DEVELOPMENT.md` Rust gate through `build-gate`, `git diff --check`, `openspec validate --all --strict`, and `openspec validate --archived`; verify each observed gate is green before integration.
