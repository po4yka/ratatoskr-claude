## 1. Artifact projection and stored evidence

- [x] 1.1 Add the synthetic Artifact fixture sequence and the compiling regression test `crates/claude-archive/tests/export_projection.rs::parses_artifact_records_without_flattening_version_payloads`; run `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive parses_artifact_records_without_flattening_version_payloads` and confirm its assertion fails because the projection has no Artifact record.
- [x] 1.2 Extend the consumer-export projection with typed Artifact identity, relationships, version evidence, parser/raw provenance, and source payload bytes for the separate BlobStore ingest boundary; rerun `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive parses_artifact_records_without_flattening_version_payloads` and confirm it passes.
- [x] 1.3 Add the compiling regression test `crates/claude-archive/tests/artifacts.rs::reconciles_repeated_and_changed_artifact_versions_into_one_chain`; run `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive reconciles_repeated_and_changed_artifact_versions_into_one_chain` and confirm its chain-length assertion fails because reconciliation is absent.
- [x] 1.4 Implement immutable, idempotent Artifact-version reconciliation and extend the current schema definition in place with Artifact tables; rerun `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive reconciles_repeated_and_changed_artifact_versions_into_one_chain` and `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive --test schema` and confirm both pass.

## 2. Safe portable Artifact representation

- [x] 2.1 Add the compiling regression tests `crates/claude-archive/tests/artifact_portable.rs::unknown_artifact_type_is_unrenderable_without_a_derivative` and `crates/claude-archive/tests/artifact_portable.rs::portable_artifact_output_is_byte_deterministic`; run each with `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive --test artifact_portable` and confirm the rendering-status and byte-equality assertions fail because no portable Artifact output exists.
- [x] 2.2 Implement deterministic portable Artifact JSON and allowlisted inert text/Markdown derivatives, retaining unknown raw evidence with stable `unrenderable` status and no derivative; rerun `build-gate -- cargo nextest run --locked -p ratatoskr-claude-archive --test artifact_portable` and confirm both tests pass.

## 3. Documentation and complete validation

- [x] 3.1 Update `README.md` with the supported Artifact evidence and portable-rendering boundary; no failing test applies because this task documents behavior already enforced by the preceding tests.
- [x] 3.2 Run the complete local gate from `DEVELOPMENT.md` plus `build-gate -- cargo nextest run --locked --workspace`, `cargo fmt --all -- --check`, and `build-gate -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; confirm every command passes and inspect the final diff for raw-evidence, privacy, and scope regressions.
