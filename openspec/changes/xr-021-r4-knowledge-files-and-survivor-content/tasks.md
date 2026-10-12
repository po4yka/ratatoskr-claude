## 0. Plan

Documentation: no failing test can precede it.

- [x] 0.1 This change (XR-021 CONTRACTS.md R4-02 and R4-03), validated with `openspec validate --all --strict`.

## 1. The import ingests knowledge files (R4-02 rules 1, 2, 4, 5, 6)

- [x] 1.1 RED: `crates/claude-archive/tests/knowledge_file_import.rs` adds `an_archive_with_a_verified_knowledge_file_imports_complete`, `a_digest_mismatch_is_quarantined_and_the_import_is_partial`, `a_missing_declared_digest_is_quarantined`, `an_invalid_media_type_is_quarantined`, `a_referenced_only_file_stays_a_missing_gap_without_a_blob`, `a_later_export_that_carries_the_bytes_upgrades_a_referenced_source` and `a_store_failure_while_ingesting_is_retried_not_failed`. They fail because the import reports `partial`, `assets_partial` and `knowledge_file_missing_bytes`, writes no blob and never attempts the ingest, so a faulted store is not even reached. `a_referenced_only_file_stays_a_missing_gap_without_a_blob` already passes and guards the honesty rule.
- [x] 1.2 GREEN: `PreparedInitialImport::knowledge_outcomes`, the ingest in `prepare_initial_import`, `ReparseError::KnowledgeIngest`, the source persistence in `initial_projection.rs`.

## 2. The knowledge blobs follow the registration protocol (R4-02 rule 3)

- [x] 2.1 RED: `tests/knowledge_file_import.rs::an_eraser_that_removed_a_knowledge_blob_before_registration_does_not_leave_a_dangling_row` fails because the import transaction references a blob nobody registered.
- [x] 2.2 GREEN: `register_blobs` lists the knowledge blobs and stages their bytes.

## 3. Per-export versions of projects and conversations (R4-03 rules 1 and 2)

- [x] 3.1 RED: `crates/claude-archive/tests/privacy_deletion_survivor_content.rs::a_cumulative_series_records_each_exports_own_version` fails because the schema has neither the column nor the table.
- [x] 3.2 GREEN: `schema.sql`, `entity_versions.rs`, `Serialize` and `Deserialize` on the parsed types, digests and snapshots recorded by the import and by reparse, reparse comparing an export's own digest.

## 4. Deletion restores survivors (R4-03 rules 3 to 6)

- [x] 4.1 RED: `tests/privacy_deletion_survivor_content.rs` (its series fixture lives in `tests/survivor_series/mod.rs` to stay under the 850-line cap) adds `deleting_the_newest_export_restores_the_intermediate_project_conversation_and_parts`, `a_knowledge_file_only_the_deleted_export_presented_is_removed_with_its_blob`, `deleting_the_middle_export_keeps_the_newest_content`, `deleting_the_oldest_export_changes_no_content`, `deleting_two_exports_in_turn_restores_the_oldest_content`, `an_edit_repeated_by_a_retained_export_is_kept`, `tenant_deletion_leaves_no_snapshot` and `the_survivor_fact_carries_the_restored_content_under_the_earliest_retained_export`, and `tests/privacy_deletion_survivors.rs::only_the_subjects_the_deleted_export_last_announced_are_re_announced` asserts content. They fail because the retained rows and the survivor fact carry the deleted export's content.
- [x] 4.2 GREEN: `privacy_deletion/restore.rs`, the source closure in `planning_closure.rs`, the removal of the deleted archive's facts under retained subjects, `reannounce.rs` naming the earliest retained presenter, `HANDLED_REFERENCES` and its `pg_constraint` test kept current.

- [x] 4.3 RED: `tests/privacy_deletion_survivor_content.rs::a_part_only_the_deleted_export_appended_is_removed_even_when_the_row_is_current` imports a non-cumulative newest export; the part the deleted export appended stays because the row's digest is a retained export's.
- [x] 4.4 GREEN: `restore.rs` rebuilds every conversation the request keeps because a retained export also observed it, and `reannounce.rs` announces a survivor again when its latest fact, under a retained archive, differs from the restored conversation, dropping the earlier facts of that conversation from this service's outbox that carried the deleted part. Whether Knowledge replaces its own copy is not asserted: it is an open cross-repository gap recorded in `design.md`.

- [x] 4.5 RED: `tests/privacy_deletion_restore_scope.rs::a_deletion_leaves_the_rows_of_another_account_alone` fails because the stale scans and the snapshot cleanups of `restore.rs` read and rewrite every account's rows.
- [x] 4.6 GREEN: the scans and cleanups of `restore.rs` are scoped to the request's accounts and its own inventory.

## 5. Documentation (R3-19 style)

Documentation: no failing test can precede it.

- [x] 5.1 `DEVELOPMENT.md` plan item 5 and the `README.md` import description say that the import ingests knowledge files and a deletion restores survivors.

## 6. Gate

- [x] 6.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict` against PostgreSQL and a JetStream-enabled `nats-server`.
