## 1. Contracts pin

- [x] 1.1 Configuration, so there is no test to fail first: pin `identifiers`, `ai-archive-contracts`, `operation-contracts` and `event-envelope` to the XR-021 contracts commit, add `ratatoskr-error-contracts` at the same revision, refresh `Cargo.lock`. The red evidence for the pin is task 6.1, which cannot decode `user_requested` at the old pin.

## 2. Operator listener

- [x] 2.1 RED: `crates/claude-archive/tests/config.rs` expects the default `127.0.0.1:9086` with the message "the operator listener default is loopback port 9086" and fails on 9084.
- [x] 2.2 GREEN: change the default in `crates/claude-archive/src/config.rs` and the two mentions in `DEVELOPMENT.md`.

## 3. Receipt listener

- [x] 3.1 RED: in `services/claude-archive/tests/admin.rs` add `capabilities_document_is_served_without_claims` and `receipt_refusals_are_error_envelopes`, one case per refusal (401 without claims, 401 with Authorization, 400 wrong or missing Content-Type, 400 identity mismatch, 413 over the limit, 405 for PUT); each answer must be `application/json` and parse as `ErrorEnvelope`.
- [x] 3.2 GREEN: add `services/claude-archive/src/fault.rs`, mount the capabilities route beside the receipt route, answer 405 through the route fallback.
- [x] 3.3 REFACTOR: replace the literal path and header strings with the `platform_receipt` constants. No new test.

## 4. Incomplete-import warning

- [x] 4.1 RED: `an_incomplete_import_reports_partial_success_with_the_contract_warning` builds the report for a non-complete summary and asserts `warnings == [incomplete_import_warning()]` and `validate().is_ok()`.
- [x] 4.2 GREEN: set the warning in `operation_report` when the status is `PartiallySucceeded`.

## 5. Generic ack-gated pump

- [x] 5.1 RED: `crates/claude-archive/tests/operation_outbox.rs` covers `conversation_added_envelope_is_published` against a real JetStream, `an_unknown_event_type_is_a_hard_error`, `subject_for_event_type` for every closed type, and the pending query returning an unpublished tombstone ordered by `occurred_at, event_id`.
- [x] 5.2 GREEN: generalise `operation_outbox.rs` over the closed list, add the `event_type` CHECK to `schema.sql`.

## 6. Contract tombstones

- [x] 6.1 RED: in `crates/claude-archive/tests/privacy_deletion/closure_cases.rs` add `tombstone_outbox_row_is_a_complete_event_envelope`, `privacy_tombstone_does_not_abort_when_provider_tombstone_exists` and `non_user_tenant_ref_is_rejected_at_planning_when_tombstones_exist`.
- [x] 6.2 GREEN: add `privacy_deletion/tombstones.rs` building `AiArchiveTombstone` envelopes through `ArchiveOutbox::enqueue_tombstone_in`, with `ON CONFLICT DO NOTHING`; planning rejects a non-user tenant reference and keeps unpublished tombstones of earlier requests.

## 7. Fact projection

- [x] 7.1 RED: `crates/claude-archive/tests/contract_projection.rs` imports the synthetic export and expects `archive.imported`, `project.added`, `conversation.added` and `artifact.added` facts in order, `AiConversationAdded::validate` to pass, the owner to be `user:<Platform user>`, an identical reparse to add nothing and a changed one to add one `conversation.updated`.
- [x] 7.2 GREEN: add `contract_projection.rs` and call it from the import-completion and reparse persistence transactions.

## 8. Privacy deletion

- [x] 8.1 RED: `every_foreign_key_into_deletion_roots_is_handled` queries `pg_constraint` and fails for the unhandled references.
- [x] 8.2 RED: `closure_cases.rs` adds `raw_export_scope_removes_content_parts_and_project_sources`, `conversation_scope_removes_message_dependents` and `tenant_scope_deletes_reparse_runs_and_platform_imports`.
- [x] 8.3 GREEN: add `planning_closure.rs`, delete `platform_operation_imports`, `reparse_runs` and `tombstones` by export for every scope, expose `HANDLED_REFERENCES`; 8.1 and 8.2 pass.
- [x] 8.4 RED: `rows_are_removed_before_bytes_and_a_crash_between_resumes` with `FinalizationFault::AfterRowsCommitted`.
- [x] 8.5 GREEN: split execution into `execution.rs`, `rows.rs` and `tombstones.rs`, add `evidence_blob_ref` and `rows_removed_at` to the schema, make `finalize` take the evidence blob.

## 9. Documentation and gate

- [x] 9.1 Documentation, so there is no test to fail first: `DEVELOPMENT.md` (ports, receipt behaviour) and `README.md`.
- [x] 9.2 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.

## 10. Review fixes

- [x] 10.1 RED: `crates/claude-archive/tests/config.rs` expects the receipt listener default `127.0.0.1:8097`, `RATATOSKR__RECEIPT__LISTEN_ADDRESS` accepted only as loopback with a port, and the same address for both listeners refused; `services/claude-archive/tests/admin.rs` expects the receipt router to serve the receipt and capability routes and no operator route and the operator router to serve neither receipt route; `boot.rs` expects the real binary to answer the capability probe on the receipt port and `404` for the other plane's routes.
- [x] 10.2 GREEN: add `ReceiptConfig::listen_address`, replace `service_router` with `receipt_router`, bind and drain two listeners in `main.rs`, and update `DEVELOPMENT.md` and `README.md`.
- [x] 10.3 RED: `operation_outbox.rs` `a_refused_publish_backs_off_and_never_starves_the_rows_behind_it` against a real JetStream. The behaviour already exists, so the test is a regression guard: it is shown to fail against a deliberately broken `record_refusal` instead.
- [x] 10.4 RED: `contract_projection.rs` expects the reparse head to carry the first import's completeness report and warnings, an Artifact without verified bytes to be named as an `unprojected_artifact` gap, and an unstamped conversation to be named on a re-announced head.
- [x] 10.5 GREEN: add `ReportView::original`, `Omitted` and the `unprojected_*` gaps, count every skip in the loaders, re-announce the head on an omission, and let the operation report agree.
- [x] 10.6 RED: `artifact_identity.rs` expects a second export of the same account to observe one row for a project, conversation, message and unscoped Artifact.
- [x] 10.7 GREEN: add `artifacts.account_id` and the message and unscoped unique indexes in `schema.sql`, scope the lookup by account, select Artifacts by account in tenant-scope planning and the portable export.
- [x] 10.8 Regression guards for behaviour that already holds, so there is no implementation step: `closure_cases.rs` and `commit_cases.rs` assert that the tombstone envelope carries the recorded request correlation id, that a non-reference correlation falls back to `privacy_request:<request id>`, that no surviving row references a missing blob after the row phase and after the resume, and that the census fails on a stale handled entry. The correlation, fallback and census guards are each shown to fail against a deliberate mutation; the dangling-reference guard only observes an invariant the implementation already keeps.
- [x] 10.9 Documentation, so there is no test to fail first: the unmapped-account refusal, the correlation fallback, the duplicate-delivery report limit and the identity rule in `DEVELOPMENT.md` and `design.md`.
- [x] 10.10 Run the full gate again.
