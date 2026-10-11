## 1. One blob reference protocol (XR-021 CONTRACTS.md R2-07)

- [x] 1.1 RED: `crates/claude-archive/tests/privacy_deletion_survivors.rs` adds `an_import_landing_between_row_removal_and_erase_keeps_its_blob`, `a_registrar_blocked_by_the_eraser_re_stores_the_blob`, `a_registrar_that_cannot_re_store_fails_retryably_and_registers_nothing` and `an_import_blocked_by_the_eraser_stores_its_parser_evidence_again` (the real importer; with its `register_blobs` call commented out it fails because the import does not wait for the digest lock). Signature-only stubs (`PrivacyDeletionExecutor::with_erase_gate`, `blob_protocol::register`, `ReceiptError::Registration`) make them compile; they fail on assertions: the first because the eraser erases a blob a committed row references, the others because registration neither waits nor checks presence.
- [x] 1.2 GREEN: `blob_protocol.rs` (`blob_lock`, `referenced_among`, `register`), `erase_bytes` per-item transaction, and the registrars in `receipt.rs`, `initial_import.rs`, `reparse.rs` and `import_artifacts.rs`.

## 2. Size guard, undeliverable rows and honest reports (R2-04 rules 2 and 4)

- [ ] 2.1 RED: `crates/claude-archive/tests/contract_projection.rs` adds `a_fact_over_the_event_limit_is_not_enqueued_and_the_import_is_partial`; it fails because the oversized conversation is enqueued and the report is `succeeded`.
- [ ] 2.2 RED: `crates/claude-archive/tests/operation_outbox.rs` adds `an_undeliverable_row_does_not_wedge_the_pump` and `transient_refusals_never_make_a_row_undeliverable` against a real JetStream with a small `max_payload`; the first fails because the pump retries the large row forever and readiness stays false.
- [ ] 2.3 GREEN: the `RATATOSKR__RECEIPT__MAX_EVENT_BYTES` key, the enqueue-time guard and the `ai_archive.facts.too_large` warning.
- [ ] 2.4 GREEN: `outbox_events.undeliverable_at` in `schema.sql`, the pump's payload comparison, `claude_archive_outbox_undeliverable_total{class}`.

## 3. Operator example and documentation (R2-10)

- [ ] 3.1 RED: `services/claude-archive/tests/boot.rs` adds `the_shipped_example_loads`; it fails because `deploy/systemd/claude.conf.example` does not exist.
- [ ] 3.2 GREEN: ship the example. `README.md` and `DEVELOPMENT.md` list the bus, accounts and size-guard variables (documentation, so no test fails first).

## 4. Gate

- [ ] 4.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict` against PostgreSQL and a JetStream-enabled `nats-server`.
