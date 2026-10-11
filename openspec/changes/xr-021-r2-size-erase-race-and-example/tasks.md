## 1. One blob reference protocol (XR-021 CONTRACTS.md R2-07)

- [x] 1.1 RED: `crates/claude-archive/tests/privacy_deletion_survivors.rs` adds `an_import_landing_between_row_removal_and_erase_keeps_its_blob`, `a_registrar_blocked_by_the_eraser_re_stores_the_blob`, `a_registrar_that_cannot_re_store_fails_retryably_and_registers_nothing` and `an_import_blocked_by_the_eraser_stores_its_parser_evidence_again` (the real importer; with its `register_blobs` call commented out it fails because the import does not wait for the digest lock). Signature-only stubs (`PrivacyDeletionExecutor::with_erase_gate`, `blob_protocol::register`, `ReceiptError::Registration`) make them compile; they fail on assertions: the first because the eraser erases a blob a committed row references, the others because registration neither waits nor checks presence.
- [x] 1.2 GREEN: `blob_protocol.rs` (`blob_lock`, `referenced_among`, `register`), `erase_bytes` per-item transaction, and the registrars in `receipt.rs`, `initial_import.rs`, `reparse.rs` and `import_artifacts.rs`.

## 2. Size guard, undeliverable rows and honest reports (R2-04 rules 2 and 4)

- [x] 2.1 RED: `crates/claude-archive/tests/fact_size_guard.rs` adds `a_fact_over_the_event_limit_is_not_enqueued_and_the_import_is_partial` (a new file, because `contract_projection.rs` is at 736 lines and the 850 line cap leaves no room): a conversation no 65 536 byte envelope can hold sits beside a small one; it fails because the oversized conversation is enqueued. `tests/config.rs` adds `the_event_size_guard_defaults_to_eight_million_bytes` and `the_event_size_guard_accepts_only_its_documented_range`; the range test fails because the key is "not recognized" (the field and the builder methods were stubs).
- [x] 2.2 RED: `crates/claude-archive/tests/operation_outbox.rs` adds `an_undeliverable_row_does_not_wedge_the_pump` against a real JetStream, padding a row past the connected server's own `max_payload`; it fails because the pass returns a broker error and the row is retried forever. It replaces `a_refused_publish_backs_off_and_never_starves_the_rows_behind_it`, whose premise (a body over the broker limit is a transient refusal) is the bug. `transient_refusals_never_make_a_row_undeliverable` keeps the backoff behaviour for a real outage (a drained client) and is a regression guard: it reads `undeliverable_at` through `to_jsonb`, so it passes before the column exists and guards the classifier afterwards.
- [x] 2.3 GREEN: the `RATATOSKR__RECEIPT__MAX_EVENT_BYTES` key, `with_max_event_bytes` on the import worker and the reparse engine, the enqueue-time guard in `contract_projection.rs`, the `fact_too_large` head gap and the `ai_archive.facts.too_large` operation warning.
- [x] 2.4 GREEN: `outbox_events.undeliverable_at` in `schema.sql`, the pump's payload comparison, `claude_archive_outbox_undeliverable_total{class}`.

## 3. Operator example and documentation (R2-10)

- [x] 3.1 RED: `services/claude-archive/tests/boot.rs` adds `the_shipped_example_loads`; it fails because `deploy/systemd/claude.conf.example` does not exist.
- [x] 3.2 GREEN: ship the example. `README.md` and `DEVELOPMENT.md` list the bus, accounts and size-guard variables (documentation, so no test fails first).

## 4. Gate

- [ ] 4.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict` against PostgreSQL and a JetStream-enabled `nats-server`.
