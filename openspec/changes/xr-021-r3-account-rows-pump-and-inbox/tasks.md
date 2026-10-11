## 1. Contracts pin (R3-16)

A dependency pin: no failing test can precede it, the existing suite staying green is the gate.

- [x] 1.1 Move all `ratatoskr-*` git dependencies to `ad16855c4e7f3d52cd118274faa3b8f3ab4da576`, refresh `Cargo.lock`, run `cargo test --workspace --locked`.

## 2. Inbox prefix (R3-01)

- [x] 2.1 RED: `crates/claude-archive/tests/inbox_prefix.rs::the_pump_completes_jetstream_requests_under_its_inbox_prefix` against a `nats-server` whose only user is the stanza of `deploy/nats/identity.conf`; it times out because the connection has no inbox prefix.
- [x] 2.2 GREEN: `INBOX_PREFIX`, `custom_inbox_prefix`, `deploy/nats/identity.conf`, the CI step that puts `nats-server` on `PATH`.

## 3. One broker connection (R3-07 rule 1)

- [x] 3.1 RED: `crates/claude-archive/tests/operation_outbox.rs` adds `the_pump_opens_one_connection_for_many_passes`, `an_empty_pass_makes_no_network_call` and `the_pump_reconnects_after_the_connection_is_closed`; the first two fail because every pass connects.
- [x] 3.2 GREEN: a lazily created, reused client with the existing connect bound.

## 4. The first mapped receipt creates the account, an unmapped user is a 403 (R3-04 rule 1, R3-03 rule 3)

- [x] 4.1 RED: `crates/claude-archive/tests/receipt.rs` adds `the_first_receipt_from_a_mapped_user_creates_the_account` and `a_tenant_token_receipt_still_requires_an_existing_account`; `services/claude-archive/tests/admin.rs` adds `an_unmapped_platform_user_is_forbidden_not_unauthenticated`, `malformed_claims_stay_unauthenticated`, `an_authorization_header_stays_unauthenticated` and `capabilities_still_carry_no_store`.
- [x] 4.2 GREEN: the account insert in the Platform receipt transaction and `Kind::AccountUnmapped`.

## 5. A failed import is reported for a duplicate and logged (R3-04 rule 2, R3-07 rule 2)

- [ ] 5.1 RED: `crates/claude-archive/tests/import_state.rs` adds `a_second_upload_of_bytes_that_failed_permanently_is_reported_failed` and `a_permanent_import_failure_is_logged_with_a_class`.
- [ ] 5.2 GREEN: the selector takes `failed` runs with an unreported bound operation; `mark_permanent_failure` logs.

## 6. Provisioning and documentation (R3-11, R3-19)

- [ ] 6.1 RED: `crates/claude-archive/tests/config.rs::the_example_role_file_exists` fails because `deploy/postgres/role.sql` does not exist.
- [ ] 6.2 GREEN: `deploy/postgres/role.sql`, the README deployment paragraph, the corrected stale paragraphs (documentation, so no test fails first).

## 7. Gate

- [ ] 7.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict` against PostgreSQL and a JetStream-enabled `nats-server`.
