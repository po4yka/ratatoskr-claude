## 0. Plan

Documentation: no failing test can precede it.

- [x] 0.1 This change (XR-021 CONTRACTS.md R5-11 rule 4 and R5-12 rules 2 and 3), validated with `openspec validate --all --strict`.

## 1. The resend schedule (R5-11 rule 4)

- [x] 1.1 RED: `crates/claude-archive/tests/tombstone_ack.rs` fails with `interval out of range` when a due tombstone has a resend count of 40, 1000 or `i32::MAX - 1`: the resend must succeed, count once and schedule exactly the cap.
- [x] 1.2 GREEN: the clamp-then-convert schedule of `mark_resent` in `operation_outbox.rs`.

## 2. The pending erasure request (R5-12 rule 3)

- [x] 2.1 RED: `crates/claude-archive/tests/stream_erasure_request.rs` fails because a tenant-scope deletion after an export deletion, with the pump off, removes the first request and loses the sequences it names.
- [x] 2.2 GREEN: the removal predicate of `privacy_deletion/planning.rs`.

## 3. Wire and documentation

Configuration and documentation: no failing test can precede it.

- [x] 3.1 The `deploy/nats/identity.conf` stanza is reordered to the Platform stanza, and the authorized-broker tests are run again.
- [x] 3.2 `DEVELOPMENT.md`, `README.md` and the announced-owner ledger design record the changes and the accepted residuals.

## 4. Gate

- [x] 4.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.
