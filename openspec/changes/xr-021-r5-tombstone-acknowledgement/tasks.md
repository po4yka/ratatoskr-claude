## 0. Plan

Documentation: no failing test can precede it.

- [x] 0.1 This change (XR-021 CONTRACTS.md R5-11 rule 4), validated with `openspec validate --all --strict`.

## 1. The state machine (R5-11 rule 4, outbox columns and pump)

- [x] 1.1 RED: `crates/claude-archive/tests/tombstone_ack.rs` (real PostgreSQL) fails because the columns do not exist: a published tombstone must get `next_resend_at = published_at + resend_after`, a due unacknowledged row must be selected and after a republish carry `resend_count + 1` and a backed-off `next_resend_at` (at most the cap), an acknowledgement (`applied`, `refused`) must set `acknowledged_at` and `ack_outcome`, an acknowledgement for another tenant's row or an unknown id must change nothing, and an unacknowledged tombstone must stay in the removal set of a later deletion while an acknowledged one leaves it (`deletion_ack::acknowledge` and the resend selection exist as signature-only stubs so the assertions fail, not the build).
- [x] 1.2 GREEN: `acknowledged_at`, `ack_outcome`, `resend_count`, `next_resend_at` in `schema.sql`, the resend selection and bookkeeping in `operation_outbox.rs`, `crates/claude-archive/src/deletion_ack.rs`, the removal-set predicate of `privacy_deletion/planning.rs`.

## 2. Configuration

- [x] 2.1 RED: `crates/claude-archive/tests/config.rs` fails because the `tombstone` section does not exist: the defaults are 600 and 21600, 29 and 86401 are refused, a cap below the interval and above 604800 is refused.
- [x] 2.2 GREEN: the `tombstone` section of `config.rs`, `deploy/systemd/claude.conf.example` and `README.md`.

## 3. The consumer and the broker (R5-11 rule 4, real broker)

- [x] 3.1 RED: `services/claude-archive/tests/tombstone_ack_broker.rs` (real broker, deployed ACL, a durable provisioned the way Edge does) fails: the first publish is unacknowledged, the same bytes with the same `Nats-Msg-Id` are republished after the injected `resend_after`, a Knowledge-identity acknowledgement stops the resend, `refused` stops it and increments `claude_archive_tombstone_refused_total`, an acknowledgement for no row is acknowledged and ignored, an undecodable message is Termed, the two gauges move, and readiness stays green while Knowledge never answers.
- [x] 3.2 GREEN: `services/claude-archive/src/deletion_acks.rs` and its supervision in `main.rs`, the gauges, the `deploy/nats/identity.conf` stanza.

## 4. Documentation

Documentation: no failing test can precede it.

- [x] 4.1 `README.md` and `DEVELOPMENT.md` describe the resend, the acknowledgement, the keys and the gauges.

## 5. Gate

- [ ] 5.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.
