## 0. Plan

Documentation: no failing test can precede it.

- [x] 0.1 This change (XR-021 CONTRACTS.md R5-12 rules 2 and 3), validated with `openspec validate --all --strict`.

## 1. The pump records the sequence (R5-12 rule 2)

- [ ] 1.1 RED: `crates/claude-archive/tests/stream_sequence.rs` (real broker) fails because `published_stream_seq` does not exist: after the pump publishes a fact the column must equal the sequence of the stream message, and a duplicate acknowledgement must keep the original sequence.
- [ ] 1.2 GREEN: `published_stream_seq` in `schema.sql` and the sequence of the `PubAck` stored by `mark_published`.

## 2. The command (R5-12 rule 3)

- [ ] 2.1 RED: `crates/claude-archive/tests/stream_erasure_request.rs` (real PostgreSQL, then real broker) fails: deleting an export, a conversation or a tenant whose facts were published must enqueue one command per distinct owner with ascending sequences, `payload.owner` equal to the facts' owner, `request_ref` `privacy_request:<request id>`, producer `ratatoskr-claude` and at most 500 per command; one test per removal path (the `rows.rs` removal, the tenant removal, `drop_stale_conversation_facts`, `enqueue_again_in`); unpublished rows add nothing and tombstones and reports are never listed; the pump publishes the command to `cmd.platform.stream_erasure.requested.v1`; a consumer standing in for Edge erases the named message and leaves another owner's untouched.
- [ ] 2.2 GREEN: `drop_published_fact_rows` called by the four paths, the command rows enqueued in the same transaction, `subject_for_event_type` and the `event_type` check learning the command type, the pump reading a command row as a `CommandEnvelope`, `deploy/nats/identity.conf`.

## 3. Documentation

Documentation: no failing test can precede it.

- [ ] 3.1 `README.md` and `DEVELOPMENT.md` describe the sequence column, the command and its limits.

## 4. Gate

- [ ] 4.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.
