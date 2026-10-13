## 0. Plan

Documentation: no failing test can precede it.

- [x] 0.1 This change (XR-021 CONTRACTS.md R6-03), validated with `openspec validate --all --strict`.

## 1. The array column (R6-03 rule 1)

- [x] 1.1 RED: `crates/claude-archive/tests/stream_sequence.rs` (real broker) fails on the assertion: `published_stream_seqs` equals the one acknowledged sequence after a publish, and a republish inside the duplicate window leaves one element.
- [x] 1.2 GREEN: the column in `schema.sql`, `mark_published` appending the sequence ascending without duplicates, `drop_published_fact_rows` reading the array, and every test that named the old column.

## 2. The mark must land (R6-03 rule 2)

- [x] 2.1 RED: `mark_published` for a row that no longer exists, or that is already published, returns `OutboxError::MarkLost` and increments `claude_archive_outbox_mark_lost_total`; today it returns `Ok`. The variant is added first and never returned.
- [x] 2.2 GREEN: `rows_affected() == 1` is checked, the error is returned and counted, and the pass reports the failure.

## 3. Serialize with the deletion (R6-03 rules 2 and 3)

- [x] 3.1 The test seam `PublishProbe` (`AfterCandidates`, `AfterPubAck`, `FailCommitOnce`) and `OutboxPublisher::with_publish_probe`. A test seam: no failing test can precede it.
- [x] 3.2 RED (a): `crates/claude-archive/tests/stream_erasure_in_flight.rs`, real PostgreSQL and `JetStream`: a deletion committed after the candidates were chosen and before the row lock leaves no message of the deleted fact on the stream and no command; today the pump publishes the deleted content.
- [x] 3.3 RED (c): the same file: a deletion started while the pump is paused after the `PubAck` does not complete until the pump commits, then enqueues one command naming the acknowledged sequence, and a stand-in for Edge erases the message; today the deletion removes the row without a command.
- [x] 3.4 GREEN: the per-row transaction with `for update skip locked`, the 5 s request timeout as a named constant, and `drop_published_fact_rows` as one `delete ... returning`.

## 4. A PubAck whose commit failed is kept (R6-03 rule 4)

- [x] 4.1 RED (b): the same file, with a stream whose duplicate window is 1 s: a failed commit after a `PubAck`, a wait, a second pass: `published_stream_seqs` holds both sequences ascending, one command names both, and after the stand-in neither message is on the stream; today only the latest sequence is recorded.
- [x] 4.2 RED (carried row deleted): the same setup with the deletion committed before the second pass: the second pass first enqueues one command for the carried sequence named `privacy_request:<the row's event id>`, a third pass enqueues nothing more, and the entry is dropped.
- [x] 4.3 GREEN: the in-memory carry map, its merge into the next mark, and the orphan check at the start of a pass.

## 5. The removal paths still work (R6-03 rule 7)

- [x] 5.1 RED: `crates/claude-archive/tests/stream_erasure_request.rs` gains a case: a fact republished under two sequences and removed by phase A produces one command naming both. The existing cases run against the array column unchanged in intent.
- [x] 5.2 GREEN: covered by 1.2 and 3.4; the case passes without further code.

## 6. Documentation (R6-03 rule 6)

Documentation: no failing test can precede it.

- [x] 6.1 `DEVELOPMENT.md` drops the "Accepted residual" paragraph and states the protocol (the array, the per-row lock and its 5 s bound, the carried sequences, the pump-originated command and its `request_ref`) and the single residual. `README.md` and the column comment in `schema.sql` follow.

## 7. Gate

- [ ] 7.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.
