## Why

Round 6 of changeset XR-021 closes the race that `DEVELOPMENT.md` recorded as an "accepted residual": a fact row in flight when its subject is deleted can leave a stream copy that no erasure command names. The binding text is XR-021 CONTRACTS.md R6-03 (it replaces the R5-12 rule 2 sentence about one `published_stream_seq` and the order "collect the sequences before the delete" for the in-flight cases); this proposal cites it and does not restate it. Three losses exist today. (a) A row selected by the pump before phase A deletes it is still published, and its sequence is never captured. (b) A `PubAck` whose mark did not land leaves the row unpublished, and a republish after the 120 s duplicate window is a new copy whose predecessor's sequence is lost. (c) A deletion that commits between the `PubAck` and the mark removes a row that has no recorded sequence.

## What Changes

- `outbox_events.published_stream_seq bigint` becomes `published_stream_seqs bigint[] not null default '{}'`: the sequence of every `PubAck` the pump received for the row, ascending, without duplicates.
- The first publication of a row is one short transaction per row: `select ... for update skip locked` (no row: skip), publish, await the `PubAck`, mark, commit. The mark must affect exactly one row; anything else is the new `OutboxError::MarkLost`, counted in `claude_archive_outbox_mark_lost_total`.
- `drop_published_fact_rows` becomes one `delete ... returning event_type, tenant_ref, published_stream_seqs`, so a deletion waits for the pump's lock and reads the committed sequences.
- A `PubAck` whose commit failed is carried in memory and merged on the next success; a carried copy whose row was deleted meanwhile is erased by a command the pump enqueues itself, named `privacy_request:<the row's event id>`.
- The pump sets the `JetStream` request timeout explicitly to 5 s, which bounds how long it holds a row lock and a pool connection.
- `PublishProbe`, a test seam modelled on `EraseGate`, pauses the pump after the candidates are chosen and after the `PubAck`, and fails one commit. Production never sets one.
- `DEVELOPMENT.md` drops the "Accepted residual" paragraph and states the protocol and the one residual that remains.

Breaking, with no shims: `schema.sql` replaces the column in place (a development database must be recreated); `drop_published_fact_rows` no longer reads before it deletes; every test that named the old column moves to the new one.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `claude-knowledge-events`: every published copy of a deleted fact gets an erasure command, and a fact row in flight is serialized with its deletion.

## Impact

Touches `schema.sql`, `operation_outbox.rs` (split into a module directory), `stream_erasure.rs`, `DEVELOPMENT.md`, `README.md` and the tests that name the old column. No contract, ACL, subject or dependency pin changes; Edge's handler and the command contract are untouched. ChatGPT is a separate work order.
