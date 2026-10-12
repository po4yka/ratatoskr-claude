## Context

XR-021 CONTRACTS.md R5-12 is the source of truth. This file records the local decisions.

## Decisions

- `drop_published_fact_rows(transaction, erasure, event_ids)` is the only function that deletes a content-bearing fact row from the outbox on behalf of a deletion. The callers choose the rows with their own predicate and hand over ids, so the function reads `published_stream_seq`, tenant and type of those rows before it deletes them and enqueues the commands in the same transaction. A row without a sequence needs no command; a tombstone or an operation report in the id list is deleted without one.
- The erasure context is the deletion request: its id gives `request_ref = privacy_request:<request id>` and its correlation id is carried into the command envelope. `enqueue_again_in` is called only by the survivor path of a deletion, so it takes the context from its caller.
- The command row is an outbox row of type `platform.stream_erasure.requested.v1`, aggregate type `privacy_request` and aggregate id equal to the request reference, with the complete `CommandEnvelope` in `envelope`. The pump reads the stored envelope by type: a fact row as an `EventEnvelope` whose `event_id` must equal the row id, the command row as a `CommandEnvelope` whose `command_id` must equal the row id and whose `command_type` must equal the row type. Both publish the stored bytes with `Nats-Msg-Id` equal to the row id.
- `published_stream_seq` is written in the same statement as `published_at`. A duplicate `PubAck` carries the sequence of the first copy, so a resent or re-published row keeps the first sequence.
- Commands are chunked per owner at 500 sequences each, ascending. The command's `tenant_id` is its owner; the producer is `ratatoskr-claude`. The `(event_type, payload_digest)` unique key cannot collide because the payload names the request and the sequences.
- Phase A does not wait for Edge. A command that is never delivered is a stuck outbox row, visible in the pump, and does not hold the deletion; the erasure is eventual, like the tombstone publication.
- Code that read every outbox row as an event envelope (`knowledge_link.rs`) reads facts only.
