## Why

Round 5 of changeset XR-021 closes the residue a deletion leaves in the events stream. Conversation, project and artifact facts carry message text on the wire and stay in `ratatoskr_events` for up to 7 days after the service removed its own outbox rows, and no producer can remove a stream message. Edge is the only identity that may erase a single message, on request. The binding text is XR-021 CONTRACTS.md R5-12 rules 2 and 3; this proposal cites it and does not restate it.

## What Changes

- The pump stores the stream sequence of the `PubAck` of every row it publishes in `published_stream_seq`. A duplicate acknowledgement keeps the original sequence.
- One function, `drop_published_fact_rows`, deletes published content-bearing fact rows from the outbox and, in the same transaction, enqueues one `platform.stream_erasure.requested.v1` command per distinct owner with the rows' sequences, ascending, at most 500 per command. The four paths that delete such rows call it: the row-removal transaction of a deletion, the tenant-scope removal, the survivor path that drops stale conversation facts, and `ArchiveOutbox::enqueue_again_in` replacing an identical row. Unpublished rows, tombstones and operation reports add nothing.
- The pump publishes the command row to `cmd.platform.stream_erasure.requested.v1`; the closed type-to-subject function and the `event_type` check learn the command type, and a command row stores the complete `CommandEnvelope`.
- `deploy/nats/identity.conf` gains the publish allow `cmd.platform.stream_erasure.requested.v1`, byte-equal to the Platform stanza.

Breaking, with no shims: `schema.sql` gains `published_stream_seq`, the command event type and the `privacy_request` aggregate type in place (a development database must be recreated); `ArchiveOutbox::enqueue_again_in` takes the erasure context it needs; code that reads every outbox envelope as an event envelope reads facts only.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `claude-knowledge-events`: a deletion asks Edge to erase the stream copy of the published facts it removes.

## Impact

Touches `schema.sql`, `operation_outbox.rs` (split if it nears 850 lines), a new `stream_erasure.rs`, `events.rs`, `knowledge_link.rs`, `contract_projection/reannounce.rs`, `privacy_deletion/rows.rs`, `privacy_deletion/tombstones.rs`, `deploy/nats/identity.conf`, `README.md` and `DEVELOPMENT.md`. Edge's handler, the ChatGPT repository and Knowledge are out of scope.
