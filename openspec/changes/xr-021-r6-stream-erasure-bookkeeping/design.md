## Context

XR-021 CONTRACTS.md R6-03 is the source of truth. This file records the local decisions.

## Decisions

- The candidates are chosen without a lock (`PENDING_SQL` is unchanged). The lock is taken per row: `begin; select 1 ... where event_id = $1 and published_at is null and undeliverable_at is null for update skip locked`. No row means deleted, held by a deletion, or already published, and the pump skips it. The pump never waits on a lock, so it cannot take part in a deadlock.
- The transaction holds one pool connection while a publish is in flight. The `JetStream` context is built with an explicit request timeout of 5 s (`PUBLISH_REQUEST_TIMEOUT`), which bounds both the lock and the connection. The pool is sized for the pump plus the other workers, and a pass is sequential, so the pump holds at most one connection at a time.
- A deletion that meets the lock waits for the pump and, under READ COMMITTED, deletes the committed row version. `drop_published_fact_rows` is therefore a single `delete ... returning event_type, tenant_ref, published_stream_seqs` and enqueues the commands from the returned rows in the same transaction. A row deleted before the lock is never published and needs no command.
- The mark appends the acknowledged sequence to the sequences the row already holds and to those the pump carries for it, keeps the array ascending and drops duplicates, in one `update` that must affect exactly one row. Zero rows is `OutboxError::MarkLost`; the transaction is dropped, the counter increments and the pass fails without setting the broker-failure state of a refusal. The failure still keeps `is_healthy` false until a later pass publishes cleanly.
- The carry map is `Arc<std::sync::Mutex<HashMap<Uuid, Carried>>>` on `OutboxPublisher`, never held across an await. An entry is added when the mark or the commit fails after a `PubAck`, merged by the next successful mark of that row, and dropped when the row is published. At the start of each pass every carried id is checked against the table; a missing row is a copy no deletion knew about, so the pump enqueues the erasure command through `enqueue_command` with `request_ref` and `aggregate_id` `privacy_request:<event id of the orphan>` and drops the entry. That reference names no real deletion request; `DEVELOPMENT.md` says so. The command's `(event_type, payload_digest)` key deduplicates a repeat.
- The probe is `PublishProbe` with two named pauses, `AfterCandidates` and `AfterPubAck`, and one fault, `FailCommitOnce`, installed by `OutboxPublisher::with_publish_probe`. `FailCommitOnce` drops the transaction instead of committing it after the `PubAck`, which is what a lost connection does.
- `operation_outbox.rs` becomes a module directory so no file passes 850 lines.

## Residual

A process crash (or a database that stays down until restart) between the broker's `PubAck` and the commit of its sequence, followed by a republish after the 120 s duplicate window, leaves one stream copy that no record names; it ages out at 7 days or 1 GiB. Closing it needs Edge to find a message by `Nats-Msg-Id`, a contract addition, not done and listed in R6-05. A crash and restart inside 120 s is covered: the republish is a duplicate `PubAck` carrying the original sequence.
