## Why

Round 5 of changeset XR-021 adopts the repository owner's stream policy: `ratatoskr_events` keeps dropping the oldest message at 7 days or 1 GiB, and this service marks a tombstone published at the broker acknowledgement and never looks at the row again, so a tombstone Knowledge did not consume in time is lost silently and the deleted data stays in Knowledge. The binding text is XR-021 CONTRACTS.md R5-11 rule 4; this proposal cites it and does not restate it.

## What Changes

- The outbox row of an `ai_archive.subject.tombstoned.v1` gets `acknowledged_at`, `ack_outcome`, `resend_count` and `next_resend_at`. When the pump marks such a row published it sets `next_resend_at = now() + resend_after`. The pump also selects published, unacknowledged tombstones that are due and publishes the stored bytes again, unchanged, with the same `Nats-Msg-Id`, then doubles the delay up to the cap. A resend never stops by count; it stops when `acknowledged_at` is set.
- A supervised consumer of the Edge-provisioned durable `ratatoskr_claude_deletion_acks` validates each `knowledge.deletion.acknowledged.v1` and sets `acknowledged_at` and `ack_outcome` on the row whose event id and tenant match. An acknowledgement for no row is acknowledged and ignored, a duplicate changes nothing, an undecodable message is Termed, and a `refused` outcome stops the resend, logs an ERROR and counts `claude_archive_tombstone_refused_total`.
- A deletion never removes an unacknowledged tombstone row, whether or not it was published.
- The keys `RATATOSKR__TOMBSTONE__RESEND_AFTER_SECONDS` (default 600, 30 to 86400) and `RATATOSKR__TOMBSTONE__RESEND_CAP_SECONDS` (default 21600, at least the first, at most 604800) configure the resend. Two gauges, `claude_archive_unacknowledged_tombstones` and `claude_archive_oldest_unacknowledged_tombstone_age_seconds`, are served on the operator listener. Readiness is not tied to them.
- `deploy/nats/identity.conf` gains the three publish allows of the new durable, byte-equal to the Platform stanza.

Breaking, with no shims: `schema.sql` gains four columns and an index on `outbox_events` in place (a development database must be recreated); a service refuses to start with a configured bus and no matching durable only by failing its consumer, which exits the process (S02 rule 5); a deletion keeps unacknowledged tombstone rows that earlier removed published ones.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `claude-knowledge-events`: a tombstone is re-sent until Knowledge acknowledges it.
- `service-runtime`: the tombstone resend keys and the acknowledgement consumer are part of the service lifecycle.

## Impact

Touches `schema.sql`, `operation_outbox.rs` (split if it nears 850 lines), a new `deletion_ack.rs`, `config.rs`, `privacy_deletion/planning.rs`, `services/claude-archive/src/main.rs` and a new consumer module, `deploy/nats/identity.conf`, `deploy/systemd/claude.conf.example`, `README.md` and `DEVELOPMENT.md`. The social repositories, ChatGPT, Knowledge and Platform are out of scope.
