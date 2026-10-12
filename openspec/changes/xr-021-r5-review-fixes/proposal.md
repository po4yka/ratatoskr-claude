## Why

The independent review of the round 5 commits found two defects that break the contract text they implement. XR-021 CONTRACTS.md R5-11 rule 4 says the tombstone resend never stops by count, but the doubling wait was multiplied before it was clamped, so a tombstone resent about 34 times (about 7.5 days at the defaults) made the `UPDATE` fail with `interval out of range`, the row was republished every pump pass and the publisher reported itself not ready during a Knowledge outage. R5-12 says the stream erasure is eventual, but a tenant-scope deletion removed an erasure request the pump had not published yet, and nothing enqueued it again, so the stream copies named by it were never erased.

## What Changes

- The next resend wait is computed in float seconds, clamped to the cap before it becomes an interval, with a bounded exponent.
- The removal set of a tenant-scope deletion excludes `platform.stream_erasure.requested.v1` rows that are not published yet.
- The Claude stanza of `deploy/nats/identity.conf` lists the three durable allows after the nine `evt.*` allows and before the erasure command, which is the order of the Platform stanza that the workspace byte-equality check compares.
- `DEVELOPMENT.md` records the erasure bookkeeping gaps for a row in flight as an accepted residual, and the announced-owner ledger design records the single `account_id` per `(subject, owner)`.

No breaking change: no schema, contract or configuration key changes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `claude-knowledge-events`: the resend schedule survives any resend count.
- `privacy-deletion`: a deletion keeps an erasure request that is not published yet.

## Impact

Touches `operation_outbox.rs`, `privacy_deletion/planning.rs`, `deploy/nats/identity.conf`, `DEVELOPMENT.md`, `README.md` and the round 5 ledger design. Cites XR-021 CONTRACTS.md R5-11 rule 4 and R5-12 rules 2 and 3.
