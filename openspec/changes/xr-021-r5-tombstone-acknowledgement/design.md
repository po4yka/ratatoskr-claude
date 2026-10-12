## Context

XR-021 CONTRACTS.md R5-11 is the source of truth. This file records the local decisions.

## Decisions

- The state machine lives in the outbox row, not in a new table: only rows of `ai_archive.subject.tombstoned.v1` use the four columns, and every other row leaves them null or zero. `mark_published` sets `next_resend_at` for a tombstone in the same statement that sets `published_at`.
- The resend selection is a second query beside the unpublished selection, with the same batch size. A due row is published again with the stored bytes and the same `Nats-Msg-Id`; the events stream collapses a repeated id inside its 120 second duplicate window, so the 600 second default is deliberate and the 30 second minimum exists for tests. A failed republish leaves the resend state alone and records the refusal with the existing failure backoff, which moves `next_attempt_at` but not `next_resend_at`; a due resend honours `next_attempt_at` too, so a refused row does not hammer the broker. After a publish the new PubAck sequence is not recorded: the sequence of the first publish is the message the deletion path may have to erase.
- The acknowledgement matches on the event id and the tenant of the row, so an acknowledgement for another owner's row changes nothing. A duplicate acknowledgement keeps the first outcome and time. The ledger and the removal sets are not touched by an acknowledgement.
- An unacknowledged tombstone survives later deletions: the removal sets of a tenant-scope deletion exclude `event_type = 'ai_archive.subject.tombstoned.v1' AND acknowledged_at IS NULL`, which replaces the "unpublished" test, and an acknowledged tombstone is removable like any published row.
- The consumer follows `x/services/x/src/extractor_reports.rs`: it opens the Edge-provisioned durable by name and never creates it, verifies the filter `evt.knowledge.deletion.acknowledged.v1`, `Explicit` acknowledgements, and a 30 second ack wait, and Terms an undecodable message, a wrong producer, a missing tenant or an invalid payload. A transient database failure is a Nak with a 2 second delay. The task ends only on an orderly shutdown; when it ends otherwise the service reports not ready and exits non-zero (S02 rule 5).
- The two gauges are computed from the table at scrape time by the pump loop that already runs every two seconds, so they cost one aggregate query per pass.
- The configuration section is `tombstone` with `resend_after_seconds` and `resend_cap_seconds`. The cap must be at least the first value; the process refuses to start otherwise.
