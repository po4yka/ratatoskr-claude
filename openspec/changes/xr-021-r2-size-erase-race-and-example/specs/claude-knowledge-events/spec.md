## ADDED Requirements

### Requirement: A fact over the event limit is not enqueued and the import says so

A conversation or Artifact fact whose serialized envelope is larger than `RATATOSKR__RECEIPT__MAX_EVENT_BYTES` (default 8000000, accepted range 65536..=8388608) SHALL NOT be enqueued. The terminal operation report of that import SHALL be `partially_succeeded` with the warning `ai_archive.facts.too_large`, plus the `ai_archive.import.incomplete` warning at most once when the import is also incomplete, and SHALL pass `OperationReported::validate()`.

#### Scenario: One oversized conversation among small facts

- **WHEN** an import produces one conversation fact larger than the limit and other facts within it
- **THEN** only the other facts are enqueued and the report is `partially_succeeded` with the too-large warning

### Requirement: An undeliverable row never wedges the pump or flips readiness

The pump SHALL compare each stored body with the connected server's `max_payload` minus 1 KiB. A larger row SHALL be marked `undeliverable_at` with `last_error = payload_too_large`, counted in `claude_archive_outbox_undeliverable_total{class}`, skipped, and SHALL NOT make readiness false. A transient refusal SHALL never mark a row undeliverable.

#### Scenario: A small payload limit

- **WHEN** the broker accepts only small messages and the outbox holds one large and one small unpublished row
- **THEN** the small row is published, the large row is marked undeliverable, and readiness stays true

#### Scenario: A broker outage

- **WHEN** the broker refuses a publish for a transient reason
- **THEN** the row records the attempt and its backoff and is not undeliverable
