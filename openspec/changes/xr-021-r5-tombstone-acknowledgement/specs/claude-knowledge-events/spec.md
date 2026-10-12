## ADDED Requirements

### Requirement: A tombstone is re-sent until Knowledge acknowledges it

The service SHALL keep the outbox row of an `ai_archive.subject.tombstoned.v1` and SHALL publish its stored envelope again, unchanged and with the same message id, once `resend_after` has passed since it was published and every doubling of that delay up to the cap after it, until the row is acknowledged. An acknowledgement from Knowledge naming the event id and the owner of the row SHALL end the resend, whether its outcome is `applied` or `refused`. An acknowledgement that names no row SHALL change nothing. A `refused` acknowledgement SHALL be counted and logged at ERROR. The resend SHALL NOT make readiness false.

#### Scenario: An unacknowledged tombstone is published again with the same message id

- **WHEN** a tombstone was published and no acknowledgement arrived before `resend_after` passed
- **THEN** the same bytes are published again with the same `Nats-Msg-Id`, the resend count rises and the next resend is later, at most the cap

#### Scenario: An acknowledgement ends the resend

- **WHEN** Knowledge's acknowledgement for the tombstone arrives
- **THEN** the row records the time and outcome and is never published again

#### Scenario: A refused acknowledgement ends the resend and is counted

- **WHEN** the acknowledgement's outcome is `refused`
- **THEN** the resend ends, an ERROR line is logged and `claude_archive_tombstone_refused_total` rises

#### Scenario: An acknowledgement for another owner or an unknown id changes nothing

- **WHEN** an acknowledgement names an event id that is no tombstone of this service, or the right id with another owner
- **THEN** no row changes and the message is acknowledged to the broker

#### Scenario: A later deletion keeps an unacknowledged tombstone

- **WHEN** a tenant-scope deletion runs while an earlier tombstone is unacknowledged
- **THEN** that tombstone row stays, and an acknowledged one is removed with the rest

#### Scenario: Knowledge never answers

- **WHEN** no acknowledgement arrives for several resend periods
- **THEN** the row keeps being published with a doubling delay up to the cap, the two gauges report the backlog and the service stays ready
