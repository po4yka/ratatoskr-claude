## ADDED Requirements

### Requirement: The resend schedule survives any resend count

The service SHALL schedule the next resend of an unacknowledged tombstone at the smaller of the cap and the doubled wait for any resend count, and SHALL NOT fail or report the publisher not ready because the count is large.

#### Scenario: A tombstone resent for months is rescheduled at the cap

- **WHEN** a due, unacknowledged tombstone whose resend count is 40, 1000 or larger is published again
- **THEN** the resend succeeds, the count rises by one and the next resend is due exactly one cap later
