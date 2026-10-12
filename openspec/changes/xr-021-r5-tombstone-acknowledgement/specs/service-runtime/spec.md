## ADDED Requirements

### Requirement: The tombstone resend is configured and its backlog is visible

The service SHALL read `RATATOSKR__TOMBSTONE__RESEND_AFTER_SECONDS` (default 600, accepted 30 to 86400) and `RATATOSKR__TOMBSTONE__RESEND_CAP_SECONDS` (default 21600, at least the first value, at most 604800) and SHALL refuse to start on a value outside its range. It SHALL serve `claude_archive_unacknowledged_tombstones` and `claude_archive_oldest_unacknowledged_tombstone_age_seconds` on the operator listener.

#### Scenario: A resend interval below the minimum is refused

- **WHEN** the resend interval is 29 seconds, or the cap is below the interval
- **THEN** configuration loading fails naming the key

#### Scenario: The backlog gauges move

- **WHEN** a tombstone is published and not acknowledged, and then acknowledged
- **THEN** the unacknowledged count is one and its oldest age grows, and both return to zero after the acknowledgement
