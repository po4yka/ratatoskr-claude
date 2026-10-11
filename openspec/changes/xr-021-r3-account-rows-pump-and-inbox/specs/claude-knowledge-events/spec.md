## ADDED Requirements

### Requirement: The pump keeps one broker connection under its own inbox prefix

The event pump SHALL open its broker connection lazily, SHALL reuse it across passes, SHALL make no network call for a pass with nothing pending, SHALL replace the connection only after the client reports it closed, and SHALL set the inbox prefix `_INBOX_claude` on every connection it opens.

#### Scenario: Many passes, one connection

- **WHEN** twenty passes run, empty and non-empty
- **THEN** the broker's total connection count grew by at most one

#### Scenario: Replies arrive under the service's ACL

- **WHEN** the pump publishes against a broker that lets the identity subscribe only to `_INBOX_claude.>`
- **THEN** the publish is acknowledged and the row is marked published

### Requirement: A duplicate of a failed import is reported and a failed import is logged

An operation bound to a `failed` run SHALL receive the `failed` operation report exactly once, and a permanently failed import SHALL be logged at WARN with a closed class and the run id.

#### Scenario: Second upload of bytes that failed permanently

- **WHEN** the same bytes are uploaded again after their import failed permanently
- **THEN** the new operation is reported `failed` once and not again on a later pass
