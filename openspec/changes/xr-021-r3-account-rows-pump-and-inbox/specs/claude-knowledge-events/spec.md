## ADDED Requirements

### Requirement: The pump keeps one broker connection under its own inbox prefix

The event pump SHALL open its broker connection lazily, SHALL reuse it across passes, SHALL make no network call for a pass with nothing pending, SHALL replace the connection only after the client reports it closed, and SHALL set the inbox prefix `_INBOX_claude` on every connection it opens.

#### Scenario: Many passes, one connection

- **WHEN** twenty passes run, empty and non-empty
- **THEN** the broker's total connection count grew by at most one

#### Scenario: A rotated seed is read after a failed pass

- **WHEN** a pass fails against the broker and the nkey seed file is replaced before the next pass
- **THEN** the next pass opens a new connection with the new seed

#### Scenario: Replies arrive under the service's ACL

- **WHEN** the pump publishes against a broker that lets the identity subscribe only to `_INBOX_claude.>`
- **THEN** the publish is acknowledged and the row is marked published

### Requirement: A duplicate of a failed import is reported and a failed import is logged

An operation bound to a `failed` run SHALL receive the `failed` operation report exactly once, and a permanently failed import SHALL be logged at WARN with a closed class and the run id.

#### Scenario: Second upload of bytes that failed permanently

- **WHEN** the same bytes are uploaded again after their import failed permanently
- **THEN** the new operation is reported `failed` once and not again on a later pass

### Requirement: Publisher health is the verdict of the last pass that needed the broker

The publisher SHALL report healthy until a pass that needs the broker fails, SHALL stay failing across passes that make no network call, and SHALL report healthy again only after a pass publishes through the broker.

#### Scenario: A pass inside the retry delay of a refused row

- **WHEN** a pass fails, and the next pass finds only the row that is waiting out its retry delay
- **THEN** the second pass returns zero published and the publisher is still not healthy
