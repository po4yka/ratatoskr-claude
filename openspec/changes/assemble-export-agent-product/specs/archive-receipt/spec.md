## ADDED Requirements

### Requirement: Verified raw receipt is a non-terminal import checkpoint

A Platform receipt SHALL atomically retain verified raw evidence, its operation correlation, and
durable import work. Raw persistence SHALL NOT create a terminal Platform operation report.

#### Scenario: Receipt survives process loss before parsing

- **WHEN** the process stops after raw persistence and before parser/import completion
- **THEN** no terminal report exists and restart discovers the same pending work

### Requirement: Duplicate evidence keeps each operation correlation

An account digest already backed by immutable raw evidence SHALL reuse that archive/import while
durably binding every new valid Platform operation.

#### Scenario: Equal bytes arrive under a new operation

- **WHEN** verified bytes match an existing Claude export under another operation identifier
- **THEN** no raw archive is duplicated and the new operation remains eligible for its own result
