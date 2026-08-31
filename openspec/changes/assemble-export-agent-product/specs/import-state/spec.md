## ADDED Requirements

### Requirement: Runtime resumes real Claude imports from durable state

The service SHALL supervise bounded workers that advance stored work through its existing
inspection, parser, normalization, persistence, and completeness stages. Each checkpoint SHALL be
durable, and restart SHALL resume rather than require receipt replay.

#### Scenario: Stored work resumes after restart

- **WHEN** raw evidence is stored and the process restarts before parsing
- **THEN** the worker completes import from the preserved bytes and records actual counts

### Requirement: Terminal completeness follows imported evidence

Only the actual parser/import outcome SHALL determine terminal completeness. Missing or malformed
evidence SHALL remain failed or explicitly incomplete.

#### Scenario: Supported synthetic archive completes

- **WHEN** a supported synthetic Claude export is parsed and persisted
- **THEN** its terminal result contains observed counts and evidence-based completeness
