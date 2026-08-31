## ADDED Requirements

### Requirement: Every correlated operation receives one terminal result

Each bound Platform operation SHALL receive one idempotent terminal report only after its durable
import result exists. Reusing an existing import SHALL still create a distinct operation outbox row.

#### Scenario: Duplicate import result is reused

- **WHEN** a second operation binds to bytes whose Claude import already completed
- **THEN** one operation-specific terminal report cites the existing result without duplicate import

### Requirement: Claude reports use the Claude ingress subject

The service SHALL publish the unchanged `platform.operation.reported.v1` document only on
`evt.ai-archive.claude.operation.reported.v1` with the stable Claude producer identity.

#### Scenario: Broker acknowledges a pending report

- **WHEN** authenticated publication is acknowledged on the Claude ingress subject
- **THEN** only then is the stable outbox row marked published
