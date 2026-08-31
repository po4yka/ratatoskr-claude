## ADDED Requirements

### Requirement: Operation report publication requires a NATS NKey identity

The archive runtime SHALL require a validated NATS NKey seed-file configuration and SHALL never
fall back to anonymous publication.

#### Scenario: The NKey seed is missing

- **WHEN** archive operation reporting is enabled without a readable NKey seed file
- **THEN** startup fails closed without serving the receipt route

### Requirement: Readiness includes import and publisher health

Configured archive readiness SHALL include supervised import-worker liveness and authenticated
publisher connectivity/authority. Failed publication SHALL retain the outbox row and fail readiness
until a successful pass recovers it.

#### Scenario: Subject permission is denied

- **WHEN** NATS denies the Claude operation-report ingress subject
- **THEN** the row stays pending and readiness reports the publisher dependency unavailable
