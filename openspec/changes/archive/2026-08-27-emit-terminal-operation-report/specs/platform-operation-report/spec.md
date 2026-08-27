## Purpose

Lets Platform-bound Claude archive receipt publish one durable, truthful terminal result so clients
can distinguish raw preservation from parser-backed completeness.

## ADDED Requirements

### Requirement: A Platform-bound receipt publishes one truthful terminal summary

After a Platform-bound Claude archive receipt has preserved and verified its raw bytes, the service
SHALL durably enqueue exactly one `platform.operation.reported.v1` terminal report for that
operation. The report SHALL use the published typed AI-archive result, identify the stored Claude
archive, and classify raw-only receipt as `partially_succeeded` with `unknown` completeness and an
explicit warning; it SHALL NOT claim parser, conversation, message, or asset coverage not observed
by this receipt.

#### Scenario: Stored raw receipt reports unknown partial completeness

- **WHEN** a Platform-bound Claude archive with verified declared hash and byte size is stored
- **THEN** its operation has one durable terminal report with result kind `ai_archive.import`,
  provider `claude`, unknown completeness, zero unobserved content counts, and an explicit gap

#### Scenario: Retried receipt does not create another report for the operation

- **WHEN** Platform retries delivery for an operation whose terminal report is already durable
- **THEN** the service leaves one report for that operation and does not create a second result

### Requirement: Terminal reports remain pending until broker acknowledgement

The service SHALL publish each pending terminal report on the workspace operation-progress event
topic with a stable message identity, and SHALL mark the report published only after JetStream
acknowledges it. A broker failure SHALL leave the durable report pending for a later retry without
changing the terminal result or exposing archive contents in telemetry.

#### Scenario: Broker failure leaves report pending

- **WHEN** publication of a pending terminal report cannot reach the configured broker
- **THEN** the report remains unpublished and a later successful publication can deliver the same
  terminal result idempotently
