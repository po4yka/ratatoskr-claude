## Why

Platform accepts a Claude archive upload as an operation, but the archive service currently has no
authenticated receipt boundary for that operation and cannot publish the terminal result that the
export agent needs to display. The producer must preserve a raw archive before reporting a bounded,
truthful completeness summary.

## What Changes

- Add a loopback-only Platform receipt route that accepts only trusted Platform identity and
  operation headers, verifies declared SHA-256 and byte size while storing the raw archive, and
  rejects direct bearer credentials.
- Persist one terminal `platform.operation.reported.v1` event with the typed AI-archive import
  summary when the raw archive has been stored; report `partial` and `unknown` rather than
  inventing parser completeness before parsing has run.
- Publish durable pending reports to the configured NATS JetStream endpoint with a stable message
  identity, marking a row published only after broker acknowledgement.
- Extend strict configuration for the Platform account mapping and the redacted event-bus endpoint.

## Capabilities

### New Capabilities

- `platform-operation-report`: Receipt of a Platform-bound Claude archive and durable publication
  of its truthful terminal operation result.

### Modified Capabilities

- `archive-receipt`: The receipt boundary accepts the Platform operation claims only through its
  trusted loopback transport and verifies the declared raw archive identity before preservation.

## Impact

- Affected code: receipt domain boundary, service router/runtime, configuration, current schema,
  and receipt/config integration tests.
- Contract dependency: `ratatoskr-operation-contracts` and `ratatoskr-ai-archive-contracts` from
  the published workspace operation-progress contract.
- Deployment: Platform forwards to the configured loopback Claude receipt listener and NATS is
  required only to deliver durable terminal reports.
