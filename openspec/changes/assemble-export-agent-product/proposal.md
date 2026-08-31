## Why

Claude raw receipt persistence currently emits terminal partial truth before restart-safe parsing and
import, while duplicate bytes and anonymous report publication can strand or misrepresent a newly
bound Platform operation.

## What Changes

- Make raw persistence a non-terminal progress stage and durably enqueue import work.
- Start and supervise the existing parser/import/completeness pipeline from the service runtime.
- Reuse one raw archive/import result while emitting an operation-specific terminal report for every
  valid duplicate receipt.
- Require configured least-privilege NKey publication and include import-worker/report-publisher
  health in readiness.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `archive-receipt`: Verified raw receipt enqueues durable work without claiming terminal success.
- `import-state`: Import processing resumes after restart and records actual completion facts.
- `platform-operation-report`: Duplicate bytes still produce one truthful terminal result for each
  bound operation.
- `service-runtime`: Credentialed publication and worker liveness are required for readiness.

## Impact

This changes the current Claude schema, receipt/import lifecycle, service composition, operation
outbox credentials/readiness, and synthetic PostgreSQL/NATS tests. It preserves one raw archive,
adds no migration or second version, and introduces no provider login or inference behavior.
