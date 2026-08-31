## Context

Claude receipt currently emits a terminal partial report when raw bytes are stored. The repository
already has immutable archive storage, a durable import state model, parser/normalization code,
operation outbox, and service health surface, but the runtime does not compose these into one
restart-safe path. Duplicate handling returns before the new operation is correlated, and publication
uses an anonymous connection to the shared subject.

## Goals / Non-Goals

**Goals:** make raw receipt non-terminal, resume the real import pipeline, report actual truth for
every operation correlation, and require an authenticated publisher with truthful readiness.

**Non-Goals:** provider login, inference, a second API/schema version, migrations, secret persistence,
or changing the fleet operation-report document.

## Decisions

### Receipt creates durable work and operation correlation atomically

The current schema definition gains idempotent operation-to-import rows and bounded work state.
Fresh and duplicate receipts both bind the operation; only fresh bytes create raw evidence. No
terminal outbox row is created at this stage.

### One supervised worker owns post-receipt processing

The runtime claims pending work with bounded recoverable ownership, runs existing inspection,
parser/normalization/persistence, records counts/completeness, and atomically creates one report row
for every unreported operation correlation. Item-local failures are durable and do not stop the
worker loop.

### Authenticated outbox state is part of readiness

The service loads a redacted NATS NKey seed-file path, connects using that identity, and publishes
only on `evt.ai-archive.claude.operation.reported.v1`. Publisher and worker health are registered in
the existing readiness surface and recover after a successful authenticated pass.

## Risks / Trade-offs

- A crash between broker acknowledgement and SQL marking may replay a stable message; JetStream and
  Platform inbox identity make that safe.
- Unsupported or malformed archives remain explicit failures and never receive invented counts.
- Current-schema edits require disposable development databases to be recreated.

## Migration Plan

Replace the raw-terminal path in place, recreate test databases from the current schema, deploy the
worker/credentialed publisher before Platform enables Claude receipt, and retain raw archives and
pending correlations during rollback.
