## Context

See proposal.md. The existing domain receipt persists immutable raw bytes and an initial import
run, while the service exposes only its operator routes. Platform is the operation authority and
forwards trusted claims to a loopback producer endpoint under the workspace `operation-progress`
specification.

## Goals / Non-Goals

**Goals:**

- Preserve the exact stream before producing a terminal operation report.
- Make the report transactional with raw receipt persistence and deliver it replay-safely.
- Keep trusted Platform claims out of public authentication paths and logs.

**Non-Goals:**

- Parsing the archive, estimating its content coverage, or claiming completeness beyond the raw
  receipt.
- Implementing Platform authentication, operation projection, or export-agent UI.
- Adding migration history or compatibility routes.

## Decisions

### Receipt claims use a dedicated loopback route

The service accepts Platform's user, device, correlation, operation, declared digest, and declared
size as required headers on `/v1/ai-archives/receipt`; the route rejects `Authorization`. The
Platform reverse proxy is the trust boundary and the listener remains loopback-only. This avoids
inventing a second device-auth implementation in the producer. A public route with independently
verified device credentials was rejected because it duplicates Platform authority.

### Receipt persistence owns the report in the same transaction

The receipt carries optional Platform operation context into the existing archive insert. On a new
export, it mints an archive identity and inserts a typed `OperationReported` JSON payload into the
transactional outbox alongside the export and import run. A partial unique constraint over the
operation-report event type and aggregate operation id makes retries converge. A separate report
table was rejected because the existing outbox is the durable event boundary.

### Raw receipt reports a bounded partial outcome

The report names the typed `ai_archive.import` result with the saved archive reference, provider
Claude, zero content counts, unknown completeness, and one warning/gap. Parser-backed counts will
replace this terminal value only when a future parser stage can prove them. Reporting `succeeded`
or `complete` at receipt time was rejected as a false backup claim.

### JetStream ack gates publication state

The runtime reconnects on bounded periodic attempts, publishes pending report rows with a stable
message identity, and commits `published_at` only after JetStream acknowledges publication. NATS
deduplication and Platform's idempotent projection make a retry after acknowledgement safe. Marking
published before acknowledgement was rejected because it loses terminal state on a broker failure.

## Risks / Trade-offs

- [The listener is misconfigured beyond loopback] → strict configuration refuses non-loopback
  addresses and deployment maps Platform only to the dedicated local port.
- [NATS is unavailable after raw storage] → the transaction commits a pending report; the export
  remains preserved while retry safely defers user-visible terminal state.
- [A payload or database failure happens while accepting bytes] → identity is verified before
  durable receipt projection, and database work rolls back rather than publishing an unsupported
  operation outcome.

## Migration Plan

1. Deploy the service with Platform account mapping, event bus endpoint, and its loopback listener.
2. Deploy Platform's receipt forwarder for the Claude port.
3. Roll back by reverting both route and producer change; already stored raw archives and pending
   outbox reports remain durable and can be replayed after re-deployment.
