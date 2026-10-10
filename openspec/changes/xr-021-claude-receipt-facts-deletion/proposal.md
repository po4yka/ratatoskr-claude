## Why

Changeset XR-021 fixes cross-repository integration defects, and several of them land in the Claude archive service. The binding text is XR-021 CONTRACTS.md sections S05, S06 (D1 to D3) and S07; this proposal cites them and does not restate them.

- Platform's finalize dead-ends on the receipt listener: there is no capability document, and every refusal is a bare status code that Edge replaces with `edge.upstream_invalid_response` (S06 D1, D2).
- A partially imported archive reports `partially_succeeded` with neither a warning nor an error, which the contracts now reject (S06 D3).
- The outbox pump publishes only operation reports, and the persisted import is never projected into `ai_archive.*` facts (S07).
- Privacy deletion fails on foreign keys into rows the planner never removes, hand-builds a tombstone document that is neither an `EventEnvelope` nor an `AiArchiveTombstone`, can abort on the `outbox_privacy_tombstone_once` index, and erases bytes before the rows that reference them are gone (S07).
- The pin is far behind the contracts and rejects the reason `user_requested`; the operator listener default 9084 collides with Threads and ChatGPT (S05).

## What Changes

- Pin all `ratatoskr-*` contract dependencies to the XR-021 contracts commit and depend on `ratatoskr-error-contracts` from the service.
- Serve `GET /v1/capabilities` on the receipt router and answer every non-2xx of the receipt route with an error envelope, including 405. Import the `platform_receipt` constants instead of local copies.
- Report an incomplete import with exactly the contract warning.
- Publish all archive event types through one ack-gated pump over a closed subject mapping, and constrain `event_type` in the schema.
- Project the persisted import into `archive.imported`, `project.added|updated`, `conversation.added|updated` and `artifact.added|updated` facts in the transaction that persists an initial import or an applied reparse. The owner is the Platform user resolved from the configured account mapping.
- Rework privacy deletion into rows first, bytes second, complete last: extend the planner with the dependent closure, delete `platform_operation_imports`, `reparse_runs` and `tombstones` of the selected exports for every scope, recompute blob sharing against the rows that remain, emit contract-conformant tombstones including one per deleted archive, and keep unpublished tombstones of earlier requests.
- Move the operator listener default to 9086.

Breaking, with no shims: `PrivacyDeletionExecutor::finalize` takes the evidence `BlobRef`; `FinalizationFault` gains `AfterRowsCommitted`; `InitialImportWorker::new` and `ReparseEngine::new` take the Platform user mapping; `schema.sql` changes in place (outbox `event_type` check, deletion request `evidence_blob_ref` and `rows_removed_at`); the operator port moves from 9084 to 9086.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `archive-receipt`: capability probe and error-envelope refusals on the receipt listener.
- `platform-operation-report`: the incomplete-import warning.
- `claude-knowledge-events`: the ack-gated generic pump and the persisted-import projection.
- `privacy-deletion`: dependent closure, rows-first ordering, contract tombstones.
- `service-runtime`: the operator listener default.

## Impact

Touches the receipt HTTP surface, the report builder, the outbox and its pump, the import and reparse persistence paths, privacy deletion, `schema.sql`, configuration defaults and documentation. Platform-side changes, ACL lines, Knowledge consumption, the cross-tenant content-addressed blob race, account erasure transport and a general registry of archive tenants are out of scope.
