## Context

XR-021 CONTRACTS.md is the single source of truth for every cross-repository contract. This change implements its Claude half and records only decisions local to this repository.

## Decisions

- The capability route is mounted on the same sub-router as the receipt route in `service_router`, under the `no_store` layer, so the capability is advertised exactly while the receipt works. A static fault table in `fault.rs` renders every refusal as an `ErrorEnvelope`; the receipt route is `post(...)` with a method fallback for 405. Diagnostics are logged once at the boundary and never enter the envelope.
- The incomplete-import warning is built only by `platform_receipt::incomplete_import_warning()` and `OperationReported::validate()` is the test oracle (S06 D3).
- The Platform user is not stored in the Claude database. The `claude_archive` account id is the archive identity and the configured `platform_accounts` mapping (Platform user, account) is the only link. The work order says the import plan's tenant id is the Platform user; the code says it is the account id, so facts and tombstones resolve the owner by reverse lookup of that mapping, supplied as a constructor parameter and never a global. An account with no mapping emits no `ai_archive.*` fact (operation reports still flow) and the import records the skip in the log by safe class.
- Privacy deletion keeps `tenant_ref` as the request scope identity and requires it to be `user:<Platform user uuid>` only when the inventory has tombstone subjects. The operator command resolves the archive account to that reference through the same mapping before planning.
- The generic pump selects unpublished rows of the closed event-type list ordered by `occurred_at, event_id` and publishes the stored envelope unchanged with `Nats-Msg-Id` equal to the row's `event_id`. An event type outside the closed list is a hard error; the schema `CHECK` makes such a row impossible to insert.
- New code lives in new files (`planning_closure.rs`, `rows.rs`, `tombstones.rs`, `contract_projection.rs`, `fault.rs`) because `planning.rs` and `execution.rs` already sit near the size limits.
- Deletion order is rows first, bytes second, complete last. Phase A deletes the scope rows and the dependents, recomputes blob sharing against the rows that remain, inserts tombstones and records `rows_removed_at` and the evidence blob key in one transaction under the tenant lock. Phase B erases bytes and marks items purged. Phase C writes the audit and completes the request. `finalizing` is no longer written.
- The projection is verified only against the synthetic export schema. The parser is bound to that schema, so no claim is made about real Claude exports.

## Risks

- The service serves operator probes and the Platform receipt on one listener. S05 allocates 8097 to the receipt and 9086 to the operator plane; a deployment that needs both ports needs a second listener, which this change does not add.
- A residual cross-tenant dedupe race on content-addressed blobs is narrowed, not closed (S07).
- Existing development databases must be recreated from the edited `schema.sql`; there are no migrations.
