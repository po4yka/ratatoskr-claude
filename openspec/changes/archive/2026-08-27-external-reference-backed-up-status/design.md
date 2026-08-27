## Context

See [proposal.md](proposal.md). The current projection records verified
availability for Project Knowledge files and Artifact versions, but projects,
conversations, and Artifact identities do not have one explicit archival status.
The current schema similarly has upstream state but no separate local-backup
outcome or transition audit.

## Goals / Non-Goals

**Goals:**

- Provide one public domain model that classifies a project, conversation, or
  Artifact as locally backed up or reference-only from verified local evidence.
- Preserve last successful evidence when acquisition authorization changes.
- Make status changes and report totals deterministic and content-free.
- Persist the first-version model and audits through an in-place schema edit.

**Non-Goals:**

- Fetching external GitHub/Drive content, acquiring Claude credentials, or
  changing the acquisition adapter.
- Treating one snapshot's absence, authorization failure, or a quarantined
  payload as deletion or successful preservation.
- Publishing a new cross-repository event contract.

## Decisions

### Derive status from a narrow evidence input

Introduce an `external_reference` domain module with a stable subject kind
(`project`, `conversation`, or `artifact`), provider identity, provider
reference metadata, and a `LocalBackupStatus`. The only evidence that can
derive `LocallyBackedUp` is verified local material that covers the observed
entity's required archived payloads. Missing, reference-only, or quarantined
material derives `ReferenceOnly`.

This reuses the existing verified-versus-quarantined availability boundary and
keeps raw provider content out of status reporting. A boolean alone was
rejected because a named two-state status makes the negative result explicit in
serialised report surfaces and leaves no room to infer it from connectivity.

### Separate authorization observations from backup observations

The reconciler accepts evidence observations separately from acquisition
authorization outcomes. Authorization outcomes are retained as contextual
observations but are excluded from the status-derivation input. Reconciliation
therefore retains the last evidence-derived status when authorization expires
without new local material.

Overwriting status from the latest acquisition result was rejected: it repeats
the legacy failure mode where an expired session can erase a known successful
backup outcome.

### Audit only status transitions

The reconciler returns an append-only, deterministic audit entry only when the
derived status differs from the entity's prior status. An audit entry carries
the subject kind, stable external ID, old/new status, evidence classification,
and observation time, but not content, titles, filenames, credentials, or
URLs. Re-observing unchanged evidence is idempotent.

Auditing every acquisition observation was rejected because it would conflate
authorization with preservation and create noisy, misleading dashboards.

### Store the model in the current schema definition

Edit `schema.sql` in place: add explicit local backup status fields to
`projects`, `conversations`, and `artifacts`; add an owned
`external_references` table for provider-reference metadata; and add a
`backup_status_audits` table constrained to the three supported entity kinds
and the two status values. The audit table stores stable IDs and safe status
metadata only. Fresh-schema tests will prove the objects and constraints.

Separate migrations or a compatibility table were rejected by the repository's
development-status rule: there is one current schema and no migration tooling.

### Add counts as a report projection

Extend `CompletenessCounts` and its aggregation with locally-backed-up and
reference-only entity counts. The report receives already-derived statuses; it
does not accept credentials or reachability state, preventing accidental
coupling to acquisition health.

## Risks / Trade-offs

- [Evidence coverage differs by entity type] → encode the coverage rule at the
  model boundary and test each supported kind before report aggregation.
- [A later source adds additional required Artifact payloads] → a new evidence
  observation recomputes status and records the resulting audited transition;
  no historic raw evidence is removed.
- [Dashboard consumers might mistake reference-only for deletion] → preserve
  upstream state separately and document the two independent dimensions in the
  public type and report field documentation.
- [Schema changes could accidentally create migration artifacts] → limit the
  persistence edit to `schema.sql` and run the fresh-schema integration test.

## Migration Plan

1. Modify the single current schema definition and its fresh-schema test; do
   not add a migration.
2. Add the domain model and RED/GREEN tests for derivation, expired
   authorization stability, transition auditing, and count aggregation.
3. Run the repository gate from `DEVELOPMENT.md`, then integrate only after all
   checks are green.

Rollback before merge is a normal revert of the one commit. There is no
deployed persisted data or migration history in this development phase.
