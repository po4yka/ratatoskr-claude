## Why

Round 5 of changeset XR-021 fixes the owner of a deletion tombstone. Today `deletion_tenant_ref` resolves the owner from the live `PLATFORM_ACCOUNTS` mapping when a deletion is planned and again when it is executed, `enqueue_tombstones` addresses every tombstone of a request to `request.tenant_ref`, and `require_platform_owner` refuses the plan when the account is no longer mapped. A mapping change between plan and execute fails the request as "not found", and an account whose archives were announced under two Platform users addresses the older user's tombstones to the wrong tenant, which Knowledge Terms as `foreign_subject` and keeps the data. The binding text is XR-021 CONTRACTS.md R5-06; this proposal cites it and does not restate it.

## What Changes

- A ledger table `claude_archive.announced_subjects` records, for every `ai_archive.*` fact the service enqueues, the owner the fact carries, in the same transaction that enqueues the fact.
- A deletion emits one tombstone per distinct owner the ledger holds for each tombstone subject, addressed to that owner. A subject that was never announced emits no tombstone and adds no gap. The ledger rows of a tombstoned subject are deleted in the row-removal transaction, and the rows of an account with the account.
- `privacy_deletion_requests.tenant_ref` stores the supplied account reference as given, and `privacy_execute` compares it with the supplied value. A re-announced survivor carries the owner of the retained archive's own ledger row.
- The facts a deletion removes from the outbox are found through the ledger owners of the account and through archive and aggregate identities, never through the request's tenant reference.
- The "open cross-repository gap" sentences about an equal `observed_at` in `DEVELOPMENT.md`, in the Round 4 design and in `reannounce.rs` say what holds now that Knowledge replaces the head on an equal time with a later event id (R5-08).

Breaking, with no shims: `PrivacyDeletionError::Conflict` for an unmappable tenant no longer exists, and `PrivacyDeletionExecutionError::InvalidTenant` is deleted; `require_platform_owner`, `deletion_tenant_ref` and the deletion use of `PlatformUsers::tenant_ref_of` are deleted; `schema.sql` gains the table in place (a development database must be recreated).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `privacy-deletion`: a deletion addresses every tombstone to the owner the subject was announced under.

## Impact

Touches `schema.sql`, a new `announced_subjects.rs`, `events.rs`, `contract_projection/reannounce.rs`, `privacy_deletion/tombstones.rs`, `privacy_deletion/rows.rs`, `privacy_deletion/planning.rs`, `privacy_deletion/planning_closure.rs`, `reparse.rs`, `services/claude-archive/src/operator_commands.rs`, `README.md` and `DEVELOPMENT.md`. Fleet-wide account erasure, the chatgpt repository and Knowledge are out of scope.
