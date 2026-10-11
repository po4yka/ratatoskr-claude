## Why

Round 3 of changeset XR-021 fixes the defects that the Round 2 native smokes and the independent verifiers found in the Claude archive service and that were re-verified against `origin/main`. The binding text is XR-021 CONTRACTS.md sections R3-01, R3-03, R3-04, R3-07, R3-11, R3-16 and R3-19; this proposal cites them and does not restate them.

- The service subscribes to the shared `_INBOX.>` namespace, so a compromised service could read the JetStream deliveries of every other service (R3-01).
- The outbox pump opens and closes a NATS connection on every pass, about every two seconds, even with nothing to publish (R3-07 rule 1).
- Every receipt from a mapped Platform user fails until an operator inserts the `claude_archive.accounts` row by hand, and the failure reaches the export agent as a 401 that makes it wipe its pairing (R3-03 rule 3, R3-04 rule 1).
- A second upload of bytes that failed permanently binds its operation to the failed run, which the selector never picks again, so the operation stays `accepted`; and a permanently failed import logs nothing (R3-04 rule 2, R3-07 rule 2).
- The example names a PostgreSQL role nothing creates, the contracts pin differs from the rest of the fleet, and two documents describe a scaffold that predates the importer (R3-11, R3-16, R3-19).

## What Changes

- The service's NATS connection sets `custom_inbox_prefix("_INBOX_claude")`; `deploy/nats/identity.conf` carries the byte-equal `CLAUDE` stanza of Platform's `ratatoskr.conf`.
- The outbox pump holds one lazily created client, makes no network call for an empty pass and replaces a closed client.
- The Platform receipt path inserts the mapped account in the receipt transaction; an unmapped Platform user answers `403 claude.account.unmapped` (`Kind::AccountUnmapped`); the tenant-token path still requires an existing account.
- The import selector also takes `failed` runs with an unreported bound operation and enqueues the `failed` report; a permanent import failure is logged at WARN with a class and the run id.
- `deploy/postgres/role.sql` provisions `ratatoskr_claude` on `ratatoskr`; the README, `DEVELOPMENT.md` and the example are corrected; every `ratatoskr-*` git dependency moves to contracts `ad16855c4e7f3d52cd118274faa3b8f3ab4da576`.

Breaking, with no shims: a service built before this round cannot complete JetStream requests under the new ACL (deploy the services of one snapshot together); an unmapped Platform user is a 403 and no longer a 401; the account row is now created by the receipt and the "seed the row" advice is gone.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `archive-receipt`: first receipt creates the account, unmapped user is a 403.
- `claude-knowledge-events`: one broker connection, the inbox prefix, failed duplicates and failed-import logging.
- `service-runtime`: the PostgreSQL role file.

## Impact

Touches `operation_outbox.rs`, `receipt.rs`, `initial_import.rs`, `import_state.rs`, `services/claude-archive/src/fault.rs` and `lib.rs`, `Cargo.toml`, `Cargo.lock`, `deploy/nats/identity.conf`, `deploy/postgres/role.sql`, `deploy/systemd/claude.conf.example`, `.github/workflows/ci.yml`, `README.md` and `DEVELOPMENT.md`. The privacy deletion path, the organization receipt scope, the account-erasure transport and Platform's side of the receipt are out of scope.
