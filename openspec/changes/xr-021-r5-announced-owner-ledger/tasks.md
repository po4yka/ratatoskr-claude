## 0. Plan

Documentation and a dependency pin: no failing test can precede them.

- [x] 0.1 The pin of every `ratatoskr-*` dependency to the Round 5 contracts revision (commit e056863) and this change, validated with `openspec validate --all --strict`.

## 1. The ledger (R5-06 rule 1)

- [x] 1.1 RED: `crates/claude-archive/tests/announced_subjects.rs` fails because `ArchiveOutbox` writes no ledger row: an import under mapping A leaves the table empty (the table and a `record` that returns without inserting exist so the assertions fail, not the build), an unmapped account emits no fact and no row, the table has no foreign key, and a re-announced survivor writes no row for the retained archive's owner.
- [x] 1.2 GREEN: `claude_archive.announced_subjects` in `schema.sql`, `announced_subjects::record` called from `ArchiveOutbox::enqueue_with` for every non-tombstone fact.

## 2. Per-subject tombstones without the live mapping (R5-06 rules 2 to 5)

- [x] 2.1 RED: `crates/claude-archive/tests/privacy_deletion_owner.rs` and `services/claude-archive/tests/privacy_delete_owner_command.rs` fail because a plan is refused when the mapping no longer lists the account, an execute answers `not_found` after a mapping change, a tenant deletion over two owners addresses every tombstone to one of them, an account that never announced is refused with a conflict, and the request is recorded under the Platform user instead of the supplied reference. A file-scan test fails while `require_platform_owner` and `deletion_tenant_ref` exist.
- [x] 2.2 GREEN: `require_platform_owner`, `deletion_tenant_ref` and `PrivacyDeletionError`'s unmappable-tenant use are deleted, `collect_subjects` reads the owners per subject from the ledger, `enqueue_tombstones` emits one tombstone per `(subject, owner)`, the ledger rows of a tombstoned subject are deleted in phase A, the survivor path reads the retained archive's owner, the outbox removal sets no longer read the request's tenant reference, and the plan and execute commands use the supplied reference unchanged.

## 3. Documentation

Documentation: no failing test can precede it.

- [x] 3.1 `DEVELOPMENT.md`, the Round 4 design and the comments of `contract_projection/reannounce.rs` state what holds now that Knowledge replaces the head on an equal `observed_at` with a later event id (R5-08), pointing at the Knowledge test and at R5-08, and `README.md` describes the ledger.

## 4. Gate

- [x] 4.1 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line check, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo deny --locked check` and `openspec validate --all --strict`.
