## 1. Current schema and persisted provenance

- [x] 1.1 RED — add `schema_exposes_portable_privacy_reparse_and_provenance_relations` to `crates/claude-archive/tests/schema.rs`; apply the current schema twice against PostgreSQL 17 and confirm relation/constraint assertions fail for missing observations, extracted artifacts, portable exports, deletion, reparse, migration, and outbox-deduplication structures
- [x] 1.2 GREEN — edit only `schema.sql` in place to add the required current-schema relations, checks, and uniqueness; rerun the exact schema test and verify it passes without a migration file, migration dependency, later schema version, or cross-schema foreign key

## 2. Portable state and byte-deterministic package

- [x] 2.1 RED — add `identical_state_produces_byte_identical_zip` to new `crates/claude-archive/tests/portable_export.rs`, introducing only the public signature/stub required to compile; run the exact test through `build-gate` and confirm its non-empty/fixed-SHA assertion fails because no portable ZIP is produced
- [x] 2.2 GREEN — implement the tenant-scoped portable state model, recursive canonical JSON, deterministic graph/Project Knowledge/Artifact projections, readable inert Markdown, fixed ZIP metadata/member order, and atomic path publication; rerun the exact test and verify two exports match the reviewed fixed SHA-256 golden digest
- [x] 2.3 RED — add `manifest_lists_json_markdown_and_verified_asset_members` to `portable_export.rs`; run it and confirm member digest/provenance or verified-asset assertions fail because the initial package omits required evidence
- [x] 2.4 GREEN — implement final canonical manifest generation, snapshot/raw/parser/completeness provenance, verified BlobStore asset copying, and unavailable-asset warnings; rerun the exact test and verify every non-manifest member has matching length/media type/SHA-256/provenance
- [x] 2.5 RED — add `unsafe_names_resolve_to_inert_unique_paths` to `portable_export.rs`; run it and confirm traversal, absolute, control-character, or collision fixtures produce an unsafe or duplicate member path
- [x] 2.6 GREEN — derive paths from stable opaque identities plus digest suffixes, keep provider names only as inert data, resolve collisions deterministically, and rerun the path-safety test until every member remains in its assigned directory
- [x] 2.7 RED — add `unreadable_verified_asset_aborts_without_archive` to `portable_export.rs`; run it and confirm a completed destination remains or success is returned after the test removes its owned fixture blob
- [x] 2.8 GREEN — fail closed on BlobStore verification/read/output errors, clean only the owned temporary sibling, publish by atomic rename, and rerun the exact failure test to verify no completed output exists

## 3. Portable selection and command boundary

- [x] 3.1 RED — add `tenant_project_and_time_filters_exclude_unselected_evidence` to `portable_export.rs`; run it and confirm another tenant/project or out-of-range observation appears in members or provenance
- [x] 3.2 GREEN — apply tenant authorization and optional exact project/inclusive observed-time predicates in the repository read boundary before rendering; rerun the test and verify stable selected output plus manifest filter values
- [x] 3.3 RED — add `portable_export_command_requires_tenant_and_output` to new `services/claude-archive/tests/portable_export_command.rs`; run it and confirm missing/duplicate/invalid argument, JSON-report, or exit-mapping assertions fail
- [x] 3.4 GREEN — implement `portable-export` command parsing/execution with stable JSON stdout, redacted stderr, exit 0/1/2 mapping, and no private telemetry labels; rerun the service command test

## 4. Exact BlobStore erasure

- [x] 4.1 RED — add `erase_is_exact_and_idempotent` to `crates/claude-archive/tests/blob_store.rs`; run it through `build-gate` and confirm the selected object remains after the compile-only erase seam is invoked
- [x] 4.2 GREEN — implement exact locally owned BlobStore erasure, preserving immutable store/verify behavior; rerun the test and verify two erasures succeed while a sibling object remains readable
- [x] 4.3 RED — add `erase_refuses_foreign_and_malformed_references` to `blob_store.rs`; run it and confirm at least one owner/algorithm/digest/root-containment refusal assertion fails
- [x] 4.4 GREEN — enforce owner, digest algorithm, lowercase digest shape, configured-root containment, and exact-file resolution; rerun the refusal test and verify no local object changes

## 5. Privacy deletion planning

- [x] 5.1 RED — add `deletion_inventory_enumerates_complete_scope` to new `crates/claude-archive/tests/privacy_deletion.rs`; seed every required Claude-owned row/blob category, run against PostgreSQL 17, and confirm category/item equality fails because the planner omits part of the closure
- [x] 5.2 GREEN — implement tenant-locked deletion repository/planner and deterministic content-free inventory from explicit export observations until every seeded category appears exactly once and totals are derived from items
- [x] 5.3 RED — add `deletion_scope_does_not_disclose_cross_tenant_subjects` to `privacy_deletion.rs`; run it and confirm cross-tenant and unknown scopes differ publicly or create a durable request
- [x] 5.4 GREEN — enforce tenant authorization and uniform not-found behavior at the planner boundary; rerun the test and verify no request/evidence mutation for either case
- [x] 5.5 RED — add `conversation_plan_includes_containing_archives_and_only_unprovenanced_collateral` to `privacy_deletion.rs`; run it and confirm a conversation observed in two exports yields wrong raw-retention or collateral actions
- [x] 5.6 GREEN — implement raw-export, conversation, and tenant closure semantics from explicit observations; rerun the test and verify independently evidenced state remains without absence-based deletion

## 6. Privacy deletion execution and Knowledge propagation

- [x] 6.1 RED — add `deletion_finalization_is_atomic_with_audit_and_tombstones` to `privacy_deletion.rs` with a hand-written finalization fault; run it and confirm at least one database removal/audit/`user_requested` outbox effect commits alone
- [x] 6.2 GREEN — implement resumable plan/purge/finalize execution and one owned transaction for normalized/provenance erasure, content-free audit, and deduplicated Knowledge tombstones; rerun the atomicity test and verify all three effects commit together or none do
- [x] 6.3 RED — add `completed_deletion_replay_returns_original_report` to `privacy_deletion.rs`; run it and confirm replay duplicates an audit, tombstone, deletion item outcome, or removal count
- [x] 6.4 GREEN — reconcile requests by stable idempotency identity and uniqueness constraints; rerun the replay test and verify the original report returns with zero new effects
- [x] 6.5 RED — add `tenant_deletion_retains_blob_referenced_by_another_tenant` to `privacy_deletion.rs`; run it and confirm byte-identical shared content becomes unreadable to the surviving tenant
- [x] 6.6 GREEN — add fresh reachability proof under the tenant privacy gate; rerun the test and verify shared items are `retained_shared` while exclusive raw/extracted blobs are absent
- [x] 6.7 RED — add `tenant_deletion_retains_only_content_free_audit` to `privacy_deletion.rs`; run it and confirm a forbidden source digest, external identifier, title, filename, body, payload, or blob reference survives
- [x] 6.8 GREEN — constrain terminal audit/evidence serialization to allowed opaque operational fields; rerun the privacy residue test and verify all source/normalized content is gone

## 7. Privacy deletion command boundary

- [x] 7.1 RED — add `privacy_delete_plan_requires_exactly_one_tenant_scope` to new `services/claude-archive/tests/privacy_delete_command.rs`; run it and confirm missing/combined scope assertions fail
- [x] 7.2 GREEN — implement `privacy-delete plan` grammar for tenant/export/conversation scope, request identity, stable content-free JSON, and exit 0/1/2 mapping; rerun the exact plan test
- [x] 7.3 RED — add `privacy_delete_execute_requires_confirmation` to `privacy_delete_command.rs`; run it and confirm execute can proceed without the exact durable request and `--confirm`
- [x] 7.4 GREEN — implement confirmed resumable execution with redacted diagnostics and no content in normal logs; rerun the execute test and verify invalid invocation causes no mutation

## 8. Parser registry version discovery

- [x] 8.1 RED — add `compatible_versions_and_exact_lookup_are_deterministic` to `crates/claude-archive/tests/parser_registry.rs`; run it and confirm exact resolution/order assertions fail while ordinary ambiguity remains unchanged
- [x] 8.2 GREEN — add comparable declared parser versions, exact compatible lookup, deterministic discovery, and executable parser boundary; rerun the test without making ordinary intake select through ambiguity

## 9. Reparse planning apply and command

- [x] 9.1 RED — add `reparse_dry_run_matches_immediate_apply_without_writes` to new `crates/claude-archive/tests/reparse.rs` with hand-written deterministic old/new parsers; run it and confirm dry/apply report equality, prospective event, database row, or blob-count fidelity fails
- [x] 9.2 GREEN — implement verified raw read, hostile reinspection/extraction, exact newer parser execution, validation/reconciliation, immutable fingerprinted plan, stable JSON report, and apply transaction through one comparison path; rerun the test and verify dry-run fidelity plus zero side effects
- [x] 9.3 RED — add `reparse_apply_is_idempotent_for_same_fingerprints` to `reparse.rs`; run it and confirm the second apply creates a duplicate run, revision, extracted artifact, completeness row, or outbox fact
- [x] 9.4 GREEN — add execution uniqueness and prior-result reconciliation; rerun the test and verify the second apply returns the original report with zero new evidence
- [x] 9.5 RED — add `reparse_omission_retains_existing_subject_with_warning` to `reparse.rs`; run it and confirm an omitted conversation is removed or a deletion tombstone is predicted
- [x] 9.6 GREEN — classify omissions as `proposed_removal` coverage warnings, preserve current projections, and emit no deletion fact; rerun the omission test
- [x] 9.7 RED — add `reparse_command_requires_tenant_archive_parser_and_preserves_dry_run` to new `services/claude-archive/tests/reparse_command.rs`; run it and confirm required argument, exact `NAME@VERSION`, JSON report, or exit assertions fail
- [x] 9.8 GREEN — implement the `reparse` operator command with optional `--dry-run`, stable JSON stdout, redacted diagnostics, and exit 0/1/2 mapping; rerun the service command test

## 10. Parser-version migration reports

- [x] 10.1 RED — add `migration_report_classifies_each_archive_once_and_derives_totals` to new `crates/claude-archive/tests/parser_migration.rs`; run it with reordered eligible/current/unsupported/missing/privacy-blocked inputs and confirm entry order, uniqueness, serialization, or totals differ
- [x] 10.2 GREEN — implement deterministic tenant-scoped migration planning over reparse and derive totals from sorted per-archive entries; rerun the test and verify byte-identical reports across source order
- [x] 10.3 RED — add `migration_apply_reports_partial_when_one_archive_fails` to `parser_migration.rs`; run it and confirm one archive-local failure stops a later eligible archive or is reported as full success
- [x] 10.4 GREEN — apply eligible entries independently through reparse, retain non-eligible classifications, and return explicit partial status while preserving successful archives; rerun the partial test
- [x] 10.5 RED — add `parser_migrate_command_requires_tenant_parser_and_preserves_dry_run` to new `services/claude-archive/tests/parser_migrate_command.rs`; run it and confirm invocation, dry-run side-effect, report, or exit assertions fail
- [x] 10.6 GREEN — implement `parser-migrate` with exact target parser, stable JSON, side-effect-free `--dry-run`, and exit 1 for operational partial results; verify no database migration artifact or tooling is added

## 11. Owner-provided fixture discovery and golden admission

- [x] 11.1 RED — add `fixture_admission_rejects_raw_private_or_unapproved_candidates` to new `crates/claude-archive/tests/fixture_admission.rs`; run one table case at a time and confirm at least one raw archive, forbidden field, unsafe path, unlisted file, missing review, or nondeterministic expectation is incorrectly admitted
- [x] 11.2 GREEN — implement strict admission-manifest parsing, structural comparison, privacy/secret/path checks, required review/approval gates, deterministic report ordering, and `fixture-admit --candidate PATH`; rerun every unsafe case and one fully synthetic admitted case
- [x] 11.3 Documentation cannot start from a failing behavior test because it records the owner-only operational process; add `docs/testing/OWNER_FIXTURE_DISCOVERY.md` and README/TESTING links covering consent, private storage, production inspection, minimization/redaction, structural comparison, review, admission, opt-in golden blessing, support claims, and source disposition, then verify every named command/path exists and no real export/private value is tracked
- [x] 11.4 Add one non-sensitive admission manifest and read-only golden contract test; run it through `build-gate`, inspect every committed golden line, and verify ordinary tests never bless or rewrite fixtures

## 12. Refactor full gate and delivery

- [x] 12.1 Refactor only after all targeted tests are green: review public APIs with applicable Rust checklists, split any Rust file over 850 lines and oversized functions within repository limits, keep locks/transactions in stable order, and rerun affected crate/service tests with no behavior change
- [x] 12.2 Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, focused crate/service tests, and the complete Rust gate from `DEVELOPMENT.md`; wrap every compiler-backed top-level Cargo command in `build-gate`, target PostgreSQL 17 with `CLAUDE_ARCHIVE_TEST_DATABASE_URL`, and record exact outcomes
- [x] 12.3 Run `git diff --check`, `openspec validate --all --strict`, inspect byte-determinism golden changes line by line, and review the final diff for private data, raw fixtures, DB migration files/tooling, later majors, fake support claims, stale output, unbounded telemetry labels, unrelated changes, and missing call sites
- [x] 12.4 Sync all nine delta specs, verify no delta remains, archive this change, run `openspec validate --archived`, then commit only intended paths, integrate the task branch into `main`, push `main`, verify the remote SHA, and only then delete the dedicated worktree and merged task branch
