## 1. Evidence-derived entity status

- [x] 1.1 Add `crates/claude-archive/tests/external_reference.rs` with
  `verified_evidence_derives_backed_up_and_missing_evidence_derives_reference_only`; run its
  exact nextest filter and confirm the status assertion fails before implementation.
- [x] 1.2 Implement the external-reference domain model and evidence-only status derivation in
  `crates/claude-archive/src/external_reference.rs`, export it from `lib.rs`, and rerun
  `verified_evidence_derives_backed_up_and_missing_evidence_derives_reference_only` green.

## 2. Authorization independence

- [x] 2.1 Add
  `expired_authorization_does_not_mutate_last_successful_backup_evidence` to
  `crates/claude-archive/tests/external_reference.rs`; run its exact nextest filter and confirm
  the assertion that the status remains backed up fails before implementation.
- [x] 2.2 Implement reconciliation that keeps authorization observations separate from local
  evidence and rerun `expired_authorization_does_not_mutate_last_successful_backup_evidence`
  green.

## 3. Audited status transitions

- [x] 3.1 Add `verified_evidence_transition_appends_one_content_free_audit_entry` to
  `crates/claude-archive/tests/external_reference.rs`; run its exact nextest filter and confirm
  the expected transition audit entry is absent before implementation.
- [x] 3.2 Implement idempotent content-free transition auditing in the external-reference
  reconciler and rerun
  `verified_evidence_transition_appends_one_content_free_audit_entry` green.

## 4. Truthful report surfaces

- [x] 4.1 Add `backup_status_counts_ignore_expired_authorization` to
  `crates/claude-archive/tests/completeness.rs`; run its exact nextest filter and confirm the
  expected backed-up/reference-only counts are unavailable before implementation.
- [x] 4.2 Extend completeness counts and deterministic aggregation with the two
  evidence-derived entity totals, then rerun `backup_status_counts_ignore_expired_authorization`
  green.

## 5. First-version persistence model

- [x] 5.1 Extend `crates/claude-archive/tests/schema.rs` with
  `fresh_schema_exposes_backup_status_and_transition_audits`; run its exact nextest filter and
  confirm the required status/audit schema objects are absent before implementation.
- [x] 5.2 Edit only `schema.sql` to persist root-entity backup status, upstream external
  references, and constrained content-free transition audits; rerun
  `fresh_schema_exposes_backup_status_and_transition_audits` green.

## 6. Full verification

- [x] 6.1 Run `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`,
  `cargo nextest run --locked --workspace`, `cargo test --locked --doc --workspace`, and the
  fenced `DEVELOPMENT.md` gate through `build-gate` where compiler-backed; verify all pass and
  review the final diff for status/authentication separation and private-data leakage.
