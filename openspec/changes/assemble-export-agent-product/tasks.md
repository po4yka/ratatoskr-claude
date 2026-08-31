## 1. Restart-safe import

- [x] 1.1 RED — in `crates/claude-archive/tests/receipt.rs`, add
  `raw_receipt_is_nonterminal_until_restart_safe_import_completes` and assert process restart yields
  no early terminal report and an eventual evidence-based result.
- [x] 1.2 GREEN — atomically enqueue raw receipt work, supervise the existing import pipeline from
  startup, and make 1.1 pass.
- [x] 1.3 RED — add `duplicate_digest_reports_terminal_result_for_each_bound_operation`; assert one
  raw archive/import and one result for each distinct operation.
- [x] 1.4 GREEN — retain operation correlations for fresh and duplicate receipts and enqueue
  idempotent operation-specific terminal reports after import; make 1.3 pass.

## 2. Authenticated report runtime

- [x] 2.1 RED — extend outbox and readiness tests so missing/denied NATS credentials keep rows
  pending and fail archive readiness until authenticated publication recovers.
- [x] 2.2 GREEN — load a redacted NKey seed, publish on the Claude ingress subject with stable
  identity, supervise worker/publisher health, and make 2.1 pass.

## 3. Verification

- [x] 3.1 Run focused RED/GREEN and affected crate tests through `build-gate` with `--locked`.
- [x] 3.2 Run documented format, lint, build, test, schema, and strict OpenSpec gates; leave any
  unrun external PostgreSQL/NATS check unticked and report it.
