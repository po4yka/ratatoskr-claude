## 1. Platform-bound raw receipt

- [x] 1.1 Add `platform_archive_receipt_route_is_available` and
  `platform_archive_receipt_refuses_mismatched_digest_without_storing` to the service receipt
  tests; run each and confirm the missing route or identity enforcement fails its asserted status
  and persistence condition.
- [x] 1.2 Implement the trusted loopback receipt route, claim validation, and declared stream
  identity checks; rerun the two receipt tests and verify both pass without accepting direct
  credentials.

## 2. Terminal operation result

- [x] 2.1 Add `platform_archive_receipt_records_one_unknown_partial_terminal_report` to the
  receipt tests; run it and confirm it fails because a stored Platform receipt has no typed report.
- [x] 2.2 Extend the current schema and receipt transaction to persist one typed Claude raw-receipt
  `platform.operation.reported.v1` outbox record per operation; rerun the terminal-report test and
  verify provider, unknown completeness, zero unobserved counts, and explicit gap pass.

## 3. Durable publication and runtime

- [x] 3.1 Add `event_bus_url_loads_without_rendering_its_value` to config tests; run it and
  confirm it fails because the strict configuration rejects the event-bus key.
- [x] 3.2 Add the Platform account mapping and redacted event-bus configuration, then run the
  config test and verify the endpoint parses without appearing in debug output.
- [x] 3.3 Implement the bounded background publisher that marks a pending report published only
  after JetStream acknowledgement; verify with a focused publisher test or documented broker
  integration check that a failed publish leaves the record pending.

## 4. Validation and lifecycle

- [x] 4.1 Run `cargo fmt --all -- --check`, `cargo deny check`, gated clippy/build/tests, and
  `openspec validate --all --strict`; record the observed results.
- [x] 4.2 Sync this change's delta specifications into the current specs, archive it after every
  task is checked, and verify `openspec validate --archived` passes.
