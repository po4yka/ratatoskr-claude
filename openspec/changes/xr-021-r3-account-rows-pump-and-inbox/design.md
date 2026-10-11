## Context

XR-021 CONTRACTS.md R3-01, R3-03, R3-04, R3-07, R3-11, R3-16 and R3-19 are the source of truth. This file records the local decisions.

## Decisions

- The inbox prefix is one `const INBOX_PREFIX: &str = "_INBOX_claude"` beside the subject constants of the outbox, applied through one function that builds the connect options, so the test calls the same function the pump does.
- The pump keeps its public signature `publish_pending_once(endpoint, seed_path)`. The publisher holds a `tokio::sync::Mutex<Option<async_nats::Client>>`: a pass first asks the database whether a row is pending and returns `Ok(0)` without touching the network when none is; otherwise it uses the held client, replacing it when `connection_state()` is `Disconnected`-closed. async-nats reconnects by itself while merely disconnected, so only a closed client (one whose state it cannot recover from) is dropped.
- The account row is inserted in the receipt transaction on the Platform path only, with `on conflict (account_id) do nothing`, exactly the statement of CONTRACTS.md R3-04 rule 1. `ensure_account` keeps its select for the tenant-token path.
- `Kind::AccountUnmapped` is a new member of the closed `Kind` set in `fault.rs`; the mapping happens where `platform_claims` finds no mapping, and `UnknownTenant` remains the tenant-token result.
- The selector of `process_pending_once` adds `failed` runs that have a bound operation with `reported_at is null`; the report it enqueues is the one `mark_permanent_failure` enqueues, so both paths share the report builder. `mark_permanent_failure` logs `class` and the run id and never an archive path, name or content.
- The example role file and the example share the role, database and port by a test that parses both.
