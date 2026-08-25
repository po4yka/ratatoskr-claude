# Design - bootstrap service scaffold

## Context

The repository has no code. The structural precedent is ratatoskr-instagram, whose archived `scaffold-instagram-service` change solved this exact milestone under the same fleet regime; this design follows its proven shapes and records where claude deliberately differs. Binding constraints: development status forbids migrations and any second major version; behaviour visible to several repositories belongs to the workspace store, so the BlobStore work here implements the fleet `blob-references` contract locally without a cross-repository code dependency.

## Goals / Non-Goals

**Goals:**
- One application workspace that boots, validates config, serves health, stores blobs immutably, and provisions its schema.
- Fleet controls landing together with the first manifest: clippy.toml thresholds, deny.toml, pinned toolchain, committed Cargo.lock, product ci.yml mirroring DEVELOPMENT.md.
- Every spec scenario realized by a named test.

**Non-Goals:**
- Receipt/import logic, parsers, completeness, events, portable export (plan items 2+).
- NATS integration, organization/Compliance adapters.
- A shared BlobStore service or dependency on another repository's code.

## Decisions

### D1: Two-member workspace, mirroring instagram

Root `Cargo.toml` with `resolver = "3"`, members `crates/claude-archive` (domain library: config, telemetry, blob_store, database, errors, test_support) and `services/claude-archive` (binary + admin router). Exact-pinned dependencies (`=x.y.z`) in `[workspace.dependencies]`; edition 2024; `rust-version = "1.97"`; `rust-toolchain.toml` pins channel 1.97.0. Workspace lints identical to the fleet table (forbid unsafe_code, deny missing_docs, clippy pedantic plus denies for unwrap/expect/panic/indexing_slicing/string_slice and friends). Alternative considered: single-crate service - rejected, the two-member split keeps domain logic testable without booting a server and matches every sibling.

### D2: Hand-rolled strict environment configuration

A finite `Config` struct loaded from `RATATOSKR__*` variables (double-underscore sections), collecting every violation into `ConfigError { violations }` instead of failing on the first. Required: `RATATOSKR__STORAGE__BLOB_ROOT`, `RATATOSKR__STORAGE__DATABASE_URL` (held as a redacting secret wrapper). Defaults: admin listener `127.0.0.1:9084`, log filter `info`, database connections 8, acquire timeout 5000 ms, shutdown timeout 10000 ms. Unknown keys are violations. `std::env::var/var_os` are banned outside this module via `clippy.toml` disallowed-methods. Alternatives: figment/config crates - rejected; siblings hand-roll this and the strictness requirements (unknown-key rejection, redaction, loopback enforcement) fit in one small module. Port 9084 chosen: 9082 instagram, 9083 github are taken.

### D3: Telemetry via tracing + Prometheus recorder

tracing-subscriber JSON events to stderr, `EnvFilter` built from the configured filter string (not RUST_LOG), metrics-exporter-prometheus recorder handle rendered by `/metrics`, a `claude_build_info` gauge, install-once semantics. Filter construction is a pure function so the level-gating scenario is unit-tested without installing a global subscriber. No event ever carries payload bytes, titles, or filenames; the privacy scenario is asserted on error formatting paths.

### D4: Typed errors per concern, no IntoResponse yet

`ConfigError` (violations, keys only), `StoreError` (Unavailable/Io/Collision/Mismatch/Missing/InvalidMediaType/InvalidIdentity), `PersistenceError` (Connect/Schema/Query with sqlx source), `TelemetryError`, plus a service-level fatal path in main. All operator-facing text value-free. HTTP mapping stays inline in health handlers until plan item 2 introduces the API plane; the platform fault-envelope pattern is the known destination, cited here to prevent ad hoc mappings later.

### D5: Local BlobStore adapter implementing the fleet contract

Concrete struct `BlobStore { root, owner: "ratatoskr-claude-archive" }`, not a trait (YAGNI until a second backend exists). Keys are content-addressed: `root/sha256/<hh>/<remaining hex>`; writes stream to a staging file while hashing, `sync_all`, then hard-link into place; an existing target is verified against digest+length (matching bytes: idempotent dedup success; diverging bytes: `Collision`, never overwritten). Reads resolve by reference and re-hash; tampered bytes yield `Mismatch`, satisfying the workspace spec's "treats the artifact as missing rather than changed" posture via an explicit integrity error. References carry owner service, algorithm identifier, lowercase hex digest (hand-rolled hex, fleet convention), media type, length. Alternative: depend on extractor's blob-store crate - rejected, cross-repo code dependencies are out of bounds for this change; the behavioural contract, not the code, is what the fleet shares.

### D6: Schema conventions and lock ordinal

Single root `schema.sql`, embedded with `include_str!`, applied inside one transaction guarded by `pg_advisory_xact_lock(0x7261_7461_736b_7206)` - the "ratatoskr" prefix with repository ordinal **06** (01 platform, 02 extractor, 03 github, 04 knowledge, 05 instagram taken). Presence check via `to_regnamespace('claude_archive')` makes re-application idempotent. Tables follow the AGENTS.md conceptual list: accounts, organizations, exports, import_runs, projects, project_sources, conversations, messages, message_relations, content_parts, artifacts, artifact_versions, assets, revisions, tombstones, completeness_reports, outbox, inbox. Conventions inherited from the sibling: app-minted UUIDv7 primary keys (no database defaults), closed vocabularies as text + named CHECK constraints (acquisition modes, import states, upstream states, tombstone reasons), timestamptz everywhere, hashes as bytea named `*_hash`, within-schema foreign keys only, cross-schema references as plain `*_ref` uuid columns. Uniqueness: exports by archive hash; externally identified records by external id within parent scope. Columns stay conservative; the definition is edited in place by later slices while the development status holds.

### D7: Disposable-database test harness

Feature `test-support` on the library, consumed by the crate's own dev-dependency (self path-dependency pattern), exposing `TestDatabase::create()`: creates `claude_archive_test_<uuidv7>` through an admin connection, applies the schema, returns a size-2 pool. Override `CLAUDE_ARCHIVE_TEST_DATABASE_URL`, default `postgres://claude:claude@127.0.0.1:5438/claude` served by a committed compose.yaml (postgres:17, ICU initdb arguments identical to the CI service container). Port 5438 chosen: 5435 github, 5436 instagram, 5437 threads scratch are taken. A missing database fails tests loudly; nothing skips.

### D8: Bootstrap order and readiness probes

main: `check-config` subcommand (validate + redacted dump, exit 78 on invalid) -> load -> init telemetry -> refuse without database URL -> connect + ping -> apply schema -> ensure blob root exists and is writable -> bind listener -> spawn prober (interval copies `select 1` and a blob-root write-probe into readiness facts) -> mark startup complete -> serve until SIGTERM/SIGINT -> drain bounded by shutdown timeout -> close pool. `/health/live` never consults dependencies; `/health/ready` renders sorted named checks (BlobStore, Database, Drain, Startup) with closed-vocabulary reasons, 503 when any required check fails. Every response sets `Cache-Control: no-store`. Exit codes: 0 clean, 1 runtime failure, 78 configuration.

### D9: Gate composition

DEVELOPMENT.md documents the exact product gate and ci.yml runs the identical list with an awk drift guard comparing the two: `cargo fetch --locked`, `cargo deny --locked check`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line-per-.rs-file ratchet, `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo test --workspace --locked --doc`, `cargo build --workspace --locked --release`. clippy.toml carries the fleet thresholds (functions 100 lines, 7 arguments, nesting depth 5, msrv, test allowances, disallowed-methods). The pre-existing docs-only OpenSpec checks remain untouched alongside. Postgres in CI runs as a digest-pinned service container matching compose.yaml.

## Risks / Trade-offs

- [Local BlobRef diverges from a future shared identifiers crate] -> Shape matches the workspace contract field-for-field; revisiting is a contained type swap when plan item 2 lands.
- [Schema defined before import logic exists] -> Conservative columns only; dev status allows editing the definition in place, and schema tests pin the inventory so edits are deliberate.
- [Hand-rolled config duplicates sibling code] -> Accepted fleet-wide trade-off; ~one module, strict-local rules, and no new dependency surface.
- [Tests require a running Postgres] -> compose.yaml + documented URL override; CI supplies the service container; failures are loud by design.
