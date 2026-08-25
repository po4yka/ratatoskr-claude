# Tasks - bootstrap service scaffold

## 1. Workspace scaffolding

- [x] 1.1 Create `rust-toolchain.toml` (channel 1.97.0, minimal profile, clippy+rustfmt), root `Cargo.toml` (resolver 3, members crates/claude-archive + services/claude-archive, exact-pinned workspace dependencies, fleet lints table), both crate manifests with `[lints] workspace = true`, `clippy.toml` (msrv, thresholds 100/7/5, test allowances, disallowed std::env methods), `rustfmt.toml`, `deny.toml`, and empty module skeletons so the tree compiles. Verification: `cargo fetch --locked && cargo build --workspace --locked` succeeds. This task cannot start from a failing test: it is manifest and toolchain configuration that every later test needs to compile.

- [x] 1.2 Add `compose.yaml` (postgres:17 digest-pinned, port 127.0.0.1:5438, database/user `claude`, ICU initdb arguments) and document the test database URL in DEVELOPMENT.md. Verification: `docker compose up -d && psql` reachable at the documented URL. This task cannot start from a failing test: local infrastructure provisioning.

## 2. Typed configuration

- [x] 2.1 Add failing tests in `crates/claude-archive/tests/config.rs`: `minimal_environment_loads_with_defaults` (only required values set; defaults asserted for listener 127.0.0.1:9084, filter "info", connections 8, timeouts), `missing_required_value_names_field` (absent BLOB_ROOT -> violation naming `RATATOSKR__STORAGE__BLOB_ROOT`), `malformed_value_names_field_and_hides_value` (bad listen address -> violation names field, message does not contain the bad value), `unknown_key_rejected`. Back them with a stub `Config::load` returning `Err(ConfigError::unimplemented())` so the tests fail on assertions, not compilation. Run: `cargo test -p ratatoskr-claude-archive --test config --locked` and confirm all four fail for the stated reason.

- [x] 2.2 Implement the strict loader in `crates/claude-archive/src/config.rs`: RATATOSKR__ prefix parsing, section structs, secret-wrapped DATABASE_URL with redacting Debug, violation collection, loopback listen-address enforcement, documented Default impls. Verification: the four tests from 2.1 pass.

## 3. Telemetry

- [x] 3.1 Add failing test in `crates/claude-archive/tests/telemetry.rs`: `filter_honors_configured_level` (a pure filter builder enables debug events at "debug" and disables them at "info"). Stub the builder to return a filter that always disables, so the assertion fails for the stated reason. Run and confirm failure.

- [x] 3.2 Implement telemetry in `crates/claude-archive/src/telemetry.rs`: EnvFilter construction from config, JSON stderr layer, Prometheus recorder handle returned to the caller, install-once guard, claude_build_info gauge. Verification: the 3.1 test passes; `/metrics` rendering function covered by the admin tests in group 5.

## 4. Blob store adapter

- [x] 4.1 Add failing tests in `crates/claude-archive/tests/blob_store.rs` against a tempdir-backed store: `roundtrip_returns_original_bytes`; `reference_fields_match_contract` (owner `ratatoskr-claude-archive`, algorithm sha256, lowercase hex digest, media type, length); `identical_bytes_deduplicate_to_one_object` (same reference, single file under root); `different_bytes_produce_different_references`; `overwrite_refused_and_bytes_unchanged` (write divergent bytes via raw path manipulation then store/read again -> Collision or Mismatch error, original bytes intact, error Display contains no substring of either payload); `tampered_storage_fails_read` (flip byte on disk -> Mismatch); `rejects_invalid_media_type`. Stub `BlobStore::store` to return `Err(StoreError::Unavailable)` so tests fail on assertions. Run and confirm failures.

- [x] 4.2 Implement `crates/claude-archive/src/blob_store.rs`: staging write + streaming SHA-256 + sync_all + hard-link into content-addressed path, existing-target verification (dedup vs Collision), resolve + re-hash read, BlobRef struct, hand-rolled hex encoding, media-type shape validation. Verification: all 4.1 tests pass.

## 5. Health endpoints

- [x] 5.1 Add failing tests in `services/claude-archive/tests/admin.rs` driving the router through tower ServiceExt: `live_reports_process_health` (GET /health/live -> 200, body {"state":"live","role":"archive"}), `ready_lists_failing_component` (probe reporting database down -> 503 with check named Database and reason DependencyUnavailable), `ready_succeeds_when_all_checks_pass` (200, checks sorted by name), `responses_carry_no_store` (Cache-Control header on live). Stub router has no routes so requests 404 and fail for the stated reason. Run and confirm failures.

- [x] 5.2 Implement `admin_router()` in `services/claude-archive/src/lib.rs`: routes /health/live, /health/ready, /metrics, /version; ReadinessFacts shared state fed by probes; sorted checks with closed-vocabulary reasons; no-store middleware. Verification: all 5.1 tests pass.

## 6. Schema and disposable databases

- [x] 6.1 Add failing tests in `crates/claude-archive/tests/schema.rs` using TestDatabase: `fresh_database_provisions_every_owned_table` (all eighteen claude_archive tables exist), `second_database_has_same_inventory`, `duplicate_archive_hash_rejected`, `duplicate_conversation_external_id_within_account_rejected`, `definition_creates_nothing_outside_claude_archive` (query information_schema). Stub TestDatabase::create returns an error so tests fail for the stated reason. Requires the compose database from 1.2 running. Run and confirm failures.

- [x] 6.2 Implement root `schema.sql` (eighteen tables per design D6: UUIDv7 keys, text+CHECK vocabularies, timestamptz, bytea hashes, within-schema FKs, parent-scoped unique constraints, outbox/inbox partial indexes) and `crates/claude-archive/src/database.rs` (`Database::connect`, `apply_schema` inside one transaction under `pg_advisory_xact_lock(0x7261_7461_736b_7206)` with to_regnamespace presence check) plus feature-gated `src/test_support.rs` (TestDatabase::create with URL override CLAUDE_ARCHIVE_TEST_DATABASE_URL, TEST_POOL_SIZE=2, drop on drop). Verification: all 6.1 tests pass against two independent databases.

## 7. Service boot wiring

- [x] 7.1 Add failing boot test in `services/claude-archive/tests/boot.rs`: spawn the real binary via CARGO_BIN_EXE_ with valid env against the compose database, poll /health/live until 200, assert /health/ready reaches 200 with Database and BlobStore checks passing, send SIGTERM, assert exit code 0 within the shutdown timeout. Also assert `check-config` exits 78 with invalid env. Runs before wiring exists and fails because startup does not come up. Requires compose database from 1.2.

- [x] 7.2 Implement `services/claude-archive/src/main.rs` bootstrap order per design D8 (check-config subcommand, config load, telemetry init, connect+apply schema, blob root ensure, bind listener, readiness prober, startup-complete flag, graceful SIGTERM/SIGINT drain bounded by shutdown timeout, pool close, exit codes 0/1/78). Verification: boot test from 7.1 passes.

## 8. Documentation, CI, gate

- [x] 8.1 Update DEVELOPMENT.md "Current validation" with the exact product gate command list (fetch/deny/fmt/clippy/file-ratchet/build/test/doc-test/release-build) keeping the docs-only OpenSpec block; update README status paragraph to state the scaffold, health endpoints, blob store, and schema exist while import logic remains planned. These are documentation changes and cannot start from a failing test.

- [x] 8.2 Add `.github/workflows/ci.yml` with a gate job (digest-pinned postgres service container matching compose.yaml, permissions contents:read, persist-credentials false, SHA-pinned actions) whose cargo `- run:` lines equal the DEVELOPMENT.md list, ending with the awk drift-guard step comparing them, plus the 850-line ratchet step. This is workflow configuration and cannot start from a failing test. Verification: `act`-equivalent dry run not available; verified locally by executing the same command sequence below.

- [x] 8.3 Run the full gate green: docs-only block (`git diff --check`, `openspec validate --all --strict`, `openspec validate --archived`) plus every product command from DEVELOPMENT.md, all exiting 0.
