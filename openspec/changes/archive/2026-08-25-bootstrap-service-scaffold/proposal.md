# Bootstrap service scaffold

## Why

The repository is in architecture bootstrap with no code, no manifest, and no product CI. Plan item 1 of `docs/IMPLEMENTATION_PLAN.md` calls for the service skeleton every later slice builds on: a runnable Ratatoskr Claude Archive binary with finite typed config, structured telemetry, typed errors, health endpoints, a BlobStore adapter that satisfies the fleet `blob-references` contract, and the first-version `claude_archive` schema. Landing it also brings the controls the fleet requires with the first manifest: `clippy.toml` size limits, `deny.toml`, a pinned toolchain, and a product `ci.yml` whose command list matches this repository's DEVELOPMENT.md.

## What Changes

- Add a Cargo workspace with one application crate (`ratatoskr-claude`) and pinned `rust-toolchain.toml`; commit `Cargo.lock`.
- Add finite typed configuration loaded from the environment with validation and documented defaults; invalid configuration refuses to boot.
- Add structured telemetry initialization (tracing) with level controlled by configuration; no chat bodies, titles, filenames, or raw archive content in logs.
- Add typed error types distinguishing storage/config/HTTP failure classes without exposing archive content, mapped to HTTP responses.
- Add health endpoints (`/health/live`, `/health/ready`) over axum reporting process liveness and readiness of owned components.
- Add a local filesystem BlobStore adapter: content-addressed keys under the service's own root, SHA-256 digests, write-once immutability (an existing object is never rewritten), digest verification on read, and a `BlobRef`-compatible reference carrying owner service, digest algorithm+hex, media type, and byte length per the workspace `blob-references` spec.
- Add `schema.sql` defining the first-version `claude_archive` PostgreSQL schema (accounts/workspaces, exports, import runs, projects, project knowledge sources, conversations/messages/content parts, artifacts/versions, assets, revisions/tombstones, completeness reports, outbox/inbox), edited in place by later changes; no migration tooling, no v2.
- Add a test harness that creates a throwaway database from `schema.sql` for schema tests, plus unit/integration tests for config parsing, health handlers, and BlobStore write/read/immutability/digest-mismatch behavior.
- Add `.github/workflows/ci.yml` running the gate commands recorded in DEVELOPMENT.md (including a test invocation), plus `clippy.toml` with the fleet thresholds (functions 100 lines, 7 arguments, nesting depth 5) and an 850-line file limit check; update DEVELOPMENT.md and README status to match reality.
- Out of scope: receipt/import logic, parsers, completeness, events, portable export (plan items 2+).

## Capabilities

### New Capabilities

- `service-runtime`: The service process boots from typed environment configuration, initializes structured telemetry, serves health endpoints, and refuses invalid configuration or unready dependencies at startup.
- `blob-store`: Content-addressed, write-once local blob storage owned by this service, producing and resolving `BlobRef` references with verified SHA-256 digests.
- `claude-archive-schema`: The first-version `claude_archive` PostgreSQL schema that this service exclusively owns, creatable from a single `schema.sql` definition with no cross-schema writes.

### Modified Capabilities

(none - the repository has no specs yet)

## Impact

- New code: Cargo workspace manifests, `ratatoskr-claude` crate (config, telemetry, errors, http health, blob store modules), `schema.sql`, tests.
- New tooling/config: `rust-toolchain.toml`, `clippy.toml`, `deny.toml`, `.github/workflows/ci.yml`.
- Documentation: DEVELOPMENT.md gains the exact product gate commands; README status moves from "no importer/persistence exists" to "scaffold exists, import logic planned".
- Dependencies introduced: tokio, axum, serde, tracing/tracing-subscriber, thiserror, sha2, hex, uuid, sqlx (postgres), figment or envy-style env loading, tempfile/testcontainers-style harness for schema tests. All pinned via committed `Cargo.lock` and run with `--locked`.
- Fleet gates affected: first-manifest rules satisfied (`ci.yml` invoking a test, `clippy.toml` beside `Cargo.toml`); OpenSpec docs-only gate unchanged.
