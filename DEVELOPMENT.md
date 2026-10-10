# Developing Ratatoskr Claude Archive

Status: Accepted (implementation plan items 1–6 complete). Last reviewed: 2026-08-27.

The service scaffold exists: typed strict configuration, structured telemetry, operator health routes, typed errors, a content-addressed BlobStore adapter with capped streaming ingest, and the first-version `claude_archive` schema applied at startup. Authenticated tenant-scoped archive receipt exists: claims verify against known accounts/organizations before storage, archives hash and count while streaming under `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES`, raw bytes land write-once, re-delivered digests report an explicit duplicate outcome, and each accepted receipt starts a durable crash-resumable import run driven by guarded state transitions. Plan item 3 adds bounded, non-executing ZIP structure inspection; direct bounded extraction to BlobStore with raw-digest provenance; conservative active-media quarantine; and an exact-match versioned parser-declaration registry. Plan item 4 adds the first exact consumer-export parser for the documented synthetic schema, with projects, conversations, messages, ordered text/Markdown parts, parser stamps, and inert unknown evidence covered by committed golden tests. Plan item 6 adds first-class synthetic Artifacts, immutable version-chain reconciliation across repeated observations, verified-or-quarantined BlobRef payload retention, and a deterministic safe portable Artifact representation. It renders only verified `text/plain` and `text/markdown` bytes; all other Artifact types remain normalized JSON evidence with an explicit `unrenderable` reason. Real-export validation remains a required protected-fixture follow-up; schema detection beyond the declared synthetic identifier, Compliance adapter, and complete project-export writer are not implemented yet.

## Facts and deletion

An initial import and every reparse that changes a row enqueue the `ai_archive.*` facts for it through `crates/claude-archive/src/contract_projection.rs`, built from the persisted rows and owned by the Platform user of the account. Privacy deletion is rows first, bytes second, complete last: `privacy_deletion/rows.rs` removes the scope rows and their dependents in one transaction and records `rows_removed_at`, `execution.rs` erases the bytes and completes the request, and `tombstones.rs` writes the contract tombstones: one per deleted archive, per deleted conversation and project (linked to Knowledge or not), and per Knowledge-linked Artifact. A conversation or project that another export also observed survives and is not tombstoned by itself, but the archive tombstone of the deleted export still drops it from Knowledge; whether Knowledge should keep it is an open decision. The added, updated or unchanged decision of a fact compares only with the facts of the same tenant, and an Artifact only with those of its own conversation and project scope. A test queries `pg_constraint` and fails when a foreign key into a deletion root is neither cascading nor listed in `HANDLED_REFERENCES`.

## Project Knowledge and completeness

Plan item 5 promotes observed project instructions to first-class records and maps exact
synthetic Project Knowledge file references. Matching supplied bytes become verified BlobRefs;
digest or media-type anomalies remain inert quarantined evidence. Per-archive and cumulative
reports count verified, missing, quarantined, and unknown evidence in deterministic order;
reference-only files remain explicitly unbacked up, and absence never proves deletion or coverage.

Plan item 8 publishes immutable import, project, conversation, Artifact, and explicit tombstone
facts through the Claude-owned transactional outbox using the pinned AI-archive contract. Every
subject event carries its raw-export provenance; Knowledge completion envelopes are admitted only
when their owner, subject, archive, and content digest match a previously published revision.

## Toolchain

Rust 1.97.0 (pinned in `rust-toolchain.toml`), Tokio, axum, SQLx/PostgreSQL without the migrate feature, tracing with JSON logs to stderr plus Prometheus metrics, and a local content-addressed BlobStore. The database schema is one file (`schema.sql`) applied under an advisory lock; there are no migrations while the development status holds.

## Code size limits

`clippy.toml` sits beside `Cargo.toml` carrying the fleet thresholds: functions at most 100 lines, signatures at most 7 arguments, block nesting at most depth 5, MSRV-aware suggestions, and `std::env::var`/`var_os` disallowed outside the config module. `.github/workflows/ci.yml` additionally fails any `.rs` file longer than 850 lines. An exception is a site-level `#[expect(...)]` with a reason, never a raised threshold.

### Rust — also the CI gate

```bash
cargo fetch --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
cargo test --workspace --locked
cargo test --workspace --locked --doc
cargo build --workspace --locked --release
```

`cargo deny --locked check` runs in its own `deny` job in `.github/workflows/ci.yml`, separate from
the gate above, so a new RustSec advisory cannot hide a clippy or test failure behind it.

`.github/workflows/ci.yml` runs this list against PostgreSQL 17 (service container in CI,
`compose.yaml` on a laptop: user/password/database `claude`, published on `127.0.0.1:5438`). The
suite creates disposable databases from the embedded schema per test; without the server the suite
fails rather than skips. CI additionally runs the 850-line file ratchet and a guard asserting this
command list is byte-identical to `.github/workflows/ci.yml`.

## Docs-only OpenSpec gate

In addition to the product gate:

```bash
git diff --check
openspec validate --all --strict
openspec validate --archived
```

`.github/workflows/openspec.yml` runs the two OpenSpec commands in CI.

## Local run

```bash
docker compose up -d
cargo run -p ratatoskr-claude-archive-service
# operator plane on 127.0.0.1:9086: /health/live /health/ready /metrics /version
# Platform receipt on 127.0.0.1:8097: POST /v1/ai-archives/receipt, GET /v1/capabilities
```

Configuration comes from `RATATOSKR__*` environment variables; required:
`RATATOSKR__STORAGE__BLOB_ROOT` and `RATATOSKR__STORAGE__DATABASE_URL`. Defaults: operator listener
`127.0.0.1:9086` (`RATATOSKR__ADMIN__LISTEN_ADDRESS`), receipt listener `127.0.0.1:8097`
(`RATATOSKR__RECEIPT__LISTEN_ADDRESS`; both loopback, and configuration refuses the same address for both), log filter `info`, 8 database connections, 5 s acquire timeout, 10 s shutdown
bound, and a 10 GiB maximum accepted archive size (`RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES`).
`<binary> check-config` validates and prints the redacted effective configuration.

Tests use disposable databases created from `schema.sql`; override their location with
`CLAUDE_ARCHIVE_TEST_DATABASE_URL`. The suite never skips when the server is missing — it fails.

The archive event pump is proven against a real JetStream, never a fake: `CLAUDE_ARCHIVE_TEST_NATS_URL` must name a
`nats-server -js` and the pump tests fail, they do not skip, when it is unset. CI starts the broker in the
`Start disposable JetStream` step of `ci.yml`; locally `docker compose up -d postgres nats` starts it on
`127.0.0.1:4238`, so the variable is `nats://127.0.0.1:4238`.

## Platform receipt listener

The process binds two loopback listeners (XR-021 CONTRACTS.md S05). The operator listener
(`RATATOSKR__ADMIN__LISTEN_ADDRESS`, default `127.0.0.1:9086`) serves `/health/live`, `/health/ready`,
`/metrics` and `/version` and nothing else. The receipt listener (`RATATOSKR__RECEIPT__LISTEN_ADDRESS`,
default `127.0.0.1:8097`) serves the private Platform receipt and nothing else, so the operator plane is
never reachable on the port Platform forwards to. Each router refuses the other's routes with `404`, a
test per router proves it, and configuration refuses the same address for both.

`POST /v1/ai-archives/receipt` takes the Edge-minted claims named by
`ratatoskr_ai_archive_contracts::platform_receipt` and answers `202 Accepted` with an empty body. Every other
answer is `application/json` carrying an `ErrorEnvelope` (`claude.auth.unauthenticated` 401,
`claude.request.invalid` 400, `claude.request.too_large` 413, `claude.request.method_not_allowed` 405,
`claude.server.internal` 500), because Edge replaces any other body with `edge.upstream_invalid_response`.
`GET /v1/capabilities` answers the unauthenticated capability document `{"service":"claude","capabilities":
["ai_archive.receipt"]}` on the receipt listener, where the receipt route is mounted.
`RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS` maps each Platform user to its archive account; the same mapping
decides the Platform owner of every `ai_archive.*` fact and every deletion tombstone, and an account without a
mapping emits no fact.

## Facts, gaps and identity

A subject the projection cannot announce (a row with no parser stamp, a message with no usable provider id,
an Artifact with no verified bytes) is named on the `ai_archive.archive.imported.v1` head as an
`unprojected_project`, `unprojected_conversation` or `unprojected_artifact` gap with the number of subjects
lost, and an archive that lost one is never announced as `complete`. A reparse re-announces the head under
the same archive id with the status, counts and warnings of the export's first persisted report, so a
reparse never changes what Knowledge was told about completeness. The operation report of an import agrees
with its head; the one exception is a duplicate delivery of an archive that is already imported, whose
report is rebuilt from the persisted completeness report alone and does not count omissions.

An Artifact is identified by its account, its scope (message, conversation, project, or none) and its
provider id, each backed by a unique index, so a later export of the same account observes the existing row.
The same provider id in a different scope is a different Artifact.

## Privacy deletion refusal and correlation

Deletion needs the Platform owner of the archive to write its tombstones, and the owner exists only in
`RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS`. A deletion of an account that has a raw archive but no mapping is
therefore refused at planning with `conflict`, and nothing is deleted: an account that was mapped when it was
imported and has lost its mapping since holds facts Knowledge still serves, and deleting its rows without a
tombstone would leave that data there. The remedy is to restore the mapping and plan again. The operator
records `privacy-delete:<request id>` as the request correlation id and every tombstone envelope carries
exactly that; the correlation id is free text in the request row, so a request recorded under a value that
is not a namespaced reference is correlated to `privacy_request:<request id>` instead and execution never
stalls on a label.

## Workflow

1. Persist the immutable raw personal/organization export before parsing.
2. Detect acquisition/schema and select a versioned parser.
3. Preserve unknown records and produce evidence-based completeness.
4. Reconcile projects, instructions, project knowledge, conversation graphs, files, Artifacts/versions, and external references without overwriting history.
5. Test archive limits, graph/version integrity, missing assets, interruption, privacy deletion, Compliance cursors, and portable export.

CI uses synthetic fixtures and never Claude session cookies or personal exports.

## Consumer-export parser validation boundary

The first consumer-export projection parser is verified only against committed,
synthetic fixtures for `claude-export-2026-08-synthetic`. Validation against a
minimized and owner-provided real Claude export is a required follow-up blocker:
until that protected fixture is supplied and exercised locally, the parser does
not claim compatibility with a current provider export.

## What a clone needs before you plan a change

A change is planned with OpenSpec, which is a CLI a clone installs for itself. Use the version
`.github/workflows/openspec.yml` pins, so your terminal and the gate answer the same:

```bash
npm install --global @fission-ai/openspec@1.10.0
```

Cross-repository behaviour lives in a store, and registering one is per-machine state that no
repository can turn on for you — the same kind of step as `git config core.hooksPath .githooks`:

```bash
git clone git@github.com:po4yka/ratatoskr-workspace.git <path>
openspec store register <path> --id ratatoskr-workspace
```

`openspec doctor` reports whether both are in place.

## The Rust skills in this repository

`.agents/skills/` holds eighteen Rust skills vendored from `po4yka/rust-skills`, and
`.claude/skills/` symlinks to them. Unlike the steps above this needs nothing from your machine: the
files are in the tree, so a fresh clone already has them.

Update them with the catalogue and never by hand:

```bash
npx skills update
```

That rewrites `.agents/skills/` and `skills-lock.json` from the catalogue. Run it in one repository,
read the diff, then apply the same change to every Ratatoskr repository whose stack is Rust.
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy differs from the others.
