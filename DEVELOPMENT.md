# Developing Ratatoskr Claude Archive

Status: Accepted (implementation plan items 1–6 and 8 complete). Last reviewed: 2026-10-11.

The service receives, imports, publishes, reparses, exports and deletes archives, verified against the synthetic export schema `claude-export-2026-08-synthetic` only: typed strict configuration, structured telemetry, operator health routes, typed errors, a content-addressed BlobStore adapter with capped streaming ingest, and the first-version `claude_archive` schema applied at startup. The Platform receipt authenticates by the configured user mapping (creating the archive account on first use), hashes and counts the stream under `RATATOSKR__LIMITS__MAX_ARCHIVE_BYTES`, lands raw bytes write-once, reports a re-delivered digest as an explicit duplicate, and starts a durable crash-resumable import run driven by guarded state transitions. The import inspects the container with bounded, non-executing ZIP checks, extracts directly to the BlobStore with raw-digest provenance, quarantines active media conservatively, selects an exact-match versioned parser, and runs the first consumer-export parser (projects, conversations, messages, ordered text and Markdown parts, parser stamps, inert unknown evidence, first-class Artifacts with immutable version chains, and Project Knowledge files, whose supplied bytes the import ingests and verifies) covered by committed golden tests. Artifact bytes render only as verified `text/plain` and `text/markdown`; every other Artifact type stays normalized JSON evidence with an explicit `unrenderable` reason. Completed imports and reparses publish the `ai_archive.*` facts and the operation report through one JetStream pump; the operator commands are `check-config`, `reparse`, `parser-migrate`, `portable-export` and `privacy-delete`. Real-export validation remains a required protected-fixture follow-up; schema detection beyond the declared synthetic identifier, the Enterprise organization-export and Compliance API adapters, and a complete project-export writer are not implemented.

## Facts and deletion

An initial import and every reparse that changes a row enqueue the `ai_archive.*` facts for it through `crates/claude-archive/src/contract_projection.rs`, built from the persisted rows and owned by the Platform user of the account. Privacy deletion is rows first, bytes second, complete last: `privacy_deletion/rows.rs` removes the scope rows and their dependents in one transaction and records `rows_removed_at`, `execution.rs` erases the bytes and completes the request, and `tombstones.rs` writes the contract tombstones: one per deleted archive, per deleted conversation and project (linked to Knowledge or not), and per Knowledge-linked Artifact. A conversation, project or Artifact that a retained export also observed survives and is not tombstoned by itself. A project or conversation is one shared row per account that holds the content of the export imported last, so the same transaction first restores every survivor whose row holds content no retained export presented (`privacy_deletion/restore.rs`, below). Its facts were announced under the deleted archive, so the archive tombstone would drop it from Knowledge; the transaction therefore enqueues an `updated` fact built from the restored rows under the earliest retained export that presented that content, stamped after the tombstone (`contract_projection/reannounce.rs`). A survivor fact identical to a stored one replaces that row with a new event, because the unique payload key would otherwise keep the row Knowledge already consumed. The deleted archives' import heads and the facts of a deleted archive that remain under a retained subject leave the outbox with the rest of their facts, in every scope. The added, updated or unchanged decision of a fact compares only with the facts of the same tenant, and an Artifact only with those of its own conversation and project scope. A test queries `pg_constraint` and fails when a foreign key into a deletion root is neither cascading nor listed in `HANDLED_REFERENCES`.

Erasure and registration share one blob reference protocol (`blob_protocol.rs`, XR-021 CONTRACTS.md R2-07). The blob store is content-addressed, so an import of identical bytes can land between the row-removal commit and the byte erase. The eraser therefore handles one blob per transaction: the tenant lock, then the digest lock (`pg_advisory_xact_lock(hashtextextended('blob:' || digest_hex, 0))`), then the same reference check the row phase used, then it keeps the blob (`retain_shared`) or erases it (`purged`), committing while it holds the lock. The registrars (the receipt, the import and reparse evidence, the Artifact payloads) take the tenant lock, then the digest locks in ascending order, before the first row that references a digest, and check the blob is present. A missing blob is stored again from the bytes the registrar still holds; the receipt holds none and fails with the retryable `Erased` class, registering nothing. No committed row references a missing blob.

A project or conversation is one shared row per account however many cumulative exports contain it (XR-021 CONTRACTS.md R4-03). Each export records the digest of the version it presented in `export_observations.content_digest`, `entity_versions` keeps one snapshot per distinct version while some export names it, and `projects.content_digest` and `conversations.content_digest` name the content the shared row holds. The import writes the newest version into the row (title and project of a conversation included; a content part keeps the first part written at its index), and reparse compares the digest its own export presented. The row-removal phase of a deletion rewrites every surviving project and conversation whose row holds content no retained export presented: a project takes the newest retained version, and a conversation has its parts written again from the retained versions in export order (every conversation a retained export also observed is rebuilt, because the part history is append-only and a retained export that is not cumulative leaves the row current), so the row, its parts, its revisions and its snapshots hold only retained content. A survivor whose latest fact differs from the rebuilt conversation is announced again and its earlier facts are dropped from this service's outbox; that fixes this service's copy only (open cross-repository gap: Knowledge keeps a head unless the incoming `observed_at` is later, and the fact for a retained archive carries that archive's original `imported_at`, so the deleted part can stay in Knowledge for a non-cumulative retained export until Knowledge treats an equal `observed_at` with a later event id as current). The restore queries run over the accounts the request owns only. A project source no retained export observed is removed and its blob is queued through the erase protocol above; a source a retained export observed stays. A tenant deletion leaves no snapshot. A source row keeps the best evidence any export supplied and is not versioned, so a retained export that only references a file keeps bytes that a deleted export supplied, and a later verified upload with different bytes for the same file id leaves the replaced blob referenced by no row, so a deletion cannot find it (open gap; the fix is a versioned source or a durable erase queue).

## Project Knowledge and completeness

Plan item 5 promotes observed project instructions to first-class records and maps exact
synthetic Project Knowledge file references. Matching supplied bytes become verified BlobRefs;
digest or media-type anomalies remain inert quarantined evidence. Per-archive and cumulative
reports count verified, missing, quarantined, and unknown evidence in deterministic order;
reference-only files remain explicitly unbacked up, and absence never proves deletion or coverage.

The import runs this ingest on the production path (XR-021 CONTRACTS.md R4-02): `prepare_initial_import` ingests every knowledge file in file order before the completeness report is built, so an archive whose only knowledge file has correct bytes and a matching digest ends `completed` with a `succeeded` operation. The source row records the outcome: a verified file keeps `blob_ref`, `content_hash`, `byte_size` and `locally_backed_up`, a quarantined file keeps its evidence blob with `locally_backed_up` false, and a reference holds no blob. A later export that carries the bytes upgrades a source an earlier export only referenced, and an export that only references the file leaves better evidence alone. Each source is recorded as observed by the importing export, and `projects.local_backup_status` is `locally_backed_up` exactly when the project has sources and all are verified. A blob store failure while ingesting is retried by the next pass and never fails the import. The knowledge blobs are registered under the digest lock like every other blob the import references.

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

The tests that start a broker of their own (`inbox_prefix.rs`, whose only user is the stanza of `deploy/nats/identity.conf`, `pump_health.rs`, and the connection-count tests of `operation_outbox.rs`; the helper is `Broker` in the `test-support` feature of the library) run the `nats-server` binary found on `PATH`, on ports 57820 to 57839, and fail when it is missing. `ci.yml` copies the binary out of the pinned NATS container in the `Put nats-server on PATH` step; locally `brew install nats-server` provides it. The service sets the inbox prefix `_INBOX_claude` on every connection it opens, because the deployed ACL lets the identity subscribe to that prefix only (XR-021 CONTRACTS.md R3-01): a build without the prefix cannot complete a `JetStream` request, so reload NATS and deploy the services of one snapshot together.

## Platform receipt listener

The process binds two loopback listeners (XR-021 CONTRACTS.md S05). The operator listener
(`RATATOSKR__ADMIN__LISTEN_ADDRESS`, default `127.0.0.1:9086`) serves `/health/live`, `/health/ready`,
`/metrics` and `/version` and nothing else. The receipt listener (`RATATOSKR__RECEIPT__LISTEN_ADDRESS`,
default `127.0.0.1:8097`) serves the private Platform receipt and nothing else, so the operator plane is
never reachable on the port Platform forwards to. Each router refuses the other's routes with `404`, a
test per router proves it, and configuration refuses the same address for both.

`POST /v1/ai-archives/receipt` takes the Edge-minted claims named by
`ratatoskr_ai_archive_contracts::platform_receipt` and answers `202 Accepted` with an empty body. Every other
answer is `application/json` carrying an `ErrorEnvelope` (`claude.auth.unauthenticated` 401 for missing or
malformed claims or an `Authorization` header, `claude.account.unmapped` 403 for a Platform user that is not in the
mapping, `claude.request.invalid` 400, `claude.request.too_large` 413, `claude.request.method_not_allowed` 405,
`claude.server.internal` 500), because Edge replaces any other body with `edge.upstream_invalid_response`.
`GET /v1/capabilities` answers the unauthenticated capability document `{"service":"claude","capabilities":
["ai_archive.receipt"]}` on the receipt listener, where the receipt route is mounted.
`RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS` maps each Platform user to its archive account, one to one (a user or an account that appears twice is refused at startup); the same mapping
decides the Platform owner of every `ai_archive.*` fact and every deletion tombstone, and an account without a
mapping emits no fact. The mapping is also the receipt's authentication: the first receipt from a mapped user creates the
archive account row in its own transaction (`platform-account:<account uuid>` as the external id), so nothing is inserted
by hand, and a Platform user outside the mapping is refused with 403, which Edge relays as a refusal of that user and not
of the device. A tenant-token receipt (the library entry points) still requires an existing account or organization.
Every refused receipt is logged at WARN with a closed `class` (`unauthenticated`, `unmapped_account`, `invalid_request`,
`payload_too_large`, `method_not_allowed`) and the operation id, never an archive path, name or content.

## Event bus, accounts and size limits

The receipt flow is dormant without three keys, and `deploy/systemd/claude.conf.example` sets all of them (the boot test `the_shipped_example_loads` loads it through the loader and through `check-config`): `RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS` (the one-to-one Platform user to archive account mapping), `RATATOSKR__RECEIPT__EVENT_BUS_URL` (the `nats://` or `tls://` endpoint that carries every operation report and `ai_archive.*` fact) and `RATATOSKR__RECEIPT__EVENT_BUS_NKEY_SEED_PATH` (the absolute path of the service identity's seed, `/etc/ratatoskr/claude.nkey` in the example, read at runtime and never logged). The bus URL and the seed path are required together with the accounts.

`RATATOSKR__RECEIPT__MAX_EVENT_BYTES` (default 8000000, accepted 65536 to 8388608) bounds the serialized envelope of every project, conversation and Artifact fact. A fact over it is never truncated or split and is not enqueued: the import head names a `fact_too_large` gap, so the archive is not announced as complete, and the terminal operation report is `partially_succeeded` with the warning `ai_archive.facts.too_large` beside the one `ai_archive.import.incomplete` that applies. The pump is the backstop for a broker whose limit is lower: it reads `max_payload` from the connected server, and a stored envelope larger than that minus 1 KiB is marked `outbox_events.undeliverable_at` with `last_error = payload_too_large`, counted in `claude_archive_outbox_undeliverable_total{class}`, skipped, and never makes readiness false. A transient refusal (an unreachable broker, a missing acknowledgement) only records the attempt and backs off.

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
