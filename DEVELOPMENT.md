# Developing Ratatoskr Claude Archive

> Status: Proposed  
> Last reviewed: 2026-08-20

Architecture bootstrap: export importer, parser registry, schema, Compliance adapter, Artifact handling, and portable exporter are not implemented.

## Intended toolchain

Rust/Tokio, safe archive handling, streaming SHA-256, SQLx/PostgreSQL, content-addressed BlobStore, Serde/JSON Schema, NATS, fixture-driven parsers, tracing, and testcontainers.

## Code size limits

There is no code here yet, so no limit is enforced yet. The commit that brings the first manifest brings the configuration that carries the limits with it: `clippy.toml` beside a `Cargo.toml`, `eslint.config.js` beside a `package.json`. `fleet.yml` fails the gate when a manifest arrives without one, so the rule has a check behind it and not only this paragraph.

`ratatoskr-workspace/docs/QUALITY_GATES.md` holds the numbers the repositories with code use today, the command that measured each one, and the limits that were rejected with the reason. Read it before you choose numbers, then measure this tree. Each limit is set at the worst case the tree already has, so that the check fails on a regression and not on work that has not been done yet.

## Workflow

1. Persist the immutable raw personal/organization export before parsing.
2. Detect acquisition/schema and select a versioned parser.
3. Preserve unknown records and produce evidence-based completeness.
4. Reconcile projects, instructions, project knowledge, conversation graphs, files, Artifacts/versions, and external references without overwriting history.
5. Test archive limits, graph/version integrity, missing assets, interruption, privacy deletion, Compliance cursors, and portable export.

The first scaffold PR must document exact commands. Default CI uses synthetic fixtures and never Claude session cookies or personal exports.

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
