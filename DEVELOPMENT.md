# Developing Ratatoskr Claude Archive

> Status: Proposed  
> Last reviewed: 2026-08-17

Architecture bootstrap: export importer, parser registry, schema, Compliance adapter, Artifact handling, and portable exporter are not implemented.

## Intended toolchain

Rust/Tokio, safe archive handling, streaming SHA-256, SQLx/PostgreSQL, content-addressed BlobStore, Serde/JSON Schema, NATS, fixture-driven parsers, tracing, and testcontainers.

## Workflow

1. Persist the immutable raw personal/organization export before parsing.
2. Detect acquisition/schema and select a versioned parser.
3. Preserve unknown records and produce evidence-based completeness.
4. Reconcile projects, instructions, project knowledge, conversation graphs, files, Artifacts/versions, and external references without overwriting history.
5. Test archive limits, graph/version integrity, missing assets, interruption, privacy deletion, Compliance cursors, and portable export.

The first scaffold PR must document exact commands. Default CI uses synthetic fixtures and never Claude session cookies or personal exports.
