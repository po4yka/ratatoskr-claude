## Why

The first synthetic consumer-export parser preserves a project's instruction text only as an
optional field and has no model for Project Knowledge file evidence or an honest account of
coverage. Claude-specific project evidence would therefore either be silently omitted or be
reported as complete without proof.

## What Changes

- Extend the documented synthetic consumer-export shape with project-instruction records and
  Project Knowledge file references, retaining their provider identity, project relationship,
  parser provenance, and source location.
- Add a bounded knowledge-file ingest boundary which stores available bytes in this service's
  `BlobStore`, verifies a provider-declared SHA-256 before publishing a usable `BlobRef`, and
  retains a digest-mismatch record as quarantined inert evidence.
- Produce deterministic per-archive and cumulative completeness reports: discovered and archived
  knowledge-file counts, missing or quarantined-file gaps, unknown variants, warnings, and a
  conservative status.
- Update the synthetic fixture and tests. No real Claude export, browser session, credential,
  migration, or provider-shape compatibility claim is introduced.

## Capabilities

### New Capabilities

- `project-knowledge-files`: Parse and retain Project Knowledge source references and safely
  ingest their available bytes as verified blob evidence.
- `archive-completeness-reporting`: Calculate deterministic conservative completeness reports for
  one archive and for a cumulative set of archive reports.

### Modified Capabilities

- `consumer-export-projection-parser`: Promote observed project instructions to first-class
  normalized records and extend the declared synthetic projection shape with knowledge references.

## Impact

- Affects `crates/claude-archive/src/export_projection.rs`, the BlobStore-facing archive module,
  public projection types, synthetic fixtures, and integration tests.
- Reuses the existing pinned `sha2`, `serde`, `serde_json`, and content-addressed BlobStore; no
  dependency or lockfile change is required.
- Does not add a database migration. If schema persistence is required by the implementation, the
  editable `schema.sql` definition is changed in place and its disposable-database tests are
  updated.
- `BlobRef` continues to follow the workspace `blob-references` contract: it names verified bytes
  owned by this service and never carries content in a cross-service record.
