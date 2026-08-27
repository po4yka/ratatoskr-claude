## Why

Claude Artifacts are versioned document and canvas evidence, not ordinary assistant text. The archive currently preserves the surrounding project and conversation projection but cannot retain Artifact identity, content revisions, or a portable, honest representation.

## What Changes

- Normalize documented synthetic-export Artifacts with stable provider identity, type, title, ownership links, provenance, and complete observed version chains.
- Store each available Artifact-version payload through the content-addressed BlobStore and retain its verified `BlobRef` alongside immutable version evidence.
- Reconcile repeated snapshots conservatively, preserving every changed Artifact version without treating snapshot absence as deletion.
- Define deterministic portable Artifact output: normalized JSON for every version, a safe rendered derivative only for known inert formats, and truthful `unrenderable` output for unknown types while preserving their raw evidence.
- Add synthetic fixture sequences and regression coverage for version-chain construction, unrenderable-type truthfulness, and deterministic output.

## Capabilities

### New Capabilities

- `artifact-archive-projection`: Versioned Artifact evidence, BlobRef-backed payload retention, conservative reconciliation, and deterministic safe portable representation.

### Modified Capabilities

- `consumer-export-projection-parser`: Extend the documented synthetic consumer-export projection with its supported Artifact records and artifact references.

## Impact

- Affected code: the consumer-export parser, normalized projection/reconciliation types, BlobStore ingest boundary, and portable-output writer.
- Affected data model: current first-version schema definitions gain Artifact and Artifact-version records in place; no migration tooling or parallel API version is introduced.
- No cross-repository contract, dependency, browser automation, inference, or Compliance adapter change is included.
