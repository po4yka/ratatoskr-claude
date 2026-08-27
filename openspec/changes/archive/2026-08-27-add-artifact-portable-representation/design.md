## Context

The current parser normalizes a documented synthetic consumer-export schema,
and the BlobStore already offers verified content-addressed references. Project
Knowledge uses a safe byte-availability boundary, but Artifacts are not yet a
first-class projection or portable representation. See `proposal.md` for the
motivation and the Artifact specification for observable behaviour.

## Goals / Non-Goals

**Goals:**

- Extend the current first-version projection and schema definition in place
  with typed Artifact and Artifact-version evidence.
- Make version identity, predecessor links, source snapshots, content BlobRefs,
  and unknown provider fields available to reconciliation and portable output.
- Write a deterministic portable Artifact subtree whose only rendered forms are
  inert text and Markdown derivatives.

**Non-Goals:**

- General full-project portable export, external-reference resolution,
  Compliance ingestion, active HTML/canvas execution, browser automation, or
  semantic analysis.
- Database migration tooling, API versioning, a new production dependency, or
  interpretation of an unknown Artifact type.

## Decisions

### Keep canonical Artifact evidence separate from derived output

The projection will represent Artifact identity separately from immutable
version records. Every version retains provider revision/predecessor evidence,
payload availability, BlobRef, parser stamp, source snapshot, and inert raw
fields. Reconciliation keys by provider Artifact and version identity, then
uses a stable content fingerprint only for provider records lacking a version
identifier. This preserves a chain without treating a rendered derivative as
canonical evidence.

An alternative is storing only the latest payload per Artifact. It is rejected
because it destroys the observed revision chain and makes replay non-auditable.

### Ingest payloads through the existing BlobStore boundary

Available Artifact payloads will use the same digest-verification and
quarantine model as Project Knowledge. A declared-digest mismatch remains
inert evidence with a safe anomaly status; it is not exposed as a verified
version payload. The raw provider record remains preserved independently.

An alternative is embedding version content in normalized database JSON. It is
rejected because it duplicates mutable bytes, bypasses content-addressing, and
weakens integrity verification.

### Render only explicitly supported inert formats

Portable output will place one canonical normalized JSON file and one version
JSON file per Artifact/version under deterministic sanitized paths. `text/plain`
and `text/markdown` create `.txt` or `.md` derivatives from verified bytes.
All other types, including HTML, executable code, images, and unknown provider
types, remain normalized JSON plus a stable `unrenderable` status/reason with
no derivative.

An allowlist is chosen over MIME guessing because an optimistic renderer could
execute or falsely describe hostile content.

### Determinism is a contract, not incidental iteration order

The exporter will sort normalized Artifact and version keys, use deterministic
path components with collision suffixes based on stable identifiers, and use a
stable serializer representation. Tests compare repeated output trees and
bytes, including the unknown-type case.

## Risks / Trade-offs

- [A real Claude export differs from the synthetic shape] -> accept only the
  documented schema, preserve unrecognized Artifact data inertly, and require
  a new parser version plus minimized protected fixture for a new shape.
- [Provider revision metadata is absent or malformed] -> retain the raw record,
  use a deterministic content fingerprint only as a local reconciliation key,
  and expose missing linkage as unknown rather than inventing a chain.
- [Text bytes have an invalid declared digest] -> keep them quarantined and do
  not render them or mark them locally available.
- [Portable paths collide after sanitization] -> resolve collisions from stable
  provider identifiers rather than source ordering or random suffixes.

## Migration Plan

The current development schema definition is extended in place and test
databases are created from that definition. Deploy after the existing gate is
green. Rollback uses the previous binary against a disposable development
database; raw exports and BlobStore evidence are never rewritten or deleted by
this change.
