## Context

See `proposal.md` and the delta specifications. The current parser maps only project text fields;
the BlobStore already provides write-once, content-addressed storage and re-hashes objects at read
time. There is no real provider fixture, persistence layer for normalized projections, migration
mechanism, or external archive contract in scope.

## Goals / Non-Goals

**Goals:**

- Keep project instructions and knowledge references as independent normalized evidence.
- Make the verified-versus-reference-only-versus-quarantined distinction impossible to lose in
  typed results and completeness math.
- Reuse the existing service-owned BlobStore and workspace BlobRef contract without new packages.

**Non-Goals:**

- Real Claude-export compatibility, database persistence, external-reference fetching, malware
  scanning, Artifacts, portable export, and provider deletion/reconciliation semantics.
- Rendering, executing, parsing, or otherwise interpreting knowledge-file content.

## Decisions

### Synthetic knowledge shape remains exact and bytes are JSON octets

The supported synthetic project shape gains an optional `instructions` string and ordered
`knowledge_files` array. A file has required `id`, `filename`, and `media_type`; it may carry
`sha256` and `bytes` (an array of unsigned octets). Byte arrays avoid adding a base64 dependency
before a real provider contract exists. Wrong types and out-of-range octets are typed structural
failures; unknown fields stay inert evidence as they do elsewhere.

### Instructions and file references have their own records

`ProjectInstruction` and `ProjectKnowledgeFile` are projection records, rather than duplicating
instructions in `Project` or flattening a file into raw JSON. Each carries the parent provider
project ID and parser stamp. This removes the old optional project-instructions compatibility
field and makes an absent instruction distinct from an observed empty instruction.

### Store first, verify before available classification

A `ProjectKnowledgeIngestor` stores supplied bytes using the existing BlobStore. It independently
hashes the source bytes and verifies both the BlobRef and declared SHA-256. Matching results are
`Verified`; missing bytes are `ReferencedOnly`; a missing or mismatched declaration is
`Quarantined` and has a safe reason. Quarantined bytes remain stored under their content address,
but the type does not expose them as a verified backup, paralleling archive extraction's inert
quarantine disposition. This retains evidence without a second storage implementation.

Alternative: reject mismatches without storing them. Rejected because it loses the only observed
evidence. Alternative: publish every BlobRef and attach a boolean. Rejected because callers could
mistake dangerous bytes for a verified backup.

### Completeness is pure and conservative

`ArchiveCompletenessReport::from_ingest` calculates scalar counts and source-order warnings from
one projection and its per-file outcomes. `CumulativeCompletenessReport::from_reports` folds those
reports in caller order. Status precedence is `FailedValidation`, `AssetsPartial`,
`StructurallyPartial`, `Unknown`, `ConversationsComplete`, then `Complete`; it never upgrades a
constituent report. This is pure in-memory behavior; a later persistence/reconciliation change can
store these evidence receipts without altering their math.

## Risks / Trade-offs

- Synthetic shape differs from Claude exports → exact schema matching and the existing protected
  real-fixture follow-up prevent a false compatibility claim.
- Quarantined bytes consume storage → preserve their content-addressed evidence, report it
  explicitly, and leave retention policy to a later approved change.
- Cumulative totals can double-count repeated snapshots → reports deliberately summarize observed
  archive evidence, not reconciled current-state identity; reconciliation is out of scope.

## Migration Plan

1. Add and run each fixture-driven test in the stated red phase.
2. Implement parser records, safe ingest classification, and report calculations.
3. Update the synthetic golden and developer status, then run the full gated Rust and OpenSpec
   checks.
4. Rollback removes the parser extension and pure report/ingest API; raw archives and BlobStore
   behavior remain intact. No migration, deployment conversion, or external consumer rollout is
   required.
