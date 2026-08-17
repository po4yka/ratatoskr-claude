# Claude archive interfaces

## Inbound

Platform/device archive receipt, import/retry/cancel/reparse, portable export, privacy delete, optional organization/Compliance cursor/pull commands, and operation context.

## Outbound

Export/import/completeness, project/knowledge/conversation/Artifact/file/reference upsert, snapshot/access status, privacy-deletion, and Knowledge indexing events plus safe progress/results.

## Internal boundaries

- `ArchiveInspector`: streaming hash, inventory, limits, acquisition/schema detection.
- `ParserRegistry`: acquisition/schema -> versioned parser.
- `Reconciler`: stable identities, graph/revision/version relationships and snapshot evidence.
- `ReferenceResolver`: records local-backed-up status; delegates GitHub/Drive ownership elsewhere.
- `PortableExporter`: deterministic manifest, JSON/Markdown/files/Artifacts.
- optional Compliance adapter with independent auth/cursor policy.

Errors distinguish archive/limits/schema/parser/relation/asset/reference/completeness/storage/privacy/Compliance failures without exposing content.
