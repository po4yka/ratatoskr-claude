# Claude archive implementation plan

1. Scaffold service, config, telemetry, health, errors, BlobStore, and `claude_archive` schema.
2. Implement authenticated receipt, streaming hash, immutable raw storage, and import state.
3. Implement safe inspector/extractor and parser registry.
4. Add first parser for projects, conversations, messages, and content parts.
5. Add Project Knowledge/files and completeness reporting.
6. Add Artifacts, versions, and safe portable representation.
7. Add external-reference model and explicit local-backed-up status.
8. Publish normalized events and integrate with Knowledge/search.
9. Add portable export, privacy deletion, reparse/parser migrations, and owner-fixture discovery.
10. Add optional organization/Compliance adapter separately.

Definition of Done: raw evidence survives, parsers are safe/versioned/loss-aware, knowledge/graphs/Artifact versions validate, references are honest, portable export works, privacy/migrations/events/tests and workspace flow pass.
