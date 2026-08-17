# Claude archive data model

## Owned schema: `claude_archive.*`

- `accounts`, organizations/workspaces, acquisition connections, optional encrypted Compliance credentials.
- `exports`, raw hash/blob, acquisition, schema/parser, import/completeness metadata.
- import runs, warnings, staged/unknown records.
- `projects`, revisions, instructions, visibility/collaboration observations.
- `project_knowledge`, source revisions, files/text/blob references.
- conversations, branches, messages/revisions/content parts.
- `artifacts`, Artifact versions, relations, files/assets.
- external references with provider/url/observed metadata/local-backed-up status.
- snapshot/access states, portable exports, outbox/inbox.

## Constraints

Raw hashes/blobs are immutable. Provider IDs are scoped; surrogate derivation is recorded. Graph/version relationships preserve orphans with warnings. External references never imply ownership/content. Completeness is evidence-based. Owner/workspace authorization is mandatory. Cross-schema writes/foreign keys are forbidden.
