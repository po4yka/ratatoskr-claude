## Why

Claude Archive currently preserves neither a durable producer outbox nor a usable
normalised-event path to Knowledge.  Consumers therefore cannot safely analyse or remove
search state derived from an imported Claude export.

## What Changes

- Add a Claude Archive producer that persists import, project, conversation, Artifact, and
  explicit-tombstone facts before publishing them.
- Bind every published fact to the immutable export digest and parser identity/version defined by
  the workspace change `add-ai-archive-knowledge-intake`.
- Consume Knowledge's analysis-completed linkage fact and retain the archive-to-analysis reference.
- Publish only state carried contract payloads and blob references; no private archive or message
  bodies are copied into the event transport.

## Capabilities

### New Capabilities

- `claude-knowledge-events`: Durable Claude Archive publication and Knowledge linkage for
  normalised archive facts.

### Modified Capabilities

- None.

## Impact

Adds the initial Rust persistence/outbox implementation and event tests to this repository. It
consumes `ratatoskr-ai-archive-contracts` after the contract producer change is published and
follows the cross-repository semantics in the workspace change
`add-ai-archive-knowledge-intake`.
