## Why

The archive service can safely retain and inspect Claude exports but cannot yet turn a
detected consumer export into conservative project and conversation observations. A
first, explicitly synthetic-schema parser is needed to validate the projection boundary
without pretending that an undocumented provider shape is stable.

## What Changes

- Add a versioned consumer-export parser for a documented synthetic JSON fixture.
- Normalize projects (including description and instructions), conversations, messages,
  and ordered typed content parts into deterministic in-memory records.
- Stamp every normalized record set with the detected schema and parser version.
- Preserve unrecognized object fields and unsupported content variants as inert raw JSON
  evidence with their source location; never silently drop them.
- Add committed synthetic fixtures and deterministic mapping tests. Record validation
  against a minimized owner-provided real export as an explicit follow-up blocker.

## Capabilities

### New Capabilities

- `consumer-export-projection-parser`: Conservative normalization of the documented
  synthetic Claude consumer-export shape into project, conversation, message, and
  content-part records.

### Modified Capabilities

<!-- No existing capability requirement changes. -->

## Impact

- Adds a parser module and public projection types to `ratatoskr-claude-archive`.
- Adds only committed synthetic JSON fixtures and Rust integration tests. It moves the
  already pinned `serde_json` crate from dev-only use into this crate's production
  dependency graph; it adds no new package or lockfile resolution. No database schema,
  provider credential, browser automation, or external contract changes are introduced.
- Registers one exact parser declaration for `ConsumerExport` and its synthetic detected
  schema identifier; real-export compatibility remains unverified pending the owner fixture.
