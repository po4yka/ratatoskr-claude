## Context

See `proposal.md` and `specs/consumer-export-projection-parser/spec.md`. The
current exact-match registry intentionally declares no projection parser. The
repository has no real export fixture, and archived raw data must stay outside
Git and normal logs.

## Goals / Non-Goals

**Goals:**

- Introduce one public, deterministic parser for a small documented synthetic
  consumer-export JSON shape.
- Preserve unrecognized fields and unsupported content variants without treating
  them as successful semantic parsing.
- Make parser provenance available on every projection record.

**Non-Goals:**

- Schema detection from arbitrary exports, archive I/O, database persistence,
  reconciliation, completeness classification, project knowledge, attachments,
  Artifacts, portable export, or organization/Compliance adapters.
- Any assertion that the synthetic shape matches a live Claude export.

## Decisions

### One explicit synthetic shape and exact declaration

The parser accepts root `schema: "claude-export-2026-08-synthetic"`,
`projects`, and `conversations`. Projects contain `id`, `name`, optional
`description` and `instructions`; conversations contain `id`, optional
`project_id`, `title`, optional `created_at`, and `messages`; messages contain
`id`, optional `parent_id`, `role`, optional `model` and `created_at`, and
`content`; supported content parts are `{ "type": "text", "text": ... }`
and `{ "type": "markdown", "markdown": ... }`.

The parser's identifier and version are stable constants and its declaration
matches only `ConsumerExport` plus this exact schema. Accepting plausible
nearby JSON would convert unknown provider drift into false archive success.

### Value-first boundary conversion and ordered evidence

Parse JSON into a generic value at the boundary, then validate and convert each
known field. Every object key not consumed by that conversion becomes an
`UnknownField` holding a JSON Pointer, key, and original value. Unsupported
content part objects become ordered `Unknown` parts and also retain their raw
object. This avoids Serde's default unknown-field discard while preserving safe,
inert evidence for future parser versions.

Alternative: strongly deserialize the whole provider document. Rejected because
it either discards future fields by default or rejects an entire export when a
provider adds an innocuous field, neither of which satisfies loss-aware
archiving.

### Public in-memory projection boundary

The module returns owned public projection types, not database rows. IDs are
provider strings and relationships stay provider-string references; persistence
and UUID allocation remain a later reconciliation concern. No new schema or
migration is required.

### Committed fixture and golden mapping

The crate owns a redacted synthetic input fixture plus a normalized JSON golden
projection. Tests compare mapping completeness, repeated-parse equality,
provenance stamps, and unknown evidence. The golden has no volatile fields.

## Risks / Trade-offs

- Synthetic schema differs from current provider export → preserve the narrow
  declaration and leave a real-fixture validation blocker until the owner
  supplies a minimized, protected export.
- Generic JSON can be large → this boundary only parses already bounded,
  extracted structured candidates; streaming and archive limits remain the
  preceding intake controls.
- Unknown records grow stored evidence → this slice is in-memory only; future
  persistence must attach retained evidence to immutable raw provenance.

## Migration Plan

1. Add tests before implementation, including the committed synthetic fixture.
2. Implement the parser and register its exact descriptor.
3. Run the full repository Rust and OpenSpec gates.
4. Rollback removes the parser declaration and module; no persistent schema or
   external consumer contract changes exist.

## Open Questions

- Owner-provided minimized real export remains required for golden validation;
  until supplied, production-shape compatibility is an explicit follow-up
  blocker rather than a completeness claim.
