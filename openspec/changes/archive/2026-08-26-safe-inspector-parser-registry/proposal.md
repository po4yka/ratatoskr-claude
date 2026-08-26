## Why

Raw Claude exports are now durably received, but treating a ZIP as ordinary input
would permit path traversal, decompression bombs, unsafe link entries, and silent
schema misclassification before any normalized record exists. The next vertical
slice must establish a hostile-input boundary and make parser support explicit
before the first Claude projection parser is introduced.

## What Changes

- Inspect stored ZIP exports structurally, without executing, rendering, or
  extracting their contents, and reject unsafe paths, duplicate normalized paths,
  links/special entries, entry-count/size/compression-ratio violations, and
  invalid JSON structure where detection needs JSON.
- Extract only an accepted inventory to content-addressed `BlobRef` objects under
  per-entry and cumulative decompressed-byte limits; classify media through a
  conservative byte sniff and quarantine active or unrecognized media instead of
  rendering or executing it.
- Attach each extracted object to its immutable raw archive SHA-256 provenance so
  no derived byte can be mistaken for provider-original evidence without its
  source digest and archive entry path.
- Add a versioned parser registry whose parsers declare supported acquisition
  modes, detected schema identifiers, and capabilities; selection returns a
  typed unsupported-version outcome rather than choosing a nearby parser.
- Add strict configuration limits for archive inspection/extraction and document
  the implemented plan-item status. The implementation proposes the maintained,
  MIT-licensed `zip` crate at exact version `8.6.0`, with default features off
  and only `deflate` enabled; owner approval is required before it changes the
  production supply chain.

## Capabilities

### New Capabilities

- `archive-inspection`: Safe structural inspection and bounded extraction of a
  received Claude ZIP archive, including media quarantine and raw-digest
  provenance.
- `parser-registry`: Versioned, capability-declaring parser selection with
  explicit unsupported detected-schema outcomes.

### Modified Capabilities

- `archive-receipt`: Received raw archive evidence becomes the immutable source
  that inspector-derived blobs must cite by SHA-256 provenance.

## Impact

- Affected code: `crates/claude-archive` configuration, archive domain modules,
  BlobStore integration, public exports, integration tests, documentation, and
  OpenSpec specifications.
- Affected systems: only the local Claude Archive bounded context and its existing
  BlobStore contract; no consumer session automation, inference traffic, schema
  migration, external wire contract, or normalization of projects/conversations
  is introduced.
- Dependencies: `zip = "=8.6.0"` with `default-features = false` and
  `features = ["deflate"]` is proposed; it supports the pinned Rust toolchain
  and avoids encryption, extra compression codecs, and timestamp features. JSON
  support is already present through Serde. The implementation must retain
  synthetic hostile fixtures only and run the repository's full Rust and OpenSpec
  gates.
