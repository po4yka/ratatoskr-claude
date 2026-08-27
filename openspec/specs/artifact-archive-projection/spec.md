# artifact-archive-projection Specification

## Purpose

Preserves versioned Claude Artifact evidence and creates portable output without
executing, flattening, or misrepresenting unsupported provider content.

## Requirements

### Requirement: Artifact identity and observed versions are preserved

The system SHALL normalize each supported Artifact with its provider identity,
type, optional title and language, supported owner relationships, parser
provenance, and raw-record provenance. It SHALL retain every observed provider
version as a distinct immutable version record, including the provider revision
identifier and predecessor relationship when supplied. Each available version
payload SHALL be stored through the BlobStore and exposed only as a verified
`BlobRef` with its content hash and length.

#### Scenario: A supported Artifact produces a complete version chain

- **WHEN** a supported fixture sequence contains an Artifact with an initial
  version and a later version that names the initial version as its predecessor
- **THEN** the normalized result contains one Artifact and two ordered immutable
  version records linked by the observed predecessor relationship, each with
  the verified BlobRef for its supplied payload

### Requirement: Snapshot reconciliation is conservative and idempotent

The system SHALL reconcile repeated observations by stable Artifact identity
and observed version identity. It SHALL retain a newly observed changed version
without replacing earlier versions, and SHALL not add another version for a
repeated identical observation. Artifact absence from a later snapshot SHALL
not create a deletion or remove an earlier version.

#### Scenario: Repeated and changed Artifact snapshots reconcile without loss

- **WHEN** an Artifact is observed in three snapshots as version one, version
  one again, and version two
- **THEN** reconciliation returns the same two-version chain on every replay,
  preserves version one, and records no deletion from any snapshot absence

### Requirement: Portable Artifact output is safe and truthful

The system SHALL produce deterministic portable Artifact output containing a
normalized JSON record for each Artifact and version, including BlobRef,
provenance, predecessor, and rendering status. It SHALL create a rendered
derivative only for supported inert textual formats and SHALL never execute,
render active HTML, or infer a representation for unsupported types. For an
unsupported type it SHALL retain the raw normalized evidence and mark the
version `unrenderable` with an explicit reason.

#### Scenario: An unknown Artifact type remains available but unrenderable

- **WHEN** a supported fixture contains an Artifact version whose provider type
  has no safe renderer
- **THEN** portable output contains its normalized JSON and raw evidence,
  contains no rendered derivative, and reports `unrenderable` with the stable
  reason that the type is unsupported

### Requirement: Portable Artifact output is deterministic

The system SHALL order portable Artifact records, version records, paths, and
serialized object fields deterministically from the normalized evidence. For
identical normalized input, it SHALL produce identical bytes without timestamps,
random identifiers, host paths, or process-dependent ordering.

#### Scenario: Repeated portable output is byte-identical

- **WHEN** a caller exports the same reconciled Artifact projection twice
- **THEN** the two portable output trees contain the same relative paths and
  byte-identical file contents
