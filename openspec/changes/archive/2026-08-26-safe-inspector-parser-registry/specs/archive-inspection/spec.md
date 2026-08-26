## Purpose

Defines the hostile-input boundary that examines and extracts received Claude ZIP
exports without executing active content or allowing archive metadata to escape
the archive's immutable raw evidence.

## ADDED Requirements

### Requirement: Inspection rejects unsafe ZIP structure before extraction

The service SHALL inspect only a received raw ZIP archive's structure before
extracting any entry, SHALL reject absolute, traversal, duplicate-normalized, or
otherwise ambiguous paths; links and special files; encrypted entries; and any
archive exceeding configured entry-count, per-entry, total-uncompressed, or
compression-ratio limits. It SHALL not execute, render, or recursively extract
archive content.

#### Scenario: A hostile path is rejected without derived bytes

- **WHEN** an archive inventory contains a path traversal, absolute path, or two
  paths that normalize to the same portable archive path
- **THEN** inspection returns a typed unsafe-archive outcome and no entry BlobRef
  is created

#### Scenario: A bomb or unsafe entry is rejected within configured limits

- **WHEN** an archive declares too many entries, an unsafe link/special/encrypted
  entry, an excessive declared compression ratio, or an entry or total expansion
  above its configured bound
- **THEN** inspection returns the matching typed limit or unsafe-entry outcome
  before the service materializes any derived bytes

### Requirement: Extraction is bounded and preserves raw provenance

The service SHALL extract only an accepted inventory through bounded streaming
into its own BlobStore, SHALL enforce per-entry and cumulative decompressed-byte
limits against actual delivered bytes, and SHALL return one derived artifact
record for every stored entry carrying the raw archive SHA-256 digest, original
validated entry path, BlobRef, and disposition. It SHALL leave no complete BlobRef
for an entry that exceeds a limit.

#### Scenario: Accepted entry links exactly to its raw archive

- **WHEN** a valid ZIP is inspected and an entry is extracted
- **THEN** the result contains a BlobRef whose read-back equals that entry's bytes
  and provenance whose raw digest equals the independently known received ZIP
  digest and whose path equals the inspected entry path

#### Scenario: Actual decompression past a bound leaves no stored entry

- **WHEN** a ZIP entry expands beyond its per-entry or remaining cumulative byte
  limit while it is streamed
- **THEN** extraction returns a typed limit outcome and no complete BlobRef is
  returned for that entry

### Requirement: Active and unrecognized media remain quarantined data

The service SHALL classify extracted bytes with a conservative byte sniff, SHALL
retain JSON as structured-candidate data, and SHALL mark HTML, executable/script,
archive, and unrecognized media as quarantined data. Quarantine SHALL neither
render nor execute the bytes and SHALL preserve the same BlobRef and provenance
for later authorized handling.

#### Scenario: HTML is retained but not treated as renderable content

- **WHEN** an accepted export contains HTML bytes
- **THEN** extraction stores the bytes as a BlobRef with a quarantined disposition
  and never reports them as parsed, rendered, or executed
