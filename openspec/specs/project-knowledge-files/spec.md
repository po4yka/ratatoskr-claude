# project-knowledge-files Specification

## Purpose

Preserves Claude Project Knowledge source evidence and retains available file bytes only after
digest verification, while keeping anomalous bytes inert and explicitly quarantined.

## Requirements

### Requirement: Project Knowledge references remain explicit evidence

The system SHALL normalize every documented synthetic Project Knowledge file reference with its
provider identifier, owning project identifier, provider filename, declared media type, optional
declared SHA-256, parser provenance, and whether bytes were supplied. A reference without supplied
bytes SHALL remain a reference and SHALL NOT be represented as a locally backed-up file.

#### Scenario: A project exposes its known and reference-only knowledge files

- **WHEN** a supported synthetic export contains one knowledge file with bytes and one without
- **THEN** the projection retains both provider references in source order and only the supplied
  file is eligible for local blob ingestion

### Requirement: Available knowledge bytes require digest verification

The system SHALL store supplied knowledge-file bytes through this service's content-addressed
BlobStore and SHALL publish an available `BlobRef` only when its SHA-256 and length agree with the
provider-declared digest and the stored bytes. A successful result SHALL retain the provider file
identity and project relationship alongside the verified reference.

#### Scenario: Matching project knowledge bytes become a verified local backup

- **WHEN** a supplied knowledge file's declared SHA-256 matches its bytes
- **THEN** the ingest result contains a `BlobRef` whose digest and length identify those bytes and
  marks the file as locally backed up

### Requirement: Digest anomalies are retained but quarantined

The system SHALL retain supplied bytes whose declared digest does not match as inert quarantined
evidence and SHALL emit a stable anomaly warning without echoing file content. It SHALL NOT expose
the quarantined `BlobRef` as a verified local backup or silently discard the provider reference.

#### Scenario: A digest mismatch does not become an available file

- **WHEN** a knowledge file supplies bytes whose SHA-256 differs from the provider declaration
- **THEN** the result marks that file quarantined, includes its safe anomaly reason, and reports no
  verified local backup for it
