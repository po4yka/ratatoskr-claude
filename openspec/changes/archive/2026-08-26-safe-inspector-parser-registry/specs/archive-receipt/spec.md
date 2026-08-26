## MODIFIED Requirements

### Requirement: The raw archive is stored immutably and stays readable through its reference

An accepted receipt SHALL place the original bytes content-addressed in this
service's blob store and return a reference shaped like the fleet
`blob-references` contract, and the bytes read back through that reference SHALL
equal the delivered bytes. Every inspection-derived artifact SHALL cite this raw
archive's SHA-256 digest and its validated archive entry path as provenance; the
derived artifact SHALL NOT replace, alter, or be represented as the raw archive.

#### Scenario: Receipt yields a reference whose read-back equals the upload

- **WHEN** an archive is accepted and its returned reference is read back
- **THEN** the read bytes equal the uploaded archive exactly, including across
  chunk boundaries larger than any single delivery chunk

#### Scenario: Derived entry retains raw-receipt provenance

- **WHEN** a later archive inspection extracts one accepted entry from a received
  archive
- **THEN** that entry record identifies the exact SHA-256 digest of the raw
  receipt and its validated path while the raw receipt remains independently
  readable through its own BlobRef
