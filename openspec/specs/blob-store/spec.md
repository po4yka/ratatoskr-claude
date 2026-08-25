# Blob store

## Purpose

Defines the local content-addressed byte store owned by this service: write-once immutability, verified digests, and durable references shaped like the workspace `blob-references` contract so other repositories can resolve archived bytes without a shared blob service.

## Requirements

### Requirement: Bytes are stored under a content-addressed path owned by this service

The service SHALL store each object beneath its own configured root keyed by the SHA-256 digest of its bytes, SHALL derive storage paths only from that digest, and SHALL NOT read or write outside its own root.

#### Scenario: Identical bytes deduplicate to one object

- **WHEN** the same bytes are stored twice
- **THEN** both stores return the same reference and exactly one object exists for that digest

#### Scenario: Different bytes produce distinct references

- **WHEN** two different payloads are stored
- **THEN** the references differ because their digests differ

### Requirement: A stored object yields a durable reference carrying owner, digest, media type, and length

Storing bytes SHALL produce a reference identifying the owning service, a SHA-256 digest with its algorithm, the caller-declared media type, and the exact byte length, matching the fleet `blob-references` contract.

#### Scenario: Reference fields describe the stored bytes

- **WHEN** a payload is stored with a declared media type
- **THEN** the returned reference carries this service as owner, the lowercase hex SHA-256 digest of the payload, the sha256 algorithm identifier, the declared media type, and the payload length

### Requirement: Stored objects are immutable

The store SHALL refuse any operation that would change the bytes of an existing object, and SHALL treat storing bytes whose digest already exists as successful idempotent deduplication.

#### Scenario: Overwriting existing bytes is refused

- **WHEN** a write targets the location of an existing object
- **THEN** the operation fails with an immutability error and the previously stored bytes are unchanged

#### Scenario: Re-storing identical bytes succeeds without duplication

- **WHEN** bytes are stored whose digest already exists in the store
- **THEN** the operation succeeds, returns the same reference, and leaves the original object intact

### Requirement: Reads verify integrity against the recorded digest

Reading an object SHALL return exactly the stored bytes, and the store SHALL fail the read when the bytes found at the reference do not hash to the recorded digest rather than returning altered content.

#### Scenario: Round trip returns the original bytes

- **WHEN** a stored object is read through its reference
- **THEN** the returned bytes are identical to the payload that was stored

#### Scenario: Tampered storage fails the read

- **WHEN** the bytes found under a reference have been modified after storing
- **THEN** reading that reference fails with an integrity error instead of returning the altered bytes
