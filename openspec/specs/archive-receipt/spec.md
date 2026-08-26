# archive-receipt Specification

## Purpose
Defines how this service accepts an official Claude archive into custody: who may hand one over, how the bytes are accounted while they stream in, where the original bytes land, and what the caller is told when the same archive has already been received.

## Requirements

### Requirement: Receipt requires an authenticated tenant scope resolved before any byte is stored

Receipt SHALL require a principal naming exactly one scope - an account or an organization known to this archive - and SHALL refuse a principal that resolves to no tenant, claims both scopes, or claims a scope inconsistent with its acquisition mode, before reading or storing any archive bytes.

#### Scenario: Principal resolving to no tenant is refused before storage

- **WHEN** a receipt attempt presents a principal whose identifier matches no account and no organization
- **THEN** the receipt fails with a refused error, and neither an export record nor stored bytes exist for the attempt

#### Scenario: A principal claiming both scopes is refused

- **WHEN** a receipt attempt presents one principal claiming an account and an organization at the same time
- **THEN** the receipt fails with a refused error, and nothing about the upload is recorded

### Requirement: Archive bytes are hashed and sized while streaming under a configured cap

Receipt SHALL consume the archive as a stream, computing the SHA-256 digest and byte count incrementally, SHALL enforce the configured maximum archive size during the stream, and SHALL refuse an empty archive; a refused stream SHALL leave no export record and no complete stored object.

#### Scenario: An oversized upload is refused mid-stream with nothing durable behind

- **WHEN** an archive stream exceeds the configured maximum byte size before it ends
- **THEN** the receipt fails with a limit-exceeded error named by the cap, and afterwards there is no export row for those bytes and no object readable at their would-be digest path

#### Scenario: An empty archive is refused

- **WHEN** a receipt attempt delivers zero bytes
- **THEN** the receipt fails with an empty-archive error and no export record exists

#### Scenario: The accepted archive records its exact digest and size

- **WHEN** an archive within the cap is fully received
- **THEN** the stored export carries the SHA-256 of the delivered bytes and their exact length as reported by independent re-hash and re-count

### Requirement: The raw archive is stored immutably and stays readable through its reference

An accepted receipt SHALL place the original bytes content-addressed in this service's blob store and return a reference shaped like the fleet `blob-references` contract, and the bytes read back through that reference SHALL equal the delivered bytes.

#### Scenario: Receipt yields a reference whose read-back equals the upload

- **WHEN** an archive is accepted and its returned reference is read back
- **THEN** the read bytes equal the uploaded archive exactly, including across chunk boundaries larger than any single delivery chunk

### Requirement: Re-delivering a known digest produces an explicit duplicate outcome

When the delivered archive's digest already exists, receipt SHALL report a duplicate outcome naming the existing export instead of storing a second snapshot record or rewriting any bytes; delivering different bytes SHALL produce a distinct new export.

#### Scenario: The same archive received twice yields one export and a duplicate verdict

- **WHEN** identical archive bytes are delivered through two receipt attempts
- **THEN** the first outcome reports a newly stored export, the second reports a duplicate naming that same export, exactly one export row exists for the digest, and exactly one stored object holds the bytes

#### Scenario: Different archives remain distinct exports

- **WHEN** two receipts deliver archives whose digests differ
- **THEN** each outcome reports a distinct stored export and both raw objects are independently readable

### Requirement: Every accepted receipt starts an import run at the initial state

A stored outcome SHALL create exactly one import run for the new export in the initial state, so the import state machine can take over from a durable beginning.

#### Scenario: A stored receipt leaves a fresh import run ready to advance

- **WHEN** a receipt completes with a stored outcome
- **THEN** an import run exists for that export whose recorded state is the initial pipeline state
