# Archive reparse

## Purpose

Allows preserved raw Claude exports to be replayed through an explicitly newer compatible parser while previewing exact effects and converging under retries.

## Requirements

### Requirement: Reparse uses verified preserved evidence and an exact newer parser

The `reparse` command SHALL require tenant and archive scope plus an exact registered parser identity. It SHALL verify stored raw bytes against their immutable reference, inspect them again under current hostile-input limits, and proceed only when the target parser supports the archive's acquisition mode and detected schema and is newer than the parser recorded for the current projection.

#### Scenario: Missing raw bytes prevent reparse

- **WHEN** the selected export row exists but its raw blob is absent or fails verification
- **THEN** reparse records a failed report without changing normalized state, revisions, blobs, or outbox facts

#### Scenario: Incompatible parser is refused

- **WHEN** the requested parser does not declare support for the archive's acquisition mode and structural evidence
- **THEN** reparse reports the archive as unsupported and performs no parser execution or writes

### Requirement: Dry run and apply share one deterministic comparison

Dry-run mode SHALL execute verification, inspection, parser selection, parsing, validation, reconciliation, and comparison through the same code path as apply mode, stopping before persistence. Its report SHALL identify the exact target parser, raw digest, additions, changed revisions, unchanged identities, proposed removals, warnings, completeness, and downstream facts that an immediate apply over unchanged inputs would produce.

#### Scenario: Dry run predicts immediate apply

- **WHEN** a dry run succeeds and apply is invoked immediately with the same raw digest, parser-registry fingerprint, and current projection revision
- **THEN** apply reports the same additions, changes, unchanged identities, warnings, completeness, and event subjects as the dry run

#### Scenario: Dry run has no side effects

- **WHEN** a dry run completes successfully
- **THEN** database rows, blob bytes, normalized revisions, audit records, and outbox facts are byte-for-byte unchanged

### Requirement: Applied reparse converges without duplicate evidence

Apply mode SHALL create a reprocessing run and parser-stamped normalized revisions only where the new parser changes validated evidence. Replaying the same raw digest and target parser over the same current projection SHALL return the prior applied result without duplicate revisions, reports, assets, or outbox facts.

#### Scenario: Reparse replay is idempotent

- **WHEN** an already applied archive, raw digest, target parser, and input projection revision are applied again
- **THEN** no new normalized revision or outbox fact is created and the original applied report is returned

#### Scenario: Unchanged parser output remains observable

- **WHEN** a newer parser produces normalized evidence identical to the current projection
- **THEN** the applied report classifies the archive as unchanged, records the newer parser execution, and creates no normalized update fact

### Requirement: Reparse never infers deletion from parser absence

Records absent from newer parser output SHALL be reported as proposed removals or coverage regressions and SHALL not be hard-deleted by reparse. Privacy deletion remains the only local hard-erasure workflow.

#### Scenario: New parser omits an existing conversation

- **WHEN** a newer parser output lacks a conversation present in the current projection without authoritative deletion evidence
- **THEN** reparse retains the conversation, records a missing or coverage warning, and emits no deletion tombstone
