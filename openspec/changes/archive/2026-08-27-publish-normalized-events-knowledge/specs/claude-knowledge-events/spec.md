## Purpose

Publish durable, provenance-bound Claude Archive facts that Knowledge can analyse and remove from
search without claiming authority over provider evidence or archive retention.

## ADDED Requirements

### Requirement: Provenance-bound normalised fact publication

The Claude Archive service SHALL persist an outbox fact for every completed import and each
normalised project, conversation, and Artifact addition or update. Every per-subject event SHALL
contain the immutable export digest and parser name/version required by the workspace
`add-ai-archive-knowledge-intake` contract, and SHALL carry references rather than private raw
archive bytes.

#### Scenario: Completed import publishes its normalised records

- **WHEN** an official Claude export completes normalisation with one project, conversation, and Artifact
- **THEN** the outbox contains the import fact and one contract-conformant fact for each normalised record

### Requirement: Analysis linkage is revision specific

The Claude Archive service SHALL record a Knowledge analysis-completed fact only when its archive,
owner, typed subject, and content digest match an existing published revision. The linkage SHALL
not replace the underlying archive evidence or make a later revision appear analysed.

#### Scenario: Knowledge completion links exactly one revision

- **WHEN** Knowledge reports completion for a published conversation content digest
- **THEN** the archive records a linkage to that digest and leaves any different digest unlinked

### Requirement: Explicit tombstones propagate removal

The Claude Archive service SHALL publish an `ai_archive.subject.tombstoned.v1` fact only for an
explicitly recorded tombstone. It SHALL preserve the tombstone provenance and SHALL NOT publish a
removal merely because a subject is absent from a later export.

#### Scenario: Explicit deletion removes the downstream subject

- **WHEN** a Claude conversation receives an explicit provider deletion tombstone
- **THEN** the outbox contains one provenance-bound subject-tombstoned fact for that conversation

#### Scenario: Snapshot absence does not remove the downstream subject

- **WHEN** a later export omits a previously observed Claude conversation without explicit deletion evidence
- **THEN** the outbox contains no subject-tombstoned fact for that conversation
