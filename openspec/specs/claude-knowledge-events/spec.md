# claude-knowledge-events Specification

## Purpose
Publish durable, provenance-bound Claude Archive facts that Knowledge can analyse and remove from
search without claiming authority over provider evidence or archive retention.

## Requirements

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

The Claude Archive service SHALL publish an `ai_archive.subject.tombstoned.v1` fact only for an explicitly recorded provider tombstone or an authorized privacy deletion that removes the subject's last retained evidence. It SHALL preserve non-sensitive tombstone provenance, SHALL use `reason = "user_requested"` for privacy deletion, and SHALL NOT publish a removal merely because a subject is absent from a later export. Privacy deletion SHALL enqueue its replay-safe removal facts in the same database transaction that commits normalized erasure and the content-free completion audit.

#### Scenario: Explicit deletion removes the downstream subject

- **WHEN** a Claude conversation receives an explicit provider deletion tombstone
- **THEN** the outbox contains one provenance-bound subject-tombstoned fact for that conversation

#### Scenario: Privacy deletion atomically removes the downstream subject

- **WHEN** authorized privacy deletion removes the final retained evidence for a published Claude conversation
- **THEN** normalized erasure, content-free completion audit, and exactly one `user_requested` subject-tombstoned outbox fact commit together or none commit

#### Scenario: Snapshot absence does not remove the downstream subject

- **WHEN** a later export omits a previously observed Claude conversation without explicit deletion evidence
- **THEN** the outbox contains no subject-tombstoned fact for that conversation
