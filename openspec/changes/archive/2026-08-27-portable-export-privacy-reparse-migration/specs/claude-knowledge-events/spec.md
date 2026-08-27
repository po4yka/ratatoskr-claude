## MODIFIED Requirements

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
