## ADDED Requirements

### Requirement: One ack-gated pump publishes every archive event type

The publisher SHALL publish every unpublished outbox row whose event type is in the closed list, on the subject the closed mapping names, with the row's event id as the message id, and SHALL mark a row published only after the broker acknowledged it. An event type outside the closed list SHALL be a hard error.

#### Scenario: Conversation fact reaches the bus

- **WHEN** a conversation-added envelope is enqueued and the pump runs against a real JetStream
- **THEN** the message appears on `evt.ai_archive.conversation.added.v1` and the row is marked published

### Requirement: A completed import is projected into archive facts

An import or reparse that changes persisted rows SHALL enqueue, in the transaction that persists them, the archive-imported fact, one project fact per project, one conversation fact per conversation and one artifact fact per Artifact, owned by the Platform user of the account. An unchanged revision SHALL add nothing.

#### Scenario: Reparse with changed content

- **WHEN** a reparse changes the content of one conversation
- **THEN** exactly one conversation-updated fact is added and its contract validation passes, and the re-announced head carries the completeness report of the first import

### Requirement: A subject that cannot be projected is named on the import head

A subject the projection cannot announce SHALL be counted in a gap of kind `unprojected_project`, `unprojected_conversation` or `unprojected_artifact` on the import head, SHALL prevent the archive from being announced as complete, and SHALL cause the head to be re-announced on a reparse.

#### Scenario: Artifact without verified bytes

- **WHEN** an import contains an Artifact whose versions have no verified bytes
- **THEN** the head carries an `unprojected_artifact` gap with an affected count of one and no Artifact fact exists for it

### Requirement: An Artifact has one identity per account and scope

A later export of the same account that contains an Artifact again SHALL observe the existing row for message, conversation, project and unscoped Artifacts alike.

#### Scenario: Second export of the same account

- **WHEN** two exports of one account each contain the same Artifact
- **THEN** one Artifact row exists and both exports observe it

### Requirement: A refused publish backs off without starving later rows

A publish the broker refuses SHALL leave its row unpublished, count the attempt, record a safe error class and delay the next attempt, and the rows behind it SHALL be published in the same pass.

#### Scenario: Oversized payload

- **WHEN** one pending row exceeds the broker payload limit beside a deliverable row
- **THEN** the oversized row has one attempt, an error class and a future attempt time, the deliverable row is published, and the pass reports a non-fatal broker error
