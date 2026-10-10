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
- **THEN** exactly one conversation-updated fact is added and its contract validation passes
