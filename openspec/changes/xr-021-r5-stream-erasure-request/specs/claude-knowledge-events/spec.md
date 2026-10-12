## ADDED Requirements

### Requirement: A deletion asks Edge to erase the stream copy of the published facts it removes

The pump SHALL record the stream sequence of the acknowledgement of every row it publishes. Every code path that deletes a published `ai_archive.archive.imported`, `conversation.added|updated`, `project.added|updated` or `artifact.added|updated` row from the outbox SHALL read those sequences before the delete and, in the same transaction, enqueue one `platform.stream_erasure.requested.v1` command per distinct owner of the rows, with ascending sequences and at most 500 per command, naming the deletion request as `privacy_request:<request id>`. A tombstone and an operation report SHALL never be listed, and a row that was never published SHALL add nothing. The pump SHALL publish the command to `cmd.platform.stream_erasure.requested.v1`.

#### Scenario: Deleting a published conversation enqueues one command per owner

- **WHEN** a deletion removes published fact rows of two owners
- **THEN** the outbox holds one command per owner, each naming that owner and the ascending sequences of that owner's rows, and the rows are gone

#### Scenario: Every removal path asks for the erasure

- **WHEN** a fact row leaves the outbox through the row-removal transaction, the tenant-scope removal, the survivor path or an identical-row replacement
- **THEN** a command names its sequence

#### Scenario: Unpublished rows, tombstones and reports add nothing

- **WHEN** the removed rows were never published, or are tombstones or operation reports
- **THEN** no command is enqueued for them

#### Scenario: Edge erases exactly the named message

- **WHEN** the command is published and a consumer standing in for Edge erases the named sequences
- **THEN** the fact message is gone from the stream and another owner's message is untouched
