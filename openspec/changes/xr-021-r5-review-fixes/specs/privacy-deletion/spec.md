## ADDED Requirements

### Requirement: A deletion keeps an erasure request that is not published yet

A tenant-scope deletion SHALL NOT remove a `platform.stream_erasure.requested.v1` outbox row that has not been published, so the erasure of the stream copies it names is still requested.

#### Scenario: A later deletion with the pump off

- **WHEN** a deletion enqueued an erasure request, the pump has not published it, and a tenant-scope deletion of the same account runs
- **THEN** the first request row survives and the sequences it names are still requested
