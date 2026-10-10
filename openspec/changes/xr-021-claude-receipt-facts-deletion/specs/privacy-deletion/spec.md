## MODIFIED Requirements

### Requirement: Finalization couples database erasure audit and downstream removal

A deletion SHALL become terminally completed only after every exclusively owned blob in its inventory is absent and every shared blob is proven reachable from the rows that remain. The row-removal transaction SHALL remove the selected normalized and provenance records and enqueue the authoritative `ai_archive.subject.tombstoned.v1` outbox facts with `reason = "user_requested"`. The completion transaction SHALL append an immutable content-free audit outcome and drop the request items. If either transaction fails, none of its database effects SHALL commit and the durable request SHALL remain resumable.

#### Scenario: Final transaction failure is atomic

- **WHEN** persistence fails while the rows of a deletion are being removed
- **THEN** normalized row removal, provenance removal, completion audit and Knowledge tombstone outbox insertion are all absent, the durable request remains `planned`, and a retry can run from the recorded inventory

#### Scenario: Completed deletion has matching audit and outbox evidence

- **WHEN** a deletion completes
- **THEN** its completion audit category counts match the executed inventory and every no-longer-evidenced downstream subject has exactly one replay-safe tombstone outbox fact

## ADDED Requirements

### Requirement: Deletion removes rows first, bytes second, and completes last

Deletion SHALL delete the scope rows and their dependents and record the rows-removed instant in one transaction under the tenant lock, then erase bytes, then write the audit and complete the request. At no commit point SHALL a surviving row reference a missing blob, and a crash at any point SHALL converge on re-execution.

#### Scenario: Crash between the row phase and the byte phase

- **WHEN** execution stops after the rows are committed
- **THEN** the scope rows are gone, the request is `purging`, the raw blob still exists, and re-execution erases it and completes the request

### Requirement: The deletion closure covers every foreign key into a deletion root

Deletion SHALL remove the dependents of every removed project, conversation, message and Artifact, and the `platform_operation_imports`, `reparse_runs` and `tombstones` of the selected exports, for every scope. A test SHALL fail when a foreign key into a deletion root is neither cascading nor handled.

#### Scenario: Raw export deletion with dependents

- **WHEN** a raw export that observed a project with a source and a message with content parts and an asset is deleted
- **THEN** the request completes and the dependents, the operation import and the reparse run are gone

### Requirement: Tombstones are complete envelopes owned by the Platform user

A user-requested deletion SHALL enqueue one `ai_archive.subject.tombstoned.v1` envelope per deleted archive, per removed conversation and project, and per Knowledge-linked Artifact, owned by the Platform user, with reason `user_requested`, and SHALL NOT abort because a tombstone for the same aggregate already exists. A request whose tenant reference is not a Platform user reference SHALL be rejected at planning when it has tombstone subjects.

#### Scenario: Archive deletion tombstone

- **WHEN** an archive is deleted
- **THEN** the outbox row decodes as an `EventEnvelope` whose payload is an `AiArchiveTombstone` with reason `user_requested`, an existing evidence blob and the correlation id of the request

### Requirement: Tombstones carry the correlation id of the request

The envelope of a tombstone SHALL carry the correlation id the deletion request was recorded under, and when that value is not a namespaced reference it SHALL carry `privacy_request:<request id>` so the deletion still completes.

#### Scenario: Unparsable stored correlation

- **WHEN** a request stored under a correlation id that is not a namespaced reference is executed
- **THEN** the deletion completes and its tombstones carry `privacy_request:<request id>`

### Requirement: Subjects a retained export holds are announced again after the tombstone

When a deletion tombstones an archive, every project, conversation and Artifact that stays because a retained export also observed it, and whose latest `ai_archive.*` fact names a deleted archive, SHALL be announced again in the same transaction as an `updated` fact whose `import_provenance` names the earliest retained export that observed it. The fact SHALL be stamped after the archive tombstone so it is published after it, and a survivor whose latest fact already names a retained archive SHALL NOT be announced again.

#### Scenario: Oldest of three cumulative exports is deleted

- **WHEN** the first of three exports that each observed the same project, conversation and Artifact is deleted
- **THEN** after the archive tombstone the outbox holds one project, one conversation and one Artifact `updated` fact naming the second export, and no fact naming the third

### Requirement: Deletion removes the import head of every deleted export

Deletion SHALL remove the `ai_archive.archive.imported.v1` outbox rows of the deleted exports in every scope, because their `source_export` names bytes that are erased.

#### Scenario: Raw export scope

- **WHEN** one raw export is deleted
- **THEN** its import head leaves the outbox and the heads of the retained exports remain
