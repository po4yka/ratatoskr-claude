## ADDED Requirements

### Requirement: Survivors are restored to a version a retained export holds

Each export SHALL record the digest of the version of every project and conversation it presented, and one snapshot per distinct version SHALL be kept while some export names it. When a deletion removes the export that wrote the content of a shared project or conversation row, the row-removal transaction SHALL rewrite every surviving project and conversation whose current digest no retained export presented: a project takes the newest retained version, and a conversation is written again from the retained versions in export order, so that the row, its content parts, its revisions and its snapshots hold only retained content before the survivor is announced. A tenant-scope deletion SHALL leave no snapshot.

#### Scenario: Deleting the newest of three cumulative exports restores the intermediate content

- **WHEN** three cumulative exports edit a project description and instructions, a conversation title, a message part at a new index and add a message and a knowledge file, and the newest export is deleted
- **THEN** the project, the conversation title and the message parts hold the intermediate export's content, the message only the newest export held is gone, and no table of the schema contains a byte of the newest export's text

#### Scenario: Deleting the oldest export changes no content

- **WHEN** the oldest of three cumulative exports is deleted
- **THEN** the project and conversation rows hold the content they held before

#### Scenario: Tenant deletion leaves no snapshot

- **WHEN** a tenant deletion completes
- **THEN** `entity_versions` holds no row of that tenant

### Requirement: A project source no retained export observed is removed with its blob

Each project source SHALL be recorded as observed by the exports that presented it. A deletion SHALL remove a source that no retained export observed, together with its blob unless a remaining row references the blob, and SHALL keep a source a retained export observed together with its blob.

#### Scenario: A knowledge file only the deleted export presented

- **WHEN** the newest of three cumulative exports added a verified knowledge file and is deleted
- **THEN** the source row is gone and its blob is absent from the blob store, while a source a retained export observed stays with its blob

### Requirement: The survivor fact carries restored content under a retained provenance

The facts of a deleted archive that remain under a retained project, conversation or Artifact SHALL be removed with the deletion, and the survivor fact built by the row-removal transaction SHALL carry the restored content and name the earliest retained export that presented it.

#### Scenario: The re-announced project is the restored one

- **WHEN** the newest of three cumulative exports edited a project description and is deleted
- **THEN** the `ai_archive.project.updated.v1` fact enqueued after the tombstone carries the intermediate description and names the earliest retained export that presented it
