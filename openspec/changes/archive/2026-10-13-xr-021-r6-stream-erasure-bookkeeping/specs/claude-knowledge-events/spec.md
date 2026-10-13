## ADDED Requirements

### Requirement: Every published copy of a deleted fact is erased, and a fact row in flight is serialized with its deletion

The pump SHALL record the stream sequence of every acknowledgement it receives for a row in `published_stream_seqs`, ascending and without duplicates, and this replaces the single sequence of the stream erasure request requirement. The first publication of a row SHALL be one short transaction that locks the row with `for update skip locked`, publishes, marks and commits; a row that is deleted or locked SHALL be skipped and never published. The mark SHALL affect exactly one row, and any other result SHALL be a counted error. A deletion SHALL read the sequences of the rows it removes in the statement that removes them. An acknowledgement whose commit failed SHALL be carried in memory and merged into the next successful mark of that row, and a carried copy whose row was deleted SHALL be erased by a command the pump enqueues itself.

#### Scenario: A row deleted before the pump locks it is never published

- **WHEN** the pump has chosen a candidate and a deletion of its subject commits before the pump locks the row
- **THEN** the pump skips the row, the stream holds no message of the deleted fact and no erasure command is enqueued for it

#### Scenario: A deletion inside the acknowledgement window waits for the pump

- **WHEN** a deletion starts while the pump holds the row after the acknowledgement
- **THEN** the deletion does not complete until the pump commits, and then enqueues a command naming the acknowledged sequence

#### Scenario: A republish after a failed commit keeps both sequences

- **WHEN** a commit fails after an acknowledgement and the row is republished after the duplicate window
- **THEN** `published_stream_seqs` holds both sequences ascending and a deletion enqueues one command naming both

#### Scenario: A carried copy whose row was deleted is erased by the pump

- **WHEN** a row whose acknowledgement was carried is deleted before its retry
- **THEN** the pump enqueues one erasure command for the carried sequences in the owner's name, and a later pass enqueues nothing more

#### Scenario: A mark that finds no row is counted

- **WHEN** the mark of an acknowledged row affects zero rows
- **THEN** the pass fails with a mark-lost error and `claude_archive_outbox_mark_lost_total` increases

#### Scenario: A duplicate acknowledgement adds nothing

- **WHEN** the broker answers a republish inside the duplicate window with the original sequence
- **THEN** `published_stream_seqs` still holds that sequence once
