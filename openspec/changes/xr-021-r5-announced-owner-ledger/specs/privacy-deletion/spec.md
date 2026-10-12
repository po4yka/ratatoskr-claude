## ADDED Requirements

### Requirement: A deletion addresses every tombstone to the owner the subject was announced under

The service SHALL record, in the same transaction that enqueues each `ai_archive.*` fact, the owner that fact carries in `claude_archive.announced_subjects`. A deletion SHALL emit one tombstone for every distinct owner the ledger holds for each tombstone subject, addressed to that owner, and SHALL NOT read the configured Platform account mapping. A subject that was never announced SHALL emit no tombstone and SHALL NOT fail the request. A deletion request SHALL be recorded under the account reference its caller supplied, and executing it SHALL compare that stored reference with the supplied one.

#### Scenario: A mapping change between plan and execute changes nothing

- **WHEN** an account is imported under Platform user A, the mapping then no longer lists the account, and a deletion is planned and executed
- **THEN** both succeed and the tombstones name Platform user A as their owner

#### Scenario: Archives announced under two users are tombstoned to their own owners

- **WHEN** two archives of one account were announced under Platform users A and B and the tenant is deleted
- **THEN** every shared conversation has one tombstone to A and one to B, each archive is tombstoned to its own owner, and no tombstone names a third user

#### Scenario: An account that never announced anything completes quietly

- **WHEN** an account with no ledger row is deleted
- **THEN** the request completes with no tombstone and no recorded gap

#### Scenario: A deleted subject leaves its ledger rows behind nowhere

- **WHEN** a deletion tombstones a conversation
- **THEN** the ledger holds no row of that conversation and every other subject's rows are untouched

#### Scenario: A re-announced survivor carries the retained archive's owner

- **WHEN** a deletion keeps a subject because a retained export also observed it
- **THEN** the survivor fact names the owner of the retained archive's ledger row, whatever the configured mapping says
