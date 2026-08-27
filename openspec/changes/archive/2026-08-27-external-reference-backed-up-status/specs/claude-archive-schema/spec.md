## MODIFIED Requirements

### Requirement: The owned schema is created from a single definition file

A fresh database SHALL be fully provisioned by executing the repository's single schema definition,
producing every `claude_archive` table the service owns, and the definition SHALL NOT depend on any
prior schema state or migration history. The definition SHALL persist explicit local backup status
for projects, conversations, and Artifacts, upstream external-reference identity, and content-free
backup-status transition audits.

#### Scenario: Fresh database provisions every owned table

- **WHEN** the schema definition executes against an empty database
- **THEN** the `claude_archive` schema exists containing the owned tables for accounts, organizations,
  exports, import runs, projects, project sources, conversations, messages, message relations,
  content parts, artifacts, artifact versions, assets, external references, backup-status audits,
  revisions, tombstones, completeness reports, and outbox/inbox

#### Scenario: Provisioning is repeatable across throwaway databases

- **WHEN** the schema definition executes against two independent empty databases
- **THEN** both succeed and expose the same set of owned tables
