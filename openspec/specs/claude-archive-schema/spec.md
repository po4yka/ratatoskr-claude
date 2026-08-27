# Claude archive schema

## Purpose

Defines the first-version PostgreSQL schema this service exclusively owns: the `claude_archive` schema holding accounts, organizations, exports, import runs, projects, project knowledge sources, conversation graphs, Artifacts, assets, revisions, tombstones, completeness evidence, and outbox/inbox records, created from a single in-place-edited definition with no migration tooling.

## Requirements

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

### Requirement: The service writes nothing outside its schema

The definition SHALL create objects only inside the `claude_archive` schema, and every relationship it declares SHALL stay within that schema with no cross-schema foreign keys or shared-table writes.

#### Scenario: Definition creates no objects outside claude_archive

- **WHEN** the schema definition executes against an empty database
- **THEN** no tables are created outside the `claude_archive` schema and every foreign key resolves to a table inside it

### Requirement: Provider identities are stably constrained

The definition SHALL constrain stable provider identity pairs so repeated observations of the same external record cannot create duplicates: export archives by content hash, and owned records by their external identity within their parent scope.

#### Scenario: Duplicate archive hash is rejected

- **WHEN** a second export row is inserted with the same archive content hash as an existing row
- **THEN** the insert violates a uniqueness constraint and fails

### Requirement: Current schema records export deletion and reparse lifecycles

The single current `schema.sql` SHALL define explicit export-to-entity observations, persisted extracted-artifact references, portable-export records, tenant-owned privacy deletion requests, deletion inventory items, content-free deletion audit outcomes, reparse runs, and parser-migration reports with idempotency constraints, terminal-state checks, correlation identifiers, and foreign keys only within `claude_archive`. Schema changes SHALL be made in place and SHALL remain repeatably applicable.

#### Scenario: Fresh schema exposes lifecycle relations and constraints

- **WHEN** the current schema is applied twice to a fresh PostgreSQL database
- **THEN** all portable-export, privacy-deletion, reparse, and parser-migration relations exist once with tenant ownership, uniqueness, state, and local foreign-key constraints intact

### Requirement: Retained deletion audit does not retain private content

Deletion audit and report records SHALL retain only internal request or subject identifiers, scope class, parser identity when applicable, category counts, timestamps, correlation identifiers, terminal outcome, non-sensitive error codes, and the blob reference of a content-free deletion evidence document. They SHALL NOT retain message bodies, titles, filenames, raw payloads, external account or organization references, source archive digests, or source-content blob references.

#### Scenario: Tenant deletion leaves only content-free audit evidence

- **WHEN** tenant deletion completes and the owned schema is queried for the deleted tenant
- **THEN** no source or normalized content remains and the surviving audit row contains only allowed operational fields

#### Scenario: Duplicate external identity within a scope is rejected

- **WHEN** a record is inserted whose external provider identifier already exists for the same parent scope, such as a conversation UUID inside one account
- **THEN** the insert violates a uniqueness constraint and fails
