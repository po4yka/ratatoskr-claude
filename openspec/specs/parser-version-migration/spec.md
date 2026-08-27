# Parser version migration

## Purpose

Provides deterministic planning, execution, and reporting for advancing retained archives to a declared parser version without introducing database schema migrations.

## Requirements

### Requirement: Migration planning classifies every selected archive exactly once

The parser-version migration command SHALL require an explicit tenant scope and exact target parser identity. A plan SHALL classify every selected archive as eligible, already current, unsupported, raw evidence missing, privacy blocked, or failed inspection, and SHALL produce deterministic per-archive entries plus totals derived from those entries.

#### Scenario: Report totals match entries

- **WHEN** a migration plan contains archives across all supported outcome classes
- **THEN** every selected archive appears exactly once and each summary total equals the number of entries carrying that status

#### Scenario: Reordered database results produce the same report

- **WHEN** the same selected archives are returned in different database orders
- **THEN** migration reports serialize entries in stable archive-identity order and have identical totals and content

### Requirement: Migration apply delegates to the reparse contract

Applying a parser migration SHALL run archive reparse for each eligible archive, preserve plan classification for non-eligible archives, continue after an archive-local failure, and produce a final report that distinguishes applied, unchanged, failed, and skipped outcomes without converting partial success into full success.

#### Scenario: One archive failure remains visible

- **WHEN** two eligible archives are migrated and reparse fails for one after the other applies successfully
- **THEN** the final report marks one applied and one failed, reports a partial terminal result, and does not roll back or misclassify the successful archive

### Requirement: Parser migration is not database migration tooling

The command SHALL change only parser execution records and normalized revisions inside the single current schema. It SHALL NOT create migration files, schema-version ledgers, parallel schema versions, API version negotiation, or a later contract major version.

#### Scenario: Migration command leaves schema lifecycle unchanged

- **WHEN** a parser-version migration is planned or applied
- **THEN** the database continues to initialize solely from the repository's current `schema.sql` and no database migration artifact is created or invoked
