# parser-registry Specification

## Purpose

Defines deterministic parser selection for detected Claude export structures so
normalization starts only when an explicitly versioned parser declares support.

## Requirements

### Requirement: Parsers declare versioned schema and capability support

Every registered parser SHALL expose a stable parser identifier and parser
version, the acquisition modes and detected schema identifiers it supports, and
the capabilities it can produce. A parser declaration SHALL not imply support
for absent capabilities or a different schema identifier.

#### Scenario: Parser metadata states its exact support boundary

- **WHEN** a caller enumerates the registry
- **THEN** every parser description contains its identifier, version, supported
  acquisition modes, detected schema identifiers, and declared capabilities

### Requirement: Registry selection is exact and explicit

The registry SHALL select a parser only when one registered declaration exactly
supports the detected acquisition mode and schema identifier and satisfies the
requested capability set. It SHALL return a typed unsupported-schema or
unsupported-capability outcome when no declaration matches, and SHALL reject an
ambiguous registration rather than selecting by order or nearest version.

#### Scenario: Known schema and capability select the declared parser

- **WHEN** a detected structure and requested capabilities exactly match one
  registered parser declaration
- **THEN** selection returns that parser and its declared version

#### Scenario: Unknown version remains explicitly unsupported

- **WHEN** inspection detects a schema identifier for which no parser declaration
  exists
- **THEN** selection returns a typed unsupported-schema outcome naming the
  detected identifier and does not invoke any parser

#### Scenario: Missing capability is not silently downgraded

- **WHEN** a parser supports the detected schema but lacks a requested capability
- **THEN** selection returns a typed unsupported-capability outcome and does not
  select that parser

### Requirement: Registry resolves exact parser identities for operator workflows

The registry SHALL support exact lookup by parser name and declared parser version and SHALL expose compatible registered identities in deterministic version order for a supplied acquisition mode and inspected archive. Exact lookup SHALL refuse an identity whose declared signals do not match inspected evidence.

#### Scenario: Exact compatible parser resolves once

- **WHEN** reparse requests a registered parser identity compatible with the archive's acquisition mode and signals
- **THEN** the registry returns exactly that parser identity and its executable parser without applying automatic selection

#### Scenario: Compatible versions have deterministic order

- **WHEN** compatible parser registrations were inserted in different orders
- **THEN** version discovery returns the same unique identities in declared version order

### Requirement: Automatic intake remains ambiguity-safe

Adding exact lookup and version discovery SHALL NOT cause ordinary intake to silently pick a parser when multiple declarations match. Automatic selection SHALL continue to return every ambiguous identity and perform no parse.

#### Scenario: Two compatible versions remain ambiguous at intake

- **WHEN** ordinary intake sees two matching versions and no exact operator target
- **THEN** selection returns an ambiguity containing both identities and neither parser executes
