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
