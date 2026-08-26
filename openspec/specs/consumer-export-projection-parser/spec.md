# consumer-export-projection-parser Specification

## Purpose

Defines conservative, reproducible normalization of the documented synthetic
Claude consumer-export fixture while retaining evidence that the parser cannot
interpret.

## Requirements

### Requirement: Parse the documented synthetic consumer-export shape

The system SHALL accept only the declared synthetic consumer-export schema
identifier and normalize its projects, conversations, messages, and ordered
content parts. A project SHALL expose its provider identifier, name,
description, and instructions when present. A conversation SHALL retain its
provider identifier, optional project relationship, title, and messages. A
message SHALL retain its provider identifier, optional parent relationship,
role, model, timestamp, and ordered content parts.

#### Scenario: Complete synthetic export maps every supported record

- **WHEN** a fixture contains projects, conversations, nested messages, and
  text and Markdown content parts in the documented shape
- **THEN** the normalized record set contains every supported source record and
  relationship exactly once, with content-part ordering unchanged

### Requirement: Projection records carry parser provenance

The system SHALL return the exact detected schema identifier and stable parser
identifier/version that interpreted a record set. Every normalized project,
conversation, message, and content part SHALL carry that same parser-version
stamp.

#### Scenario: A parsed record set exposes consistent provenance

- **WHEN** a caller parses a supported synthetic export
- **THEN** the result and all normalized records identify the declared schema
  and parser version that produced them

### Requirement: Mapping is deterministic

For identical supported input bytes, the system SHALL produce structurally
equal normalized records and unknown-evidence records in a stable order. It
SHALL not mint time-, process-, or iteration-dependent identifiers.

#### Scenario: Repeated parsing produces the same projection

- **WHEN** the same supported fixture is parsed twice
- **THEN** both normalized results compare equal and serialize identically

### Requirement: Unknown provider data remains available as inert evidence

The system SHALL retain every unrecognized object field with its JSON-pointer
location, field name, and original JSON value. It SHALL preserve an unsupported
content-part variant as an ordered unknown content part with its original
object. Unknown data SHALL not be interpreted, executed, or discarded.

#### Scenario: Future fields and a future content variant are retained

- **WHEN** a supported fixture includes unrecognized fields and a content part
  whose type is not supported
- **THEN** the result contains their original JSON values and source locations,
  while supported records still normalize successfully

### Requirement: Unsupported or malformed structural input fails explicitly

The system SHALL return a typed parse failure for an absent or unsupported
schema identifier, a wrong structural type, or missing required provider
identifiers. It SHALL not claim a partial normalized success for those errors.

#### Scenario: Missing required message identifier is refused

- **WHEN** a synthetic message lacks its required provider identifier
- **THEN** parsing returns a typed invalid-structure failure naming the source
  location and no normalized record set
