## MODIFIED Requirements

### Requirement: Parse the documented synthetic consumer-export shape

The system SHALL accept only the declared synthetic consumer-export schema identifier and normalize
its projects, first-class project instructions, project knowledge references, conversations,
messages, and ordered content parts. A project SHALL expose its provider identifier, name, and
description. An observed project instruction SHALL be a separate normalized record with its owning
project identifier, instruction text, and parser provenance. A project knowledge reference SHALL
expose its provider identifier, owning project identifier, filename, declared media type, optional
declared digest, and optional supplied bytes without claiming a local backup before ingest. A
conversation SHALL retain its provider identifier, optional project relationship, title, and
messages. A message SHALL retain its provider identifier, optional parent relationship, role,
model, timestamp, and ordered content parts.

#### Scenario: Complete synthetic export maps every supported record

- **WHEN** a fixture contains projects, project instructions, Project Knowledge references,
  conversations, nested messages, and text and Markdown content parts in the documented shape
- **THEN** the normalized record set contains every supported source record and relationship
  exactly once, with source ordering unchanged
