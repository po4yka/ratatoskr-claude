## Purpose

Defines the private, owner-authorized evidence workflow that turns a newly observed real Claude export shape into reviewed non-personal deterministic parser golden tests.

## ADDED Requirements

### Requirement: Real exports remain private owner-authorized evidence

The fixture discovery process SHALL accept a real export only through an owner-controlled private location after explicit authorization for parser discovery. It SHALL record acquisition mode, receive time, immutable digest, provider-visible schema signals, access scope, and disposition outside Git, and SHALL never copy the original archive, personal conversations, filenames, account or organization identifiers, project knowledge, Artifact content, or embedded assets into the repository.

#### Scenario: Raw owner export cannot enter the fixture tree

- **WHEN** fixture admission scans a candidate tree containing a raw export archive or an unredacted owner identifier
- **THEN** admission fails and no candidate file is added to the golden fixture manifest

### Requirement: Derived cases are minimized and structurally faithful

An owner-authorized export SHALL first pass the production hostile-input inspector in private storage. Derived fixtures SHALL retain only the smallest structures needed to exercise each observed schema variant, replace content and identifiers with synthetic values, preserve ordering and unknown variants where semantically relevant, and carry a non-sensitive manifest linking private evidence to the parser/schema decision without exposing the private digest in ordinary test output.

#### Scenario: Minimization preserves parser decisions

- **WHEN** a redacted derived fixture is compared with the private source through the discovery tool
- **THEN** both yield the same schema signals, parser selection, record-variant inventory, and relationship shape for the admitted case while the derived fixture contains no source content values

### Requirement: Golden admission requires independent review gates

A derived fixture SHALL become a committed parser golden only after deterministic parse comparison, secret and personal-data scanning, hostile-path validation, consent and license review, and explicit owner approval are recorded in its non-sensitive manifest. Golden output updates SHALL be opt-in and reviewed rather than automatically blessed.

#### Scenario: Missing approval blocks golden admission

- **WHEN** a deterministic derived fixture lacks any required review result or owner approval
- **THEN** fixture admission fails and the parser support matrix remains unchanged

### Requirement: Real schema support claims follow admitted evidence

The service SHALL continue to label a provider schema unsupported until at least one admitted owner-derived golden exercises its detector and parser, including unknown-record preservation and completeness output. A newly observed incompatible shape SHALL start a new private discovery record and SHALL not silently broaden an existing parser declaration.

#### Scenario: Undocumented shape remains unsupported

- **WHEN** inspection observes a real-export shape not represented by admitted golden evidence
- **THEN** automatic parser selection returns unsupported, preserves the raw archive privately, and reports missing support without claiming a complete import
