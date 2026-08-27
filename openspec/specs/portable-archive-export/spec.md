# Portable archive export

## Purpose

Lets an owner take a deterministic, independently verifiable local copy of selected normalized Claude archive evidence and every available verified asset.

## Requirements

### Requirement: Portable export is tenant-scoped and filterable

The service SHALL require an authenticated tenant scope and SHALL load only that tenant's archive evidence. It SHALL support optional exact project and inclusive observed-time filters, apply them before rendering or copying assets, and record the filters in the manifest without treating excluded or absent evidence as deleted.

#### Scenario: Tenant and project selection exclude unrelated evidence

- **WHEN** an export is requested for one tenant and project while persisted state also contains another tenant and project
- **THEN** the ZIP contains only the selected tenant's matching evidence and its manifest records the applied project filter

### Requirement: Portable ZIP bytes are deterministic and independently verifiable

Identical selected state SHALL produce byte-identical ZIP output. The ZIP SHALL contain canonical normalized JSON, readable Markdown projections, every selected verified asset, and a final canonical manifest that lists every other member in lexicographic path order with its SHA-256 digest, byte length, media type when known, completeness or availability, source snapshot identifiers, raw-export digest, and parser identity.

#### Scenario: Identical state has a stable golden digest

- **WHEN** the byte-determinism golden test exports the same selected state twice
- **THEN** the two ZIP byte sequences, archive SHA-256 digests, member order, and manifest member digests are identical

#### Scenario: Manifest verifies JSON Markdown and assets

- **WHEN** selected state contains a project, graph conversation, Artifact versions, Project Knowledge, and a verified asset
- **THEN** the ZIP contains their canonical JSON and readable Markdown projections, the verified bytes, and matching manifest entries for every member

### Requirement: Paths and publication are fail-closed

Output member paths SHALL derive from sanitized stable identities rather than provider filenames, collisions SHALL resolve deterministically, and active content SHALL remain inert data. A verified asset SHALL be read only through its owned integrity-checked blob reference; verification or output failure SHALL leave no completed destination archive.

#### Scenario: Unsafe names cannot escape or collide

- **WHEN** selected provider filenames contain traversal, absolute-path, control-character, or colliding values
- **THEN** every ZIP member remains under its assigned export directory with unique deterministic paths and no provider content is executed

#### Scenario: Missing claimed asset aborts publication

- **WHEN** an asset marked locally verified cannot be read and verified against its blob reference
- **THEN** portable export fails, reports the asset unavailable, and leaves no completed output archive
