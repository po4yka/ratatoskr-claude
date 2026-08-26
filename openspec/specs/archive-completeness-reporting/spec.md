# archive-completeness-reporting Specification

## Purpose

Produces conservative, reproducible coverage summaries for one Claude archive and a set of
archives, so missing or anomalous Project Knowledge evidence cannot be hidden by aggregate counts.

## Requirements

### Requirement: Per-archive completeness reports name counts and gaps

The system SHALL produce one deterministic report for each parsed archive containing project,
instruction, knowledge-reference, verified-knowledge, missing-knowledge, quarantined-knowledge,
and unknown-variant counts plus ordered warnings. A report SHALL classify digest mismatch or
missing bytes as `AssetsPartial`, retained unknown provider data as `StructurallyPartial`, and
only a known fully verified fixture with no gaps or warnings as `Complete`.

#### Scenario: A fixture report exposes partial project knowledge coverage

- **WHEN** a parsed fixture contains one verified knowledge file, one reference without bytes, and
  one digest anomaly
- **THEN** its report contains the three respective counts and classifies the archive as
  `AssetsPartial` with warnings for each gap

### Requirement: Cumulative reports preserve constituent gaps

The system SHALL calculate a deterministic cumulative report by summing all count fields from its
per-archive inputs, retaining each source warning in stable archive/input order, and choosing the
most conservative constituent status. It SHALL NOT infer a missing file, deletion, or full
coverage from the absence of a category in one archive.

#### Scenario: Cumulative math retains two archive warnings

- **WHEN** two per-archive reports contain disjoint verified and missing knowledge counts and
  distinct warnings
- **THEN** the cumulative report equals their count-wise sum, retains both warnings in input
  order, and remains at least as partial as either input
