## MODIFIED Requirements

### Requirement: Per-archive completeness reports name counts and gaps

The system SHALL produce one deterministic report for each parsed archive containing project,
instruction, knowledge-reference, verified-knowledge, missing-knowledge, quarantined-knowledge,
locally-backed-up-entity, reference-only-entity, and unknown-variant counts plus ordered warnings.
A report SHALL classify digest mismatch or missing bytes as `AssetsPartial`, retained unknown
provider data as `StructurallyPartial`, and only a known fully verified fixture with no gaps or
warnings as `Complete`. Locally backed-up and reference-only entity counts SHALL derive from
verified local evidence and SHALL NOT derive from authorization, connectivity, or upstream
reachability.

#### Scenario: A fixture report exposes partial project knowledge coverage

- **WHEN** a parsed fixture contains one verified knowledge file, one reference without bytes, and
  one digest anomaly
- **THEN** its report contains the three respective counts and classifies the archive as
  `AssetsPartial` with warnings for each gap

#### Scenario: A report distinguishes backup evidence from expired authorization

- **WHEN** an archive report includes one locally backed-up conversation, one reference-only
  conversation, and a later expired authorization outcome without new local evidence
- **THEN** the report retains one locally-backed-up and one reference-only entity count without
  changing either count because of the authorization outcome
