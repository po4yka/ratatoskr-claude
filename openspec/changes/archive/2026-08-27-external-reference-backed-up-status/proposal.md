## Why

An upstream Claude project, conversation, or Artifact can be observed without
being locally preserved. Connectivity and credential state are not archival
evidence, so a later expired credential must not erase the last verified
backup outcome or cause dashboards to overstate coverage.

## What Changes

- Introduce a first-version external-reference model for upstream Claude
  projects, conversations, and Artifacts that are known locally but lack local
  preservation evidence.
- Derive each entity's explicit local-backed-up status solely from verified
  local evidence; retain the last successful status when authorization changes
  or expires.
- Record status transitions as content-free audit entries and expose truthful
  aggregate counts for completeness/report surfaces.
- Extend the owned schema definition in place for the status and audit
  evidence; no migrations or compatibility routes are introduced.

## Capabilities

### New Capabilities

- `external-reference-backup-status`: Explicit, evidence-derived local backup
  status and audited transitions for Claude-side projects, conversations, and
  Artifacts.

### Modified Capabilities

- `archive-completeness-reporting`: Completeness counts report locally backed
  up and reference-only Claude-side entities without deriving either result
  from authorization state.
- `claude-archive-schema`: The first-version owned schema persists
  entity-level backup status and its transition audit records.

## Impact

- Affected code: a new domain model/reconciler, public exports, completeness
  reporting, schema definition, and focused Rust tests.
- Affected surfaces: local dashboard/report consumers gain evidence-based
  backed-up and not-backed-up counts; no cross-repository wire contract,
  consumer login automation, or provider acquisition is added.
- Dependencies: none.
