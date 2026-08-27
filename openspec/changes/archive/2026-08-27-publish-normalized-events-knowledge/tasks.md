## 1. Shared contract intake

- [x] 1.1 Add a failing contract-fixture conformance test for Claude import, project, conversation, Artifact, and tombstone event payloads.
- [x] 1.2 Pin the published `ratatoskr-ai-archive-contracts` revision and make the producer conformance test pass.

## 2. Durable publication

- [x] 2.1 Add a failing integration test showing one completed import creates state-carried outbox facts with export digest and parser provenance.
- [x] 2.2 Implement normalised-record persistence and transactional outbox publication; verify the import integration test passes.
- [x] 2.3 Add a failing integration test showing an explicit tombstone publishes one removal while snapshot absence publishes none.
- [x] 2.4 Implement idempotent tombstone publication; verify the deletion propagation test passes.

## 3. Knowledge linkage and verification

- [x] 3.1 Add a failing integration test showing Knowledge completion links only the exact archive subject digest.
- [x] 3.2 Implement completion-linkage validation and persistence; verify the linkage round-trip test passes.
- [x] 3.3 Run the repository gate, `openspec validate --all --strict`, and review the scoped diff.
