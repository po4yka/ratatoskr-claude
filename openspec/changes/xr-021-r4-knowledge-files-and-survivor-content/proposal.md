## Why

Round 4 of changeset XR-021 fixes two defects that the Round 3 native smokes and the independent verifiers found in the Claude archive service and that were re-verified against `origin/main`. The binding text is XR-021 CONTRACTS.md R4-02 and R4-03; this proposal cites them and does not restate them.

- The production import never runs `ProjectKnowledgeIngestor`. A Claude archive that carries any project knowledge file with correct bytes and a matching digest ends `partially_succeeded` with completeness `assets_partial`, and the verified bytes are not kept, because the completeness report is built with an empty outcome list and every `project_sources` row is written without a blob (R4-02).
- A project, conversation, message part and knowledge file is one shared row per account however many cumulative exports contain it. Deleting the newest of several cumulative exports keeps that export's project description, conversation title, appended content parts and knowledge files in the retained rows, and the survivor fact then announces them to Knowledge under a retained export's provenance, after the tombstone that was meant to remove them (R4-03, the mirror of ChatGPT `23bbcb4`).

## What Changes

- `prepare_initial_import` ingests every knowledge file in file order, before the completeness report is built, and `persist_initial_projection` writes the blob reference, digest, size and backup flag of each source, records the source as observed by the importing export and sets `projects.local_backup_status`. A store failure while ingesting is retried, never a permanent failure. The knowledge blobs follow the registration protocol of `blob_protocol`.
- `export_observations` gains a nullable `content_digest`, `entity_versions` keeps one snapshot per distinct project and conversation version while some export names it, and `projects` and `conversations` name the digest of the content their row holds. The import writes the newest version into the shared row, records the digest each export presented and reparse compares an export's own digest.
- The row-removal phase of a deletion restores every surviving project and conversation whose current digest no retained export presented, removes the project sources that no retained export observed (their blobs go through the erase protocol), removes the facts of a deleted archive that remain under a retained subject, and prunes unreferenced snapshots. The survivor fact carries the restored content under the earliest retained export that presented it.

Breaking, with no shims: `schema.sql` gains `export_observations.content_digest`, `projects.content_digest`, `conversations.content_digest` and `entity_versions` in place (a development database must be recreated); the parsed `Conversation`, `Message`, `ContentPart` and `ParserStamp` types derive `Serialize` and `Deserialize`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `project-knowledge-files`: the import path ingests, the outcomes feed completeness, the source row carries the blob and digest.
- `privacy-deletion`: survivors are restored to a version a retained export holds, and sources observed only by deleted exports are removed with their blobs.

## Impact

Touches `initial_import.rs`, a new `initial_projection.rs`, `import_artifacts.rs`, `project_knowledge.rs`, `reparse.rs`, a new `entity_versions.rs`, `export_projection.rs`, `privacy_deletion/rows.rs`, a new `privacy_deletion/restore.rs`, `privacy_deletion/planning_closure.rs`, `contract_projection/reannounce.rs`, `schema.sql`, `README.md` and `DEVELOPMENT.md`. The account-erasure transport, image or asset OCR, the organization receipt scope, Platform's side of the receipt, Knowledge's analysis of knowledge-file text, the pump and receipt code fixed in Round 3 and any wire-shape change are out of scope.
