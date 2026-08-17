# Claude archive testing strategy

Synthetic fixtures cover personal/org exports, projects/instructions, Project Knowledge, branches, files, Artifacts with versions, external references, unknown records, missing/orphan relations, duplicates, malformed/large archives, and partial assets.

Required tests:

- Hash/idempotent intake, crash recovery, archive path/count/size/decompression/MIME limits.
- Schema detection/parser version and unknown preservation.
- Project/knowledge/conversation graph and Artifact version reconciliation.
- External reference local-backed-up truthfulness.
- Completeness and missing/access/deletion semantics.
- Deterministic portable export and safe paths/viewers.
- Privacy deletion, authorization, migrations, outbox/inbox, redacted telemetry.
- Optional Compliance cursor/redelivery/auth with fakes.
- Workspace export-agent -> Claude -> Knowledge flow.

No real personal export is committed; sanitized owner fixtures need explicit review.
