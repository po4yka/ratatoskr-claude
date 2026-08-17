# Claude archive threat model

## Assets

Chats, projects/instructions, Project Knowledge, uploaded files, Artifacts/code, external references, raw exports, organization data, Compliance credentials, and portable exports.

## Threats and controls

- **Archive bomb/path traversal/symlink:** strict inventory, file/count/size/decompression/path limits and isolated extraction.
- **Malicious Artifact/code/HTML:** never execute during import; sandbox/escape viewers and treat as downloadable data by default.
- **Parser confusion/data loss:** explicit schema/version, raw preservation, unknown records, completeness warnings.
- **External-reference overclaim/leak:** separate metadata from verified local blob and authorize resolution.
- **Cross-user/workspace disclosure:** owner/workspace authorization at archive/blob/query/export boundaries.
- **Compliance credential/cursor compromise:** encrypted least privilege, bounded pull, audit, rotation/revoke.
- **False deletion/access state:** conservative snapshots and explicit access-lost/unknown states.
- **Telemetry leak:** no source text, filenames, artifact contents, or raw provider errors.

Re-review for Artifact execution/rendering, live GitHub/Drive resolution, collaboration, organization administration, or automatic deletion sync.
