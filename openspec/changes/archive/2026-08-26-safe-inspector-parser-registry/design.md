## Context

See [proposal.md](proposal.md). Receipt already stores a bounded, immutable raw
archive and creates its import run; the service has no ZIP inspection, derived
entry representation, or parser selection. The current development status
forbids migrations, later major contract versions, and compatibility layers.

## Goals / Non-Goals

**Goals:**

- Create an in-process ZIP inspection boundary whose only inputs are a raw
  BlobRef and configured limits, and whose only extraction outputs are BlobRefs
  with immutable raw-digest provenance.
- Detect structure conservatively and select only exact versioned parser
  declarations, while leaving actual project/conversation parsing for plan item
  4.
- Exercise the hostile archive controls, registry matrix, and provenance link
  through public integration tests using synthetic ZIPs.

**Non-Goals:**

- No database change, parser implementation, project/conversation projection,
  portable export, active-content viewer, recursive nested-archive extraction,
  browser/session automation, or external contract change.
- No completeness claim beyond the structurally observed inventory and explicit
  unsupported parser outcome.

## Decisions

### Inspect central-directory metadata before exposing entry readers

The inspector will open only raw BlobStore bytes, enumerate ZIP entries, and
build a validated immutable inventory before it asks an entry for content.
Portable paths are accepted only when UTF-8, relative, component-normalized,
non-empty, and unique; absolute paths, `.`/`..`, backslash ambiguity, control
characters, duplicate normalized paths, encrypted entries, links, devices, and
other special Unix modes are typed refusals. Directories are metadata only.

The inventory rejects a count above `max_archive_entries`, an entry with declared
uncompressed size over `max_entry_bytes`, total declared uncompressed size over
`max_total_extracted_bytes`, and a declared compression ratio above
`max_compression_ratio`. For an entry with zero compressed bytes and nonzero
uncompressed bytes, the ratio is unbounded and therefore rejected. Nested ZIPs
are retained as quarantined bytes rather than recursively opened, which gives
this slice a nesting depth of zero.

This proposes `zip = "=8.6.0"`, MIT-licensed, with default features off and only
`deflate` enabled. It supports Rust 1.88 and therefore the repository's pinned
Rust 1.97. Using an external `unzip` process would add an execution/temporary-path
boundary and make limits harder to enforce; hand-parsing ZIP would create a
larger security surface. The implementation will never call the crate's
filesystem extraction helpers: the currently disclosed path-canonicalization
advisory affected those helpers before 2.3.0, whereas this design validates
paths and streams entry readers directly to BlobStore. Adding this production
dependency still requires the repository owner's explicit approval before apply.

### Stream accepted entries directly to BlobStore with a real-byte budget

Extraction reopens the raw ZIP after inspection and processes the validated entry
paths in inventory order. Each entry reader is passed to `BlobStore::store_stream`
with `min(max_entry_bytes, remaining_total)`, so actual expanded bytes—not just
attacker-controlled metadata—are bounded. A limit failure has no completed BlobRef
for the offending entry; the returned extraction outcome does not advertise a
partial object. The extractor never uses an archive filename as a local path and
does not materialize a temporary directory.

Each success becomes an `ExtractedArtifact` containing `BlobRef`, validated entry
path, and `RawArchiveProvenance { raw_digest_sha256 }`. The raw digest is checked
as a lowercase SHA-256 identity before extraction. This is a domain record only
in this slice; durable asset rows arrive with the later file/completeness slice.

### Classify bytes conservatively and quarantine by default

Extraction reads only a bounded header prefix before forwarding the full reader
to storage. JSON is a structured candidate only after UTF-8 and a JSON leading
token check; HTML, script/executable signatures, nested archives, and every
unrecognized type are `Quarantined`. Quarantine still preserves bytes and
provenance, but exposes neither rendering nor execution behavior. File extensions
may help diagnostics but never override the byte classification.

### Use declarations rather than heuristic parser fallback

`DetectedSchema` carries acquisition mode, stable detected schema identifier,
and structural evidence summary. `ParserDescriptor` carries parser id, parser
version, exact supported acquisition modes/schema identifiers, and capability
set. Registry construction rejects two descriptors with an overlapping exact
support tuple. Selection either returns its single matching descriptor or a typed
unsupported-schema/capability outcome; it never picks a nearest version or a
parser that has not declared every requested capability.

The first real parser will register in plan item 4. This slice supplies the
registry and controlled test-only descriptors, so it cannot fabricate successful
Claude parsing.

## Risks / Trade-offs

- ZIP-library parser vulnerability or dependency drift → pin reviewed `zip`
  8.6.0 in the lockfile, run `cargo deny`, never call filesystem extraction
  helpers, and keep validation/limits in this service rather than trusting
  archive metadata alone.
- Valid future provider entries may be rejected as unknown → preserve the raw
  archive and return a typed refusal/unsupported result for later parser work.
- Cumulative output cap cannot roll back already immutable earlier entries → the
  extraction result identifies only completed entries, reports the limit failure,
  and makes no completeness claim; future durable import-run orchestration owns
  terminal partial/quarantined handling.
- ZIP central-directory metadata can be forged → metadata limits prevent early
  resource abuse, while BlobStore streaming enforces actual byte limits.

## Migration Plan

1. Obtain explicit owner approval for the maintained ZIP reader dependency and
   its supply-chain impact.
2. Add public RED tests one behavior at a time, run each to its predicted
   assertion failure, then add the minimum implementation.
3. Run the full product and OpenSpec gates, archive the change, commit, integrate
   into `main`, push, and remove the task worktree only after the push succeeds.
4. Rollback is a normal revert of the one commit; raw archive receipt bytes and
   the current schema remain unchanged because this slice adds no durable schema
   data.
