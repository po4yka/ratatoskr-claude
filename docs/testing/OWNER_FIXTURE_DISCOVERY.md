# Owner fixture discovery and golden admission

Real Claude exports are private evidence, never repository fixtures. An owner who wants a new
provider shape supported keeps the original export in access-controlled local storage, records
explicit consent and source/license authority, and runs production archive inspection without
copying raw bytes, names, account identifiers, message bodies, file contents, or digests into an
issue, log, terminal transcript, CI artifact, or pull request.

The owner creates a separate candidate directory containing only a minimized synthetic reproduction:

- `derived.json` reproduces the smallest structural shape with invented values;
- `observed-structure.json` is a content-free structural inventory;
- `expected-structure.json` is the independently reviewed expected inventory;
- `manifest.json` lists those exact paths, the acquisition/schema class, an opaque private evidence
  record identifier, `synthetic_only: true`, and every consent, license, privacy, path,
  determinism, independent-review, and owner-approval gate.

Run the read-only gate before copying anything into the checkout:

```sh
cargo run -p ratatoskr-claude-archive-service -- fixture-admit --candidate /private/candidate
```

The command rejects archives, symlinks, unlisted/unsafe paths, non-JSON inputs, common secret or
personal-data markers, review gaps, and a mismatch between observed and expected structure. It does
not move, redact, rewrite, or bless files. Review the report and the candidate line by line. A second
reviewer confirms that all remaining values are invented and that the structural expectation is
deterministic. The repository owner gives the final approval.

Only after every gate passes may the minimized synthetic files and an explicit read-only golden
expectation be added under `crates/claude-archive/tests/fixtures/` and
`crates/claude-archive/tests/golden/`. Ordinary tests compare them and never rewrite or bless them;
updating a golden is a separate, opt-in, reviewed edit. Passing admission proves only the specific
synthetic structure covered by the golden. It does not claim general support for all Claude export
variants.

After admission, retain or delete the original private export according to the owner's existing
retention policy. Record that disposition only in the protected evidence system. Never add its path,
digest, title, filenames, conversation text, Artifact content, account data, or organization data to
this repository.
