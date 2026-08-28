## 1. Reproduce the failing gate

- [x] 1.1 Reproduced on head `87188f5421cb82ef9e8d4991bbd55f4545ead024` (this is CI configuration and a dependency-registry fact, not a new unit test, so the failing gate command is the failing test): `build-gate cargo deny --locked check` exits 1 with `error[yanked]: detected yanked crate (try 'cargo update -p chacha20')` at `Cargo.lock:19`, resolving `chacha20 v0.10.1 <- rand v0.10.2 <- async-nats v0.50.0 <- ratatoskr-claude-archive v0.1.0`. `build-gate cargo deny check advisories` (the exact command `.github/workflows/advisories.yml` runs) fails identically. Cross-checked independently against the local crates.io sparse-index cache at `~/.cargo/registry/index/index.crates.io-1949cf8c6b5b557f/.cache/ch/ac/chacha20`: `0.10.0` and `0.10.1` both carry `"yanked":true`, `0.10.2` carries `"yanked":false`.

## 2. Move the lockfile off the yanked version

- [x] 2.1 Ran `cargo update -p chacha20 --dry-run` first and confirmed it only moves `chacha20 0.10.1 -> 0.10.2` with `10 unchanged dependencies behind latest`; then ran `cargo update -p chacha20` and committed the two-line `Cargo.lock` diff. No `Cargo.toml` edit: `chacha20` is transitive and `async-nats = "=0.50.0"` stays exactly as pinned.

## 3. Verify the repair

- [x] 3.1 `build-gate cargo deny --locked check` now reports `advisories ok, bans ok, licenses ok, sources ok`.
- [x] 3.2 `build-gate cargo deny check advisories` (the literal `advisories.yml` command) now reports `advisories ok`.
- [x] 3.3 Ran the full documented gate from `DEVELOPMENT.md` end to end against PostgreSQL 17 on `127.0.0.1:5438`: `cargo fetch --locked`, `cargo deny --locked check`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, the 850-line file ratchet, `cargo build --workspace --locked`, `cargo test --workspace --locked` (24 tests passed across both crates), `cargo test --workspace --locked --doc` (0 doc-tests, `ok`), and `cargo build --workspace --locked --release`. All steps passed.
- [x] 3.4 `uvx zizmor@1.29.0 --persona pedantic --min-severity low .github/workflows/` reports "No findings to report."
- [x] 3.5 `openspec validate --all --strict` and `openspec validate --archived` both pass (20/20 and 10/10 respectively).
