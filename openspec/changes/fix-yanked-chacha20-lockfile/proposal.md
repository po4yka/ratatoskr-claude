## Why

`Cargo.lock` locks `chacha20 0.10.1`, pulled in transitively through `rand 0.10.2` from the pinned `async-nats = "=0.50.0"` dependency. crates.io yanked `chacha20 0.10.1` (and `0.10.0`) after the last green CI run on 2026-08-27T16:32Z; only `0.10.2` remains unyanked in that line. `deny.toml` sets `[advisories] yanked = "deny"`, so `cargo deny check` now fails on the locked resolution with `error[yanked]: detected yanked crate`, blocking both the `ci` workflow's `gate` job (which runs `cargo deny --locked check` immediately after `cargo fetch --locked`) and the `advisories` workflow's `cargo deny check advisories` step.

## What Changes

- Run `cargo update -p chacha20` to move the locked resolution from the yanked `0.10.1` to the current `0.10.2`, and commit the resulting `Cargo.lock`.
- No `Cargo.toml` edit: `chacha20` is a transitive dependency, not a direct one, and `async-nats = "=0.50.0"` stays pinned exactly as before.

## Capabilities

No contract or externally-visible behaviour changes; this only moves a transitive lockfile entry to an unyanked patch version of the same crate. `skip_specs: true` is set in the change manifest.

## Impact

- `Cargo.lock` (`chacha20` entry only: version and checksum).
- `.github/workflows/ci.yml` `gate` job (`cargo deny --locked check`).
- `.github/workflows/advisories.yml` `advisories` step (`cargo deny check advisories`).
- No other lockfile entry moves and no source code changes.
