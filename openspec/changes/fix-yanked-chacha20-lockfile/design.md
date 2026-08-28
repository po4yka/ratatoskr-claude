## Context

See [proposal.md](proposal.md). `deny.toml` enforces `[advisories] yanked = "deny"`: any locked dependency graph entry that crates.io marks yanked is a hard `cargo deny check` failure, because a yanked version is no longer guaranteed to be fetchable and cannot be reproduced by a fresh `cargo fetch --locked`. `chacha20 0.10.1` was yanked from crates.io after the repository's last green CI run and before this fix; `chacha20 0.10.2` is the next version in the same line and remains unyanked.

## Goals / Non-Goals

**Goals:**

- Move the locked `chacha20` resolution off the yanked version with the smallest possible lockfile diff.
- Keep `async-nats = "=0.50.0"` pinned exactly as it already is; this is not a dependency-upgrade change.

**Non-Goals:**

- Do not widen or change any direct dependency version requirement in `Cargo.toml`.
- Do not update any other transitive dependency; `cargo update -p chacha20` is scoped to the one crate.

## Decisions

Run `cargo update -p chacha20`, which SemVer-bumps only the `chacha20` lockfile entry from `0.10.1` to `0.10.2` (dry-run confirmed: "Locking 1 package to latest Rust 1.97 compatible version... 10 unchanged dependencies behind latest"). `rand 0.10.2` already accepts `chacha20 ^0.10`, so no dependent's requirement needs to change and no other entry in `Cargo.lock` moves.

An alternative — pinning `async-nats` to a different version to change the transitive graph — was considered and rejected: it would touch a direct dependency pin for a problem that is entirely in a transitive leaf, and there is no evidence any other version of `async-nats` in the allowed range resolves a materially different graph.

## Risks / Trade-offs

- [`chacha20 0.10.2` itself gets yanked in the future] → the same `cargo deny check` gate will catch it again immediately; no different handling needed.
- [Some other transitive dependency also becomes yanked before this change lands] → `cargo deny --locked check` will report it as a second, independent `error[yanked]` entry; this change only addresses the `chacha20` entry that is failing today.

## Migration Plan

Commit the `Cargo.lock` diff. No rollout ordering, no data migration, no rollback beyond reverting the single commit if `chacha20 0.10.2` is later found to have some other problem.
