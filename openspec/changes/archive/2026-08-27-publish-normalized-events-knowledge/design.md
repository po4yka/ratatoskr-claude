## Context

See proposal.md for motivation. This repository has no runtime implementation yet, so the change
introduces the smallest durable producer slice. The wire semantics live in the workspace change
`add-ai-archive-knowledge-intake`; this design does not duplicate that contract.

## Goals / Non-Goals

**Goals:**

- Store state-carried event payloads and their publication state atomically with the normalised
  archive revision that caused them.
- Keep the exact export digest and parser identity/version in every normalised-subject event.
- Make tombstone publication idempotent and distinguish explicit deletion from snapshot absence.

**Non-Goals:**

- Performing model analysis, embeddings, or search ranking in Claude Archive.
- Delivering raw archive bytes or message bodies over a broadly distributed event channel.
- Inferring deletion from a consumer export snapshot.

## Decisions

- Use one owned PostgreSQL schema definition, created fresh in tests, with a transactional outbox.
  This satisfies at-least-once publication without cross-schema writes; no migration layer is
  introduced under the development-status rule.
- Model published payloads with `ratatoskr-ai-archive-contracts` instead of local JSON shapes.
  The contract fixtures are the producer conformance oracle.
- Store Knowledge completion as a validated linkage keyed by archive id, typed subject, and
  content digest. This keeps derived analysis separate from immutable archive evidence.
- Represent removal as an explicit tombstone record, not reconciliation by missing snapshots.

## Risks / Trade-offs

- [Contract revision is not yet available to this worktree] -> Publish and pin the contracts branch
  before adding the producer dependency; compile against its exact commit.
- [At-least-once delivery can redeliver facts] -> use stable outbox ids and consumer-safe complete
  state carried payloads.
- [Knowledge has no project/Artifact analysis family yet] -> publish the agreed intake facts, but
  do not manufacture generic analyses; Knowledge owns scheduling once its family is implemented.

## Migration Plan

1. Publish the shared contracts and update the exact dependency revision.
2. Deploy producer schema and outbox together in the development environment.
3. Enable publisher consumption after the outbox is populated; redelivery is safe by event id.
4. Roll back by stopping delivery; stored raw evidence and outbox facts remain intact for replay.
