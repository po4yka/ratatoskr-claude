# Import state

## Purpose

Defines the durable progress machine for one import pass over a received archive: where progress lives, which transitions are legal, what a restart resumes from, and why finished work stays finished.

## ADDED Requirements

### Requirement: State transitions are durable and guarded

The state machine SHALL persist each transition as a single atomic update that applies only when the recorded state is the transition's expected origin, SHALL admit only documented successor states, and SHALL fail an attempt from any other recorded state while leaving that recorded state unchanged.

#### Scenario: Advancing from the recorded state persists the successor

- **WHEN** a run in its recorded pipeline state is advanced to one of that state's documented successors
- **THEN** the recorded state becomes the successor and survives an independent fresh read of the same record

#### Scenario: Advancing from an unexpected recorded state is refused without change

- **WHEN** a transition names an expected origin that differs from the run's recorded state
- **THEN** the attempt fails with a conflict error and the recorded state afterwards is exactly what it was before the attempt

### Requirement: Replays of an already-applied transition are idempotent

Re-applying a transition whose target equals the currently recorded state SHALL report success-without-change rather than an error or a second effect, so replayed commands cannot duplicate or regress progress.

#### Scenario: Replaying an applied transition reports applied-no-change

- **WHEN** the same origin-to-target transition is applied again after it has already succeeded
- **THEN** the outcome reports that the target state was already current and the recorded state remains the target

### Requirement: An interrupted run resumes from its last recorded state

A run whose process stopped between transitions SHALL resume when a later process reads its recorded state and continues advancing; no completed transition is repeated as new work and no earlier state is re-entered.

#### Scenario: Restart mid-pipeline continues from the recorded state

- **WHEN** a run has advanced partway through the pipeline, the process stops, and a fresh process opens the same database
- **THEN** the fresh process observes the last recorded state and advances the run through the remaining states to completion with no state revisited

### Requirement: Terminal states accept no further transitions

The machine SHALL give every terminal state no outgoing edges, so an attempt to move a finished run to any different state SHALL fail while the run remains in that terminal state; re-reporting the terminal outcome itself stays an idempotent no-change result rather than a second effect.

#### Scenario: A completed run refuses movement to another state

- **WHEN** a transition whose target differs from the recorded state is attempted against a run recorded in a terminal state
- **THEN** the attempt fails with a conflict naming the recorded state and the run afterwards is still recorded in that terminal state
