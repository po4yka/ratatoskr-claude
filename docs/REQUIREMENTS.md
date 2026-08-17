# Claude archive requirements

## Goals

1. Preserve personal/organization exports immutably before normalization.
2. Version parsers and preserve unknown records.
3. Reconstruct projects, instructions, Project Knowledge, conversation graphs, files, Artifacts and Artifact versions where evidenced.
4. Represent external GitHub/Drive references separately from locally backed-up content.
5. Report completeness and support portable export plus optional Compliance ingestion.

## Non-goals

Anthropic inference, consumer browser login/cookies, treating external references as automatically backed up, or claiming complete account restoration when exports do not support it.

## Requirements

- Raw archive hash/blob and acquisition metadata are durable first.
- Archive parsing is bounded/path-safe and no content executes.
- Graphs and Artifact versions are immutable and relation-validated.
- Unknown variants survive import.
- Missing snapshot data/access loss does not automatically mean deletion.
- External references state whether local content was resolved/backed up.
- Knowledge receives authorized normalized references, not archive authority.

First slice: synthetic export -> raw blob -> versioned parser -> project/conversation plus Artifact/knowledge example -> completeness -> portable export.
