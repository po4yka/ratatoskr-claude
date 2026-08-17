# Claude archive domain model

## Terms

- **Provider export:** immutable personal/organization archive and parser/completeness metadata.
- **Project:** workspace metadata, instructions, visibility, knowledge, references, and conversations.
- **Project Knowledge:** uploaded/pasted documents or text available to a project.
- **Conversation/message revision:** graph nodes and immutable revisions/content parts.
- **Artifact:** generated interactive/document/code object with stable identity and versions.
- **External reference:** GitHub/Drive or other link that may not have local content.
- **Access state:** present, missing from snapshot, access lost, explicitly deleted, or unknown.
- **Completeness report:** evidence-based counts, missing assets/relations, unknown variants, limitations.

## Invariants

1. Raw export precedes normalized state.
2. Project Knowledge and Artifacts remain distinct from chats.
3. Artifact versions and conversation branches are not flattened destructively.
4. External reference is not marked backed up without local verified content.
5. Unknown records survive.
6. Snapshot absence/access loss does not prove deletion.
7. Consumer ingestion never automates browser sessions.
