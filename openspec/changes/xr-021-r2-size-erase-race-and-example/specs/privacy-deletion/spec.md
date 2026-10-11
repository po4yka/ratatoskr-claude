## ADDED Requirements

### Requirement: Erasure re-checks references under the digest lock

For each blob in its inventory, a deletion SHALL take the tenant lock and then the digest lock in one transaction, recompute whether any surviving row references the digest, and either retain the blob and mark its item `retain_shared` or erase the bytes and mark the item `purged`, committing while the digest lock is held. No committed row SHALL reference a missing blob.

#### Scenario: An import lands between the row phase and the byte phase

- **WHEN** a deletion has committed its row phase and a second tenant then registers a reference to the same digest
- **THEN** resuming the deletion keeps the bytes, marks the item `retain_shared`, and the second tenant's row still resolves to its blob

### Requirement: Registration takes the digest lock and verifies presence

A transaction that inserts the first row referencing a digest SHALL take the tenant lock, then the digest locks in ascending order, and then verify the blob is present. A missing blob SHALL be stored again from bytes the registrar still holds; otherwise the registration SHALL fail with a retryable class and register nothing.

#### Scenario: The registrar waits for the eraser and stores the bytes again

- **WHEN** an eraser holds the digest lock after its re-check and a registrar that holds the bytes waits for it
- **THEN** after the eraser commits the registrar finds the blob missing, stores it again from its bytes, and commits a row that resolves

#### Scenario: A registrar without bytes fails retryably

- **WHEN** the eraser removes the blob while a receipt registration waits and the receipt holds no bytes
- **THEN** the registration fails with the retryable erased class and no export row is written
