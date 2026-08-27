## ADDED Requirements

### Requirement: Archive-owned blobs erase only after retained reachability is checked

The BlobStore SHALL provide idempotent erasure for an exact locally owned blob reference. Privacy deletion SHALL invoke erasure only after a fresh database reachability check proves that no retained raw archive, extracted artifact, normalized asset, or portable export refers to the same content address; otherwise it SHALL record the blob as retained-shared.

#### Scenario: Shared content survives one tenant deletion

- **WHEN** two retained tenant records refer to the same content-addressed object and one tenant's deletion executes
- **THEN** the object remains verifiable, the deletion item is classified retained-shared, and the surviving tenant's reference is unchanged

#### Scenario: Exclusive object erasure is idempotent

- **WHEN** an exclusively referenced object is erased and the same deletion item is replayed
- **THEN** both calls succeed, the object remains absent, and no path outside the exact owned content address is touched

### Requirement: Blob erasure rejects foreign or malformed references

Erasure SHALL reject foreign owners, unsupported digest algorithms, malformed content addresses, and any resolved target outside the configured object root without deleting bytes.

#### Scenario: Foreign reference cannot delete local bytes

- **WHEN** erasure receives a validly shaped blob reference owned by another service
- **THEN** the call is refused and every local object remains unchanged
