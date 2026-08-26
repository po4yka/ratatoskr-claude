# Blob store

## ADDED Requirements

### Requirement: Bytes may be ingested as an unbounded stream under a caller-supplied cap

The store SHALL accept a byte stream, consuming it in bounded chunks while computing the digest incrementally, SHALL refuse the ingest when the stream exceeds the caller-supplied maximum before it ends or when it delivers no bytes at all, and SHALL leave either nothing or one complete immutable object behind; a completed stream ingest SHALL produce the same reference that storing those bytes through the buffered path produces.

#### Scenario: A streamed payload matches the buffered reference for the same bytes

- **WHEN** the same bytes are stored once by streaming them in small chunks and once through the buffered path
- **THEN** both paths return references equal in every field and exactly one object exists

#### Scenario: A stream exceeding the cap is refused mid-stream leaving nothing complete behind

- **WHEN** a stream delivers more bytes than its declared maximum
- **THEN** the ingest fails with a limit-exceeded error, no object is readable for any prefix of the stream's digest path, and no staging file remains from the attempt

#### Scenario: An empty stream is refused

- **WHEN** a stream delivers zero bytes
- **THEN** the ingest fails with an empty-input error and nothing is stored
