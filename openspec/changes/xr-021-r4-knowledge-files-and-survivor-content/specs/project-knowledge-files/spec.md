## MODIFIED Requirements

### Requirement: Available knowledge bytes require digest verification

The system SHALL store supplied knowledge-file bytes through this service's content-addressed BlobStore and SHALL publish an available `BlobRef` only when its SHA-256 and length agree with the provider-declared digest and the stored bytes. A successful result SHALL retain the provider file identity and project relationship alongside the verified reference. The import SHALL run this ingest for every knowledge file of an archive before it builds the completeness report, SHALL record the outcome on the source row (`blob_ref`, `content_hash`, `byte_size` and `locally_backed_up`), SHALL record the source as observed by the importing export, and SHALL treat a store failure while ingesting as retryable.

#### Scenario: Matching project knowledge bytes become a verified local backup

- **WHEN** a supplied knowledge file's declared SHA-256 matches its bytes
- **THEN** the ingest result contains a `BlobRef` whose digest and length identify those bytes and marks the file as locally backed up

#### Scenario: An archive whose only knowledge file is verified imports complete

- **WHEN** an archive with one project and one knowledge file with correct bytes and a matching digest is received and imported
- **THEN** the run is `completed`, the completeness report is `complete` with one verified knowledge file and no missing asset, the source row names the blob and is locally backed up, the project is `locally_backed_up`, and the Platform operation is reported `succeeded` without a warning

#### Scenario: A later export that carries the bytes upgrades a referenced source

- **WHEN** an earlier export only referenced a knowledge file and a later export of the same project supplies matching bytes
- **THEN** the same source row becomes verified and names the blob

#### Scenario: A store failure while ingesting is retried

- **WHEN** the blob store fails while a knowledge file is ingested
- **THEN** the import is not marked failed and the next pass ingests again

### Requirement: Digest anomalies are retained but quarantined

The system SHALL retain supplied bytes whose declared digest does not match as inert quarantined evidence and SHALL emit a stable anomaly warning without echoing file content. It SHALL NOT expose the quarantined `BlobRef` as a verified local backup or silently discard the provider reference. A quarantined file SHALL keep its evidence blob in the source row with `locally_backed_up` false and SHALL keep the import partial.

#### Scenario: A digest mismatch does not become an available file

- **WHEN** a knowledge file supplies bytes whose SHA-256 differs from the provider declaration
- **THEN** the result marks that file quarantined, includes its safe anomaly reason, and reports no verified local backup for it

#### Scenario: A quarantined file keeps the import partial

- **WHEN** an archive is imported whose knowledge file has a digest mismatch, a missing declared digest or an invalid media type
- **THEN** the source row holds the evidence blob and is not locally backed up, the completeness report is `assets_partial` with the anomaly warning, and the operation is `partially_succeeded`

### Requirement: Knowledge blobs follow the registration protocol

The system SHALL register every stored knowledge-file blob under the digest lock before the first row that references it, and SHALL store the blob again from the bytes it holds when an erasure removed it since it was ingested, so that no committed source row references a missing blob.

#### Scenario: An eraser removed the blob before registration

- **WHEN** an eraser removes a knowledge blob after ingest stored it and before the import transaction registers it
- **THEN** the import stores the blob again from its staged bytes and no committed source row references a missing blob
