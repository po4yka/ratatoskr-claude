## ADDED Requirements

### Requirement: The receipt listener advertises its capability without claims

While the receipt route is mounted, the service SHALL answer `GET /v1/capabilities` with `200`, `application/json` and the document `{"service":"claude","capabilities":["ai_archive.receipt"]}`, requiring no header and no claim.

#### Scenario: Capability document without headers

- **WHEN** a client sends `GET /v1/capabilities` with no headers to the service router
- **THEN** the answer is `200` with a body equal to the contract capability document for `claude`

### Requirement: Every non-success answer of the receipt route is an error envelope

Every non-2xx answer of the receipt route, including a method the route does not accept, SHALL be `application/json` carrying an `ErrorEnvelope` with a non-empty code.

#### Scenario: Wrong method on the Platform receipt path

- **WHEN** a client sends `PUT /v1/ai-archives/receipt`
- **THEN** the answer is `405` with an `ErrorEnvelope` whose code is `claude.request.method_not_allowed`
