## ADDED Requirements

### Requirement: An incomplete import reports the contract warning

An import whose completeness is not `complete` SHALL be reported as `partially_succeeded` with exactly one warning, the contract incomplete-import warning, and no error.

#### Scenario: Partial import report is valid

- **WHEN** the terminal report is built for a non-complete import summary
- **THEN** its warnings equal the contract incomplete-import warning and the contract validation of the report passes
