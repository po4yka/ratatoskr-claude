## ADDED Requirements

### Requirement: The PostgreSQL role of the service is provisioned by a checked-in file

The repository SHALL ship `deploy/postgres/role.sql` creating the role named in the shipped example on the database and port the example names.

#### Scenario: The example and the role file agree

- **WHEN** the role in the example's database URL is compared with the role the file creates
- **THEN** they are equal, the database is `ratatoskr` and the port is 5432
