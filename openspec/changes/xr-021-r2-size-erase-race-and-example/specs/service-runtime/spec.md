## ADDED Requirements

### Requirement: The shipped operator example loads

The repository SHALL ship `deploy/systemd/claude.conf.example` naming every key the receipt flow needs, with absolute secret paths and only the ports of the workspace table.

#### Scenario: The example is loaded through the configuration loader

- **WHEN** the example is loaded through `Config::from_environment` with temporary files for its absolute paths
- **THEN** the operator listener is `127.0.0.1:9086`, the receipt listener is `127.0.0.1:8097`, the Platform accounts mapping and the event bus URL and seed path are set
