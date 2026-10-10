## ADDED Requirements

### Requirement: The operator listener default is loopback port 9086

The service SHALL bind the operator listener to `127.0.0.1:9086` unless configured otherwise.

#### Scenario: Default listener address

- **WHEN** the service loads configuration with no listener address set
- **THEN** the operator listener address is `127.0.0.1:9086`
