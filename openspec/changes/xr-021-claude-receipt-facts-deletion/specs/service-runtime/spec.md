## ADDED Requirements

### Requirement: The operator listener default is loopback port 9086

The service SHALL bind the operator listener to `127.0.0.1:9086` unless configured otherwise.

#### Scenario: Default listener address

- **WHEN** the service loads configuration with no listener address set
- **THEN** the operator listener address is `127.0.0.1:9086`

### Requirement: The Platform receipt is served on its own loopback listener

The service SHALL bind the Platform receipt listener to `127.0.0.1:8097` unless configured otherwise, serve the receipt and capability routes only there, serve the operator routes only on the operator listener, and refuse a configuration that gives both the same address or a non-loopback receipt address.

#### Scenario: Separate listeners

- **WHEN** the service runs with its default listeners
- **THEN** the capability probe answers on port 8097 and the operator health routes answer on port 9086, and each answers `404` for the other's routes

#### Scenario: Shared address is refused

- **WHEN** configuration sets the operator listener to `127.0.0.1:8097`
- **THEN** loading fails naming `RATATOSKR__RECEIPT__LISTEN_ADDRESS`
