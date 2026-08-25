# Service runtime

## Purpose

Defines how the Ratatoskr Claude Archive process boots: finite typed configuration loaded from the environment, structured telemetry honoring the configured level, and health endpoints reporting liveness and readiness of owned components.

## Requirements

### Requirement: Configuration is finite, typed, and validated before serving

The service SHALL derive its entire runtime configuration from a typed structure populated from the environment with documented defaults for optional values, SHALL refuse to serve when a required value is missing or a value is invalid, and SHALL name the offending field in the failure.

#### Scenario: Minimal valid environment produces working configuration

- **WHEN** the service starts with only the required values set
- **THEN** configuration loads with defaults applied for every optional field

#### Scenario: Missing required value refuses startup naming the field

- **WHEN** the service starts without a required value such as the blob store root
- **THEN** startup fails with an error identifying that field and no listener is bound

#### Scenario: Malformed value refuses startup naming the field

- **WHEN** a value fails validation, such as an unparsable bind address or an unknown log level
- **THEN** startup fails with an error identifying that field and the reason

### Requirement: Telemetry honors the configured level

The service SHALL initialize structured telemetry from validated configuration so that events below the configured level are not emitted.

#### Scenario: Configured level gates event emission

- **WHEN** telemetry is initialized at a given level
- **THEN** an event at that level or above is emitted and an event below it is not

### Requirement: Liveness endpoint reports process health

The service SHALL expose a liveness endpoint returning success while the process is running.

#### Scenario: Liveness returns success when the process serves

- **WHEN** the running service receives a request to its liveness path
- **THEN** it responds 200 with a JSON body declaring ok status

### Requirement: Readiness reports availability of owned components

The service SHALL expose a readiness endpoint that succeeds only when every component marked required by configuration is available, and reports which requirement failed otherwise.

#### Scenario: Unready required component yields not-ready readiness

- **WHEN** a required component such as the configured database is unavailable
- **THEN** the readiness path responds 503 and its body identifies the failing component

#### Scenario: All components available yields ready

- **WHEN** every required component check passes
- **THEN** the readiness path responds 200 with ok status

### Requirement: Failures never expose archive content

Error responses and log output produced by the service SHALL carry failure class, field names, identifiers, and counts only, and SHALL NOT include stored bytes, message bodies, titles, filenames, or raw provider payloads.

#### Scenario: Error text contains no payload content

- **WHEN** any operation fails on input whose bytes were handed to the service
- **THEN** neither the error response nor its logged form contains any substring of those bytes
