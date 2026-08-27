## MODIFIED Requirements

### Requirement: Receipt requires an authenticated tenant scope resolved before any byte is stored

Receipt SHALL require a principal naming exactly one scope - an account or an organization known to
this archive - and SHALL refuse a principal that resolves to no tenant, claims both scopes, or
claims a scope inconsistent with its acquisition mode, before reading or storing any archive bytes.
A Platform-bound receipt SHALL accept scope and operation claims only from its configured loopback
transport, SHALL refuse direct bearer credentials, and SHALL verify that the declared SHA-256 and
byte size equal the stored raw bytes before it reports acceptance.

#### Scenario: Principal resolving to no tenant is refused before storage

- **WHEN** a receipt attempt presents a principal whose identifier matches no account and no organization
- **THEN** the receipt fails with a refused error, and neither an export record nor stored bytes exist for the attempt

#### Scenario: A principal claiming both scopes is refused

- **WHEN** a receipt attempt presents one principal claiming an account and an organization at the same time
- **THEN** the receipt fails with a refused error, and nothing about the upload is recorded

#### Scenario: Direct Platform receipt credentials are refused

- **WHEN** a direct caller presents a bearer credential to the Platform receipt route
- **THEN** the route refuses the request and no archive bytes or operation report are stored

#### Scenario: Declared identity differs from delivered bytes

- **WHEN** a Platform-bound receipt declares a hash or byte size that differs from the delivered archive
- **THEN** the route refuses the request, leaves no stored export or terminal operation report, and does not claim an import result
