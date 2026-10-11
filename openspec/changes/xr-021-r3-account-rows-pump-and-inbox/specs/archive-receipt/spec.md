## ADDED Requirements

### Requirement: The first receipt from a mapped Platform user creates the archive account

The Platform receipt path SHALL create the mapped archive account in the receipt transaction when no row exists, without changing an existing row, and SHALL answer an unmapped Platform user with `403 claude.account.unmapped` (not retryable) instead of a 401. A receipt authenticated by a tenant token SHALL still require an existing account or organization.

#### Scenario: No account row exists yet

- **WHEN** a mapped Platform user's receipt arrives and no `claude_archive.accounts` row exists
- **THEN** the receipt is accepted and the row exists with `external_account_id` equal to `platform-account:` followed by the account id, and a second receipt changes nothing

#### Scenario: The Platform user is not mapped

- **WHEN** a receipt carries claims for a Platform user that is not in the configured mapping
- **THEN** the answer is 403 with code `claude.account.unmapped` and `retryable` false

#### Scenario: A tenant token names no existing account

- **WHEN** a tenant-token receipt names an account with no row
- **THEN** the receipt is refused as an unknown tenant and no row is created
