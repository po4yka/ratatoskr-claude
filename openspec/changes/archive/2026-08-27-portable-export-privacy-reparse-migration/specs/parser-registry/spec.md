## ADDED Requirements

### Requirement: Registry resolves exact parser identities for operator workflows

The registry SHALL support exact lookup by parser name and declared parser version and SHALL expose compatible registered identities in deterministic version order for a supplied acquisition mode and inspected archive. Exact lookup SHALL refuse an identity whose declared signals do not match inspected evidence.

#### Scenario: Exact compatible parser resolves once

- **WHEN** reparse requests a registered parser identity compatible with the archive's acquisition mode and signals
- **THEN** the registry returns exactly that parser identity and its executable parser without applying automatic selection

#### Scenario: Compatible versions have deterministic order

- **WHEN** compatible parser registrations were inserted in different orders
- **THEN** version discovery returns the same unique identities in declared version order

### Requirement: Automatic intake remains ambiguity-safe

Adding exact lookup and version discovery SHALL NOT cause ordinary intake to silently pick a parser when multiple declarations match. Automatic selection SHALL continue to return every ambiguous identity and perform no parse.

#### Scenario: Two compatible versions remain ambiguous at intake

- **WHEN** ordinary intake sees two matching versions and no exact operator target
- **THEN** selection returns an ambiguity containing both identities and neither parser executes
