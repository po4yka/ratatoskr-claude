# external-reference-backup-status Specification

## Purpose

Makes local preservation of upstream Claude entities explicit, evidence-based,
and auditable so a reference is never represented as an available backup.

## Requirements

### Requirement: Upstream references carry explicit local backup status

The system SHALL represent an observed upstream Claude project, conversation,
or Artifact as an external reference when local preservation evidence is absent
or incomplete. Every represented project, conversation, and Artifact SHALL
expose exactly one local backup status derived only from verified local archive
evidence for that entity; connectivity, credential validity, and upstream
reachability SHALL NOT contribute to the status.

#### Scenario: An observed conversation without local evidence stays reference-only

- **WHEN** a conversation is observed from a supported archive but no verified
  local preservation evidence exists for it
- **THEN** its external reference and entity status report it as not locally
  backed up

#### Scenario: Verified local evidence marks an Artifact backed up

- **WHEN** an Artifact has verified local content evidence for every required
  observed payload
- **THEN** its entity status reports it as locally backed up

### Requirement: Authorization state cannot mutate backup evidence

The system SHALL retain the latest status derived from verified local evidence
when an acquisition authorization expires, is revoked, or otherwise becomes
unavailable. It SHALL report authorization as separate acquisition state and
SHALL NOT convert an existing locally backed-up status to not backed up, or the
reverse, without a change in local evidence.

#### Scenario: Expired authorization preserves the latest successful backup result

- **WHEN** a project was previously derived as locally backed up and a later
  acquisition attempt reports expired authorization without new local evidence
- **THEN** the project remains locally backed up and the authorization outcome
  does not create a backup-status transition

### Requirement: Backup-status transitions are audit-visible

The system SHALL append one content-free audit record for each change in an
entity's derived local backup status. Each audit record SHALL identify the
entity kind and stable external identity, prior and new status, the local
evidence basis, and the observation time; unchanged re-observations SHALL NOT
create another transition record.

#### Scenario: New verified evidence creates one audited transition

- **WHEN** a reference-only Artifact gains verified local preservation evidence
- **THEN** its status becomes locally backed up and exactly one audit record
  reports the transition without including private content, titles, filenames,
  credentials, or URLs
