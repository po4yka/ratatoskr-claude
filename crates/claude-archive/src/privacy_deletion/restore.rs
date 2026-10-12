//! Puts the records a deletion keeps back to what the retained exports hold (XR-021 CONTRACTS.md
//! R4-03).
//!
//! A project or conversation is one shared row that carries the content of the export imported
//! last, and a conversation's content parts keep the first part written at each index. Deleting
//! the export that wrote some of that content must not leave the content behind in the row, the
//! part history, the snapshots or the fact announced for the survivor. Each export names the
//! digest of the version it presented, so a record is stale exactly when its row holds content no
//! retained export presented. It is then rebuilt from the versions the retained exports name,
//! oldest export first, so the row ends as the newest retained export left it. The part history
//! is written again with the importer's own rule, which makes the result equal to an import of
//! only the retained exports. A conversation the deleted exports also observed is rebuilt even
//! when its row is current, because the part history is append-only: an export that is not
//! cumulative leaves the row current and the history holding text only the deleted export wrote.

use ratatoskr_identifiers::WireTimestamp;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::PrivacyDeletionExecutionError;
use crate::Conversation;
use crate::entity_versions::{CONVERSATION, PROJECT, ProjectVersion};
use crate::reparse::persist_messages;

type Tx<'a> = Transaction<'a, Postgres>;

/// One version a retained export presented: the export, the digest and the stored snapshot.
type Held = (Uuid, String, serde_json::Value);

/// Restores every surviving project that keeps content no retained export presented and every
/// surviving conversation that a deleted export observed or that keeps such content, refreshes
/// the backup status of the projects of `accounts`, and drops the versions that no export names
/// any longer. It runs in the row-removal transaction, after the scope's rows and observations
/// are gone and before any survivor fact is built.
pub(super) async fn restore_retained_versions(
    tx: &mut Tx<'_>,
    request_id: Uuid,
    accounts: &[Uuid],
) -> Result<(), PrivacyDeletionExecutionError> {
    // A record the deletion removed has no use for its versions. The request's own inventory names
    // them, so no other tenant's version is read.
    sqlx::query(
        "delete from claude_archive.entity_versions v
         using claude_archive.privacy_deletion_items i
         where i.request_id = $1 and i.action = 'remove_record'
           and i.item_kind = v.entity_kind and i.subject_id = v.entity_id::text",
    )
    .bind(request_id)
    .execute(&mut **tx)
    .await?;
    for id in stale(tx, PROJECT, accounts).await? {
        let held = retained_versions(tx, PROJECT, id).await?;
        if let Some(newest) = held.last() {
            restore_project(tx, id, newest).await?;
        }
    }
    let mut conversations = stale(tx, CONVERSATION, accounts).await?;
    conversations.extend(survivors(tx, request_id).await?);
    conversations.sort_unstable();
    conversations.dedup();
    for id in conversations {
        let held = retained_versions(tx, CONVERSATION, id).await?;
        restore_conversation(tx, id, &held).await?;
    }
    sqlx::query(
        "delete from claude_archive.entity_versions v
         where (exists (select 1 from claude_archive.projects p
                        where v.entity_kind = 'project' and p.project_id = v.entity_id
                          and p.account_id = any($1))
                or exists (select 1 from claude_archive.conversations c
                           where v.entity_kind = 'conversation' and c.conversation_id = v.entity_id
                             and c.account_id = any($1)))
           and not exists (select 1 from claude_archive.export_observations o
                           where o.subject_kind = v.entity_kind and o.subject_id = v.entity_id
                             and o.content_digest = v.content_digest)",
    )
    .bind(accounts)
    .execute(&mut **tx)
    .await?;
    refresh_backup_status(tx, accounts).await?;
    Ok(())
}

/// The surviving records of `accounts` whose row holds content that no retained export presented,
/// among those a retained export still observes. A record no retained export observes keeps
/// whatever it holds: the scope removes it, or no export ever observed it. Only the accounts the
/// request owns (and locked) are read, so another tenant's row is never rewritten.
async fn stale(tx: &mut Tx<'_>, kind: &str, accounts: &[Uuid]) -> Result<Vec<Uuid>, sqlx::Error> {
    let (table, key) = if kind == PROJECT {
        ("projects", "project_id")
    } else {
        ("conversations", "conversation_id")
    };
    sqlx::query_scalar(&format!(
        "select r.{key} from claude_archive.{table} r
         where r.account_id = any($2) and r.content_digest is not null
           and exists (select 1 from claude_archive.export_observations o
                       where o.subject_kind = $1 and o.subject_id = r.{key})
           and not exists (select 1 from claude_archive.export_observations o
                           where o.subject_kind = $1 and o.subject_id = r.{key}
                             and o.content_digest = r.content_digest)
         order by r.{key}"
    ))
    .bind(kind)
    .bind(accounts)
    .fetch_all(&mut **tx)
    .await
}

/// The conversations the request keeps because a retained export also observed them.
async fn survivors(tx: &mut Tx<'_>, request_id: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "select c.conversation_id from claude_archive.conversations c
         join claude_archive.privacy_deletion_items i
           on i.subject_id = c.conversation_id::text and i.item_kind = 'conversation'
         where i.request_id = $1 and i.action = 'retain_evidenced'
         order by c.conversation_id",
    )
    .bind(request_id)
    .fetch_all(&mut **tx)
    .await
}

/// The versions retained exports presented of one record, oldest export first (by `received_at`,
/// then export id).
async fn retained_versions(
    tx: &mut Tx<'_>,
    kind: &str,
    id: Uuid,
) -> Result<Vec<Held>, sqlx::Error> {
    sqlx::query_as(
        "select o.export_id, o.content_digest, v.snapshot
         from claude_archive.export_observations o
         join claude_archive.exports e on e.export_id = o.export_id
         join claude_archive.entity_versions v
           on v.entity_kind = o.subject_kind and v.entity_id = o.subject_id
          and v.content_digest = o.content_digest
         where o.subject_kind = $1 and o.subject_id = $2
         order by e.received_at, e.export_id",
    )
    .bind(kind)
    .bind(id)
    .fetch_all(&mut **tx)
    .await
}

/// A project holds one state, so it takes the newest retained version.
async fn restore_project(
    tx: &mut Tx<'_>,
    id: Uuid,
    (_, digest, snapshot): &Held,
) -> Result<(), PrivacyDeletionExecutionError> {
    let version: ProjectVersion = serde_json::from_value(snapshot.clone())?;
    sqlx::query(
        "update claude_archive.projects
         set title = $2, description = $3, instructions = $4, parser_name = $5,
             parser_version = $6, content_digest = $7, updated_at = now()
         where project_id = $1",
    )
    .bind(id)
    .bind(&version.title)
    .bind(&version.description)
    .bind(&version.instructions)
    .bind(&version.parser_name)
    .bind(&version.parser_version)
    .bind(digest)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A conversation's content parts are a history, so they are written again from the retained
/// versions in the order the exports arrived, and the header ends as the newest one left it.
async fn restore_conversation(
    tx: &mut Tx<'_>,
    id: Uuid,
    held: &[Held],
) -> Result<(), PrivacyDeletionExecutionError> {
    let Some((_, digest, snapshot)) = held.last() else {
        // No retained export names a stored version, so there is nothing to rebuild from and
        // the row keeps what it holds.
        return Ok(());
    };
    sqlx::query(
        "delete from claude_archive.content_parts
         where message_id in (select message_id from claude_archive.messages
                              where conversation_id = $1)",
    )
    .bind(id)
    .execute(&mut **tx)
    .await?;
    let mut previous: Option<&str> = None;
    for (export_id, digest, snapshot) in held {
        if previous == Some(digest.as_str()) {
            continue;
        }
        previous = Some(digest.as_str());
        let conversation: Conversation = serde_json::from_value(snapshot.clone())?;
        persist_messages(tx, *export_id, id, &conversation).await?;
    }
    let newest: Conversation = serde_json::from_value(snapshot.clone())?;
    let created = newest
        .created_at
        .as_deref()
        .and_then(|value| WireTimestamp::parse(value).ok())
        .map(|instant| instant.to_string());
    sqlx::query(
        "update claude_archive.conversations c
         set title = $2, provider_created_at = $3::timestamptz, parser_name = $4,
             parser_version = $5, content_digest = $6, last_seen_at = now(),
             project_id = (select p.project_id from claude_archive.projects p
                           where p.account_id = c.account_id and p.external_project_id = $7)
         where c.conversation_id = $1",
    )
    .bind(id)
    .bind(&newest.title)
    .bind(created)
    .bind(&newest.parser.parser_identifier)
    .bind(&newest.parser.parser_version)
    .bind(digest)
    .bind(&newest.project_external_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A project is `locally_backed_up` exactly when it has sources and all are verified, so the
/// removal of a source can change it (XR-021 CONTRACTS.md R4-02 rule 5).
async fn refresh_backup_status(tx: &mut Tx<'_>, accounts: &[Uuid]) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update claude_archive.projects p set local_backup_status = case
             when exists (select 1 from claude_archive.project_sources s
                          where s.project_id = p.project_id)
              and not exists (select 1 from claude_archive.project_sources s
                              where s.project_id = p.project_id and not s.locally_backed_up)
             then 'locally_backed_up' else 'reference_only' end
         where p.account_id = any($1)",
    )
    .bind(accounts)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
