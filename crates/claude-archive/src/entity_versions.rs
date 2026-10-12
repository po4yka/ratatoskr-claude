//! The versions of the records that cumulative exports share (XR-021 CONTRACTS.md R4-03).
//!
//! A project or conversation is one row per account however many exports contain it, and the row
//! holds the content of the export imported last. Each export that observes the record names the
//! digest of the version it presented, and `entity_versions` keeps one snapshot per distinct
//! digest while some export names it. A privacy deletion that removes the export which wrote the
//! content of the row restores the record from the snapshots the retained exports still name, so
//! no content that only a deleted export held survives (see `privacy_deletion::restore`).

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::reparse::encode_hex;
use crate::{ContentPart, Conversation};

type Tx<'a> = Transaction<'a, Postgres>;

/// The observation kinds that carry a version.
pub(crate) const PROJECT: &str = "project";
pub(crate) const CONVERSATION: &str = "conversation";

/// What one export presented of a project: the content of the project row and the parser stamp
/// the row carries with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectVersion {
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) instructions: Option<String>,
    pub(crate) parser_name: String,
    pub(crate) parser_version: String,
}

impl ProjectVersion {
    /// The digest of the content, which excludes the parser stamp: a newer parser release that
    /// reads identical content presents the same version.
    pub(crate) fn digest(&self) -> String {
        let content = serde_json::json!([self.title, self.description, self.instructions]);
        encode_hex(&Sha256::digest(content.to_string().as_bytes()))
    }
}

/// The digest of the version of a conversation an export presented: its title, project, creation
/// time and the ordered messages with their parts. The parser stamps and the inert unknown fields
/// are not content, so a newer parser that reads the same conversation presents the same version.
pub(crate) fn conversation_digest(conversation: &Conversation) -> String {
    let messages: Vec<serde_json::Value> = conversation
        .messages
        .iter()
        .map(|message| {
            let parts: Vec<serde_json::Value> = message
                .content
                .iter()
                .map(|part| match part {
                    ContentPart::Text { text, .. } => serde_json::json!(["text", text]),
                    ContentPart::Markdown { markdown, .. } => {
                        serde_json::json!(["markdown", markdown])
                    }
                    ContentPart::Unknown { raw, .. } => serde_json::json!(["unknown", raw]),
                })
                .collect();
            serde_json::json!([
                message.external_id,
                message.parent_external_id,
                message.role,
                message.model,
                message.created_at,
                parts
            ])
        })
        .collect();
    let content = serde_json::json!([
        conversation.title,
        conversation.project_external_id,
        conversation.created_at,
        messages
    ]);
    encode_hex(&Sha256::digest(content.to_string().as_bytes()))
}

/// Records that `export_id` observed the record and which version of it the export presented,
/// keeps the snapshot of that version, and drops the versions no export names any longer.
pub(crate) async fn present(
    tx: &mut Tx<'_>,
    export_id: Uuid,
    kind: &str,
    entity_id: Uuid,
    digest: &str,
    snapshot: serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into claude_archive.export_observations
         (export_id, subject_kind, subject_id, content_digest) values ($1, $2, $3, $4)
         on conflict (export_id, subject_kind, subject_id)
         do update set content_digest = excluded.content_digest",
    )
    .bind(export_id)
    .bind(kind)
    .bind(entity_id)
    .bind(digest)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "insert into claude_archive.entity_versions
         (entity_kind, entity_id, content_digest, snapshot) values ($1, $2, $3, $4)
         on conflict do nothing",
    )
    .bind(kind)
    .bind(entity_id)
    .bind(digest)
    .bind(snapshot)
    .execute(&mut **tx)
    .await?;
    prune(tx, kind, entity_id).await
}

/// Drops the versions of one record that no export names. The export no longer presents its
/// earlier version, and a version nobody presents is not evidence of anything.
pub(crate) async fn prune(tx: &mut Tx<'_>, kind: &str, entity_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "delete from claude_archive.entity_versions v
         where v.entity_kind = $1 and v.entity_id = $2
           and not exists (select 1 from claude_archive.export_observations o
                           where o.subject_kind = v.entity_kind and o.subject_id = v.entity_id
                             and o.content_digest = v.content_digest)",
    )
    .bind(kind)
    .bind(entity_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
