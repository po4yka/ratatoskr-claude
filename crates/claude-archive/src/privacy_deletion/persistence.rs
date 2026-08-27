//! Durable privacy-deletion inventory persistence.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::DeletionInventory;

#[expect(
    clippy::too_many_arguments,
    reason = "the persisted scope stays explicit"
)]
pub(super) async fn persist_scoped_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_ref: &str,
    request_id: Uuid,
    request_key: &str,
    correlation_id: &str,
    scope_kind: &str,
    scope_id: Option<Uuid>,
    inventory: &DeletionInventory,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into claude_archive.privacy_deletion_requests
         (request_id, tenant_ref, request_key, scope_kind, scope_id, state, correlation_id)
         values ($1, $2, $3, $4, $5, 'planned', $6)",
    )
    .bind(request_id)
    .bind(tenant_ref)
    .bind(request_key)
    .bind(scope_kind)
    .bind(scope_id)
    .bind(correlation_id)
    .execute(&mut **transaction)
    .await?;

    for (index, item) in inventory.items.iter().enumerate() {
        sqlx::query(
            "insert into claude_archive.privacy_deletion_items
             (request_id, item_index, item_kind, subject_id, action, blob_ref)
             values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(request_id)
        .bind(i32::try_from(index).unwrap_or(i32::MAX))
        .bind(item.kind.as_str())
        .bind(&item.subject_id)
        .bind(item.action.as_str())
        .bind(&item.blob_ref)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}
