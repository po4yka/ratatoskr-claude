//! Reverse lookup from an archive account to the Platform user that owns it.
//!
//! The Platform user id exists only in the configured `platform_accounts` mapping (Platform user,
//! archive account); the archive database never stores it. Every `ai_archive.*` fact and every
//! privacy-deletion tombstone is owned by that user as `user:<uuid>`, which is the tenant
//! Knowledge indexes under (XR-021 CONTRACTS.md S07).

use std::collections::HashMap;

use ratatoskr_identifiers::{TenantRef, UserId};
use uuid::Uuid;

/// The configured mapping, turned around so an archive account resolves to its Platform user.
#[derive(Debug, Clone, Default)]
pub struct PlatformUsers {
    users_by_account: HashMap<Uuid, Uuid>,
}

impl PlatformUsers {
    /// Builds the lookup from `(Platform user, archive account)` pairs, the shape of
    /// `RATATOSKR__RECEIPT__PLATFORM_ACCOUNTS`.
    #[must_use]
    pub fn new(platform_accounts: &[(Uuid, Uuid)]) -> Self {
        Self {
            users_by_account: platform_accounts
                .iter()
                .map(|(user, account)| (*account, *user))
                .collect(),
        }
    }

    /// The owner of everything stored for `account`, when the account is mapped.
    #[must_use]
    pub fn owner_of(&self, account: Uuid) -> Option<TenantRef> {
        self.users_by_account
            .get(&account)
            .map(|user| TenantRef::of_user(UserId(*user)))
    }

    /// The wire spelling of [`Self::owner_of`], used as the tenant reference of a deletion request.
    #[must_use]
    pub fn tenant_ref_of(&self, account: Uuid) -> Option<String> {
        self.owner_of(account).map(|owner| owner.to_string())
    }
}
