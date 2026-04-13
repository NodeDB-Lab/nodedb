//! Applier-side helpers for replicated permissions and ownership.
//!
//! All `install_replicated_*` methods mutate only the in-memory
//! caches — the catalog redb write happens in
//! `catalog_entry::apply::{permission, owner}`. The handler's
//! single-node fallback path writes both, matching the pattern
//! used by every other replicated DDL family.
//!
//! `prepare_*` helpers build the catalog-shape record on the
//! proposer side without mutating anything, so the proposer and
//! every follower's applier converge on identical state.

use crate::control::security::catalog::{StoredOwner, StoredPermission};
use crate::control::security::identity::Permission;
use crate::control::security::time::now_secs;
use crate::types::TenantId;

use super::store::PermissionStore;
use super::types::{Grant, format_permission, owner_key, parse_permission};

/// Build a `StoredOwner` ready for proposing as
/// `CatalogEntry::PutOwner`. Pure construction, no state mutated.
pub fn prepare_owner(
    object_type: &str,
    tenant_id: TenantId,
    object_name: &str,
    owner_username: &str,
) -> StoredOwner {
    StoredOwner {
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
        tenant_id: tenant_id.as_u32(),
        owner_username: owner_username.to_string(),
    }
}

impl PermissionStore {
    /// Build a `StoredPermission` ready for replication via
    /// `CatalogEntry::PutPermission`. No state is mutated.
    pub fn prepare_permission(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        granted_by: &str,
    ) -> StoredPermission {
        StoredPermission {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission: format_permission(permission),
            granted_by: granted_by.to_string(),
            granted_at: now_secs(),
        }
    }

    /// Whether a grant already exists (proposer-side pre-check).
    pub fn permission_exists(&self, target: &str, grantee: &str, permission: Permission) -> bool {
        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        grants.contains(&Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        })
    }

    /// Install (insert-or-keep) a replicated permission grant into
    /// the in-memory set on every node.
    pub fn install_replicated_permission(&self, stored: &StoredPermission) {
        let Some(perm) = parse_permission(&stored.permission) else {
            tracing::warn!(
                permission = %stored.permission,
                "install_replicated_permission: unknown permission name — skipping"
            );
            return;
        };
        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        grants.insert(Grant {
            target: stored.target.clone(),
            grantee: stored.grantee.clone(),
            permission: perm,
        });
    }

    /// Remove a replicated permission grant from the in-memory set
    /// on every node. Returns `true` if a grant was removed.
    pub fn install_replicated_revoke(&self, target: &str, grantee: &str, permission: &str) -> bool {
        let Some(perm) = parse_permission(permission) else {
            tracing::warn!(
                permission,
                "install_replicated_revoke: unknown permission name — skipping"
            );
            return false;
        };
        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        grants.remove(&Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission: perm,
        })
    }

    /// Whether an owner record already exists (proposer-side
    /// pre-check). `tenant_id` is the raw `u32` value.
    pub fn owner_exists(&self, object_type: &str, tenant_id: u32, object_name: &str) -> bool {
        let key = owner_key(object_type, tenant_id, object_name);
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(p) => p.into_inner(),
        };
        owners.contains_key(&key)
    }

    /// Install (insert-or-replace) a replicated owner record into
    /// the in-memory map on every node.
    pub fn install_replicated_owner(&self, stored: &StoredOwner) {
        let key = owner_key(&stored.object_type, stored.tenant_id, &stored.object_name);
        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => p.into_inner(),
        };
        owners.insert(key, stored.owner_username.clone());
    }

    /// Remove a replicated owner record from the in-memory map on
    /// every node. Returns `true` if a record was removed.
    pub fn install_replicated_remove_owner(
        &self,
        object_type: &str,
        tenant_id: u32,
        object_name: &str,
    ) -> bool {
        let key = owner_key(object_type, tenant_id, object_name);
        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => p.into_inner(),
        };
        owners.remove(&key).is_some()
    }
}
