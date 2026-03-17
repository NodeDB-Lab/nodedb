//! Collection-level permission grants and ownership tracking.
//!
//! Grants: `GRANT READ ON collection TO user/role`
//! Ownership: every collection has an owner with implicit full permissions.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::types::TenantId;

use super::catalog::{StoredOwner, StoredPermission, SystemCatalog};
use super::identity::{AuthenticatedIdentity, Permission};
use super::role::RoleStore;

/// A permission grant record (in-memory).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Grant {
    /// Target: "cluster", "tenant:1", "collection:1:users"
    pub target: String,
    /// Grantee: role name or "user:username"
    pub grantee: String,
    /// Permission type.
    pub permission: Permission,
}

/// Ownership record (in-memory).
#[derive(Debug, Clone)]
pub struct OwnerRecord {
    pub object_type: String,
    pub object_name: String,
    pub tenant_id: TenantId,
    pub owner_username: String,
}

/// Permission store: grants + ownership with in-memory cache and redb persistence.
pub struct PermissionStore {
    grants: RwLock<HashSet<Grant>>,
    /// "collection:{tenant_id}:{name}" → owner username
    owners: RwLock<HashMap<String, String>>,
}

impl Default for PermissionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PermissionStore {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashSet::new()),
            owners: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored_perms = catalog.load_all_permissions()?;
        let mut grants = self.grants.write().map_err(|e| crate::Error::Internal {
            detail: format!("permission store lock poisoned: {e}"),
        })?;
        for sp in stored_perms {
            if let Some(perm) = parse_permission(&sp.permission) {
                grants.insert(Grant {
                    target: sp.target,
                    grantee: sp.grantee,
                    permission: perm,
                });
            }
        }

        let stored_owners = catalog.load_all_owners()?;
        let mut owners = self.owners.write().map_err(|e| crate::Error::Internal {
            detail: format!("owner store lock poisoned: {e}"),
        })?;
        for so in stored_owners {
            let key = owner_key(&so.object_type, so.tenant_id, &so.object_name);
            owners.insert(key, so.owner_username);
        }

        let gc = grants.len();
        let oc = owners.len();
        if gc > 0 || oc > 0 {
            tracing::info!(grants = gc, owners = oc, "loaded permissions from catalog");
        }
        Ok(())
    }

    // ── Grants ──────────────────────────────────────────────────────

    /// Grant a permission on a target to a grantee (role name or "user:username").
    pub fn grant(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        granted_by: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let grant = Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        };

        if let Some(catalog) = catalog {
            catalog.put_permission(&StoredPermission {
                target: target.to_string(),
                grantee: grantee.to_string(),
                permission: format_permission(permission),
                granted_by: granted_by.to_string(),
                granted_at: now_secs(),
            })?;
        }

        let mut grants = self.grants.write().map_err(|e| crate::Error::Internal {
            detail: format!("permission store lock poisoned: {e}"),
        })?;
        grants.insert(grant);
        Ok(())
    }

    /// Revoke a permission.
    pub fn revoke(
        &self,
        target: &str,
        grantee: &str,
        permission: Permission,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<bool> {
        let grant = Grant {
            target: target.to_string(),
            grantee: grantee.to_string(),
            permission,
        };

        if let Some(catalog) = catalog {
            catalog.delete_permission(target, grantee, &format_permission(permission))?;
        }

        let mut grants = self.grants.write().map_err(|e| crate::Error::Internal {
            detail: format!("permission store lock poisoned: {e}"),
        })?;
        Ok(grants.remove(&grant))
    }

    /// Check if an identity has a specific permission on a collection.
    ///
    /// Checks in order:
    /// 1. Superuser → always allowed
    /// 2. Ownership → owner has all permissions on their objects
    /// 3. Built-in role grants (from identity.rs role_grants_permission)
    /// 4. Explicit collection-level grants (on user or any of user's roles)
    /// 5. Custom role inheritance chain (via RoleStore)
    pub fn check(
        &self,
        identity: &AuthenticatedIdentity,
        permission: Permission,
        collection: &str,
        role_store: &RoleStore,
    ) -> bool {
        if identity.is_superuser {
            return true;
        }

        // Check ownership.
        let target = collection_target(identity.tenant_id, collection);
        if self.is_owner(&target, &identity.username) {
            return true;
        }

        // Check built-in roles.
        for role in &identity.roles {
            if super::identity::role_grants_permission(role, permission) {
                return true;
            }
        }

        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(_) => return false,
        };

        // Check explicit user grant.
        let user_grantee = format!("user:{}", identity.username);
        if grants.contains(&Grant {
            target: target.clone(),
            grantee: user_grantee,
            permission,
        }) {
            return true;
        }

        // Check grants on any of the user's roles (including inheritance chain).
        for role in &identity.roles {
            let chain = role_store.resolve_inheritance(role);
            for ancestor in &chain {
                let role_grantee = ancestor.to_string();
                if grants.contains(&Grant {
                    target: target.clone(),
                    grantee: role_grantee,
                    permission,
                }) {
                    return true;
                }
            }
        }

        false
    }

    /// List all grants for a grantee.
    pub fn grants_for(&self, grantee: &str) -> Vec<Grant> {
        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        grants
            .iter()
            .filter(|g| g.grantee == grantee)
            .cloned()
            .collect()
    }

    /// List all grants on a target.
    pub fn grants_on(&self, target: &str) -> Vec<Grant> {
        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        grants
            .iter()
            .filter(|g| g.target == target)
            .cloned()
            .collect()
    }

    // ── Ownership ───────────────────────────────────────────────────

    /// Set the owner of an object.
    pub fn set_owner(
        &self,
        object_type: &str,
        tenant_id: TenantId,
        object_name: &str,
        owner_username: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let key = owner_key(object_type, tenant_id.as_u32(), object_name);

        if let Some(catalog) = catalog {
            catalog.put_owner(&StoredOwner {
                object_type: object_type.to_string(),
                object_name: object_name.to_string(),
                tenant_id: tenant_id.as_u32(),
                owner_username: owner_username.to_string(),
            })?;
        }

        let mut owners = self.owners.write().map_err(|e| crate::Error::Internal {
            detail: format!("owner store lock poisoned: {e}"),
        })?;
        owners.insert(key, owner_username.to_string());
        Ok(())
    }

    /// Get the owner of an object.
    pub fn get_owner(
        &self,
        object_type: &str,
        tenant_id: TenantId,
        object_name: &str,
    ) -> Option<String> {
        let key = owner_key(object_type, tenant_id.as_u32(), object_name);
        let owners = self.owners.read().ok()?;
        owners.get(&key).cloned()
    }

    fn is_owner(&self, target: &str, username: &str) -> bool {
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(_) => return false,
        };
        owners.get(target).is_some_and(|o| o == username)
    }
}

fn collection_target(tenant_id: TenantId, collection: &str) -> String {
    format!("collection:{}:{}", tenant_id.as_u32(), collection)
}

fn owner_key(object_type: &str, tenant_id: u32, object_name: &str) -> String {
    SystemCatalog::owner_key(object_type, tenant_id, object_name)
}

/// Parse a permission name (case-insensitive). Also accepts SQL aliases.
pub fn parse_permission(s: &str) -> Option<Permission> {
    match s.to_ascii_lowercase().as_str() {
        "read" | "select" => Some(Permission::Read),
        "write" | "insert" | "update" | "delete" => Some(Permission::Write),
        "create" => Some(Permission::Create),
        "drop" => Some(Permission::Drop),
        "alter" => Some(Permission::Alter),
        "admin" => Some(Permission::Admin),
        "monitor" => Some(Permission::Monitor),
        _ => None,
    }
}

fn format_permission(p: Permission) -> String {
    match p {
        Permission::Read => "read".into(),
        Permission::Write => "write".into(),
        Permission::Create => "create".into(),
        Permission::Drop => "drop".into(),
        Permission::Alter => "alter".into(),
        Permission::Admin => "admin".into(),
        Permission::Monitor => "monitor".into(),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{AuthMethod, Role};

    fn identity(username: &str, roles: Vec<Role>, superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 1,
            username: username.into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::Trust,
            roles,
            is_superuser: superuser,
        }
    }

    #[test]
    fn superuser_always_allowed() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        let id = identity("admin", vec![], true);
        assert!(store.check(&id, Permission::Write, "secret", &roles));
    }

    #[test]
    fn owner_has_all_permissions() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        store
            .set_owner("collection", TenantId::new(1), "users", "alice", None)
            .unwrap();

        let id = identity("alice", vec![], false);
        assert!(store.check(&id, Permission::Read, "users", &roles));
        assert!(store.check(&id, Permission::Write, "users", &roles));
        assert!(store.check(&id, Permission::Drop, "users", &roles));
    }

    #[test]
    fn non_owner_denied_without_grant() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        store
            .set_owner("collection", TenantId::new(1), "users", "alice", None)
            .unwrap();

        let id = identity("bob", vec![], false);
        assert!(!store.check(&id, Permission::Write, "users", &roles));
    }

    #[test]
    fn explicit_user_grant() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        let target = collection_target(TenantId::new(1), "orders");
        store
            .grant(&target, "user:bob", Permission::Read, "admin", None)
            .unwrap();

        let id = identity("bob", vec![], false);
        assert!(store.check(&id, Permission::Read, "orders", &roles));
        assert!(!store.check(&id, Permission::Write, "orders", &roles));
    }

    #[test]
    fn grant_on_role() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        let target = collection_target(TenantId::new(1), "reports");
        store
            .grant(&target, "readonly", Permission::Read, "admin", None)
            .unwrap();

        let id = identity("viewer", vec![Role::Custom("readonly".into())], false);
        // Custom("readonly") doesn't match the built-in Role::ReadOnly for
        // role_grants_permission, but the explicit grant on "readonly" string matches.
        assert!(store.check(&id, Permission::Read, "reports", &roles));
    }

    #[test]
    fn inherited_role_grant() {
        let role_store = RoleStore::new();
        role_store
            .create_role("analyst", TenantId::new(1), Some("readonly"), None)
            .unwrap();

        let perm_store = PermissionStore::new();
        let target = collection_target(TenantId::new(1), "data");
        // Grant to the parent "readonly" role.
        perm_store
            .grant(&target, "readonly", Permission::Read, "admin", None)
            .unwrap();

        // User with "analyst" role should inherit the grant via "readonly" parent.
        let id = identity("alice", vec![Role::Custom("analyst".into())], false);
        assert!(perm_store.check(&id, Permission::Read, "data", &role_store));
    }

    #[test]
    fn revoke_removes_grant() {
        let store = PermissionStore::new();
        let target = collection_target(TenantId::new(1), "users");
        store
            .grant(&target, "user:bob", Permission::Read, "admin", None)
            .unwrap();
        assert!(
            store
                .revoke(&target, "user:bob", Permission::Read, None)
                .unwrap()
        );

        let roles = RoleStore::new();
        let id = identity("bob", vec![], false);
        assert!(!store.check(&id, Permission::Read, "users", &roles));
    }

    #[test]
    fn builtin_role_still_works() {
        let store = PermissionStore::new();
        let roles = RoleStore::new();
        let id = identity("writer", vec![Role::ReadWrite], false);
        // ReadWrite built-in grants Read + Write on any collection.
        assert!(store.check(&id, Permission::Read, "anything", &roles));
        assert!(store.check(&id, Permission::Write, "anything", &roles));
        assert!(!store.check(&id, Permission::Drop, "anything", &roles));
    }
}
