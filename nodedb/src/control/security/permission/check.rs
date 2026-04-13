//! Permission evaluation: `check`, `check_function`, `is_owner`.
//!
//! Multi-layer order: superuser → owner → built-in role → explicit
//! user grant → role grants (with custom-role inheritance).

use crate::control::security::identity::{self, AuthenticatedIdentity, Permission};
use crate::control::security::role::RoleStore;

use super::store::PermissionStore;
use super::types::{Grant, collection_target, function_target};

impl PermissionStore {
    /// Check if an identity has a specific permission on a collection.
    ///
    /// Checks in order:
    /// 1. Superuser → always allowed
    /// 2. Ownership → owner has all permissions on their objects
    /// 3. Built-in role grants (from identity.rs role_grants_permission)
    /// 4. Explicit collection-level grants (on user or any of user's roles)
    /// 5. Custom role inheritance chain (via `RoleStore`)
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

        let target = collection_target(identity.tenant_id, collection);
        if self.is_owner(&target, &identity.username) {
            return true;
        }

        for role in &identity.roles {
            if identity::role_grants_permission(role, permission) {
                return true;
            }
        }

        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };

        let user_grantee = format!("user:{}", identity.username);
        if grants.contains(&Grant {
            target: target.clone(),
            grantee: user_grantee,
            permission,
        }) {
            return true;
        }

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

    /// Check if an identity has EXECUTE permission on a function.
    ///
    /// Same multi-layer check as [`Self::check`] but uses
    /// `function:tenant:name` targets. Function owners implicitly
    /// have EXECUTE.
    pub fn check_function(
        &self,
        identity: &AuthenticatedIdentity,
        function_name: &str,
        role_store: &RoleStore,
    ) -> bool {
        if identity.is_superuser {
            return true;
        }

        let target = function_target(identity.tenant_id, function_name);

        if self.is_owner(&target, &identity.username) {
            return true;
        }

        for role in &identity.roles {
            if identity::role_grants_permission(role, Permission::Execute) {
                return true;
            }
        }

        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };

        let user_grantee = format!("user:{}", identity.username);
        if grants.contains(&Grant {
            target: target.clone(),
            grantee: user_grantee,
            permission: Permission::Execute,
        }) {
            return true;
        }

        for role in &identity.roles {
            let chain = role_store.resolve_inheritance(role);
            for ancestor in &chain {
                if grants.contains(&Grant {
                    target: target.clone(),
                    grantee: ancestor.to_string(),
                    permission: Permission::Execute,
                }) {
                    return true;
                }
            }
        }

        false
    }

    /// Lookup helper: is `username` recorded as the owner of `target`?
    pub(super) fn is_owner(&self, target: &str, username: &str) -> bool {
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned — recovering data");
                p.into_inner()
            }
        };
        owners.get(target).is_some_and(|o| o == username)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{AuthMethod, Role};
    use crate::types::TenantId;

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
        perm_store
            .grant(&target, "readonly", Permission::Read, "admin", None)
            .unwrap();

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
        assert!(store.check(&id, Permission::Read, "anything", &roles));
        assert!(store.check(&id, Permission::Write, "anything", &roles));
        assert!(!store.check(&id, Permission::Drop, "anything", &roles));
    }
}
