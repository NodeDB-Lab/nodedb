//! Custom role management with inheritance.
//!
//! Built-in roles (Superuser, TenantAdmin, ReadWrite, ReadOnly, Monitor) are
//! defined in `identity.rs`. This module manages user-defined custom roles
//! with optional single-parent inheritance.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::TenantId;

use super::catalog::{StoredRole, SystemCatalog};
use super::identity::Role;

/// In-memory custom role record.
#[derive(Debug, Clone)]
pub struct CustomRole {
    pub name: String,
    pub tenant_id: TenantId,
    /// Parent role for inheritance. None = standalone.
    pub parent: Option<String>,
    pub created_at: u64,
}

/// Custom role store with in-memory cache and redb persistence.
pub struct RoleStore {
    roles: RwLock<HashMap<String, CustomRole>>,
}

impl Default for RoleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RoleStore {
    pub fn new() -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored = catalog.load_all_roles()?;
        let mut roles = self.roles.write().map_err(|e| crate::Error::Internal {
            detail: format!("role store lock poisoned: {e}"),
        })?;
        for s in stored {
            let role = CustomRole {
                name: s.name.clone(),
                tenant_id: TenantId::new(s.tenant_id),
                parent: if s.parent.is_empty() {
                    None
                } else {
                    Some(s.parent)
                },
                created_at: s.created_at,
            };
            roles.insert(role.name.clone(), role);
        }
        if !roles.is_empty() {
            tracing::info!(count = roles.len(), "loaded custom roles from catalog");
        }
        Ok(())
    }

    // ── Cluster replication hooks ──────────────────────────────
    //
    // Symmetric partners to `CredentialStore::install_replicated_user`:
    // the production metadata applier calls these on every node
    // after writing / removing a `StoredRole` via
    // `CatalogEntry::PutRole` / `DeleteRole`.

    /// Install a replicated `StoredRole` into the in-memory cache.
    /// Never touches the catalog — the applier handles redb
    /// separately via `SystemCatalog::put_role`. Upsert semantics.
    pub fn install_replicated_role(&self, stored: &StoredRole) {
        let custom = CustomRole {
            name: stored.name.clone(),
            tenant_id: TenantId::new(stored.tenant_id),
            parent: if stored.parent.is_empty() {
                None
            } else {
                Some(stored.parent.clone())
            },
            created_at: stored.created_at,
        };
        let mut roles = self.roles.write().unwrap_or_else(|p| p.into_inner());
        roles.insert(stored.name.clone(), custom);
    }

    /// Remove a replicated role from the in-memory cache.
    pub fn install_replicated_drop_role(&self, name: &str) {
        let mut roles = self.roles.write().unwrap_or_else(|p| p.into_inner());
        roles.remove(name);
    }

    /// Build a `StoredRole` ready for replication via
    /// `CatalogEntry::PutRole`, without writing to redb or the
    /// in-memory cache. Performs the same validation
    /// `create_role` does (built-in name rejection, duplicate
    /// check, parent existence).
    pub fn prepare_role(
        &self,
        name: &str,
        tenant_id: TenantId,
        parent: Option<&str>,
    ) -> crate::Result<StoredRole> {
        if is_builtin(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("'{name}' is a built-in role and cannot be created"),
            });
        }
        let roles = self.roles.read().map_err(|e| crate::Error::Internal {
            detail: format!("role store lock poisoned: {e}"),
        })?;
        if roles.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("role '{name}' already exists"),
            });
        }
        if let Some(parent_name) = parent
            && !is_builtin(parent_name)
            && !roles.contains_key(parent_name)
        {
            return Err(crate::Error::BadRequest {
                detail: format!("parent role '{parent_name}' does not exist"),
            });
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Ok(StoredRole {
            name: name.to_string(),
            tenant_id: tenant_id.as_u32(),
            parent: parent.unwrap_or("").to_string(),
            created_at: now,
        })
    }

    /// Create a custom role. Returns error if it already exists or would create a cycle.
    pub fn create_role(
        &self,
        name: &str,
        tenant_id: TenantId,
        parent: Option<&str>,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        // Reject built-in role names.
        if is_builtin(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("'{name}' is a built-in role and cannot be created"),
            });
        }

        let mut roles = self.roles.write().map_err(|e| crate::Error::Internal {
            detail: format!("role store lock poisoned: {e}"),
        })?;

        if roles.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("role '{name}' already exists"),
            });
        }

        // Validate parent exists (built-in or custom).
        if let Some(parent_name) = parent
            && !is_builtin(parent_name)
            && !roles.contains_key(parent_name)
        {
            return Err(crate::Error::BadRequest {
                detail: format!("parent role '{parent_name}' does not exist"),
            });
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let role = CustomRole {
            name: name.to_string(),
            tenant_id,
            parent: parent.map(|s| s.to_string()),
            created_at: now,
        };

        if let Some(catalog) = catalog {
            catalog.put_role(&StoredRole {
                name: name.to_string(),
                tenant_id: tenant_id.as_u32(),
                parent: parent.unwrap_or("").to_string(),
                created_at: now,
            })?;
        }

        roles.insert(name.to_string(), role);
        Ok(())
    }

    /// Drop a custom role.
    pub fn drop_role(&self, name: &str, catalog: Option<&SystemCatalog>) -> crate::Result<bool> {
        if is_builtin(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("cannot drop built-in role '{name}'"),
            });
        }

        let mut roles = self.roles.write().map_err(|e| crate::Error::Internal {
            detail: format!("role store lock poisoned: {e}"),
        })?;

        // Check no other role inherits from this one.
        let has_children = roles.values().any(|r| r.parent.as_deref() == Some(name));
        if has_children {
            return Err(crate::Error::BadRequest {
                detail: format!("cannot drop role '{name}': other roles inherit from it"),
            });
        }

        if roles.remove(name).is_some() {
            if let Some(catalog) = catalog {
                catalog.delete_role(name)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Resolve the full permission chain for a role, following inheritance.
    /// Returns a list of role names from the given role up through its ancestors.
    pub fn resolve_inheritance(&self, role: &Role) -> Vec<Role> {
        let mut chain = vec![role.clone()];

        if let Role::Custom(name) = role {
            let roles = match self.roles.read() {
                Ok(r) => r,
                Err(_) => return chain,
            };

            let mut current = name.as_str();
            let mut visited = std::collections::HashSet::new();
            visited.insert(current.to_string());

            while let Some(custom) = roles.get(current) {
                if let Some(ref parent_name) = custom.parent {
                    if !visited.insert(parent_name.clone()) {
                        break; // Cycle detected — stop.
                    }
                    let parent_role: Role = match parent_name.parse() {
                        Ok(r) => r,
                        Err(e) => match e {},
                    };
                    chain.push(parent_role);
                    if is_builtin(parent_name) {
                        break; // Built-in roles don't have parents.
                    }
                    current = parent_name;
                } else {
                    break;
                }
            }
        }

        chain
    }

    /// Look up a custom role by name. Returns None if not found.
    pub fn get_role(&self, name: &str) -> Option<CustomRole> {
        let roles = self.roles.read().ok()?;
        roles.get(name).cloned()
    }

    /// List all custom roles.
    pub fn list_roles(&self) -> Vec<CustomRole> {
        let roles = match self.roles.read() {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        roles.values().cloned().collect()
    }
}

fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        "superuser" | "tenant_admin" | "readwrite" | "readonly" | "monitor"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_custom_role() {
        let store = RoleStore::new();
        store
            .create_role("analyst", TenantId::new(1), None, None)
            .unwrap();
        assert!(store.get_role("analyst").is_some());
    }

    #[test]
    fn create_with_builtin_parent() {
        let store = RoleStore::new();
        store
            .create_role("senior_analyst", TenantId::new(1), Some("readonly"), None)
            .unwrap();
        let role = store.get_role("senior_analyst").unwrap();
        assert_eq!(role.parent.as_deref(), Some("readonly"));
    }

    #[test]
    fn create_with_custom_parent() {
        let store = RoleStore::new();
        store
            .create_role("base", TenantId::new(1), None, None)
            .unwrap();
        store
            .create_role("child", TenantId::new(1), Some("base"), None)
            .unwrap();
        assert!(store.get_role("child").is_some());
    }

    #[test]
    fn reject_builtin_name() {
        let store = RoleStore::new();
        assert!(
            store
                .create_role("superuser", TenantId::new(1), None, None)
                .is_err()
        );
    }

    #[test]
    fn reject_duplicate() {
        let store = RoleStore::new();
        store
            .create_role("analyst", TenantId::new(1), None, None)
            .unwrap();
        assert!(
            store
                .create_role("analyst", TenantId::new(1), None, None)
                .is_err()
        );
    }

    #[test]
    fn reject_nonexistent_parent() {
        let store = RoleStore::new();
        assert!(
            store
                .create_role("child", TenantId::new(1), Some("nonexistent"), None)
                .is_err()
        );
    }

    #[test]
    fn drop_role() {
        let store = RoleStore::new();
        store
            .create_role("temp", TenantId::new(1), None, None)
            .unwrap();
        assert!(store.drop_role("temp", None).unwrap());
        assert!(store.get_role("temp").is_none());
    }

    #[test]
    fn drop_builtin_rejected() {
        let store = RoleStore::new();
        assert!(store.drop_role("readonly", None).is_err());
    }

    #[test]
    fn drop_with_children_rejected() {
        let store = RoleStore::new();
        store
            .create_role("parent", TenantId::new(1), None, None)
            .unwrap();
        store
            .create_role("child", TenantId::new(1), Some("parent"), None)
            .unwrap();
        assert!(store.drop_role("parent", None).is_err());
    }

    #[test]
    fn resolve_inheritance_chain() {
        let store = RoleStore::new();
        store
            .create_role("base", TenantId::new(1), Some("readonly"), None)
            .unwrap();
        store
            .create_role("mid", TenantId::new(1), Some("base"), None)
            .unwrap();
        store
            .create_role("leaf", TenantId::new(1), Some("mid"), None)
            .unwrap();

        let chain = store.resolve_inheritance(&Role::Custom("leaf".into()));
        assert_eq!(chain.len(), 4); // leaf → mid → base → readonly
    }

    #[test]
    fn resolve_builtin_no_chain() {
        let store = RoleStore::new();
        let chain = store.resolve_inheritance(&Role::ReadOnly);
        assert_eq!(chain.len(), 1);
    }
}
