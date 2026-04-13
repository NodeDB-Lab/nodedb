//! `PermissionStore` — in-memory grants + ownership maps with
//! redb persistence. Boot replay (`load_from`) and the legacy
//! `grant` / `revoke` / `grants_on` / `grants_for` CRUD live
//! here. Evaluation lives in [`super::check`], ownership CRUD in
//! [`super::owner`], applier helpers in [`super::replication`].

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::control::security::catalog::{StoredPermission, SystemCatalog};
use crate::control::security::identity::Permission;
use crate::control::security::time::now_secs;

use super::types::{Grant, format_permission, owner_key, parse_permission};

/// Permission store: grants + ownership with in-memory cache and redb persistence.
pub struct PermissionStore {
    pub(super) grants: RwLock<HashSet<Grant>>,
    /// "collection:{tenant_id}:{name}" → owner username
    pub(super) owners: RwLock<HashMap<String, String>>,
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
        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };
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
        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned — recovering data");
                p.into_inner()
            }
        };
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

    /// Grant a permission on a target to a grantee (role name or "user:username").
    ///
    /// Direct CRUD path used by single-node mode and tests. Cluster
    /// mode flows through [`super::replication`] instead.
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

        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };
        grants.insert(grant);
        Ok(())
    }

    /// Revoke a permission. Returns `true` if a grant was removed.
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

        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };
        Ok(grants.remove(&grant))
    }

    /// List all grants for a grantee.
    pub fn grants_for(&self, grantee: &str) -> Vec<Grant> {
        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
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
            Err(p) => {
                tracing::error!("permission grants lock poisoned — recovering data");
                p.into_inner()
            }
        };
        grants
            .iter()
            .filter(|g| g.target == target)
            .cloned()
            .collect()
    }
}
