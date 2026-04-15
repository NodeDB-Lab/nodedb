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

    /// Replace the entire in-memory grants + owners state
    /// with the contents of `other`. Used by the catalog
    /// recovery sanity checker to repair a divergent registry
    /// by loading a fresh `PermissionStore` from redb and then
    /// swapping its contents into `self`. Callers keep their
    /// existing `Arc<PermissionStore>` reference stable.
    pub(crate) fn clear_and_install_from(&self, other: &Self) {
        let fresh_grants = other.snapshot_grants();
        let fresh_owners = other.snapshot_owners();
        let mut grants = match self.grants.write() {
            Ok(g) => g,
            Err(p) => {
                tracing::error!("permission grants lock poisoned during repair — recovering");
                p.into_inner()
            }
        };
        grants.clear();
        for g in fresh_grants {
            grants.insert(g);
        }
        drop(grants);
        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned during repair — recovering");
                p.into_inner()
            }
        };
        owners.clear();
        for (k, v) in fresh_owners {
            owners.insert(k, v);
        }
    }

    /// Deterministic snapshot of every grant held in memory,
    /// sorted by `(target, grantee, permission)` so diff-based
    /// callers (the recovery sanity checker) can compare
    /// against a catalog load without caring about HashSet
    /// iteration order.
    pub fn snapshot_grants(&self) -> Vec<Grant> {
        let grants = match self.grants.read() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut out: Vec<Grant> = grants.iter().cloned().collect();
        out.sort_by(|a, b| {
            let a_key = (
                a.target.clone(),
                a.grantee.clone(),
                format_permission(a.permission),
            );
            let b_key = (
                b.target.clone(),
                b.grantee.clone(),
                format_permission(b.permission),
            );
            a_key.cmp(&b_key)
        });
        out
    }

    /// Deterministic snapshot of every owner held in memory as
    /// `(owner_key, username)` pairs, sorted by key.
    /// `owner_key` is the internal `"collection:{tenant}:{name}"`
    /// composite — used by the sanity checker to cross-check
    /// against `catalog.load_all_owners()`.
    pub fn snapshot_owners(&self) -> Vec<(String, String)> {
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(p) => p.into_inner(),
        };
        let mut out: Vec<(String, String)> =
            owners.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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
