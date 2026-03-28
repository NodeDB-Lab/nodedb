//! Scope grant management: GRANT/REVOKE SCOPE TO/FROM ORG/USER/TEAM.
//!
//! Effective scopes for a user = user scopes UNION team scopes UNION org scopes.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use tracing::info;

use crate::control::security::catalog::{StoredScopeGrant, SystemCatalog};
use crate::control::security::time::now_secs;

/// In-memory scope grant record with time-bound support.
#[derive(Debug, Clone)]
pub struct ScopeGrant {
    pub scope_name: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub granted_by: String,
    pub granted_at: u64,
    /// Unix timestamp when this grant expires. 0 = no expiry (permanent).
    pub expires_at: u64,
    /// Grace period in seconds after expiry before hard cutoff.
    pub grace_period_secs: u64,
    /// Action on expiry: "revoke_all", "grant:<scope_name>", or "" (just expire).
    pub on_expire_action: String,
}

/// Status of a time-bound scope grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeStatus {
    /// Grant is active (not expired, or no expiry set).
    Active,
    /// Grant is in grace period (expired but within grace window).
    Grace,
    /// Grant is fully expired (past grace period).
    Expired,
    /// Grant does not exist for this grantee.
    None,
}

impl std::fmt::Display for ScopeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Grace => write!(f, "grace"),
            Self::Expired => write!(f, "expired"),
            Self::None => write!(f, "none"),
        }
    }
}

impl ScopeGrant {
    /// Check the time-bound status of this grant.
    pub fn status(&self) -> ScopeStatus {
        if self.expires_at == 0 {
            return ScopeStatus::Active; // No expiry = permanent.
        }
        let now = now_secs();
        if now < self.expires_at {
            ScopeStatus::Active
        } else if now < self.expires_at + self.grace_period_secs {
            ScopeStatus::Grace
        } else {
            ScopeStatus::Expired
        }
    }

    /// Check if this grant is still effective (active or in grace period).
    pub fn is_effective(&self) -> bool {
        matches!(self.status(), ScopeStatus::Active | ScopeStatus::Grace)
    }

    fn from_stored(s: &StoredScopeGrant) -> Self {
        Self {
            scope_name: s.scope_name.clone(),
            grantee_type: s.grantee_type.clone(),
            grantee_id: s.grantee_id.clone(),
            granted_by: s.granted_by.clone(),
            granted_at: s.granted_at,
            expires_at: s.expires_at,
            grace_period_secs: s.grace_period_secs,
            on_expire_action: s.on_expire_action.clone(),
        }
    }

    fn to_stored(&self) -> StoredScopeGrant {
        StoredScopeGrant {
            scope_name: self.scope_name.clone(),
            grantee_type: self.grantee_type.clone(),
            grantee_id: self.grantee_id.clone(),
            granted_by: self.granted_by.clone(),
            granted_at: self.granted_at,
            expires_at: self.expires_at,
            grace_period_secs: self.grace_period_secs,
            on_expire_action: self.on_expire_action.clone(),
        }
    }
}

/// Thread-safe scope grant store.
pub struct ScopeGrantStore {
    /// Key: `"{scope}:{type}:{id}"` → grant.
    grants: RwLock<HashMap<String, ScopeGrant>>,
    catalog: Option<SystemCatalog>,
}

impl ScopeGrantStore {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashMap::new()),
            catalog: None,
        }
    }

    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored = catalog.load_all_scope_grants()?;
        let mut grants = HashMap::with_capacity(stored.len());
        for s in &stored {
            let key = grant_key(&s.scope_name, &s.grantee_type, &s.grantee_id);
            grants.insert(key, ScopeGrant::from_stored(s));
        }
        if !grants.is_empty() {
            info!(count = grants.len(), "scope grants loaded from catalog");
        }
        Ok(Self {
            grants: RwLock::new(grants),
            catalog: Some(catalog),
        })
    }

    /// Grant a scope to a user, role, org, or team.
    ///
    /// `expires_at` = 0 means permanent (no expiry).
    /// `grace_period_secs` = seconds after expiry before hard cutoff.
    /// `on_expire_action` = "revoke_all", "grant:<scope>", or "" (just expire).
    #[allow(clippy::too_many_arguments)]
    pub fn grant(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
        granted_by: &str,
        expires_at: u64,
        grace_period_secs: u64,
        on_expire_action: &str,
    ) -> crate::Result<()> {
        let record = ScopeGrant {
            scope_name: scope_name.into(),
            grantee_type: grantee_type.into(),
            grantee_id: grantee_id.into(),
            granted_by: granted_by.into(),
            granted_at: now_secs(),
            expires_at,
            grace_period_secs,
            on_expire_action: on_expire_action.into(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_scope_grant(&record.to_stored())?;
        }

        let key = grant_key(scope_name, grantee_type, grantee_id);
        let mut grants = self.grants.write().unwrap_or_else(|p| p.into_inner());
        grants.insert(key, record);
        info!(scope = %scope_name, grantee_type, grantee_id, "scope granted");
        Ok(())
    }

    /// Revoke a scope grant.
    pub fn revoke(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_scope_grant(scope_name, grantee_type, grantee_id)?;
        }
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let mut grants = self.grants.write().unwrap_or_else(|p| p.into_inner());
        Ok(grants.remove(&key).is_some())
    }

    /// Get all effective scope names granted to a specific grantee.
    /// Filters out expired grants.
    pub fn scopes_for(&self, grantee_type: &str, grantee_id: &str) -> Vec<String> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| {
                g.grantee_type == grantee_type && g.grantee_id == grantee_id && g.is_effective()
            })
            .map(|g| g.scope_name.clone())
            .collect()
    }

    /// Get the status of a specific scope grant.
    pub fn scope_status(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> ScopeStatus {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .get(&key)
            .map(|g| g.status())
            .unwrap_or(ScopeStatus::None)
    }

    /// Get the expiry timestamp of a scope grant. Returns 0 if permanent or not found.
    pub fn scope_expires_at(&self, scope_name: &str, grantee_type: &str, grantee_id: &str) -> u64 {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants.get(&key).map(|g| g.expires_at).unwrap_or(0)
    }

    /// Renew a scope grant by extending its expiry.
    pub fn renew(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
        extend_secs: u64,
    ) -> crate::Result<bool> {
        let key = grant_key(scope_name, grantee_type, grantee_id);
        let mut grants = self.grants.write().unwrap_or_else(|p| p.into_inner());
        if let Some(g) = grants.get_mut(&key) {
            if g.expires_at == 0 {
                return Ok(true); // Already permanent.
            }
            let now = now_secs();
            // Extend from current expiry or from now (whichever is later).
            let base = g.expires_at.max(now);
            g.expires_at = base + extend_secs;
            if let Some(ref catalog) = self.catalog {
                let _ = catalog.put_scope_grant(&g.to_stored());
            }
            info!(scope = %scope_name, grantee_type, grantee_id, new_expires = g.expires_at, "scope renewed");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List grants expiring within the given window (seconds from now).
    pub fn expiring_within(&self, window_secs: u64) -> Vec<ScopeGrant> {
        let now = now_secs();
        let deadline = now + window_secs;
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| g.expires_at > 0 && g.expires_at <= deadline && g.is_effective())
            .cloned()
            .collect()
    }

    /// Resolve effective scopes for a user.
    ///
    /// Collects: user's direct scopes + org scopes for each org membership.
    /// Filters out expired grants.
    pub fn effective_scopes(&self, user_id: &str, org_ids: &[String]) -> HashSet<String> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        let mut effective = HashSet::new();

        for g in grants.values() {
            if !g.is_effective() {
                continue; // Skip expired grants.
            }
            // Direct user grant.
            if g.grantee_type == "user" && g.grantee_id == user_id {
                effective.insert(g.scope_name.clone());
            }
            // Org grant (user inherits via membership).
            if g.grantee_type == "org" && org_ids.contains(&g.grantee_id) {
                effective.insert(g.scope_name.clone());
            }
        }

        effective
    }

    /// Check if a user (directly or via orgs) has a specific scope.
    pub fn has_scope(&self, user_id: &str, org_ids: &[String], scope_name: &str) -> bool {
        self.effective_scopes(user_id, org_ids).contains(scope_name)
    }

    /// List all grants, optionally filtered by scope name.
    pub fn list(&self, scope_filter: Option<&str>) -> Vec<ScopeGrant> {
        let grants = self.grants.read().unwrap_or_else(|p| p.into_inner());
        grants
            .values()
            .filter(|g| scope_filter.is_none_or(|s| g.scope_name == s))
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.grants.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

impl Default for ScopeGrantStore {
    fn default() -> Self {
        Self::new()
    }
}

fn grant_key(scope: &str, grantee_type: &str, grantee_id: &str) -> String {
    format!("{scope}:{grantee_type}:{grantee_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_and_check() {
        let store = ScopeGrantStore::new();
        store
            .grant("profile:read", "user", "u1", "admin", 0, 0, "")
            .unwrap();

        assert!(store.has_scope("u1", &[], "profile:read"));
        assert!(!store.has_scope("u1", &[], "orders:write"));
        assert!(!store.has_scope("u2", &[], "profile:read"));
    }

    #[test]
    fn org_scope_inheritance() {
        let store = ScopeGrantStore::new();
        store
            .grant("pro:all", "org", "acme", "admin", 0, 0, "")
            .unwrap();

        // User u1 is member of acme → inherits pro:all.
        assert!(store.has_scope("u1", &["acme".into()], "pro:all"));
        // User u2 is NOT member → doesn't inherit.
        assert!(!store.has_scope("u2", &[], "pro:all"));
    }

    #[test]
    fn effective_scopes_union() {
        let store = ScopeGrantStore::new();
        store
            .grant("scope_a", "user", "u1", "admin", 0, 0, "")
            .unwrap();
        store
            .grant("scope_b", "org", "acme", "admin", 0, 0, "")
            .unwrap();
        store
            .grant("scope_c", "org", "beta", "admin", 0, 0, "")
            .unwrap();

        let effective = store.effective_scopes("u1", &["acme".into()]);
        assert!(effective.contains("scope_a")); // Direct user grant.
        assert!(effective.contains("scope_b")); // Via acme org.
        assert!(!effective.contains("scope_c")); // Not member of beta.
    }

    #[test]
    fn revoke_removes_grant() {
        let store = ScopeGrantStore::new();
        store.grant("s1", "user", "u1", "admin", 0, 0, "").unwrap();
        assert!(store.has_scope("u1", &[], "s1"));

        store.revoke("s1", "user", "u1").unwrap();
        assert!(!store.has_scope("u1", &[], "s1"));
    }
}
