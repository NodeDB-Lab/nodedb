//! Organization store: in-memory cache + redb persistence.
//!
//! Manages `_system.orgs` and `_system.org_members` records.
//! Supports JIT org creation from JWT `$auth.org_id` claim.
//! Org status overrides member status (suspended org = all members blocked).

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::info;

use crate::control::security::catalog::{StoredOrg, StoredOrgMember, SystemCatalog};
use crate::control::security::time::now_secs;

/// In-memory organization record.
#[derive(Debug, Clone)]
pub struct OrgRecord {
    pub org_id: String,
    pub name: String,
    pub tenant_id: u32,
    pub status: String,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
}

impl OrgRecord {
    fn from_stored(s: &StoredOrg) -> Self {
        Self {
            org_id: s.org_id.clone(),
            name: s.name.clone(),
            tenant_id: s.tenant_id,
            status: s.status.clone(),
            created_at: s.created_at,
            metadata: s.metadata.clone(),
        }
    }

    fn to_stored(&self) -> StoredOrg {
        StoredOrg {
            org_id: self.org_id.clone(),
            name: self.name.clone(),
            tenant_id: self.tenant_id,
            status: self.status.clone(),
            created_at: self.created_at,
            metadata: self.metadata.clone(),
        }
    }
}

/// In-memory org membership record.
#[derive(Debug, Clone)]
pub struct OrgMemberRecord {
    pub auth_user_id: String,
    pub org_id: String,
    pub role: String,
    pub joined_at: u64,
}

impl OrgMemberRecord {
    #[allow(dead_code)] // Used when persistent member loading is added.
    fn from_stored(s: &StoredOrgMember) -> Self {
        Self {
            auth_user_id: s.auth_user_id.clone(),
            org_id: s.org_id.clone(),
            role: s.role.clone(),
            joined_at: s.joined_at,
        }
    }

    fn to_stored(&self) -> StoredOrgMember {
        StoredOrgMember {
            auth_user_id: self.auth_user_id.clone(),
            org_id: self.org_id.clone(),
            role: self.role.clone(),
            joined_at: self.joined_at,
        }
    }
}

/// Thread-safe organization store.
pub struct OrgStore {
    orgs: RwLock<HashMap<String, OrgRecord>>,
    /// Key: `"{org_id}:{user_id}"` → membership.
    members: RwLock<HashMap<String, OrgMemberRecord>>,
    catalog: Option<SystemCatalog>,
}

impl OrgStore {
    pub fn new() -> Self {
        Self {
            orgs: RwLock::new(HashMap::new()),
            members: RwLock::new(HashMap::new()),
            catalog: None,
        }
    }

    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored_orgs = catalog.load_all_orgs()?;
        let mut orgs = HashMap::with_capacity(stored_orgs.len());
        for s in &stored_orgs {
            orgs.insert(s.org_id.clone(), OrgRecord::from_stored(s));
        }
        // Members are loaded lazily per-org, not all at once.
        if !orgs.is_empty() {
            info!(count = orgs.len(), "orgs loaded from catalog");
        }
        Ok(Self {
            orgs: RwLock::new(orgs),
            members: RwLock::new(HashMap::new()),
            catalog: Some(catalog),
        })
    }

    /// Create a new organization.
    pub fn create_org(&self, org_id: &str, name: &str, tenant_id: u32) -> crate::Result<()> {
        let now = now_secs();
        let record = OrgRecord {
            org_id: org_id.into(),
            name: name.into(),
            tenant_id,
            status: "active".into(),
            created_at: now,
            metadata: HashMap::new(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_org(&record.to_stored())?;
        }

        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        orgs.insert(org_id.into(), record);
        info!(org_id = %org_id, name = %name, "org created");
        Ok(())
    }

    /// Get an organization by ID.
    pub fn get(&self, org_id: &str) -> Option<OrgRecord> {
        let orgs = self.orgs.read().unwrap_or_else(|p| p.into_inner());
        orgs.get(org_id).cloned()
    }

    /// Check if an org is active. Returns false if org doesn't exist.
    pub fn is_active(&self, org_id: &str) -> bool {
        self.get(org_id).is_some_and(|o| o.status == "active")
    }

    /// Set org status. Suspended org blocks all members.
    pub fn set_status(&self, org_id: &str, status: &str) -> crate::Result<bool> {
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        if let Some(org) = orgs.get_mut(org_id) {
            org.status = status.into();
            if let Some(ref catalog) = self.catalog {
                catalog.put_org(&org.to_stored())?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Drop an organization.
    pub fn drop_org(&self, org_id: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_org(org_id)?;
        }
        let mut orgs = self.orgs.write().unwrap_or_else(|p| p.into_inner());
        Ok(orgs.remove(org_id).is_some())
    }

    /// List all organizations, optionally filtered by tenant.
    pub fn list(&self, tenant_filter: Option<u32>) -> Vec<OrgRecord> {
        let orgs = self.orgs.read().unwrap_or_else(|p| p.into_inner());
        orgs.values()
            .filter(|o| tenant_filter.is_none_or(|t| o.tenant_id == t))
            .cloned()
            .collect()
    }

    // ── Membership ──────────────────────────────────────────────────

    fn member_key(org_id: &str, user_id: &str) -> String {
        format!("{org_id}:{user_id}")
    }

    /// Add a member to an organization.
    pub fn add_member(&self, org_id: &str, user_id: &str, role: &str) -> crate::Result<()> {
        let record = OrgMemberRecord {
            auth_user_id: user_id.into(),
            org_id: org_id.into(),
            role: role.into(),
            joined_at: now_secs(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_org_member(&record.to_stored())?;
        }

        let key = Self::member_key(org_id, user_id);
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        members.insert(key, record);
        Ok(())
    }

    /// Remove a member from an organization.
    pub fn remove_member(&self, org_id: &str, user_id: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_org_member(org_id, user_id)?;
        }
        let key = Self::member_key(org_id, user_id);
        let mut members = self.members.write().unwrap_or_else(|p| p.into_inner());
        Ok(members.remove(&key).is_some())
    }

    /// List members of an organization.
    pub fn members_of(&self, org_id: &str) -> Vec<OrgMemberRecord> {
        let prefix = format!("{org_id}:");
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// List all orgs a user belongs to.
    pub fn orgs_for_user(&self, user_id: &str) -> Vec<String> {
        let suffix = format!(":{user_id}");
        let members = self.members.read().unwrap_or_else(|p| p.into_inner());
        members
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .map(|(_, v)| v.org_id.clone())
            .collect()
    }

    /// JIT org creation: create org if it doesn't exist.
    pub fn ensure_org(&self, org_id: &str, tenant_id: u32) -> crate::Result<()> {
        if self.get(org_id).is_some() {
            return Ok(());
        }
        self.create_org(org_id, org_id, tenant_id)?;
        info!(org_id = %org_id, "JIT org created");
        Ok(())
    }

    pub fn catalog(&self) -> Option<&SystemCatalog> {
        self.catalog.as_ref()
    }

    pub fn count(&self) -> usize {
        self.orgs.read().unwrap_or_else(|p| p.into_inner()).len()
    }
}

impl Default for OrgStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get_org() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme Corp", 1).unwrap();
        let org = store.get("acme").unwrap();
        assert_eq!(org.name, "Acme Corp");
        assert_eq!(org.status, "active");
    }

    #[test]
    fn set_org_status() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        assert!(store.is_active("acme"));

        store.set_status("acme", "suspended").unwrap();
        assert!(!store.is_active("acme"));
    }

    #[test]
    fn membership() {
        let store = OrgStore::new();
        store.create_org("acme", "Acme", 1).unwrap();
        store.add_member("acme", "user_42", "member").unwrap();
        store.add_member("acme", "user_99", "admin").unwrap();

        assert_eq!(store.members_of("acme").len(), 2);
        assert_eq!(store.orgs_for_user("user_42"), vec!["acme"]);

        store.remove_member("acme", "user_42").unwrap();
        assert_eq!(store.members_of("acme").len(), 1);
    }

    #[test]
    fn ensure_org_idempotent() {
        let store = OrgStore::new();
        store.ensure_org("acme", 1).unwrap();
        store.ensure_org("acme", 1).unwrap(); // No error.
        assert_eq!(store.count(), 1);
    }
}
