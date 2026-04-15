//! `RlsPolicyStore` — in-memory policy registry keyed by
//! `(tenant_id, collection)`. CRUD + scoped query methods live
//! here; evaluation logic is in [`super::eval`].

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::{PolicyType, RlsPolicy, policy_key};

/// RLS policy store: manages policies per tenant+collection.
pub struct RlsPolicyStore {
    /// Key: `"{tenant_id}:{collection}"` → list of policies.
    pub(super) policies: RwLock<HashMap<String, Vec<RlsPolicy>>>,
}

impl Default for RlsPolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RlsPolicyStore {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire a read lock, recovering from `RwLock` poisoning.
    pub(super) fn lock_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, Vec<RlsPolicy>>> {
        self.policies.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Acquire a write lock, recovering from `RwLock` poisoning.
    pub(super) fn lock_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<String, Vec<RlsPolicy>>> {
        self.policies.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Create or replace an RLS policy.
    pub fn create_policy(&self, policy: RlsPolicy) -> crate::Result<()> {
        let key = policy_key(policy.tenant_id, &policy.collection);
        let mut policies = self.lock_write();
        let list = policies.entry(key).or_default();

        if let Some(existing) = list.iter_mut().find(|p| p.name == policy.name) {
            *existing = policy;
        } else {
            list.push(policy);
        }
        Ok(())
    }

    /// Drop an RLS policy. Returns `true` if a policy was removed.
    pub fn drop_policy(&self, tenant_id: u32, collection: &str, policy_name: &str) -> bool {
        let key = policy_key(tenant_id, collection);
        let mut policies = self.lock_write();
        if let Some(list) = policies.get_mut(&key) {
            let before = list.len();
            list.retain(|p| p.name != policy_name);
            list.len() < before
        } else {
            false
        }
    }

    /// Get all enabled read policies for a tenant+collection.
    pub fn read_policies(&self, tenant_id: u32, collection: &str) -> Vec<RlsPolicy> {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies
            .get(&key)
            .map(|list| {
                list.iter()
                    .filter(|p| {
                        p.enabled && matches!(p.policy_type, PolicyType::Read | PolicyType::All)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all enabled write policies for a tenant+collection.
    pub fn write_policies(&self, tenant_id: u32, collection: &str) -> Vec<RlsPolicy> {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies
            .get(&key)
            .map(|list| {
                list.iter()
                    .filter(|p| {
                        p.enabled && matches!(p.policy_type, PolicyType::Write | PolicyType::All)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Flat list of all policies (all tenants, all collections).
    /// Used by the recovery verifier.
    pub fn list_all_flat(&self) -> Vec<RlsPolicy> {
        let policies = self.lock_read();
        policies.values().flat_map(|v| v.iter().cloned()).collect()
    }

    /// Clear all in-memory policies and reload from the catalog.
    /// Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let stored = catalog.load_all_rls_policies()?;
        let mut policies = self.lock_write();
        policies.clear();
        for s in stored {
            match s.to_runtime() {
                Ok(rp) => {
                    let key = super::types::policy_key(rp.tenant_id, &rp.collection);
                    policies.entry(key).or_default().push(rp);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "rls_store.clear_and_reload: skipping unparseable policy");
                }
            }
        }
        Ok(())
    }

    /// Total policies across all collections.
    pub fn policy_count(&self) -> usize {
        self.policies
            .read()
            .map(|g| g.values().map(|v| v.len()).sum())
            .unwrap_or(0)
    }

    /// Get all policies for a tenant+collection.
    pub fn all_policies(&self, tenant_id: u32, collection: &str) -> Vec<RlsPolicy> {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies.get(&key).cloned().unwrap_or_default()
    }

    /// Get all policies for a tenant across all collections.
    pub fn all_policies_for_tenant(&self, tenant_id: u32) -> Vec<RlsPolicy> {
        let prefix = format!("{tenant_id}:");
        let policies = self.lock_read();
        policies
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .flat_map(|(_, list)| list.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::predicate::PolicyMode;

    pub(super) fn make_policy(name: &str, collection: &str, policy_type: PolicyType) -> RlsPolicy {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            tenant_id: 1,
            policy_type,
            predicate: Vec::new(),
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    #[test]
    fn create_and_query_policy() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("read_own", "users", PolicyType::Read))
            .unwrap();

        let read = store.read_policies(1, "users");
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].name, "read_own");

        let write = store.write_policies(1, "users");
        assert!(write.is_empty());
    }

    #[test]
    fn drop_policy() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("p1", "users", PolicyType::Read))
            .unwrap();
        assert_eq!(store.policy_count(), 1);

        assert!(store.drop_policy(1, "users", "p1"));
        assert_eq!(store.policy_count(), 0);
    }

    #[test]
    fn all_policy_type_applies_to_both() {
        let store = RlsPolicyStore::new();
        store
            .create_policy(make_policy("both", "data", PolicyType::All))
            .unwrap();

        assert_eq!(store.read_policies(1, "data").len(), 1);
        assert_eq!(store.write_policies(1, "data").len(), 1);
    }
}
