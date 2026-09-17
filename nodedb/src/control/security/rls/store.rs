// SPDX-License-Identifier: BUSL-1.1

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
    /// Monotonic per-tenant version, bumped on every policy create/drop/reload.
    /// A cached plan stamps this at build time so a plan that read policy
    /// state at one version is never served after that state changes.
    tenant_versions: RwLock<HashMap<u64, u64>>,
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
            tenant_versions: RwLock::new(HashMap::new()),
        }
    }

    /// Bump and return a tenant's policy version. `pub(super)` so the
    /// replicated-applier path in `replication.rs` can call it too.
    pub(super) fn bump_tenant_version(&self, tenant_id: u64) -> u64 {
        let mut versions = self
            .tenant_versions
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let v = versions.entry(tenant_id).or_insert(0);
        *v += 1;
        *v
    }

    /// Current policy version for a tenant. `0` if never mutated.
    pub fn tenant_version(&self, tenant_id: u64) -> u64 {
        *self
            .tenant_versions
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&tenant_id)
            .unwrap_or(&0)
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

        let tenant_id = policy.tenant_id;
        if let Some(existing) = list.iter_mut().find(|p| p.name == policy.name) {
            *existing = policy;
        } else {
            list.push(policy);
        }
        drop(policies);
        self.bump_tenant_version(tenant_id);
        Ok(())
    }

    /// Drop an RLS policy. Returns `true` if a policy was removed.
    pub fn drop_policy(&self, tenant_id: u64, collection: &str, policy_name: &str) -> bool {
        let key = policy_key(tenant_id, collection);
        let removed = {
            let mut policies = self.lock_write();
            if let Some(list) = policies.get_mut(&key) {
                let before = list.len();
                list.retain(|p| p.name != policy_name);
                list.len() < before
            } else {
                false
            }
        };
        if removed {
            self.bump_tenant_version(tenant_id);
        }
        removed
    }

    /// Get all enabled read policies for a tenant+collection.
    pub fn read_policies(&self, tenant_id: u64, collection: &str) -> Vec<RlsPolicy> {
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
    pub fn write_policies(&self, tenant_id: u64, collection: &str) -> Vec<RlsPolicy> {
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
    ///
    /// Every stored row is installed: a row that cannot be compiled against
    /// its collection installs as a restrictive deny-all
    /// (`StoredRlsPolicy::rehydrate`), never dropped.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::SystemCatalog,
    ) -> crate::Result<()> {
        let stored = catalog.load_all_rls_policies()?;
        let mut affected_tenants: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut policies = self.lock_write();
        for list in policies.values() {
            affected_tenants.extend(list.iter().map(|p| p.tenant_id));
        }
        policies.clear();
        for s in stored {
            let rp = s.rehydrate(catalog);
            affected_tenants.insert(rp.tenant_id);
            let key = super::types::policy_key(rp.tenant_id, &rp.collection);
            policies.entry(key).or_default().push(rp);
        }
        drop(policies);
        for tenant_id in affected_tenants {
            self.bump_tenant_version(tenant_id);
        }
        Ok(())
    }

    /// Total policies across all collections.
    pub fn policy_count(&self) -> usize {
        // Recover from a poisoned lock rather than reporting zero: this count
        // feeds the recovery-check verifier, and a spurious 0 reads as "the
        // registry lost every policy" and provokes a repair that isn't needed.
        self.lock_read().values().map(|v| v.len()).sum()
    }

    /// Get all policies for a tenant+collection.
    pub fn all_policies(&self, tenant_id: u64, collection: &str) -> Vec<RlsPolicy> {
        let key = policy_key(tenant_id, collection);
        let policies = self.lock_read();
        policies.get(&key).cloned().unwrap_or_default()
    }

    /// Get all policies for a tenant across all collections.
    pub fn all_policies_for_tenant(&self, tenant_id: u64) -> Vec<RlsPolicy> {
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
            display_collection: collection.into(),
            tenant_id: 1,
            policy_type,
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
    fn tenant_version_bumps_on_create_and_drop_only_when_state_changes() {
        let store = RlsPolicyStore::new();
        assert_eq!(store.tenant_version(1), 0);

        store
            .create_policy(make_policy("p1", "users", PolicyType::Read))
            .unwrap();
        assert_eq!(store.tenant_version(1), 1);

        // Dropping a policy name that does not exist changes nothing.
        assert!(!store.drop_policy(1, "users", "missing"));
        assert_eq!(store.tenant_version(1), 1);

        assert!(store.drop_policy(1, "users", "p1"));
        assert_eq!(store.tenant_version(1), 2);

        // A different tenant's version is untouched.
        assert_eq!(store.tenant_version(2), 0);
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
