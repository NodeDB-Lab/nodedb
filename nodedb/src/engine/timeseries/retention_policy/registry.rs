//! In-memory retention policy registry.
//!
//! Loaded from the system catalog on startup. All mutations go through
//! catalog first, then update this registry for fast runtime access.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::RetentionPolicyDef;

/// Thread-safe in-memory registry of retention policies.
///
/// Keyed by `(tenant_id, policy_name)`. Lives on the Control Plane (`Send + Sync`).
pub struct RetentionPolicyRegistry {
    policies: RwLock<HashMap<(u32, String), RetentionPolicyDef>>,
}

impl RetentionPolicyRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Bulk-load policies from catalog on startup.
    pub fn load(&self, defs: Vec<RetentionPolicyDef>) {
        let mut map = self.policies.write().expect("registry lock poisoned");
        map.clear();
        for def in defs {
            let key = (def.tenant_id, def.name.clone());
            map.insert(key, def);
        }
    }

    /// Register a new or updated policy.
    pub fn register(&self, def: RetentionPolicyDef) {
        let key = (def.tenant_id, def.name.clone());
        self.policies
            .write()
            .expect("registry lock poisoned")
            .insert(key, def);
    }

    /// Remove a policy.
    pub fn unregister(&self, tenant_id: u32, name: &str) {
        self.policies
            .write()
            .expect("registry lock poisoned")
            .remove(&(tenant_id, name.to_string()));
    }

    /// Get a policy by name.
    pub fn get(&self, tenant_id: u32, name: &str) -> Option<RetentionPolicyDef> {
        self.policies
            .read()
            .expect("registry lock poisoned")
            .get(&(tenant_id, name.to_string()))
            .cloned()
    }

    /// Get the policy for a specific collection.
    pub fn get_for_collection(
        &self,
        tenant_id: u32,
        collection: &str,
    ) -> Option<RetentionPolicyDef> {
        self.policies
            .read()
            .expect("registry lock poisoned")
            .values()
            .find(|p| p.tenant_id == tenant_id && p.collection == collection)
            .cloned()
    }

    /// List all enabled policies (all tenants).
    pub fn list_all_enabled(&self) -> Vec<RetentionPolicyDef> {
        self.policies
            .read()
            .expect("registry lock poisoned")
            .values()
            .filter(|p| p.enabled)
            .cloned()
            .collect()
    }

    /// List all policies for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<RetentionPolicyDef> {
        self.policies
            .read()
            .expect("registry lock poisoned")
            .values()
            .filter(|p| p.tenant_id == tenant_id)
            .cloned()
            .collect()
    }
}

impl Default for RetentionPolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::TierDef;

    fn make_policy(tenant_id: u32, name: &str, collection: &str) -> RetentionPolicyDef {
        RetentionPolicyDef {
            tenant_id,
            name: name.into(),
            collection: collection.into(),
            tiers: vec![TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms: 604_800_000,
                archive: None,
            }],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn register_and_get() {
        let reg = RetentionPolicyRegistry::new();
        let p = make_policy(1, "p1", "metrics");
        reg.register(p);

        let found = reg.get(1, "p1").unwrap();
        assert_eq!(found.collection, "metrics");
        assert!(reg.get(1, "nonexistent").is_none());
        assert!(reg.get(2, "p1").is_none());
    }

    #[test]
    fn get_for_collection() {
        let reg = RetentionPolicyRegistry::new();
        reg.register(make_policy(1, "p1", "metrics"));
        reg.register(make_policy(1, "p2", "logs"));

        let found = reg.get_for_collection(1, "metrics").unwrap();
        assert_eq!(found.name, "p1");
        assert!(reg.get_for_collection(1, "other").is_none());
    }

    #[test]
    fn unregister() {
        let reg = RetentionPolicyRegistry::new();
        reg.register(make_policy(1, "p1", "metrics"));
        reg.unregister(1, "p1");
        assert!(reg.get(1, "p1").is_none());
    }

    #[test]
    fn list_enabled() {
        let reg = RetentionPolicyRegistry::new();
        reg.register(make_policy(1, "p1", "m1"));
        let mut disabled = make_policy(1, "p2", "m2");
        disabled.enabled = false;
        reg.register(disabled);

        let enabled = reg.list_all_enabled();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "p1");
    }

    #[test]
    fn bulk_load() {
        let reg = RetentionPolicyRegistry::new();
        reg.register(make_policy(1, "old", "old_coll"));

        let defs = vec![make_policy(1, "a", "c1"), make_policy(2, "b", "c2")];
        reg.load(defs);

        assert!(reg.get(1, "old").is_none());
        assert!(reg.get(1, "a").is_some());
        assert!(reg.get(2, "b").is_some());
    }
}
