//! In-memory registry of consumer groups.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! Thread-safe (RwLock) — reads are concurrent, writes are exclusive.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::ConsumerGroupDef;

/// In-memory consumer group registry.
///
/// Groups are keyed by `(tenant_id, stream_name, group_name)`.
pub struct GroupRegistry {
    groups: RwLock<HashMap<(u32, String, String), ConsumerGroupDef>>,
}

impl GroupRegistry {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new consumer group.
    pub fn register(&self, def: ConsumerGroupDef) {
        let key = (def.tenant_id, def.stream_name.clone(), def.name.clone());
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// Unregister a consumer group. Returns true if it existed.
    pub fn unregister(&self, tenant_id: u32, stream: &str, group: &str) -> bool {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    /// Get a group definition.
    pub fn get(&self, tenant_id: u32, stream: &str, group: &str) -> Option<ConsumerGroupDef> {
        let key = (tenant_id, stream.to_string(), group.to_string());
        let map = self.groups.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    /// List all groups (all tenants, all streams). Used by the recovery verifier.
    pub fn list_all(&self) -> Vec<ConsumerGroupDef> {
        let map = self.groups.read().unwrap_or_else(|p| p.into_inner());
        map.values().cloned().collect()
    }

    /// Clear and reload from catalog. Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_consumer_groups()?;
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for group in fresh {
            let key = (
                group.tenant_id,
                group.stream_name.clone(),
                group.name.clone(),
            );
            map.insert(key, group);
        }
        Ok(())
    }

    /// List all groups for a given stream.
    pub fn list_for_stream(&self, tenant_id: u32, stream: &str) -> Vec<ConsumerGroupDef> {
        let map = self.groups.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|g| g.tenant_id == tenant_id && g.stream_name == stream)
            .cloned()
            .collect()
    }

    /// Load all groups from the catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let groups = match catalog.load_all_consumer_groups() {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load consumer groups from catalog");
                return;
            }
        };
        if groups.is_empty() {
            return;
        }
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        for group in groups {
            let key = (
                group.tenant_id,
                group.stream_name.clone(),
                group.name.clone(),
            );
            map.insert(key, group);
        }
        tracing::info!(count = map.len(), "loaded consumer groups from catalog");
    }
}

impl Default for GroupRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(stream: &str, group: &str) -> ConsumerGroupDef {
        ConsumerGroupDef {
            tenant_id: 1,
            name: group.into(),
            stream_name: stream.into(),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn register_and_get() {
        let reg = GroupRegistry::new();
        reg.register(sample("orders_stream", "analytics"));
        assert!(reg.get(1, "orders_stream", "analytics").is_some());
        assert!(reg.get(1, "orders_stream", "nonexistent").is_none());
    }

    #[test]
    fn list_for_stream() {
        let reg = GroupRegistry::new();
        reg.register(sample("s1", "g1"));
        reg.register(sample("s1", "g2"));
        reg.register(sample("s2", "g3"));

        let s1_groups = reg.list_for_stream(1, "s1");
        assert_eq!(s1_groups.len(), 2);

        let s2_groups = reg.list_for_stream(1, "s2");
        assert_eq!(s2_groups.len(), 1);
    }

    #[test]
    fn unregister() {
        let reg = GroupRegistry::new();
        reg.register(sample("s", "g"));
        assert!(reg.unregister(1, "s", "g"));
        assert!(!reg.unregister(1, "s", "g"));
    }
}
