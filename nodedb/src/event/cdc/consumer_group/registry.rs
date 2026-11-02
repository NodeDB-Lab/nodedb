// SPDX-License-Identifier: BUSL-1.1

//! In-memory registry of consumer groups.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! Thread-safe (RwLock) — reads are concurrent, writes are exclusive.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::Mutex;

use super::types::ConsumerGroupDef;
use crate::types::DatabaseId;

/// Scoped identity of a consumer group:
/// `(database_id, tenant_id, stream_name, group_name)`.
///
/// The database and tenant are part of the key so a group name reused across
/// databases can never resolve to another database's registration.
pub type GroupKey = (DatabaseId, u64, String, String);

/// In-memory consumer group registry.
///
/// Groups are keyed by [`GroupKey`].
pub struct GroupRegistry {
    groups: RwLock<HashMap<GroupKey, ConsumerGroupDef>>,
    /// Lifecycle locks outlive group registrations so a DROP/CREATE cycle
    /// cannot allow an in-flight commit to target a removed incarnation.
    lifecycle_locks: RwLock<HashMap<GroupKey, Arc<Mutex<()>>>>,
}

impl GroupRegistry {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            lifecycle_locks: RwLock::new(HashMap::new()),
        }
    }

    /// Return the stable lifecycle lock for one consumer-group identity.
    ///
    /// The lock is intentionally never removed: a newly created group with the
    /// same identity must serialize with work from the previous incarnation.
    pub fn lifecycle_lock(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> Arc<Mutex<()>> {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut locks = self
            .lifecycle_locks
            .write()
            .unwrap_or_else(|p| p.into_inner());
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Register a new consumer group.
    pub fn register(&self, def: ConsumerGroupDef) {
        let key = (
            def.database_id,
            def.tenant_id,
            def.stream_name.clone(),
            def.name.clone(),
        );
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// Unregister a consumer group. Returns true if it existed.
    pub fn unregister(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> bool {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
        let mut map = self.groups.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    /// Get a group definition.
    pub fn get(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
        group: &str,
    ) -> Option<ConsumerGroupDef> {
        let key = (
            database_id,
            tenant_id,
            stream.to_string(),
            group.to_string(),
        );
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
                group.database_id,
                group.tenant_id,
                group.stream_name.clone(),
                group.name.clone(),
            );
            map.insert(key, group);
        }
        Ok(())
    }

    /// List all groups for a given stream.
    pub fn list_for_stream(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream: &str,
    ) -> Vec<ConsumerGroupDef> {
        let map = self.groups.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|g| {
                g.database_id == database_id && g.tenant_id == tenant_id && g.stream_name == stream
            })
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
                group.database_id,
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
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: group.into(),
            stream_name: stream.into(),
            owner: "admin".into(),
            created_at: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn register_and_get() {
        let reg = GroupRegistry::new();
        reg.register(sample("orders_stream", "analytics"));
        assert!(
            reg.get(DatabaseId::DEFAULT, 1, "orders_stream", "analytics")
                .is_some()
        );
        assert!(
            reg.get(DatabaseId::DEFAULT, 1, "orders_stream", "nonexistent")
                .is_none()
        );
    }

    #[test]
    fn list_for_stream() {
        let reg = GroupRegistry::new();
        reg.register(sample("s1", "g1"));
        reg.register(sample("s1", "g2"));
        reg.register(sample("s2", "g3"));

        let s1_groups = reg.list_for_stream(DatabaseId::DEFAULT, 1, "s1");
        assert_eq!(s1_groups.len(), 2);

        let s2_groups = reg.list_for_stream(DatabaseId::DEFAULT, 1, "s2");
        assert_eq!(s2_groups.len(), 1);
    }

    #[test]
    fn unregister() {
        let reg = GroupRegistry::new();
        reg.register(sample("s", "g"));
        assert!(reg.unregister(DatabaseId::DEFAULT, 1, "s", "g"));
        assert!(!reg.unregister(DatabaseId::DEFAULT, 1, "s", "g"));
    }

    #[tokio::test]
    async fn lifecycle_lock_survives_unregister_and_recreate() {
        let reg = GroupRegistry::new();
        let lock = reg.lifecycle_lock(DatabaseId::DEFAULT, 1, "s", "g");
        reg.register(sample("s", "g"));
        assert!(reg.unregister(DatabaseId::DEFAULT, 1, "s", "g"));
        reg.register(sample("s", "g"));
        assert!(Arc::ptr_eq(
            &lock,
            &reg.lifecycle_lock(DatabaseId::DEFAULT, 1, "s", "g")
        ));
    }
}
