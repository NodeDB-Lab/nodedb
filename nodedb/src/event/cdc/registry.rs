//! In-memory registry of active change streams.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! Queried by the CDC router on every WriteEvent for matching streams.
//!
//! Thread-safe (RwLock) — reads are concurrent, writes are exclusive.
//!
//! Streams are indexed by `(tenant_id, collection)` so the CDC router's
//! hot path scans only the matching bucket rather than the full fleet.
//! Wildcard (`*`) streams live in a per-tenant wildcard bucket and are
//! merged into every lookup for that tenant.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::stream_def::ChangeStreamDef;

/// Wildcard collection literal — matches every collection in the tenant.
const WILDCARD: &str = "*";

struct Inner {
    /// Canonical store, keyed by `(tenant_id, stream_name)`.
    by_name: HashMap<(u32, String), Arc<ChangeStreamDef>>,
    /// Routing index: `(tenant_id, collection)` → matching streams.
    /// Wildcard streams are stored under `(tenant_id, "*")`.
    by_collection: HashMap<(u32, String), Vec<Arc<ChangeStreamDef>>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            by_name: HashMap::new(),
            by_collection: HashMap::new(),
        }
    }

    fn insert(&mut self, def: ChangeStreamDef) {
        let key_name = (def.tenant_id, def.name.clone());
        let key_coll = (def.tenant_id, def.collection.clone());
        let def_arc = Arc::new(def);

        // Replace any prior entry under this name — removes its collection-index entry first.
        if let Some(prev) = self.by_name.remove(&key_name) {
            let prev_coll_key = (prev.tenant_id, prev.collection.clone());
            if let Some(bucket) = self.by_collection.get_mut(&prev_coll_key) {
                bucket.retain(|d| d.name != prev.name);
                if bucket.is_empty() {
                    self.by_collection.remove(&prev_coll_key);
                }
            }
        }

        self.by_collection
            .entry(key_coll)
            .or_default()
            .push(Arc::clone(&def_arc));
        self.by_name.insert(key_name, def_arc);
    }

    fn remove(&mut self, tenant_id: u32, name: &str) -> bool {
        let key_name = (tenant_id, name.to_string());
        let Some(prev) = self.by_name.remove(&key_name) else {
            return false;
        };
        let key_coll = (prev.tenant_id, prev.collection.clone());
        if let Some(bucket) = self.by_collection.get_mut(&key_coll) {
            bucket.retain(|d| d.name != prev.name);
            if bucket.is_empty() {
                self.by_collection.remove(&key_coll);
            }
        }
        true
    }
}

pub struct StreamRegistry {
    inner: RwLock<Inner>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner::new()),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Register a new change stream.
    pub fn register(&self, def: ChangeStreamDef) {
        self.write().insert(def);
    }

    /// Unregister a change stream by name. Returns true if it existed.
    pub fn unregister(&self, tenant_id: u32, name: &str) -> bool {
        self.write().remove(tenant_id, name)
    }

    /// Get a stream definition by name.
    pub fn get(&self, tenant_id: u32, name: &str) -> Option<ChangeStreamDef> {
        let key = (tenant_id, name.to_string());
        self.read().by_name.get(&key).map(|a| (**a).clone())
    }

    /// Find all streams matching `(tenant_id, collection)`. Returns shared
    /// `Arc<ChangeStreamDef>` handles so the router hot path is
    /// O(matching_streams refcount bumps), not a deep clone per match.
    pub fn find_matching(&self, tenant_id: u32, collection: &str) -> Vec<Arc<ChangeStreamDef>> {
        let inner = self.read();
        let mut out: Vec<Arc<ChangeStreamDef>> = Vec::new();
        if let Some(bucket) = inner
            .by_collection
            .get(&(tenant_id, collection.to_string()))
        {
            out.extend(bucket.iter().cloned());
        }
        if collection != WILDCARD
            && let Some(bucket) = inner.by_collection.get(&(tenant_id, WILDCARD.to_string()))
        {
            out.extend(bucket.iter().cloned());
        }
        out
    }

    /// List all streams (all tenants). Used by the recovery verifier.
    pub fn list_all(&self) -> Vec<ChangeStreamDef> {
        self.read()
            .by_name
            .values()
            .map(|a| (**a).clone())
            .collect()
    }

    /// Clear and reload from catalog. Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_change_streams()?;
        let mut inner = self.write();
        *inner = Inner::new();
        for stream in fresh {
            inner.insert(stream);
        }
        Ok(())
    }

    /// List all streams for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<ChangeStreamDef> {
        self.read()
            .by_name
            .iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|(_, a)| (**a).clone())
            .collect()
    }

    /// Load all streams from the catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let streams = match catalog.load_all_change_streams() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load change streams from catalog");
                return;
            }
        };
        if streams.is_empty() {
            return;
        }
        let mut inner = self.write();
        let count_before = inner.by_name.len();
        for stream in streams {
            inner.insert(stream);
        }
        tracing::info!(
            count = inner.by_name.len() - count_before,
            "loaded change streams from catalog"
        );
    }

    /// Total number of registered streams.
    pub fn len(&self) -> usize {
        self.read().by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for StreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::stream_def::*;

    fn sample_def(name: &str, collection: &str) -> ChangeStreamDef {
        ChangeStreamDef {
            tenant_id: 1,
            name: name.into(),
            collection: collection.into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn register_and_lookup() {
        let reg = StreamRegistry::new();
        reg.register(sample_def("orders_stream", "orders"));

        assert!(reg.get(1, "orders_stream").is_some());
        assert!(reg.get(1, "nonexistent").is_none());
        assert!(reg.get(2, "orders_stream").is_none());
    }

    #[test]
    fn find_matching_collection() {
        let reg = StreamRegistry::new();
        reg.register(sample_def("s1", "orders"));
        reg.register(sample_def("s2", "orders"));
        reg.register(sample_def("s3", "users"));

        let matches = reg.find_matching(1, "orders");
        assert_eq!(matches.len(), 2);

        let matches = reg.find_matching(1, "users");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn wildcard_matches_everything() {
        let reg = StreamRegistry::new();
        reg.register(sample_def("all", "*"));

        assert_eq!(reg.find_matching(1, "orders").len(), 1);
        assert_eq!(reg.find_matching(1, "users").len(), 1);
        assert_eq!(reg.find_matching(1, "anything").len(), 1);
    }

    #[test]
    fn unregister() {
        let reg = StreamRegistry::new();
        reg.register(sample_def("s1", "orders"));
        assert!(reg.unregister(1, "s1"));
        assert!(!reg.unregister(1, "s1"));
        assert!(reg.is_empty());
    }
}
