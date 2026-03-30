//! In-memory registry of active change streams.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! Queried by the CDC router on every WriteEvent for matching streams.
//!
//! Thread-safe (RwLock) — reads are concurrent, writes are exclusive.

use std::collections::HashMap;
use std::sync::RwLock;

use super::stream_def::ChangeStreamDef;

/// In-memory change stream registry for fast routing.
///
/// Streams are indexed by `(tenant_id, stream_name)` for DDL operations,
/// and by `(tenant_id, collection)` for event routing (multiple streams
/// can watch the same collection).
pub struct StreamRegistry {
    /// All stream definitions, keyed by `(tenant_id, stream_name)`.
    by_name: RwLock<HashMap<(u32, String), ChangeStreamDef>>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new change stream.
    pub fn register(&self, def: ChangeStreamDef) {
        let key = (def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// Unregister a change stream by name. Returns true if it existed.
    pub fn unregister(&self, tenant_id: u32, name: &str) -> bool {
        let key = (tenant_id, name.to_string());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    /// Get a stream definition by name.
    pub fn get(&self, tenant_id: u32, name: &str) -> Option<ChangeStreamDef> {
        let key = (tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    /// Find all streams that match a given (tenant_id, collection) pair.
    /// Used by the CDC router on every event for routing.
    pub fn find_matching(&self, tenant_id: u32, collection: &str) -> Vec<ChangeStreamDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|def| def.tenant_id == tenant_id && def.matches_collection(collection))
            .cloned()
            .collect()
    }

    /// List all streams for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<ChangeStreamDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|def| def.tenant_id == tenant_id)
            .cloned()
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
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        for stream in streams {
            let key = (stream.tenant_id, stream.name.clone());
            map.insert(key, stream);
        }
        tracing::info!(count = map.len(), "loaded change streams from catalog");
    }

    /// Total number of registered streams.
    pub fn len(&self) -> usize {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.len()
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
        assert!(!reg.unregister(1, "s1")); // Already gone.
        assert!(reg.is_empty());
    }
}
