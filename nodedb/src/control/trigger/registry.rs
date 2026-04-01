//! In-memory trigger registry.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.
//! Queried by the trigger fire logic on every DML operation.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::control::security::catalog::trigger_types::StoredTrigger;

/// In-memory trigger registry for fast lookup during DML.
///
/// Triggers are indexed by `(tenant_id, collection)` for efficient matching.
/// Thread-safe (RwLock) — reads are concurrent, writes are exclusive.
pub struct TriggerRegistry {
    /// (tenant_id, collection_name) → sorted list of triggers.
    by_collection: RwLock<HashMap<(u32, String), Vec<StoredTrigger>>>,
}

impl Default for TriggerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            by_collection: RwLock::new(HashMap::new()),
        }
    }

    /// Load all triggers (all tenants) from the catalog on startup.
    pub fn load_all(&self, catalog: &crate::control::security::catalog::types::SystemCatalog) {
        let triggers = match catalog.load_all_triggers() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load triggers from catalog");
                return;
            }
        };
        if triggers.is_empty() {
            return;
        }
        let mut map = match self.by_collection.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        for trigger in triggers {
            let key = (trigger.tenant_id, trigger.collection.clone());
            map.entry(key).or_default().push(trigger);
        }
        for list in map.values_mut() {
            list.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        }
        let count: usize = map.values().map(|v| v.len()).sum();
        tracing::info!(triggers = count, "loaded triggers from catalog");
    }

    /// Load all triggers for a tenant from the catalog.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
        tenant_id: u32,
    ) {
        let triggers = match catalog.load_triggers_for_tenant(tenant_id) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(tenant_id, error = %e, "failed to load triggers");
                return;
            }
        };

        let mut map = match self.by_collection.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };

        for trigger in triggers {
            let key = (trigger.tenant_id, trigger.collection.clone());
            map.entry(key).or_default().push(trigger);
        }

        // Sort each list by (priority, name) for deterministic execution order.
        for list in map.values_mut() {
            list.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        }
    }

    /// Register a new trigger (called by CREATE TRIGGER DDL).
    pub fn register(&self, trigger: StoredTrigger) {
        let mut map = match self.by_collection.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        let key = (trigger.tenant_id, trigger.collection.clone());
        let list = map.entry(key).or_default();

        // Remove existing trigger with same name (for CREATE OR REPLACE).
        list.retain(|t| t.name != trigger.name);
        list.push(trigger);
        list.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    }

    /// Unregister a trigger (called by DROP TRIGGER DDL).
    pub fn unregister(&self, tenant_id: u32, name: &str) {
        let mut map = match self.by_collection.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        for list in map.values_mut() {
            list.retain(|t| !(t.tenant_id == tenant_id && t.name == name));
        }
    }

    /// Enable or disable a trigger.
    pub fn set_enabled(&self, tenant_id: u32, name: &str, enabled: bool) {
        let mut map = match self.by_collection.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        for list in map.values_mut() {
            for t in list.iter_mut() {
                if t.tenant_id == tenant_id && t.name == name {
                    t.enabled = enabled;
                }
            }
        }
    }

    /// Get all enabled triggers for a (tenant, collection) matching an event.
    ///
    /// Returns triggers sorted by (priority, name) — deterministic execution order.
    pub fn get_matching(
        &self,
        tenant_id: u32,
        collection: &str,
        event: DmlEvent,
    ) -> Vec<StoredTrigger> {
        let map = match self.by_collection.read() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        let key = (tenant_id, collection.to_string());
        match map.get(&key) {
            Some(list) => list
                .iter()
                .filter(|t| t.enabled && matches_event(t, event))
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// List all triggers for a tenant (for SHOW TRIGGERS).
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<StoredTrigger> {
        let map = match self.by_collection.read() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        let mut result = Vec::new();
        for list in map.values() {
            for t in list {
                if t.tenant_id == tenant_id {
                    result.push(t.clone());
                }
            }
        }
        result
    }
}

/// DML event type for trigger matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlEvent {
    Insert,
    Update,
    Delete,
}

impl DmlEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

fn matches_event(trigger: &StoredTrigger, event: DmlEvent) -> bool {
    match event {
        DmlEvent::Insert => trigger.events.on_insert,
        DmlEvent::Update => trigger.events.on_update,
        DmlEvent::Delete => trigger.events.on_delete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::trigger_types::*;

    fn sample(name: &str, coll: &str, timing: TriggerTiming, insert: bool) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 1,
            name: name.to_string(),
            collection: coll.to_string(),
            timing,
            events: TriggerEvents {
                on_insert: insert,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN RETURN; END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::default(),
            security: TriggerSecurity::default(),
            batch_mode: TriggerBatchMode::default(),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn register_and_match() {
        let reg = TriggerRegistry::new();
        reg.register(sample("t1", "orders", TriggerTiming::After, true));
        let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].name, "t1");
    }

    #[test]
    fn no_match_wrong_event() {
        let reg = TriggerRegistry::new();
        reg.register(sample("t1", "orders", TriggerTiming::After, true));
        assert!(reg.get_matching(1, "orders", DmlEvent::Delete).is_empty());
    }

    #[test]
    fn no_match_wrong_collection() {
        let reg = TriggerRegistry::new();
        reg.register(sample("t1", "orders", TriggerTiming::After, true));
        assert!(reg.get_matching(1, "users", DmlEvent::Insert).is_empty());
    }

    #[test]
    fn disabled_not_matched() {
        let reg = TriggerRegistry::new();
        reg.register(sample("t1", "orders", TriggerTiming::After, true));
        reg.set_enabled(1, "t1", false);
        assert!(reg.get_matching(1, "orders", DmlEvent::Insert).is_empty());
    }

    #[test]
    fn sorted_by_priority() {
        let reg = TriggerRegistry::new();
        let mut t2 = sample("b_trigger", "c", TriggerTiming::After, true);
        t2.priority = 10;
        let mut t1 = sample("a_trigger", "c", TriggerTiming::After, true);
        t1.priority = 5;
        reg.register(t2);
        reg.register(t1);
        let matched = reg.get_matching(1, "c", DmlEvent::Insert);
        assert_eq!(matched[0].name, "a_trigger"); // priority 5 first
        assert_eq!(matched[1].name, "b_trigger"); // priority 10 second
    }

    #[test]
    fn unregister() {
        let reg = TriggerRegistry::new();
        reg.register(sample("t1", "c", TriggerTiming::After, true));
        reg.unregister(1, "t1");
        assert!(reg.get_matching(1, "c", DmlEvent::Insert).is_empty());
    }
}
