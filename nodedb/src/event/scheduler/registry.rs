//! In-memory registry of scheduled jobs.
//!
//! Loaded from the system catalog on startup. Updated by DDL handlers.

use std::collections::HashMap;
use std::sync::RwLock;

use super::types::ScheduleDef;

/// In-memory schedule registry.
pub struct ScheduleRegistry {
    /// (tenant_id, schedule_name) → ScheduleDef.
    by_name: RwLock<HashMap<(u32, String), ScheduleDef>>,
}

impl ScheduleRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, def: ScheduleDef) {
        let key = (def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    pub fn unregister(&self, tenant_id: u32, name: &str) -> bool {
        let key = (tenant_id, name.to_string());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    pub fn get(&self, tenant_id: u32, name: &str) -> Option<ScheduleDef> {
        let key = (tenant_id, name.to_string());
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).cloned()
    }

    /// Update an existing schedule definition (ALTER SCHEDULE).
    pub fn update(&self, def: ScheduleDef) {
        let key = (def.tenant_id, def.name.clone());
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, def);
    }

    /// List all enabled schedules (all tenants). Used by the scheduler loop.
    pub fn list_all_enabled(&self) -> Vec<ScheduleDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values().filter(|s| s.enabled).cloned().collect()
    }

    /// List all schedules for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<ScheduleDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|s| s.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Load from catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let schedules = match catalog.load_all_schedules() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load schedules from catalog");
                return;
            }
        };
        if schedules.is_empty() {
            return;
        }
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        for sched in schedules {
            let key = (sched.tenant_id, sched.name.clone());
            map.insert(key, sched);
        }
        tracing::info!(count = map.len(), "loaded schedules from catalog");
    }
}

impl Default for ScheduleRegistry {
    fn default() -> Self {
        Self::new()
    }
}
