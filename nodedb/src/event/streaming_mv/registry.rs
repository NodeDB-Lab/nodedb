//! In-memory registry of streaming materialized views.

use std::collections::HashMap;
use std::sync::RwLock;

use super::state::MvState;
use super::types::StreamingMvDef;

/// In-memory streaming MV registry.
pub struct MvRegistry {
    /// (tenant_id, mv_name) → definition.
    defs: RwLock<HashMap<(u32, String), StreamingMvDef>>,
    /// (tenant_id, mv_name) → live aggregate state.
    states: RwLock<HashMap<(u32, String), std::sync::Arc<MvState>>>,
}

impl MvRegistry {
    pub fn new() -> Self {
        Self {
            defs: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Register a streaming MV and create its state.
    pub fn register(&self, def: StreamingMvDef) {
        let key = (def.tenant_id, def.name.clone());
        let state = std::sync::Arc::new(MvState::new(
            def.name.clone(),
            def.group_by_columns.clone(),
            def.aggregates.clone(),
        ));

        let mut defs = self.defs.write().unwrap_or_else(|p| p.into_inner());
        defs.insert(key.clone(), def);

        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        states.insert(key, state);
    }

    /// Unregister a streaming MV. Returns true if it existed.
    pub fn unregister(&self, tenant_id: u32, name: &str) -> bool {
        let key = (tenant_id, name.to_string());
        let mut defs = self.defs.write().unwrap_or_else(|p| p.into_inner());
        let existed = defs.remove(&key).is_some();

        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        states.remove(&key);

        existed
    }

    /// Get the definition of a streaming MV.
    pub fn get_def(&self, tenant_id: u32, name: &str) -> Option<StreamingMvDef> {
        let key = (tenant_id, name.to_string());
        let defs = self.defs.read().unwrap_or_else(|p| p.into_inner());
        defs.get(&key).cloned()
    }

    /// Get the live state of a streaming MV.
    pub fn get_state(&self, tenant_id: u32, name: &str) -> Option<std::sync::Arc<MvState>> {
        let key = (tenant_id, name.to_string());
        let states = self.states.read().unwrap_or_else(|p| p.into_inner());
        states.get(&key).cloned()
    }

    /// Find all MVs that source from a given stream.
    pub fn find_by_source(
        &self,
        tenant_id: u32,
        stream_name: &str,
    ) -> Vec<std::sync::Arc<MvState>> {
        let defs = self.defs.read().unwrap_or_else(|p| p.into_inner());
        let states = self.states.read().unwrap_or_else(|p| p.into_inner());

        defs.iter()
            .filter(|((tid, _), def)| *tid == tenant_id && def.source_stream == stream_name)
            .filter_map(|(key, _)| states.get(key).cloned())
            .collect()
    }

    /// Clear all entries and reload from catalog.
    /// Used by the recovery verifier repair path.
    pub fn clear_and_reload(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let fresh = catalog.load_all_streaming_mvs()?;
        let mut defs = self.defs.write().unwrap_or_else(|p| p.into_inner());
        let mut states = self.states.write().unwrap_or_else(|p| p.into_inner());
        defs.clear();
        states.clear();
        for mv in fresh {
            let key = (mv.tenant_id, mv.name.clone());
            let state = std::sync::Arc::new(crate::event::streaming_mv::state::MvState::new(
                mv.name.clone(),
                mv.group_by_columns.clone(),
                mv.aggregates.clone(),
            ));
            defs.insert(key.clone(), mv);
            states.insert(key, state);
        }
        Ok(())
    }

    /// List all MV definitions (all tenants).
    pub fn list_all(&self) -> Vec<StreamingMvDef> {
        let defs = self.defs.read().unwrap_or_else(|p| p.into_inner());
        defs.values().cloned().collect()
    }

    /// List all MV definitions for a tenant.
    pub fn list_for_tenant(&self, tenant_id: u32) -> Vec<StreamingMvDef> {
        let defs = self.defs.read().unwrap_or_else(|p| p.into_inner());
        defs.values()
            .filter(|d| d.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Load from catalog on startup.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) {
        let mvs = match catalog.load_all_streaming_mvs() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load streaming MVs from catalog");
                return;
            }
        };
        for mv in mvs {
            self.register(mv);
        }
    }
}

impl Default for MvRegistry {
    fn default() -> Self {
        Self::new()
    }
}
