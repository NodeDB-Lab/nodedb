// SPDX-License-Identifier: BUSL-1.1

//! In-memory sequence registry with lock-free counters.
//!
//! Loaded from catalog on startup. Provides nextval/currval/setval operations.
//! State is persisted back to the catalog on checkpoint/shutdown.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::security::catalog::types::SystemCatalog;

use super::format::{self, FormatContext, ResetScope};
use super::gap_free::GapFreeManager;
use super::types::{SequenceError, SequenceHandle};

/// In-memory registry of all sequences, keyed by
/// `"{database_id}:{tenant_id}:{name}"`.
///
/// Loaded from the system catalog on startup. `nextval` operates on lock-free
/// atomic counters — the RwLock is only held during create/drop/alter (DDL).
///
/// The database segment scopes the entry. Two databases in one tenant can hold
/// a same-named sequence, and a shared key makes both hand out one counter.
pub struct SequenceRegistry {
    /// Sequences keyed by `"{database_id}:{tenant_id}:{name}"`.
    sequences: RwLock<HashMap<String, SequenceHandle>>,
    /// GAP_FREE reservation manager (shared across all sequences).
    gap_free: GapFreeManager,
}

impl SequenceRegistry {
    pub fn new() -> Self {
        Self {
            sequences: RwLock::new(HashMap::new()),
            gap_free: GapFreeManager::new(),
        }
    }

    /// Access the GAP_FREE manager (for commit/rollback from transaction lifecycle).
    pub fn gap_free_manager(&self) -> &GapFreeManager {
        &self.gap_free
    }

    /// Load all sequences from the catalog on startup.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) {
        let all_defs = match catalog.load_all_sequences() {
            Ok(defs) => defs,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load sequences from catalog");
                return;
            }
        };

        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());
        for def in all_defs {
            let key = registry_key(def.database_id, def.tenant_id, &def.name);
            // Load persisted state if available.
            let state = catalog
                .get_sequence_state(def.database_id, def.tenant_id, &def.name)
                .ok()
                .flatten();
            map.insert(key, SequenceHandle::new(def, state));
        }
    }

    /// Create a new sequence. Returns error if it already exists.
    pub fn create(&self, def: StoredSequence) -> Result<(), SequenceError> {
        let key = registry_key(def.database_id, def.tenant_id, &def.name);
        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());

        if map.contains_key(&key) {
            return Err(SequenceError::AlreadyExists {
                name: def.name.clone(),
            });
        }

        map.insert(key, SequenceHandle::new(def, None));
        Ok(())
    }

    /// Remove a sequence. Returns error if not found.
    pub fn remove(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Result<(), SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let mut map = self.sequences.write().unwrap_or_else(|p| p.into_inner());

        if map.remove(&key).is_none() {
            return Err(SequenceError::NotFound {
                name: name.to_string(),
            });
        }
        Ok(())
    }

    /// Get the next value from a sequence (lock-free on the hot path).
    ///
    /// Falls back to this connection's ephemeral overlay (see
    /// `ddl_overlay::resolve`) when the shared map has no entry yet — a
    /// `CREATE SEQUENCE` this same transaction has buffered but not
    /// committed.
    pub fn nextval(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Result<i64, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            self.check_period_reset(handle);
            return handle.nextval();
        }
        drop(map);
        super::ddl_overlay::resolve(database_id, tenant_id, name, |handle| {
            self.check_period_reset(handle);
            handle.nextval()
        })
        .unwrap_or_else(|| {
            Err(SequenceError::NotFound {
                name: name.to_string(),
            })
        })
    }

    /// Get the next value, returning a formatted string if format is defined.
    ///
    /// Returns `Ok(SequenceValue::Int(i64))` for plain sequences, or
    /// `Ok(SequenceValue::Formatted(String))` for sequences with FORMAT.
    pub fn nextval_formatted(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        tenant_code: &str,
        session_vars: &std::collections::HashMap<String, String>,
    ) -> Result<SequenceValue, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            self.check_period_reset(handle);
            let raw = handle.nextval()?;
            return Ok(Self::format_nextval(handle, raw, tenant_code, session_vars));
        }
        drop(map);
        super::ddl_overlay::resolve(
            database_id,
            tenant_id,
            name,
            |handle| -> Result<SequenceValue, SequenceError> {
                self.check_period_reset(handle);
                let raw = handle.nextval()?;
                Ok(Self::format_nextval(handle, raw, tenant_code, session_vars))
            },
        )
        .unwrap_or_else(|| {
            Err(SequenceError::NotFound {
                name: name.to_string(),
            })
        })
    }

    /// Format a freshly-advanced `raw` value per `handle`'s `FORMAT` template,
    /// or return it unformatted when the sequence has none.
    fn format_nextval(
        handle: &SequenceHandle,
        raw: i64,
        tenant_code: &str,
        session_vars: &std::collections::HashMap<String, String>,
    ) -> SequenceValue {
        match &handle.def.format_template {
            Some(tokens) => {
                let ctx = FormatContext::now(raw, tenant_code, session_vars);
                SequenceValue::Formatted(format::format_sequence_value(tokens, &ctx))
            }
            None => SequenceValue::Int(raw),
        }
    }

    /// Peek at the next value without consuming it.
    pub fn next_preview(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        tenant_code: &str,
        session_vars: &std::collections::HashMap<String, String>,
    ) -> Result<SequenceValue, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        let next_raw = handle.current_value() + handle.def.increment;

        match &handle.def.format_template {
            Some(tokens) => {
                let ctx = FormatContext::now(next_raw, tenant_code, session_vars);
                let formatted = format::format_sequence_value(tokens, &ctx);
                Ok(SequenceValue::Formatted(formatted))
            }
            None => Ok(SequenceValue::Int(next_raw)),
        }
    }

    /// Check and apply period reset if the reset scope has changed period.
    fn check_period_reset(&self, handle: &SequenceHandle) {
        if handle.def.reset_scope == ResetScope::Never {
            return;
        }

        let dt = nodedb_types::NdbDateTime::now();
        let c = dt.components();
        let new_pk =
            format::compute_period_key(&handle.def.reset_scope, c.year as u16, c.month, c.day);
        handle.check_period_reset(&new_pk);
    }

    /// Get N values from a sequence in one atomic batch.
    pub fn nextval_batch(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        n: usize,
    ) -> Result<Vec<i64>, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;
        handle.nextval_batch(n)
    }

    /// Read this NODE's counter — the last value any session on this node
    /// allocated. SQL `currval` is session-scoped and does not read this;
    /// range allocation and diagnostics do.
    pub fn node_current_value(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> Result<i64, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            return handle.currval();
        }
        drop(map);
        super::ddl_overlay::resolve(database_id, tenant_id, name, SequenceHandle::currval)
            .unwrap_or_else(|| {
                Err(SequenceError::NotFound {
                    name: name.to_string(),
                })
            })
    }

    /// Set the counter to a specific value.
    pub fn setval(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        value: i64,
    ) -> Result<i64, SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            return handle.setval(value);
        }
        drop(map);
        super::ddl_overlay::resolve(database_id, tenant_id, name, |handle| handle.setval(value))
            .unwrap_or_else(|| {
                Err(SequenceError::NotFound {
                    name: name.to_string(),
                })
            })
    }

    /// Restart a sequence at a new value (ALTER SEQUENCE ... RESTART WITH).
    pub fn restart(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        restart_value: i64,
    ) -> Result<(), SequenceError> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        let handle = map.get(&key).ok_or_else(|| SequenceError::NotFound {
            name: name.to_string(),
        })?;

        handle.setval(restart_value)?;
        Ok(())
    }

    /// List a tenant's sequences in one database. Returns (name,
    /// current_value, is_called).
    pub fn list(&self, database_id: u64, tenant_id: u64) -> Vec<(String, i64, bool)> {
        let prefix = format!("{database_id}:{tenant_id}:");
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        map.iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, handle)| {
                (
                    handle.def.name.clone(),
                    handle.current_value(),
                    handle.is_called(),
                )
            })
            .collect()
    }

    /// Persist all sequence states to the catalog (for checkpoint/shutdown).
    pub fn persist_all(&self, catalog: &SystemCatalog) {
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        for handle in map.values() {
            let state = SequenceState {
                database_id: handle.def.database_id,
                tenant_id: handle.def.tenant_id,
                name: handle.def.name.clone(),
                current_value: handle.current_value(),
                is_called: handle.is_called(),
                epoch: handle.def.epoch,
                period_key: handle.period_key(),
            };
            if let Err(e) = catalog.put_sequence_state(&state) {
                tracing::warn!(
                    sequence = %handle.def.name,
                    error = %e,
                    "failed to persist sequence state"
                );
            }
        }
    }

    /// Read-lock the sequences map (for GAP_FREE rollback access).
    pub fn sequences_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, SequenceHandle>> {
        self.sequences.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Check if a sequence exists, including one this connection has
    /// buffered a not-yet-committed `CREATE SEQUENCE` for.
    pub fn exists(&self, database_id: u64, tenant_id: u64, name: &str) -> bool {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if map.contains_key(&key) {
            return true;
        }
        drop(map);
        super::ddl_overlay::resolve(database_id, tenant_id, name, |_| ()).is_some()
    }

    /// Get a sequence definition (for SHOW SEQUENCES detail), including one
    /// this connection has buffered a not-yet-committed `CREATE SEQUENCE` for.
    pub fn get_def(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<StoredSequence> {
        let key = registry_key(database_id, tenant_id, name);
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());
        if let Some(handle) = map.get(&key) {
            return Some(handle.def.clone());
        }
        drop(map);
        super::ddl_overlay::resolve(database_id, tenant_id, name, |handle| handle.def.clone())
    }

    /// Reset all sequences attached to a collection back to their start values.
    ///
    /// Used by `TRUNCATE ... RESTART IDENTITY`. Finds all sequences whose names
    /// match the implicit pattern `{collection}_{field}_seq` for the given
    /// database and tenant, and resets each to its `start_value`.
    pub fn restart_sequences_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) {
        let prefix = format!("{database_id}:{tenant_id}:{collection}_");
        let suffix = "_seq";
        let map = self.sequences.read().unwrap_or_else(|p| p.into_inner());

        for (key, handle) in map.iter() {
            if key.starts_with(&prefix) && handle.def.name.ends_with(suffix) {
                let start = handle.def.start_value;
                if let Err(e) = handle.setval(start) {
                    tracing::warn!(
                        sequence = %handle.def.name,
                        error = %e,
                        "failed to restart sequence during TRUNCATE RESTART IDENTITY"
                    );
                }
            }
        }
    }
}

impl Default for SequenceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Return type for `nextval_formatted` — either raw integer or formatted string.
#[derive(Debug, Clone)]
pub enum SequenceValue {
    /// Raw integer value (no format template).
    Int(i64),
    /// Formatted string (format template resolved).
    Formatted(String),
}

pub(crate) fn registry_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn put(database_id: u64, tenant_id: u64, name: &str) -> CatalogEntry {
        CatalogEntry::PutSequence(Box::new(StoredSequence::new(
            database_id,
            tenant_id,
            name.to_owned(),
            "alice".into(),
        )))
    }

    #[tokio::test]
    async fn shared_map_miss_falls_back_to_a_buffered_create() {
        let registry = SequenceRegistry::new();
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put(4, 1, "orders_seq")));

            assert!(registry.exists(4, 1, "orders_seq"));
            assert_eq!(
                registry.get_def(4, 1, "orders_seq").map(|d| d.name),
                Some("orders_seq".to_owned())
            );
            assert_eq!(registry.nextval(4, 1, "orders_seq").unwrap(), 1);
            assert_eq!(registry.nextval(4, 1, "orders_seq").unwrap(), 2);
            assert_eq!(registry.node_current_value(4, 1, "orders_seq").unwrap(), 2);
            assert_eq!(registry.setval(4, 1, "orders_seq", 10).unwrap(), 10);
            assert_eq!(registry.node_current_value(4, 1, "orders_seq").unwrap(), 10);
        })
        .await;
    }

    #[tokio::test]
    async fn shared_map_entry_wins_over_a_buffered_create_of_the_same_name() {
        let registry = SequenceRegistry::new();
        registry
            .create(StoredSequence::new(
                4,
                1,
                "orders_seq".into(),
                "alice".into(),
            ))
            .unwrap();
        registry.nextval(4, 1, "orders_seq").unwrap(); // advances the shared handle to 1

        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put(4, 1, "orders_seq")));
            // The shared (committed) handle already has this name, so the
            // buffered fallback must never shadow it with a fresh counter.
            assert_eq!(registry.nextval(4, 1, "orders_seq").unwrap(), 2);
        })
        .await;
    }

    /// One name, two databases, one tenant: each database gets its own
    /// counter. Drop the `database_id` segment from the key and this fails.
    #[test]
    fn two_databases_hold_separate_counters_for_one_name() {
        let registry = SequenceRegistry::new();
        registry
            .create(StoredSequence::new(1, 7, "order_id".into(), "alice".into()))
            .unwrap();
        registry
            .create(StoredSequence::new(2, 7, "order_id".into(), "alice".into()))
            .unwrap();

        assert_eq!(registry.nextval(1, 7, "order_id").unwrap(), 1);
        assert_eq!(registry.nextval(1, 7, "order_id").unwrap(), 2);
        assert_eq!(
            registry.nextval(2, 7, "order_id").unwrap(),
            1,
            "the second database starts its own count"
        );

        assert_eq!(registry.list(1, 7).len(), 1);
        assert_eq!(registry.list(2, 7).len(), 1);

        registry.remove(1, 7, "order_id").unwrap();
        assert!(!registry.exists(1, 7, "order_id"));
        assert!(registry.exists(2, 7, "order_id"));
    }

    #[tokio::test]
    async fn no_buffer_and_no_shared_entry_is_not_found() {
        let registry = SequenceRegistry::new();
        conn_scope::scoped(async {
            assert!(!registry.exists(4, 1, "ghost_seq"));
            assert!(registry.get_def(4, 1, "ghost_seq").is_none());
            assert!(matches!(
                registry.nextval(4, 1, "ghost_seq"),
                Err(SequenceError::NotFound { .. })
            ));
        })
        .await;
    }
}
