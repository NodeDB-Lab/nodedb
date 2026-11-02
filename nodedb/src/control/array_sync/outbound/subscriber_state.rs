// SPDX-License-Identifier: BUSL-1.1

//! Per-subscriber HLC cursor persistence for outbound array sync.
//!
//! `ArraySubscriberState` records the HLC watermark of the last op delivered
//! to each `(session_id, database_id, tenant_id, array_name)` scope. This
//! lets Origin resume delivery after a reconnect without re-sending
//! already-applied ops.
//!
//! # Storage layout
//!
//! Subscriber cursors are keyed in the Origin op-log redb database under:
//!
//! ```text
//! "array.subscriber:v3:{session_id}:{database_id}:{tenant_id}:{array_name}"  →  msgpack(ArraySubscriberState)
//! ```
//!
//! A separate redb table is used so cursor writes never interfere with
//! op-log reads.

use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::{DatabaseId, sync::shape::ArrayCoordRange};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

// ─── State struct ─────────────────────────────────────────────────────────────

/// Serializable cursor for one `(session, database, tenant, array)` scope.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ArraySubscriberState {
    /// The sync session this subscriber belongs to.
    pub session_id: String,
    /// Database namespace of the subscribed array.
    pub database_id: DatabaseId,
    /// Authenticated tenant namespace of the subscribed array.
    pub tenant_id: u64,
    /// The array being subscribed.
    pub array_name: String,
    /// Highest HLC whose op has been confirmed-enqueued to this subscriber.
    ///
    /// `Hlc::ZERO` on first registration, which triggers a full backfill.
    pub last_pushed_hlc: Hlc,
    /// Optional coordinate range filter. `None` = all ops on the array.
    pub coord_range: Option<ArrayCoordRange>,
}

impl ArraySubscriberState {
    /// Construct a fresh subscriber cursor starting from `Hlc::ZERO`.
    pub fn new(
        session_id: String,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: String,
        coord_range: Option<ArrayCoordRange>,
    ) -> Self {
        Self {
            session_id,
            database_id,
            tenant_id,
            array_name,
            last_pushed_hlc: Hlc::ZERO,
            coord_range,
        }
    }
}

// ─── In-memory map ────────────────────────────────────────────────────────────

/// In-memory map of subscriber cursors.
///
/// The canonical copy is written to the backing store on every `mark_sent`.
/// On startup the backing store is loaded into memory by the owner
/// (`OriginSchemaRegistry`-style: persist once, hold in Arc).
///
/// Keyed by `(session_id, database_id, tenant_id, array_name)`.
type CursorKey = (String, DatabaseId, u64, String);

/// Thread-safe in-memory map of all active subscriber cursors.
pub struct SubscriberMap {
    inner: RwLock<HashMap<CursorKey, ArraySubscriberState>>,
    /// Backing store for persistence (Origin redb database handle).
    store: Arc<SubscriberStore>,
}

impl crate::storage::RedbBacked for SubscriberMap {
    fn redb_database(&self) -> &redb::Database {
        &self.store.db
    }
}

impl SubscriberMap {
    /// Construct from a pre-loaded backing store.
    pub fn new(store: Arc<SubscriberStore>) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            store,
        }
    }

    /// Register a new subscriber (or restore an existing one from the store).
    ///
    /// Returns the current `ArraySubscriberState` (can have a non-ZERO
    /// `last_pushed_hlc` if the subscriber previously connected).
    pub fn register(
        &self,
        session_id: &str,
        array_name: &str,
        coord_range: Option<ArrayCoordRange>,
    ) -> ArraySubscriberState {
        self.register_in_database(session_id, DatabaseId::DEFAULT, 0, array_name, coord_range)
    }

    pub fn register_in_database(
        &self,
        session_id: &str,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: &str,
        coord_range: Option<ArrayCoordRange>,
    ) -> ArraySubscriberState {
        let key = (
            session_id.to_string(),
            database_id,
            tenant_id,
            array_name.to_string(),
        );
        let persisted = self
            .store
            .load(session_id, database_id, tenant_id, array_name);
        let state = persisted.unwrap_or_else(|| {
            ArraySubscriberState::new(
                session_id.to_string(),
                database_id,
                tenant_id,
                array_name.to_string(),
                coord_range,
            )
        });
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        map.insert(key, state.clone());
        state
    }

    /// Update the tenant-0/default-database cursor for `(session_id, array_name)`.
    ///
    /// Persists the updated state immediately so restarts pick up where
    /// delivery left off.
    pub fn mark_sent(&self, session_id: &str, array_name: &str, new_hlc: Hlc) {
        let key = (
            session_id.to_string(),
            DatabaseId::DEFAULT,
            0,
            array_name.to_string(),
        );
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = map.get_mut(&key)
            && new_hlc > state.last_pushed_hlc
        {
            state.last_pushed_hlc = new_hlc;
            if let Err(e) = self.store.save(state) {
                warn!(
                    session = %session_id,
                    array = %array_name,
                    error = %e,
                    "subscriber_state: failed to persist cursor — cursor will reset on restart"
                );
            }
        }
    }

    /// Advance a cursor in its explicit database namespace.
    pub fn mark_sent_in_database(
        &self,
        session_id: &str,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: &str,
        new_hlc: Hlc,
    ) {
        let key = (
            session_id.to_string(),
            database_id,
            tenant_id,
            array_name.to_string(),
        );
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = map.get_mut(&key)
            && new_hlc > state.last_pushed_hlc
        {
            state.last_pushed_hlc = new_hlc;
            if let Err(e) = self.store.save(state) {
                warn!(session = %session_id, database_id = database_id.as_u64(), tenant_id, array = %array_name, error = %e,
                    "subscriber_state: failed to persist cursor — cursor will reset on restart");
            }
        }
    }

    /// Retrieve a cursor in its explicit database namespace.
    pub fn get_in_database(
        &self,
        session_id: &str,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: &str,
    ) -> Option<ArraySubscriberState> {
        let map = self.inner.read().unwrap_or_else(|p| p.into_inner());
        map.get(&(
            session_id.to_string(),
            database_id,
            tenant_id,
            array_name.to_string(),
        ))
        .cloned()
    }

    /// Remove all cursor entries for a session (disconnect cleanup).
    pub fn remove_session(&self, session_id: &str) {
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        map.retain(|(sid, _, _, _), _| sid != session_id);
        self.store.delete_session(session_id);
        debug!(session = %session_id, "subscriber_state: session cursors removed");
    }

    /// Get the current tenant-0/default-database cursor, if any.
    pub fn get(&self, session_id: &str, array_name: &str) -> Option<ArraySubscriberState> {
        let map = self.inner.read().unwrap_or_else(|p| p.into_inner());
        map.get(&(
            session_id.to_string(),
            DatabaseId::DEFAULT,
            0,
            array_name.to_string(),
        ))
        .cloned()
    }
}

// ─── Backing store ────────────────────────────────────────────────────────────

/// Subscriber cursor backing store (redb table in the Origin op-log database).
///
/// All methods are synchronous and thin wrappers around redb transactions.
/// Callers on the Control Plane call these directly (the Mutex-level latency
/// is acceptable; cursor writes are rare compared to op processing).
pub struct SubscriberStore {
    db: Arc<redb::Database>,
}

/// redb table for subscriber cursor persistence.
const CURSOR_TABLE: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("array_subscriber_cursors");

impl SubscriberStore {
    /// Open (or create) the cursor table in the given database.
    pub fn open(db: Arc<redb::Database>) -> crate::Result<Arc<Self>> {
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("subscriber_store begin_write: {e}"),
            })?;
            txn.open_table(CURSOR_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("subscriber_store open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("subscriber_store commit: {e}"),
            })?;
        }
        Ok(Arc::new(Self { db }))
    }

    /// An in-memory-only store for tests / no-persistence setups.
    pub fn in_memory() -> crate::Result<Arc<Self>> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| crate::Error::Storage {
                engine: "array_sync".into(),
                detail: format!("subscriber_store in_memory: {e}"),
            })?;
        Self::open(Arc::new(db))
    }

    fn cursor_key(
        session_id: &str,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: &str,
    ) -> String {
        format!(
            "array.subscriber:v3:{session_id}:{}:{tenant_id}:{array_name}",
            database_id.as_u64()
        )
    }

    /// Persist a subscriber cursor.
    fn save(&self, state: &ArraySubscriberState) -> crate::Result<()> {
        let key = Self::cursor_key(
            &state.session_id,
            state.database_id,
            state.tenant_id,
            &state.array_name,
        );
        let bytes = zerompk::to_msgpack_vec(state).map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("subscriber_store save encode: {e}"),
        })?;
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("subscriber_store save begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(CURSOR_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("subscriber_store save open_table: {e}"),
                })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("subscriber_store save insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "array_sync".into(),
            detail: format!("subscriber_store save commit: {e}"),
        })?;
        Ok(())
    }

    /// Load a subscriber cursor, returning `None` if not found.
    fn load(
        &self,
        session_id: &str,
        database_id: DatabaseId,
        tenant_id: u64,
        array_name: &str,
    ) -> Option<ArraySubscriberState> {
        let key = Self::cursor_key(session_id, database_id, tenant_id, array_name);
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(CURSOR_TABLE).ok()?;
        let bytes = table.get(key.as_str()).ok()??.value().to_vec();
        let mut state: ArraySubscriberState = zerompk::from_msgpack(&bytes).ok()?;
        state.database_id = database_id;
        state.tenant_id = tenant_id;
        Some(state)
    }

    /// Delete all cursors for a given session (disconnect cleanup).
    fn delete_session(&self, session_id: &str) {
        use redb::ReadableTable;
        let prefix = format!("array.subscriber:v3:{session_id}:");
        let Ok(txn) = self.db.begin_write() else {
            return;
        };
        let Ok(mut table) = txn.open_table(CURSOR_TABLE) else {
            return;
        };
        // Collect matching keys first (cannot delete during iteration).
        let keys_to_delete: Vec<String> = table
            .iter()
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let (k, _) = entry.ok()?;
                let key: &str = k.value();
                key.starts_with(&prefix).then(|| key.to_string())
            })
            .collect();

        for k in keys_to_delete {
            let _ = table.remove(k.as_str());
        }
        drop(table);
        let _ = txn.commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> Arc<SubscriberStore> {
        SubscriberStore::in_memory().expect("in-memory store should open")
    }

    #[test]
    fn register_fresh_starts_at_zero() {
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        let state = map.register("s1", "arr", None);
        assert_eq!(state.last_pushed_hlc, Hlc::ZERO);
        assert_eq!(state.session_id, "s1");
        assert_eq!(state.array_name, "arr");
    }

    #[test]
    fn mark_sent_advances_cursor() {
        use nodedb_array::sync::replica_id::ReplicaId;
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        map.register("s1", "arr", None);

        let hlc1 = Hlc::new(100, 0, ReplicaId::new(1)).unwrap();
        map.mark_sent("s1", "arr", hlc1);

        let state = map.get("s1", "arr").expect("state should exist");
        assert_eq!(state.last_pushed_hlc, hlc1);
    }

    #[test]
    fn mark_sent_does_not_go_backwards() {
        use nodedb_array::sync::replica_id::ReplicaId;
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        map.register("s1", "arr", None);

        let hlc2 = Hlc::new(200, 0, ReplicaId::new(1)).unwrap();
        let hlc1 = Hlc::new(100, 0, ReplicaId::new(1)).unwrap();
        map.mark_sent("s1", "arr", hlc2);
        map.mark_sent("s1", "arr", hlc1); // should be ignored

        let state = map.get("s1", "arr").expect("state should exist");
        assert_eq!(state.last_pushed_hlc, hlc2);
    }

    #[test]
    fn remove_session_clears_all_arrays() {
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        map.register("s1", "arr1", None);
        map.register("s1", "arr2", None);
        map.register("s2", "arr1", None);
        map.remove_session("s1");
        assert!(map.get("s1", "arr1").is_none());
        assert!(map.get("s1", "arr2").is_none());
        assert!(map.get("s2", "arr1").is_some());
    }

    #[test]
    fn same_name_in_different_databases_has_independent_cursor_and_persistence() {
        use nodedb_array::sync::replica_id::ReplicaId;
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        let h1 = Hlc::new(10, 0, ReplicaId::new(1)).unwrap();
        let h2 = Hlc::new(20, 0, ReplicaId::new(1)).unwrap();
        map.register_in_database("s1", db1, 0, "same", None);
        map.register_in_database("s1", db2, 0, "same", None);
        map.mark_sent_in_database("s1", db1, 0, "same", h1);
        map.mark_sent_in_database("s1", db2, 0, "same", h2);
        assert_eq!(
            map.get_in_database("s1", db1, 0, "same")
                .unwrap()
                .last_pushed_hlc,
            h1
        );
        assert_eq!(
            map.get_in_database("s1", db2, 0, "same")
                .unwrap()
                .last_pushed_hlc,
            h2
        );

        let reloaded = SubscriberMap::new(store);
        assert_eq!(
            reloaded
                .register_in_database("s1", db1, 0, "same", None)
                .last_pushed_hlc,
            h1
        );
        assert_eq!(
            reloaded
                .register_in_database("s1", db2, 0, "same", None)
                .last_pushed_hlc,
            h2
        );
    }

    #[test]
    fn same_database_and_array_are_isolated_by_tenant() {
        use nodedb_array::sync::replica_id::ReplicaId;
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        let database_id = DatabaseId::new(7);
        let tenant_one_hlc = Hlc::new(10, 0, ReplicaId::new(1)).unwrap();
        let tenant_two_hlc = Hlc::new(20, 0, ReplicaId::new(1)).unwrap();

        map.register_in_database("s1", database_id, 1, "same", None);
        map.register_in_database("s1", database_id, 2, "same", None);
        map.mark_sent_in_database("s1", database_id, 1, "same", tenant_one_hlc);
        map.mark_sent_in_database("s1", database_id, 2, "same", tenant_two_hlc);

        assert_eq!(
            map.get_in_database("s1", database_id, 1, "same")
                .unwrap()
                .last_pushed_hlc,
            tenant_one_hlc
        );
        assert_eq!(
            map.get_in_database("s1", database_id, 2, "same")
                .unwrap()
                .last_pushed_hlc,
            tenant_two_hlc
        );

        let reloaded = SubscriberMap::new(store);
        assert_eq!(
            reloaded
                .register_in_database("s1", database_id, 1, "same", None)
                .last_pushed_hlc,
            tenant_one_hlc
        );
        assert_eq!(
            reloaded
                .register_in_database("s1", database_id, 2, "same", None)
                .last_pushed_hlc,
            tenant_two_hlc
        );
    }

    #[test]
    fn cursor_persists_across_store_loads() {
        use nodedb_array::sync::replica_id::ReplicaId;
        let store = make_store();
        let map = SubscriberMap::new(Arc::clone(&store));
        map.register("s1", "arr", None);
        let hlc = Hlc::new(42, 0, ReplicaId::new(1)).unwrap();
        map.mark_sent("s1", "arr", hlc);

        // Simulate a new in-memory map reading from the same store.
        let map2 = SubscriberMap::new(Arc::clone(&store));
        let loaded = map2.register("s1", "arr", None);
        assert_eq!(loaded.last_pushed_hlc, hlc);
    }
}
