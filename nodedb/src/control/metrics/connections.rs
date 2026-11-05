// SPDX-License-Identifier: BUSL-1.1

//! Live connection counts per database across pgwire and native sessions.
//!
//! pgwire connections are folded from each listener's `SessionStore`. Native
//! connections own a private store, so they register a [`NativeConnectionGuard`]
//! that holds one count for the connection's lifetime.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Weak};

use nodedb_types::DatabaseId;

use super::database::DatabaseMetricsRegistry;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

/// Registry of pgwire session stores and the native per-database counter.
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    pgwire: RwLock<Vec<Weak<SessionStore>>>,
    native: Mutex<HashMap<DatabaseId, u64>>,
}

/// Holds one native connection count on a database until dropped.
#[must_use = "dropping the guard releases the connection count"]
#[derive(Debug)]
pub struct NativeConnectionGuard {
    tracker: Arc<ConnectionTracker>,
    db: DatabaseId,
}

impl Drop for NativeConnectionGuard {
    fn drop(&mut self) {
        let mut native = self
            .tracker
            .native
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(count) = native.get_mut(&self.db) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                native.remove(&self.db);
            }
        }
    }
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Track a pgwire listener's session store. Dead stores are pruned.
    pub fn register_pgwire(&self, store: &Arc<SessionStore>) {
        let mut stores = self.pgwire.write().unwrap_or_else(|p| p.into_inner());
        stores.retain(|weak| weak.strong_count() > 0);
        stores.push(Arc::downgrade(store));
    }

    /// Count one native connection on `db` until the guard drops.
    pub fn track_native(self: &Arc<Self>, db: DatabaseId) -> NativeConnectionGuard {
        let mut native = self.native.lock().unwrap_or_else(|p| p.into_inner());
        *native.entry(db).or_insert(0) += 1;
        drop(native);
        NativeConnectionGuard {
            tracker: Arc::clone(self),
            db,
        }
    }

    /// Sum pgwire and native connections per database.
    ///
    /// A pgwire connection without a current database counts under
    /// `DatabaseId::DEFAULT`. Databases with no connection are absent.
    pub fn connections_by_database(&self) -> HashMap<DatabaseId, u64> {
        let mut totals: HashMap<DatabaseId, u64> = self
            .native
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let stores = self.pgwire.read().unwrap_or_else(|p| p.into_inner());
        for store in stores.iter().filter_map(|weak| weak.upgrade()) {
            for (db, count) in store.connections_by_database() {
                *totals.entry(db).or_insert(0) += count;
            }
        }
        totals
    }
}

/// Write the connection gauge of every registered database.
///
/// A database with no connection is set to zero, so a closed last connection
/// clears the gauge.
pub fn publish_connections(registry: &DatabaseMetricsRegistry, state: &SharedState) {
    let totals = state.connection_tracker.connections_by_database();
    let ids: Vec<DatabaseId> = registry
        .counters
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .keys()
        .copied()
        .collect();
    for id in ids {
        registry.set_connections(id, totals.get(&id).copied().unwrap_or(0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{ConnectionId, ConnectionMetadata};

    fn addr(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn native_guard_drop_decrements_and_removes_entry() {
        let tracker = Arc::new(ConnectionTracker::new());
        let db = DatabaseId::new(5);
        let first = tracker.track_native(db);
        let second = tracker.track_native(db);
        assert_eq!(tracker.connections_by_database().get(&db), Some(&2));
        drop(first);
        assert_eq!(tracker.connections_by_database().get(&db), Some(&1));
        drop(second);
        assert!(tracker.connections_by_database().is_empty());
    }

    #[test]
    fn native_guard_drop_on_panic_unwind_decrements() {
        let tracker = Arc::new(ConnectionTracker::new());
        let db = DatabaseId::new(6);
        let t = Arc::clone(&tracker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = t.track_native(db);
            panic!("exit path");
        }));
        assert!(result.is_err());
        assert!(tracker.connections_by_database().is_empty());
    }

    #[test]
    fn sums_pgwire_and_native() {
        let tracker = Arc::new(ConnectionTracker::new());
        let store = Arc::new(SessionStore::new());
        let metadata = ConnectionMetadata {
            peer_addr: addr(4000),
            local_addr: addr(5432),
        };
        let id = ConnectionId::new(1).unwrap();
        store.register_connection(id, metadata).unwrap();
        store.set_current_database(id, DatabaseId::new(3));
        tracker.register_pgwire(&store);
        let _native = tracker.track_native(DatabaseId::new(3));
        let _other = tracker.track_native(DatabaseId::new(4));

        let totals = tracker.connections_by_database();
        assert_eq!(totals.get(&DatabaseId::new(3)), Some(&2));
        assert_eq!(totals.get(&DatabaseId::new(4)), Some(&1));

        drop(store);
        let totals = tracker.connections_by_database();
        assert_eq!(totals.get(&DatabaseId::new(3)), Some(&1));
    }
}
