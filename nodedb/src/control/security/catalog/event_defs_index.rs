// SPDX-License-Identifier: BUSL-1.1

//! In-memory index of each collection's DEFINE EVENT definitions.
//!
//! The Event Plane reads a collection's event definitions for every write
//! event. It must not read redb, so it reads this index instead.
//!
//! [`SystemCatalog`](super::system_catalog::SystemCatalog) owns the index and
//! keeps it in step with the committed `COLLECTIONS` table:
//! - `open` and `migrate_collections` rebuild it from every committed
//!   collection row (`reload_event_definitions`);
//! - `put_collection` and `put_collection_if_absent` install a row after the
//!   redb commit succeeds;
//! - `delete_collection` removes a row after the redb commit succeeds.
//!
//! Every collection writer goes through those functions: the replicated
//! catalog apply, the single-node fallback, transaction COMMIT, catalog
//! restore, and maintenance writers. DDL a transaction stages lives in the
//! catalog overlay and never reaches the table before COMMIT, so the index
//! holds committed definitions only.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_types::DatabaseId;

use redb::{ReadableDatabase, ReadableTable};

use super::collection::StoredCollection;
use super::collection_constraints::EventDefinition;
use super::system_catalog::SystemCatalog;
use super::tables::COLLECTIONS;
use super::types::catalog_err;

/// `(database, tenant, collection)`.
type IndexKey = (DatabaseId, u64, String);

/// Committed event definitions by collection. Send + Sync. A reader clones
/// the definitions out and holds no lock afterwards.
#[derive(Debug, Default)]
pub struct EventDefsIndex {
    by_collection: RwLock<HashMap<IndexKey, Arc<[EventDefinition]>>>,
}

impl EventDefsIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the whole index with the definitions of `rows`, each keyed
    /// under the database its table row is stored in.
    pub fn load_all(&self, rows: &[(DatabaseId, StoredCollection)]) {
        let mut map = HashMap::new();
        for (database_id, row) in rows {
            if let Some(defs) = active_defs(row) {
                map.insert(key_of(*database_id, row), defs);
            }
        }
        *self.write() = map;
    }

    /// Record `row`, committed under `database_id`. An inactive row, or a
    /// row with no event definitions, removes the collection's entry.
    pub fn install(&self, database_id: DatabaseId, row: &StoredCollection) {
        let key = key_of(database_id, row);
        let mut map = self.write();
        match active_defs(row) {
            Some(defs) => {
                map.insert(key, defs);
            }
            None => {
                map.remove(&key);
            }
        }
    }

    /// Forget a collection whose row was deleted.
    pub fn remove(&self, database_id: DatabaseId, tenant_id: u64, collection: &str) {
        self.write()
            .remove(&(database_id, tenant_id, collection.to_owned()));
    }

    /// The committed event definitions of a collection. `None` when it has
    /// none.
    pub fn get(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> Option<Arc<[EventDefinition]>> {
        let map = self
            .by_collection
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        map.get(&(database_id, tenant_id, collection.to_owned()))
            .cloned()
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<IndexKey, Arc<[EventDefinition]>>> {
        self.by_collection
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl SystemCatalog {
    /// Rebuild the event-definition index from every committed collection
    /// row, keyed by the database each row is stored under.
    pub fn reload_event_definitions(&self) -> crate::Result<()> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(COLLECTIONS)
            .map_err(|e| catalog_err("open collections", e))?;
        let mut rows = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iterate collections", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read collection", e))?;
            let (database_id, _) = key.value();
            let row: StoredCollection = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser collection", e))?;
            rows.push((DatabaseId::new(database_id), row));
        }
        self.event_defs.load_all(&rows);
        Ok(())
    }

    /// The committed DEFINE EVENT definitions of a collection. Reads memory
    /// only. `None` when the collection has none.
    pub fn event_definitions(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> Option<Arc<[EventDefinition]>> {
        self.event_defs.get(database_id, tenant_id, collection)
    }
}

fn key_of(database_id: DatabaseId, row: &StoredCollection) -> IndexKey {
    (database_id, row.tenant_id, row.name.clone())
}

/// The definitions an active row carries. `None` for an inactive row or an
/// empty list.
fn active_defs(row: &StoredCollection) -> Option<Arc<[EventDefinition]>> {
    (row.is_active && !row.event_defs.is_empty()).then(|| Arc::from(row.event_defs.as_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn def(name: &str) -> EventDefinition {
        EventDefinition {
            name: name.into(),
            collection: "orders".into(),
            when_condition: "INSERT".into(),
            then_action: "SELECT 1".into(),
        }
    }

    fn row(defs: Vec<EventDefinition>) -> StoredCollection {
        let mut row = StoredCollection::new(7, "orders", "admin");
        row.event_defs = defs;
        row
    }

    fn names(index: &EventDefsIndex, row: &StoredCollection) -> Vec<String> {
        index
            .get(DB, row.tenant_id, &row.name)
            .map(|defs| defs.iter().map(|d| d.name.clone()).collect())
            .unwrap_or_default()
    }

    #[test]
    fn install_records_the_definitions() {
        let index = EventDefsIndex::new();
        let r = row(vec![def("a")]);
        index.install(DB, &r);
        assert_eq!(names(&index, &r), vec!["a".to_string()]);
    }

    #[test]
    fn install_replaces_the_previous_definitions() {
        let index = EventDefsIndex::new();
        index.install(DB, &row(vec![def("a")]));
        let r = row(vec![def("b"), def("c")]);
        index.install(DB, &r);
        assert_eq!(names(&index, &r), vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn a_row_with_no_definitions_removes_the_entry() {
        let index = EventDefsIndex::new();
        index.install(DB, &row(vec![def("a")]));
        let r = row(Vec::new());
        index.install(DB, &r);
        assert!(index.get(DB, r.tenant_id, &r.name).is_none());
    }

    #[test]
    fn a_dropped_collection_fires_no_event() {
        let index = EventDefsIndex::new();
        index.install(DB, &row(vec![def("a")]));
        let mut dropped = row(vec![def("a")]);
        dropped.is_active = false;
        index.install(DB, &dropped);
        assert!(index.get(DB, dropped.tenant_id, &dropped.name).is_none());
    }

    #[test]
    fn remove_forgets_a_purged_collection() {
        let index = EventDefsIndex::new();
        let r = row(vec![def("a")]);
        index.install(DB, &r);
        index.remove(DB, r.tenant_id, &r.name);
        assert!(index.get(DB, r.tenant_id, &r.name).is_none());
    }

    #[test]
    fn load_all_skips_inactive_rows() {
        let index = EventDefsIndex::new();
        let live = row(vec![def("a")]);
        let mut gone = StoredCollection::new(7, "gone", "admin");
        gone.event_defs = vec![def("x")];
        gone.is_active = false;
        index.load_all(&[(DB, live.clone()), (DB, gone.clone())]);
        assert_eq!(names(&index, &live), vec!["a".to_string()]);
        assert!(index.get(DB, gone.tenant_id, &gone.name).is_none());
    }
}
