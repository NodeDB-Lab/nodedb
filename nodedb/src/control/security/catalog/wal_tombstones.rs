// SPDX-License-Identifier: BUSL-1.1

//! Persistent collection-tombstone set backing `_system.wal_tombstones`.
//!
//! Tombstones are written here atomically with their WAL record during
//! `execute_unregister_collection`. Startup replay reads the persisted set
//! in one O(1) lookup rather than scanning every WAL segment to rebuild
//! it — critical once segment counts grow.
//!
//! Entries are reaped by [`SystemCatalog::delete_wal_tombstones_before_lsn`]
//! once the WAL segment carrying the original tombstone record has been
//! truncated past retention (i.e. no replay will ever observe the
//! shadow-worthy writes again).

use redb::ReadableTable;

use super::types::{SystemCatalog, WAL_TOMBSTONES, catalog_err};

impl SystemCatalog {
    /// Record (or raise) a tombstone for `(database_id, tenant_id, collection)`.
    ///
    /// Idempotent: if an existing entry has a higher `purge_lsn`, it is
    /// preserved (repeated purges of the same name do not regress the
    /// shadowing LSN).
    pub fn record_wal_tombstone(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        purge_lsn: u64,
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("record_wal_tombstone txn", e))?;
        {
            let mut table = write_txn
                .open_table(WAL_TOMBSTONES)
                .map_err(|e| catalog_err("open wal_tombstones", e))?;
            let existing = table
                .get((database_id, tenant_id, collection))
                .map_err(|e| catalog_err("get wal_tombstone", e))?
                .map(|g| g.value());
            let next = existing.map_or(purge_lsn, |cur| cur.max(purge_lsn));
            table
                .insert((database_id, tenant_id, collection), next)
                .map_err(|e| catalog_err("insert wal_tombstone", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit wal_tombstone", e))
    }

    /// Load the entire tombstone set into the in-memory replay filter type.
    pub fn load_wal_tombstones(&self) -> crate::Result<nodedb_wal::TombstoneSet> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_wal_tombstones read txn", e))?;
        let table = read_txn
            .open_table(WAL_TOMBSTONES)
            .map_err(|e| catalog_err("open wal_tombstones", e))?;
        let mut set = nodedb_wal::TombstoneSet::new();
        for entry in table
            .range::<(u64, u64, &str)>(..)
            .map_err(|e| catalog_err("range wal_tombstones", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read wal_tombstone", e))?;
            let (database_id, tenant_id, collection) = key.value();
            set.insert(
                database_id,
                tenant_id,
                collection.to_string(),
                value.value(),
            );
        }
        Ok(set)
    }

    /// GC tombstones whose `purge_lsn < min_retained_lsn`. Intended to be
    /// called after WAL segment truncation advances the retained-LSN
    /// watermark: at that point no replay can observe a write shadowed by
    /// the tombstone, so keeping it is pure overhead.
    ///
    /// Returns the number of entries removed.
    pub fn delete_wal_tombstones_before_lsn(&self, min_retained_lsn: u64) -> crate::Result<usize> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("gc wal_tombstones txn", e))?;
        let removed;
        {
            let mut table = write_txn
                .open_table(WAL_TOMBSTONES)
                .map_err(|e| catalog_err("open wal_tombstones", e))?;
            // Two-pass: collect keys to delete, then delete. redb's
            // iterators don't permit concurrent mutation on the same table.
            let mut to_delete: Vec<(u64, u64, String)> = Vec::new();
            for entry in table
                .range::<(u64, u64, &str)>(..)
                .map_err(|e| catalog_err("gc range", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("gc read", e))?;
                if value.value() < min_retained_lsn {
                    let (db, tid, name) = key.value();
                    to_delete.push((db, tid, name.to_string()));
                }
            }
            removed = to_delete.len();
            for (db, tid, name) in to_delete {
                table
                    .remove((db, tid, name.as_str()))
                    .map_err(|e| catalog_err("gc remove", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("gc commit wal_tombstones", e))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn catalog() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("system.redb");
        let cat = SystemCatalog::open(&path).unwrap();
        (cat, tmp)
    }

    #[test]
    fn record_and_load_roundtrip() {
        let (cat, _tmp) = catalog();
        cat.record_wal_tombstone(7, 1, "users", 100).unwrap();
        cat.record_wal_tombstone(7, 1, "orders", 150).unwrap();
        cat.record_wal_tombstone(8, 1, "users", 200).unwrap();

        let set = cat.load_wal_tombstones().unwrap();
        assert_eq!(set.len(), 3);
        assert_eq!(set.purge_lsn(7, 1, "users"), Some(100));
        assert_eq!(set.purge_lsn(7, 1, "orders"), Some(150));
        assert_eq!(set.purge_lsn(8, 1, "users"), Some(200));
    }

    #[test]
    fn record_is_idempotent_and_monotone() {
        let (cat, _tmp) = catalog();
        cat.record_wal_tombstone(7, 1, "users", 100).unwrap();
        cat.record_wal_tombstone(7, 1, "users", 50).unwrap();
        assert_eq!(
            cat.load_wal_tombstones().unwrap().purge_lsn(7, 1, "users"),
            Some(100)
        );
        cat.record_wal_tombstone(7, 1, "users", 200).unwrap();
        assert_eq!(
            cat.load_wal_tombstones().unwrap().purge_lsn(7, 1, "users"),
            Some(200)
        );
    }

    #[test]
    fn gc_removes_only_older_entries() {
        let (cat, _tmp) = catalog();
        cat.record_wal_tombstone(7, 1, "a", 10).unwrap();
        cat.record_wal_tombstone(7, 1, "b", 100).unwrap();
        cat.record_wal_tombstone(7, 1, "c", 1000).unwrap();

        let removed = cat.delete_wal_tombstones_before_lsn(500).unwrap();
        assert_eq!(removed, 2);

        let set = cat.load_wal_tombstones().unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(set.purge_lsn(7, 1, "c"), Some(1000));
    }

    #[test]
    fn gc_threshold_is_strict_less_than() {
        let (cat, _tmp) = catalog();
        cat.record_wal_tombstone(7, 1, "a", 100).unwrap();
        // Threshold == purge_lsn: entry stays (not strictly less than).
        assert_eq!(cat.delete_wal_tombstones_before_lsn(100).unwrap(), 0);
        assert_eq!(cat.load_wal_tombstones().unwrap().len(), 1);
    }
}
