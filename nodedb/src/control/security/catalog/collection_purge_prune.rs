// SPDX-License-Identifier: BUSL-1.1

//! Retention pruning of `_system.collection_purge_log`.
//!
//! Only a replicated `PruneCollectionPurgeLog` entry prunes, at the cutoff
//! it carries. Every node applies the entry at the same log position over
//! the same rows, so every node keeps the same rows and floors.

use std::collections::BTreeMap;

use redb::{ReadableDatabase, ReadableTable, TableError};

use super::collection_purge_log::{
    COLLECTION_PURGE_FLOORS, COLLECTION_PURGE_LOG, StoredPurgeRecord,
};
use super::types::{SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Prune every row purged before `cutoff_ms`, in one write transaction.
    ///
    /// Each scope's floor rises to the highest position it pruned, and every
    /// row of the scope at or below the floor goes, so the floor alone tells
    /// what the log no longer holds. A floor never moves down. Returns the
    /// number of rows removed.
    pub fn prune_collection_purge_log(&self, cutoff_ms: u64) -> crate::Result<usize> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("purge log prune txn", e))?;
        let removed;
        {
            let mut floors = txn
                .open_table(COLLECTION_PURGE_FLOORS)
                .map_err(|e| catalog_err("open purge floors", e))?;
            let mut log = txn
                .open_table(COLLECTION_PURGE_LOG)
                .map_err(|e| catalog_err("open purge log", e))?;
            let mut rows: Vec<((u64, u64), u64, String, u64)> = Vec::new();
            for row in log
                .range(..)
                .map_err(|e| catalog_err("range purge log", e))?
            {
                let (key, value) = row.map_err(|e| catalog_err("read purge log", e))?;
                let (db, tenant, index, name) = key.value();
                let record: StoredPurgeRecord = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode purge record", e))?;
                rows.push(((db, tenant), index, name.to_string(), record.purged_at_ms));
            }
            let mut raised: BTreeMap<(u64, u64), u64> = BTreeMap::new();
            for (scope, index, _, at_ms) in &rows {
                if *at_ms < cutoff_ms {
                    let floor = raised.entry(*scope).or_insert(0);
                    *floor = (*floor).max(*index);
                }
            }
            for (scope, floor) in raised.iter_mut() {
                let stored = floors
                    .get(*scope)
                    .map_err(|e| catalog_err("read purge floor", e))?
                    .map_or(0, |v| v.value());
                *floor = (*floor).max(stored);
            }
            let mut count = 0usize;
            for (scope, index, name, _) in &rows {
                if raised.get(scope).is_some_and(|floor| index <= floor) {
                    log.remove((scope.0, scope.1, *index, name.as_str()))
                        .map_err(|e| catalog_err("prune purge log row", e))?;
                    count += 1;
                }
            }
            for (scope, floor) in raised {
                floors
                    .insert(scope, floor)
                    .map_err(|e| catalog_err("raise purge floor", e))?;
            }
            removed = count;
        }
        txn.commit()
            .map_err(|e| catalog_err("purge log prune commit", e))?;
        Ok(removed)
    }

    /// The purge time of the oldest row on record, in milliseconds. `None`
    /// when the log is empty.
    pub fn oldest_collection_purge_ms(&self) -> crate::Result<Option<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("purge log read txn", e))?;
        let log = match txn.open_table(COLLECTION_PURGE_LOG) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(catalog_err("open purge log", e)),
        };
        let mut oldest: Option<u64> = None;
        for row in log
            .range(..)
            .map_err(|e| catalog_err("range purge log", e))?
        {
            let (_, value) = row.map_err(|e| catalog_err("read purge log", e))?;
            let record: StoredPurgeRecord = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode purge record", e))?;
            oldest = Some(oldest.map_or(record.purged_at_ms, |o| o.min(record.purged_at_ms)));
        }
        Ok(oldest)
    }
}

#[cfg(test)]
mod tests {
    use super::super::collection_purge_log::tests::{DAY_MS, names, purge};
    use super::*;

    #[test]
    fn a_prune_removes_old_rows_and_raises_the_floor() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        for row in [
            purge(1, 7, 10, "a", DAY_MS),
            purge(1, 7, 20, "b", 2 * DAY_MS),
            purge(1, 7, 30, "c", 5 * DAY_MS),
            purge(1, 8, 5, "other", 4 * DAY_MS),
        ] {
            catalog.record_collection_purge(&row).unwrap();
        }
        assert_eq!(catalog.oldest_collection_purge_ms().unwrap(), Some(DAY_MS));

        assert_eq!(catalog.prune_collection_purge_log(3 * DAY_MS).unwrap(), 2);
        let tail = catalog.collection_purges_after(1, 7, 0).unwrap();
        assert_eq!(names(&tail), vec![(30, "c")]);
        assert_eq!(tail.retention_floor, 20);
        let other = catalog.collection_purges_after(1, 8, 0).unwrap();
        assert_eq!(names(&other), vec![(5, "other")]);
        assert_eq!(other.retention_floor, 0);

        // A repeat prunes nothing, and a lower cutoff moves no floor down.
        assert_eq!(catalog.prune_collection_purge_log(3 * DAY_MS).unwrap(), 0);
        assert_eq!(catalog.prune_collection_purge_log(0).unwrap(), 0);
        assert_eq!(
            catalog
                .collection_purges_after(1, 7, 0)
                .unwrap()
                .retention_floor,
            20
        );
    }

    #[test]
    fn a_pruned_row_is_not_written_again_on_replay() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        catalog
            .record_collection_purge(&purge(1, 7, 10, "a", DAY_MS))
            .unwrap();
        catalog
            .record_collection_purge(&purge(1, 7, 20, "b", 5 * DAY_MS))
            .unwrap();
        catalog.prune_collection_purge_log(2 * DAY_MS).unwrap();
        assert!(
            !catalog
                .record_collection_purge(&purge(1, 7, 10, "a", DAY_MS))
                .unwrap()
        );
        let tail = catalog.collection_purges_after(1, 7, 0).unwrap();
        assert_eq!(names(&tail), vec![(20, "b")]);
        assert_eq!(tail.retention_floor, 10);
    }
}
