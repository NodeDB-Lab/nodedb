// SPDX-License-Identifier: BUSL-1.1

//! `_system.collection_purge_log`: every collection hard-delete a sync client
//! can have missed, keyed by `(database, tenant)` and metadata log position.
//!
//! The metadata applier writes a row when it applies a `PurgeCollection`, at
//! the entry's metadata Raft log index. Every node applies the same entry at
//! the same index, and a group 0 snapshot carries the table, so every node
//! holds the same rows. A reconnecting sync client names the last position
//! it applied, and Origin replays every row past it.
//!
//! A replicated `PruneCollectionPurgeLog` entry prunes the rows purged
//! before the cutoff it carries, and raises each scope's `retention_floor`
//! to the highest pruned position (`collection_purge_prune.rs`). A client
//! whose position is below the floor can have missed a pruned purge, so it
//! gets a database resync instead.

use nodedb_types::Hlc;
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableError};

use super::types::{SystemCatalog, catalog_err};

/// Key: `(database_id, tenant_id, purge_index, collection)`. Value:
/// MessagePack [`StoredPurgeRecord`].
pub(super) const COLLECTION_PURGE_LOG: TableDefinition<(u64, u64, u64, &str), &[u8]> =
    TableDefinition::new("_system.collection_purge_log");

/// Key: `(database_id, tenant_id)`. Value: the highest purge position pruned
/// from the scope's log.
pub(super) const COLLECTION_PURGE_FLOORS: TableDefinition<(u64, u64), u64> =
    TableDefinition::new("_system.collection_purge_floors");

/// The value of one purge-log row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredPurgeRecord {
    /// Incarnation of the purged collection.
    pub incarnation: Hlc,
    /// Wall-clock milliseconds of the purge: the metadata entry's stamp.
    pub purged_at_ms: u64,
}

/// One purge-log row with its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionPurge {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Metadata Raft log index of the entry that committed the purge.
    pub purge_index: u64,
    pub name: String,
    pub record: StoredPurgeRecord,
}

/// The purge log of one `(database, tenant)` past a position, read in one
/// transaction with the scope's floor.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PurgeLogTail {
    /// The highest pruned purge position. Rows at or below it are gone.
    pub retention_floor: u64,
    /// Rows past the requested position, in position order.
    pub purges: Vec<CollectionPurge>,
}

impl SystemCatalog {
    /// Record `purge`.
    ///
    /// A row at or below the scope's floor is not written: it was pruned
    /// once, and a full-log replay on boot applies its entry again. A row
    /// already present stays as it is. Returns whether the row is on record
    /// after the call.
    pub fn record_collection_purge(&self, purge: &CollectionPurge) -> crate::Result<bool> {
        let scope = (purge.database_id, purge.tenant_id);
        let bytes = zerompk::to_msgpack_vec(&purge.record)
            .map_err(|e| catalog_err("encode purge record", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("purge log write txn", e))?;
        let recorded;
        {
            let floors = txn
                .open_table(COLLECTION_PURGE_FLOORS)
                .map_err(|e| catalog_err("open purge floors", e))?;
            let floor = floors
                .get(scope)
                .map_err(|e| catalog_err("read purge floor", e))?
                .map_or(0, |v| v.value());
            let mut log = txn
                .open_table(COLLECTION_PURGE_LOG)
                .map_err(|e| catalog_err("open purge log", e))?;
            recorded = purge.purge_index > floor;
            if recorded {
                let key = (scope.0, scope.1, purge.purge_index, purge.name.as_str());
                let present = log
                    .get(key)
                    .map_err(|e| catalog_err("read purge log row", e))?
                    .is_some();
                if !present {
                    log.insert(key, bytes.as_slice())
                        .map_err(|e| catalog_err("insert purge log row", e))?;
                }
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("purge log commit", e))?;
        Ok(recorded)
    }

    /// The floor and every row of `(database_id, tenant_id)` past
    /// `after_index`, read in one transaction.
    pub fn collection_purges_after(
        &self,
        database_id: u64,
        tenant_id: u64,
        after_index: u64,
    ) -> crate::Result<PurgeLogTail> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("purge log read txn", e))?;
        let retention_floor = match txn.open_table(COLLECTION_PURGE_FLOORS) {
            Ok(table) => table
                .get((database_id, tenant_id))
                .map_err(|e| catalog_err("read purge floor", e))?
                .map_or(0, |v| v.value()),
            Err(TableError::TableDoesNotExist(_)) => 0,
            Err(e) => return Err(catalog_err("open purge floors", e)),
        };
        let log = match txn.open_table(COLLECTION_PURGE_LOG) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(PurgeLogTail {
                    retention_floor,
                    purges: Vec::new(),
                });
            }
            Err(e) => return Err(catalog_err("open purge log", e)),
        };
        let Some(start) = after_index.checked_add(1) else {
            return Ok(PurgeLogTail {
                retention_floor,
                purges: Vec::new(),
            });
        };
        let mut purges = Vec::new();
        for row in log
            .range((database_id, tenant_id, start, "")..)
            .map_err(|e| catalog_err("range purge log", e))?
        {
            let (key, value) = row.map_err(|e| catalog_err("read purge log", e))?;
            let (db, tenant, purge_index, name) = key.value();
            if (db, tenant) != (database_id, tenant_id) {
                break;
            }
            purges.push(CollectionPurge {
                database_id: db,
                tenant_id: tenant,
                purge_index,
                name: name.to_string(),
                record: zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode purge record", e))?,
            });
        }
        Ok(PurgeLogTail {
            retention_floor,
            purges,
        })
    }

    /// The newest purge-log row of `name` in `(database_id, tenant_id)`.
    pub fn latest_collection_purge(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<CollectionPurge>> {
        Ok(self
            .collection_purges_after(database_id, tenant_id, 0)?
            .purges
            .into_iter()
            .rfind(|purge| purge.name == name))
    }

    /// Remove every purge-log row and floor of `database_id`: the database
    /// is gone, so no client syncs against it again.
    pub fn delete_collection_purge_log(&self, database_id: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("purge log delete txn", e))?;
        {
            let mut log = txn
                .open_table(COLLECTION_PURGE_LOG)
                .map_err(|e| catalog_err("open purge log", e))?;
            log.retain(|(db, _, _, _), _| db != database_id)
                .map_err(|e| catalog_err("delete purge log rows", e))?;
            let mut floors = txn
                .open_table(COLLECTION_PURGE_FLOORS)
                .map_err(|e| catalog_err("open purge floors", e))?;
            floors
                .retain(|(db, _), _| db != database_id)
                .map_err(|e| catalog_err("delete purge floors", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("purge log delete commit", e))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const DAY_MS: u64 = 86_400_000;

    pub(crate) fn purge(
        db: u64,
        tenant: u64,
        index: u64,
        name: &str,
        at_ms: u64,
    ) -> CollectionPurge {
        CollectionPurge {
            database_id: db,
            tenant_id: tenant,
            purge_index: index,
            name: name.to_string(),
            record: StoredPurgeRecord {
                incarnation: Hlc::new(index, 0),
                purged_at_ms: at_ms,
            },
        }
    }

    pub(crate) fn names(tail: &PurgeLogTail) -> Vec<(u64, &str)> {
        tail.purges
            .iter()
            .map(|p| (p.purge_index, p.name.as_str()))
            .collect()
    }

    #[test]
    fn rows_read_back_past_a_position_in_their_scope_only() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        for row in [
            purge(1, 7, 10, "orders", DAY_MS),
            purge(1, 7, 20, "users", DAY_MS),
            purge(1, 8, 15, "orders", DAY_MS),
            purge(2, 7, 12, "orders", DAY_MS),
        ] {
            assert!(catalog.record_collection_purge(&row).unwrap());
        }
        let tail = catalog.collection_purges_after(1, 7, 0).unwrap();
        assert_eq!(names(&tail), vec![(10, "orders"), (20, "users")]);
        assert_eq!(tail.retention_floor, 0);
        let tail = catalog.collection_purges_after(1, 7, 10).unwrap();
        assert_eq!(names(&tail), vec![(20, "users")]);
        assert!(
            catalog
                .collection_purges_after(1, 7, u64::MAX)
                .unwrap()
                .purges
                .is_empty()
        );
        let tail = catalog.collection_purges_after(1, 8, 0).unwrap();
        assert_eq!(names(&tail), vec![(15, "orders")]);
    }

    #[test]
    fn a_rewrite_keeps_the_first_row() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let first = purge(1, 7, 10, "orders", DAY_MS);
        catalog.record_collection_purge(&first).unwrap();
        let mut again = first.clone();
        again.record.incarnation = Hlc::ZERO;
        assert!(catalog.record_collection_purge(&again).unwrap());
        let tail = catalog.collection_purges_after(1, 7, 0).unwrap();
        assert_eq!(tail.purges, vec![first]);
    }

    #[test]
    fn the_latest_row_of_a_name_is_found() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        catalog
            .record_collection_purge(&purge(1, 7, 10, "a", DAY_MS))
            .unwrap();
        catalog
            .record_collection_purge(&purge(1, 7, 40, "a", DAY_MS))
            .unwrap();
        let latest = catalog.latest_collection_purge(1, 7, "a").unwrap();
        assert_eq!(latest.map(|p| p.purge_index), Some(40));
        assert!(
            catalog
                .latest_collection_purge(1, 7, "b")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn dropping_a_database_removes_its_rows() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        catalog
            .record_collection_purge(&purge(1, 7, 10, "a", DAY_MS))
            .unwrap();
        catalog
            .record_collection_purge(&purge(2, 7, 11, "a", DAY_MS))
            .unwrap();
        catalog.delete_collection_purge_log(1).unwrap();
        assert!(
            catalog
                .collection_purges_after(1, 7, 0)
                .unwrap()
                .purges
                .is_empty()
        );
        assert_eq!(
            catalog
                .collection_purges_after(2, 7, 0)
                .unwrap()
                .purges
                .len(),
            1
        );
    }
}
