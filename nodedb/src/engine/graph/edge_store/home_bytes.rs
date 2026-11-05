// SPDX-License-Identifier: BUSL-1.1

//! The edge store file's stored-bytes counter, split by edge homes.
//!
//! An edge lives on the key vShard of each endpoint, not on its collection's
//! home. A core stores one copy of an edge version for every home of the
//! edge that routes to it. The counter keeps one row per
//! `(database_id, tenant_id, collection, low_home, high_home)`: the two
//! endpoint homes of the counted edges, in ascending order. An edge with one
//! home has `low_home == high_home`. A reader then decides which home's lead
//! accounts each copy.
//!
//! A writer moves the counter inside the write transaction that changes the
//! rows, so the counter commits and rolls back with them.

use std::collections::HashMap;

use nodedb_types::{DatabaseId, TenantId};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::engine::storage_bytes::{DroppedScope, row_bytes};

/// `(database_id, tenant_id, collection, low_home, high_home)` → stored
/// bytes of the counted edge rows of that collection and home pair. A pair
/// with no stored bytes has no row.
pub const EDGE_STORAGE_BYTES: TableDefinition<(u64, u64, &str, u32, u32), u64> =
    TableDefinition::new("edge_storage_bytes");

/// The two endpoint homes of an edge, in ascending order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeHomes {
    pub low: u32,
    pub high: u32,
}

impl EdgeHomes {
    /// The homes of an edge whose endpoints home on `a` and `b`.
    pub fn of(a: u32, b: u32) -> Self {
        Self {
            low: a.min(b),
            high: a.max(b),
        }
    }

    /// Every distinct home, low first.
    pub fn iter(self) -> impl Iterator<Item = u32> {
        std::iter::once(self.low).chain((self.high != self.low).then_some(self.high))
    }
}

/// One counter row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeStorageBytesRow {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    /// The edge collection, in its stored form.
    pub collection: String,
    pub homes: EdgeHomes,
    pub bytes: u64,
}

/// A storage error of the edge counter.
fn counter_err(engine: &'static str, ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: engine.into(),
        detail: format!("edge storage bytes {ctx}: {e}"),
    }
}

/// Signed stored-bytes changes of one write transaction, one entry per
/// collection and home pair.
#[derive(Debug, Default)]
pub struct EdgeStorageDelta {
    entries: HashMap<(u64, u64, EdgeHomes), HashMap<String, i64>>,
}

impl EdgeStorageDelta {
    /// Add `bytes` (negative to subtract) to the change of one collection
    /// and home pair.
    pub fn add(&mut self, db: u64, tid: u64, collection: &str, homes: EdgeHomes, bytes: i64) {
        if bytes == 0 {
            return;
        }
        let names = self.entries.entry((db, tid, homes)).or_default();
        match names.get_mut(collection) {
            Some(sum) => *sum = sum.saturating_add(bytes),
            None => {
                names.insert(collection.to_owned(), bytes);
            }
        }
    }

    /// Record one stored or removed row of `key_len` key and `value_len`
    /// value bytes.
    pub fn row(
        &mut self,
        (db, tid, collection): (u64, u64, &str),
        homes: EdgeHomes,
        key_len: usize,
        value_len: usize,
        stored: bool,
    ) {
        let bytes = row_bytes(key_len, value_len);
        let change = if stored {
            bytes
        } else {
            bytes.saturating_neg()
        };
        self.add(db, tid, collection, homes, change);
    }

    /// Move every counter row by its change inside `txn`.
    ///
    /// A change that takes a row below zero is a broken invariant: the
    /// counter missed a stored row. It is reported, and the row clamps at
    /// zero. A row that reaches zero is removed.
    pub fn apply(self, txn: &WriteTransaction, engine: &'static str) -> crate::Result<()> {
        if self
            .entries
            .values()
            .all(|names| names.values().all(|c| *c == 0))
        {
            return Ok(());
        }
        let mut table = txn
            .open_table(EDGE_STORAGE_BYTES)
            .map_err(|e| counter_err(engine, "open", e))?;
        for ((db, tid, homes), names) in &self.entries {
            for (collection, change) in names {
                if *change == 0 {
                    continue;
                }
                let key = (*db, *tid, collection.as_str(), homes.low, homes.high);
                let held = table
                    .get(key)
                    .map_err(|e| counter_err(engine, "read", e))?
                    .map(|guard| guard.value())
                    .unwrap_or(0);
                let next = match u64::try_from(*change) {
                    Ok(added) => held.saturating_add(added),
                    Err(_) => {
                        let removed = change.unsigned_abs();
                        if removed > held {
                            crate::diag::storage_bytes_underflow(
                                engine,
                                *db,
                                *tid,
                                Some(collection.as_str()),
                                held,
                                removed,
                            );
                        }
                        held.saturating_sub(removed)
                    }
                };
                if next == 0 {
                    table
                        .remove(key)
                        .map_err(|e| counter_err(engine, "clear", e))?;
                } else {
                    table
                        .insert(key, next)
                        .map_err(|e| counter_err(engine, "write", e))?;
                }
            }
        }
        Ok(())
    }
}

/// Create the counter table inside `txn`, so a read transaction can open it
/// before the first counted write.
pub fn ensure_edge_storage_bytes_table(
    txn: &WriteTransaction,
    engine: &'static str,
) -> crate::Result<()> {
    txn.open_table(EDGE_STORAGE_BYTES)
        .map(drop)
        .map_err(|e| counter_err(engine, "open", e))
}

/// Every counter row of the file `db` opens. Cost is the number of counter
/// rows.
pub fn edge_storage_bytes_snapshot(
    db: &Database,
    engine: &'static str,
) -> crate::Result<Vec<EdgeStorageBytesRow>> {
    let txn = db
        .begin_read()
        .map_err(|e| counter_err(engine, "read txn", e))?;
    let table = txn
        .open_table(EDGE_STORAGE_BYTES)
        .map_err(|e| counter_err(engine, "open", e))?;
    let mut rows = Vec::new();
    for entry in table.iter().map_err(|e| counter_err(engine, "scan", e))? {
        let (key, value) = entry.map_err(|e| counter_err(engine, "read", e))?;
        let (database_id, tenant_id, collection, low, high) = key.value();
        rows.push(EdgeStorageBytesRow {
            database_id: DatabaseId::new(database_id),
            tenant_id: TenantId::new(tenant_id),
            collection: collection.to_owned(),
            homes: EdgeHomes { low, high },
            bytes: value.value(),
        });
    }
    Ok(rows)
}

/// Remove the counter rows of `scope` inside `txn`, after every counted edge
/// row of the scope is gone.
///
/// Exact counting leaves no row then. A row that remains held bytes the
/// counter never saw leave: it is reported, then removed. Returns the number
/// of rows removed.
pub fn retire_edge_storage_bytes(
    txn: &WriteTransaction,
    engine: &'static str,
    scope: DroppedScope<'_>,
) -> crate::Result<usize> {
    let (database_id, tenant_id, only, label) = match scope {
        DroppedScope::Collection {
            database_id,
            tenant_id,
            collection,
        } => (database_id, tenant_id, Some(collection), "collection"),
        DroppedScope::Tenant {
            database_id,
            tenant_id,
        } => (database_id, tenant_id, None, "tenant"),
    };
    let mut table = txn
        .open_table(EDGE_STORAGE_BYTES)
        .map_err(|e| counter_err(engine, "open", e))?;
    let start = (database_id, tenant_id, only.unwrap_or(""), 0u32, 0u32);
    let mut residue: Vec<(String, EdgeHomes, u64)> = Vec::new();
    for entry in table
        .range(start..)
        .map_err(|e| counter_err(engine, "range", e))?
    {
        let (key, value) = entry.map_err(|e| counter_err(engine, "read", e))?;
        let (d, t, collection, low, high) = key.value();
        if d != database_id || t != tenant_id {
            break;
        }
        if only.is_some_and(|only| collection != only) {
            break;
        }
        residue.push((
            collection.to_owned(),
            EdgeHomes { low, high },
            value.value(),
        ));
    }
    for (collection, homes, bytes) in &residue {
        crate::diag::storage_bytes_residue(
            engine,
            label,
            database_id,
            tenant_id,
            collection,
            *bytes,
        );
        table
            .remove((
                database_id,
                tenant_id,
                collection.as_str(),
                homes.low,
                homes.high,
            ))
            .map_err(|e| counter_err(engine, "remove", e))?;
    }
    Ok(residue.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn homes_order_and_dedup() {
        assert_eq!(EdgeHomes::of(9, 3), EdgeHomes { low: 3, high: 9 });
        assert_eq!(EdgeHomes::of(9, 3).iter().collect::<Vec<_>>(), vec![3, 9]);
        assert_eq!(EdgeHomes::of(4, 4).iter().collect::<Vec<_>>(), vec![4]);
    }

    #[test]
    fn changes_sum_per_collection_and_home_pair() {
        let mut delta = EdgeStorageDelta::default();
        let a = EdgeHomes::of(1, 2);
        let b = EdgeHomes::of(3, 3);
        delta.row((0, 1, "c"), a, 10, 5, true);
        delta.row((0, 1, "c"), a, 1, 1, true);
        delta.row((0, 1, "c"), b, 3, 0, true);
        delta.row((0, 1, "c"), b, 3, 0, false);
        assert_eq!(delta.entries[&(0, 1, a)]["c"], 17);
        assert_eq!(delta.entries[&(0, 1, b)]["c"], 0);
    }
}
