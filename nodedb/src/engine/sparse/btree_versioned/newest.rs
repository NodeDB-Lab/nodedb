// SPDX-License-Identifier: BUSL-1.1

//! The newest system-time ordinal the versioned document and index tables
//! hold.
//!
//! A committing transaction validates its reads against these values: a
//! collection, a row, or an index range is unchanged since a snapshot `S`
//! while its newest stored version, live or tombstone, sits at or below `S`.
//! Every value reads the stored keys only, so every replica that applied the
//! same versions answers alike.

use redb::{ReadableDatabase, ReadableTable};

use super::doc::DOCUMENTS_VERSIONED;
use super::index::INDEXES_VERSIONED;
use super::index_visible::upper_value_bound;
use super::key::{coll_prefix, coll_prefix_end, parse_sys_from};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// The newest version one document collection holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionNewest {
    pub database_id: u64,
    pub tenant: u64,
    pub collection: String,
    /// The newest `sys_from` of any version of any row.
    pub newest: i64,
}

/// The index entries [`SparseEngine::versioned_index_newest_in`] reads: every
/// entry of `field` in `coll` whose value lies in `[lower, upper]`. A `None`
/// bound is open.
#[derive(Debug, Clone, Copy)]
pub struct IndexNewestRange<'a> {
    pub database_id: u64,
    pub tenant: u64,
    pub coll: &'a str,
    pub field: &'a str,
    /// Inclusive lower value bound.
    pub lower: Option<&'a str>,
    /// Inclusive upper value bound.
    pub upper: Option<&'a str>,
}

/// The `"{db}:{tenant}:{coll}"` head and the `sys_from` of a versioned
/// document key `"{db}:{tenant}:{coll}:{doc_id}\x00{sys_from}"`.
fn split_doc_key(key: &str) -> Option<(&str, i64)> {
    let (head, _) = key.split_once('\x00')?;
    let (owner, _doc_id) = head.rsplit_once(':')?;
    Some((owner, parse_sys_from(key)?))
}

impl SparseEngine {
    /// The newest `sys_from` any stored version of `coll` holds, live,
    /// tombstone, or erased. `None` when the collection holds no version.
    ///
    /// Reads every version key of the collection. A key of another
    /// collection whose name extends `coll` past a `:` is skipped.
    pub fn versioned_collection_newest(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
    ) -> crate::Result<Option<i64>> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let owner = format!("{database_id}:{tenant}:{coll}");
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open documents_versioned", e))?;
        let mut newest: Option<i64> = None;
        for entry in table
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?
        {
            let (key, _) = entry.map_err(|e| redb_err("entry", e))?;
            if let Some((key_owner, sys_from)) = split_doc_key(key.value())
                && key_owner == owner
            {
                newest = newest.max(Some(sys_from));
            }
        }
        Ok(newest)
    }

    /// The newest version of every document collection the table holds, in
    /// key order. Reads every version key once.
    pub fn versioned_newest_by_collection(&self) -> crate::Result<Vec<CollectionNewest>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open documents_versioned", e))?;
        let mut out: Vec<CollectionNewest> = Vec::new();
        for entry in table.iter().map_err(|e| redb_err("iter", e))? {
            let (key, _) = entry.map_err(|e| redb_err("entry", e))?;
            let Some((owner, sys_from)) = split_doc_key(key.value()) else {
                continue;
            };
            let mut parts = owner.splitn(3, ':');
            let (Some(db), Some(tenant), Some(collection)) =
                (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let (Ok(database_id), Ok(tenant)) = (db.parse::<u64>(), tenant.parse::<u64>()) else {
                continue;
            };
            match out.last_mut() {
                Some(last)
                    if last.database_id == database_id
                        && last.tenant == tenant
                        && last.collection == collection =>
                {
                    last.newest = last.newest.max(sys_from);
                }
                _ => out.push(CollectionNewest {
                    database_id,
                    tenant,
                    collection: collection.to_string(),
                    newest: sys_from,
                }),
            }
        }
        Ok(out)
    }

    /// The newest `sys_from` of any index entry, live or tombstone, of
    /// `field` whose value lies in `[lower, upper]`. `None` when no entry
    /// does.
    ///
    /// A row that took a value of the range, left it, or was deleted while
    /// it held one wrote an entry of the range at its version, so the answer
    /// bounds every change an index read of the range observes.
    pub fn versioned_index_newest_in(&self, q: IndexNewestRange<'_>) -> crate::Result<Option<i64>> {
        let IndexNewestRange {
            database_id,
            tenant,
            coll,
            field,
            lower,
            upper,
        } = q;
        let field_prefix = format!("{database_id}:{tenant}:{coll}:{field}:");
        let lo = match lower {
            Some(lower) => format!("{field_prefix}{lower}"),
            None => field_prefix.clone(),
        };
        // Keys of `upper` itself end `"{upper}:{doc_id}"`, below
        // `"{upper};"`. Keys of a value below `upper` that sort past it in
        // key order stay below `upper_value_bound`.
        let hi = match upper {
            Some(upper) => {
                upper_value_bound(&field_prefix, upper).max(format!("{field_prefix}{upper};"))
            }
            None => format!("{database_id}:{tenant}:{coll}:{field};"),
        };
        if lo >= hi {
            return Ok(None);
        }
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open indexes_versioned", e))?;
        let mut newest: Option<i64> = None;
        for entry in table
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?
        {
            let (key, _) = entry.map_err(|e| redb_err("entry", e))?;
            let key = key.value();
            let Some((head, _)) = key.split_once('\x00') else {
                continue;
            };
            let Some(rest) = head.strip_prefix(field_prefix.as_str()) else {
                continue;
            };
            let Some((value, _doc_id)) = rest.rsplit_once(':') else {
                continue;
            };
            let in_range = lower.is_none_or(|lower| value >= lower)
                && upper.is_none_or(|upper| value <= upper);
            if in_range && let Some(sys_from) = parse_sys_from(key) {
                newest = newest.max(Some(sys_from));
            }
        }
        Ok(newest)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::StorageKey;
    use nodedb_types::Surrogate;

    use super::super::value::{VersionedIndexEntry, VersionedPut};
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = SparseEngine::open(&dir.path().join("v.redb")).expect("open");
        (engine, dir)
    }

    fn key(n: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(n))
    }

    fn put(engine: &SparseEngine, coll: &str, row: u32, sys_from: i64) {
        engine
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 1,
                coll,
                doc_id: &key(row),
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &[0xc0],
            })
            .expect("put");
    }

    fn index(engine: &SparseEngine, value: &str, row: u32, sys_from: i64, live: bool) {
        let entry = VersionedIndexEntry {
            database_id: 0,
            tenant: 1,
            coll: "c",
            field: "f",
            value,
            doc_id: &key(row),
            sys_from,
        };
        if live {
            engine.versioned_index_put(entry).expect("index put");
        } else {
            engine
                .versioned_index_tombstone(entry)
                .expect("index tombstone");
        }
    }

    #[test]
    fn a_collection_newest_counts_tombstones_and_skips_longer_names() {
        let (engine, _dir) = open_temp();
        assert_eq!(engine.versioned_collection_newest(0, 1, "c").unwrap(), None);
        put(&engine, "c", 1, 10);
        put(&engine, "c", 2, 30);
        engine
            .versioned_tombstone(0, 1, "c", &key(1), 40)
            .expect("tombstone");
        put(&engine, "c:x", 3, 99);
        assert_eq!(
            engine.versioned_collection_newest(0, 1, "c").unwrap(),
            Some(40)
        );
        let all = engine.versioned_newest_by_collection().unwrap();
        assert_eq!(
            all.iter()
                .map(|c| (c.collection.as_str(), c.newest))
                .collect::<Vec<_>>(),
            vec![("c", 40), ("c:x", 99)]
        );
    }

    #[test]
    fn an_index_range_answers_its_newest_entry_inclusive_of_both_bounds() {
        let (engine, _dir) = open_temp();
        index(&engine, "a", 1, 10, true);
        index(&engine, "b", 2, 20, true);
        index(&engine, "b", 2, 50, false);
        index(&engine, "c", 3, 70, true);
        index(&engine, "b:x", 4, 90, true);
        let range = |lower: Option<&'static str>, upper: Option<&'static str>| IndexNewestRange {
            database_id: 0,
            tenant: 1,
            coll: "c",
            field: "f",
            lower,
            upper,
        };
        let newest = |q| engine.versioned_index_newest_in(q).unwrap();
        assert_eq!(newest(range(Some("b"), Some("b"))), Some(50));
        assert_eq!(newest(range(Some("a"), Some("a"))), Some(10));
        assert_eq!(newest(range(Some("a"), Some("b"))), Some(50));
        assert_eq!(newest(range(Some("b"), Some("c"))), Some(90));
        assert_eq!(newest(range(None, Some("a"))), Some(10));
        assert_eq!(newest(range(Some("d"), None)), None);
        assert_eq!(newest(range(Some("x"), Some("y"))), None);
    }
}
