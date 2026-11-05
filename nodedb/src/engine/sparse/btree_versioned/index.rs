// SPDX-License-Identifier: BUSL-1.1

//! Versioned secondary-index operations.
//!
//! Index key: `"{database_id}:{tenant}:{coll}:{field}:{value}:{doc_id}\x00{sys_from:020}"`.
//! Value: single byte (`0x00` live, `0xFF` tombstone). The versioned index
//! is the only secondary index of every document collection.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::history_bytes::{apply_versioned_delta, insert_counted, remove_counted};
use super::index_visible::{EntryRange, for_each_visible_entry, upper_value_bound};
use super::value::{TAG_LIVE, TAG_TOMBSTONE, VersionedIndexEntry};
use crate::engine::sparse::btree::{KeyedTable, SparseEngine, invalid_storage_key_err, redb_err};
use crate::engine::storage_bytes::StorageDelta;

/// Keys carry the leading `{database_id}:` component.
pub(crate) const INDEXES_VERSIONED: TableDefinition<&str, &[u8]> =
    TableDefinition::new("indexes_versioned");

impl SparseEngine {
    /// Bootstrap: ensure the versioned index table exists.
    pub(in crate::engine::sparse) fn ensure_indexes_versioned_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let _ = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open indexes_versioned", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_index_put` inside a caller-owned write transaction.
    pub fn versioned_index_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut delta = StorageDelta::default();
        insert_counted(&mut t, &key, &[TAG_LIVE], &mut delta)?;
        apply_versioned_delta(txn, delta)
    }

    /// `versioned_index_tombstone` inside a caller-owned write transaction.
    pub fn versioned_index_tombstone_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut delta = StorageDelta::default();
        insert_counted(&mut t, &key, &[TAG_TOMBSTONE], &mut delta)?;
        apply_versioned_delta(txn, delta)
    }

    /// Physically remove one versioned index entry's redb entry inside a
    /// caller-owned write transaction. Unlike
    /// [`Self::versioned_index_tombstone_in_txn`] (which appends a
    /// tombstone marker), this deletes the entry at `sys_from` outright.
    /// Used by the transaction-rollback path to undo a
    /// `versioned_index_put_in_txn` that must not survive an aborted
    /// transaction. Removing a non-existent key is a no-op.
    pub fn versioned_index_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<()> {
        let key = e.redb_key();
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut delta = StorageDelta::default();
        remove_counted(&mut t, &key, &mut delta)?;
        apply_versioned_delta(txn, delta)
    }

    /// Whether an entry, live or tombstone, is stored at exactly `e`'s key,
    /// read inside a caller-owned write transaction.
    pub fn versioned_index_entry_exists_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        e: VersionedIndexEntry<'_>,
    ) -> crate::Result<bool> {
        let key = e.redb_key();
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        Ok(t.get(key.as_str())
            .map_err(|e| redb_err("get", e))?
            .is_some())
    }

    /// Append a versioned secondary index entry in its own transaction.
    pub fn versioned_index_put(&self, e: VersionedIndexEntry<'_>) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_index_put_in_txn(&txn, e)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append a tombstone entry for an index value that's been removed,
    /// in its own transaction.
    pub fn versioned_index_tombstone(&self, e: VersionedIndexEntry<'_>) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_index_tombstone_in_txn(&txn, e)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Remove every versioned index entry of `field` in `coll`: each value,
    /// each row and each system time, tombstones included. A dropped index
    /// leaves nothing, so a later index on the same field starts clean.
    ///
    /// Runs inside a caller-owned write transaction and does NOT commit.
    /// Returns the number of entries removed.
    pub fn versioned_index_drop_field_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        field: &str,
    ) -> crate::Result<usize> {
        let lo = format!("{database_id}:{tenant}:{coll}:{field}:");
        // `:` = 0x3A, next byte `;` = 0x3B gives a clean exclusive bound.
        let hi = format!("{database_id}:{tenant}:{coll}:{field};");
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let keys: Vec<String> = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?
            .map(|r| r.map(|(k, _)| k.value().to_string()))
            .collect::<Result<_, _>>()
            .map_err(|e| redb_err("entry", e))?;
        let mut delta = StorageDelta::default();
        for key in &keys {
            remove_counted(&mut t, key, &mut delta)?;
        }
        apply_versioned_delta(txn, delta)?;
        Ok(keys.len())
    }

    /// The doc ids whose row version visible at `sys_cutoff` (`None` =
    /// current) holds `value` under `field`, in doc-id order. The match is
    /// exact: an entry for `"a:b"` never answers a lookup of `"a"`. Every
    /// matching doc id is returned.
    pub fn versioned_index_lookup_as_of(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        field: &str,
        value: &str,
        sys_cutoff: Option<i64>,
    ) -> crate::Result<Vec<StorageKey>> {
        let field_prefix = format!("{database_id}:{tenant}:{coll}:{field}:");
        let lo = format!("{field_prefix}{value}:");
        // `:` = 0x3A, next byte `;` = 0x3B bounds every key of this value.
        // Keys of a longer value that starts with `{value}:` sort inside the
        // bound too. The exact value comparison below drops them.
        let hi = format!("{field_prefix}{value};");
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut out = Vec::new();
        for_each_visible_entry(
            &t,
            EntryRange {
                coll,
                prefix: &field_prefix,
                start: &lo,
                end: &hi,
            },
            sys_cutoff,
            |entry_value, doc_id| {
                if entry_value == value {
                    out.push(doc_id);
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(out)
    }

    /// The `(value, doc_id)` entries of `field` visible at the cut whose
    /// value lies in `[lower, upper)`, compared as strings, in value order.
    /// A `None` bound is open. Every matching entry is returned.
    pub fn versioned_index_range_as_of(
        &self,
        q: VersionedIndexRange<'_>,
    ) -> crate::Result<Vec<(String, StorageKey)>> {
        let VersionedIndexRange {
            database_id,
            tenant,
            coll,
            field,
            lower,
            upper,
            sys_cutoff,
        } = q;
        let field_prefix = format!("{database_id}:{tenant}:{coll}:{field}:");
        let lo = match lower {
            Some(lower) => format!("{field_prefix}{lower}"),
            None => field_prefix.clone(),
        };
        let hi = match upper {
            Some(upper) => upper_value_bound(&field_prefix, upper),
            None => format!("{database_id}:{tenant}:{coll}:{field};"),
        };
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut out = Vec::new();
        for_each_visible_entry(
            &t,
            EntryRange {
                coll,
                prefix: &field_prefix,
                start: &lo,
                end: &hi,
            },
            sys_cutoff,
            |value, doc_id| {
                let above_lower = lower.is_none_or(|l| value >= l);
                let below_upper = upper.is_none_or(|u| value < u);
                if above_lower && below_upper {
                    out.push((value.to_string(), doc_id));
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(out)
    }

    /// Tombstone, at `sys_from`, every index entry of `doc_id` in `coll`
    /// that is live now, inside a caller-owned write transaction. Walks the
    /// collection's whole index. Returns the `(field, value)` pairs it
    /// tombstoned, so a rollback removes exactly those entries.
    pub fn versioned_index_tombstone_document_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<Vec<(String, String)>> {
        let coll_prefix = super::key::coll_prefix(database_id, tenant, coll);
        let coll_end = super::key::coll_prefix_end(database_id, tenant, coll);
        let mut live: Vec<(String, String)> = Vec::new();
        {
            let t = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            for_each_visible_entry(
                &t,
                EntryRange {
                    coll,
                    prefix: &coll_prefix,
                    start: &coll_prefix,
                    end: &coll_end,
                },
                None,
                |field_value, entry_doc| {
                    if entry_doc == *doc_id {
                        let (field, value) = field_value.split_once(':').ok_or_else(|| {
                            invalid_storage_key_err(KeyedTable::IndexesVersioned, coll, field_value)
                        })?;
                        live.push((field.to_string(), value.to_string()));
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        }
        for (field, value) in &live {
            self.versioned_index_tombstone_in_txn(
                txn,
                VersionedIndexEntry {
                    database_id,
                    tenant,
                    coll,
                    field,
                    value,
                    doc_id,
                    sys_from,
                },
            )?;
        }
        Ok(live)
    }

    /// Every `(doc_id, value)` entry of `field` that is live now, in value
    /// order, up to `limit` entries.
    pub fn scan_index_values(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        field: &str,
        limit: usize,
    ) -> crate::Result<Vec<(StorageKey, String)>> {
        let field_prefix = format!("{database_id}:{tenant}:{coll}:{field}:");
        let hi = format!("{database_id}:{tenant}:{coll}:{field};");
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut out = Vec::new();
        for_each_visible_entry(
            &t,
            EntryRange {
                coll,
                prefix: &field_prefix,
                start: &field_prefix,
                end: &hi,
            },
            None,
            |value, doc_id| {
                if out.len() >= limit {
                    return Ok(ControlFlow::Break(()));
                }
                out.push((doc_id, value.to_string()));
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(out)
    }

    /// Append many live index entries in key order inside a caller-owned
    /// write transaction. `keys` are full versioned index keys
    /// ([`VersionedIndexEntry::redb_key`]). They are sorted and deduplicated
    /// here, then written in that order into one opened table: a backfill
    /// scans rows in id order while the index is ordered by value, and key
    /// order keeps each insert near the last. An array field can yield one
    /// value twice for a row, so deduplication is required.
    pub fn versioned_index_put_sorted_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        keys: &mut Vec<String>,
    ) -> crate::Result<()> {
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            return Ok(());
        }
        let mut t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let mut delta = StorageDelta::default();
        for key in keys.iter() {
            insert_counted(&mut t, key, &[TAG_LIVE], &mut delta)?;
        }
        apply_versioned_delta(txn, delta)
    }
}

/// Arguments for [`SparseEngine::versioned_index_range_as_of`].
#[derive(Debug, Clone, Copy)]
pub struct VersionedIndexRange<'a> {
    pub database_id: u64,
    pub tenant: u64,
    pub coll: &'a str,
    pub field: &'a str,
    /// Inclusive lower value bound.
    pub lower: Option<&'a str>,
    /// Exclusive upper value bound.
    pub upper: Option<&'a str>,
    /// System-time ordinal cut (`None` = current).
    pub sys_cutoff: Option<i64>,
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("v.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    fn idx_entry<'a>(
        coll: &'a str,
        field: &'a str,
        value: &'a str,
        doc_id: &'a StorageKey,
        sys_from: i64,
    ) -> VersionedIndexEntry<'a> {
        VersionedIndexEntry {
            database_id: 1,
            tenant: 1,
            coll,
            field,
            value,
            doc_id,
            sys_from,
        }
    }

    #[test]
    fn index_lookup_honors_cutoff_and_tombstone() {
        let (e, _d) = open_temp();
        let u1 = key(1);
        let u2 = key(2);
        e.versioned_index_put(idx_entry("c", "email", "a@x", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "email", "a@x", &u2, 150))
            .unwrap();
        e.versioned_index_tombstone(idx_entry("c", "email", "a@x", &u1, 200))
            .unwrap();

        let at_120 = e
            .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(120))
            .unwrap();
        assert_eq!(at_120, vec![u1]);

        let at_175 = e
            .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(175))
            .unwrap();
        assert_eq!(at_175.len(), 2);

        let at_250 = e
            .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(250))
            .unwrap();
        assert_eq!(at_250, vec![u2]);
    }

    #[test]
    fn versioned_index_remove_in_txn_removes_entry() {
        let (e, _d) = open_temp();
        let u1 = key(1);
        e.versioned_index_put(idx_entry("c", "email", "a@x", &u1, 100))
            .unwrap();
        let before = e
            .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(150))
            .unwrap();
        assert_eq!(before, vec![u1]);

        let txn = e.db.begin_write().unwrap();
        e.versioned_index_remove_in_txn(&txn, idx_entry("c", "email", "a@x", &u1, 100))
            .unwrap();
        txn.commit().unwrap();

        let after = e
            .versioned_index_lookup_as_of(1, 1, "c", "email", "a@x", Some(150))
            .unwrap();
        assert!(after.is_empty());
    }

    #[test]
    fn versioned_index_remove_in_txn_on_missing_key_is_ok() {
        let (e, _d) = open_temp();
        let u9 = key(9);
        let txn = e.db.begin_write().unwrap();
        let r =
            e.versioned_index_remove_in_txn(&txn, idx_entry("c", "email", "nobody@x", &u9, 999));
        assert!(r.is_ok());
        txn.commit().unwrap();
    }

    #[test]
    fn lookup_is_exact_on_values_holding_a_colon() {
        let (e, _d) = open_temp();
        let u1 = key(1);
        let u2 = key(2);
        e.versioned_index_put(idx_entry("c", "f", "a", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "f", "a:b", &u2, 100))
            .unwrap();
        assert_eq!(
            e.versioned_index_lookup_as_of(1, 1, "c", "f", "a", None)
                .unwrap(),
            vec![u1]
        );
        assert_eq!(
            e.versioned_index_lookup_as_of(1, 1, "c", "f", "a:b", None)
                .unwrap(),
            vec![u2]
        );
    }

    #[test]
    fn lookup_returns_every_match() {
        let (e, _d) = open_temp();
        let ids: Vec<StorageKey> = (1..=1500).map(key).collect();
        for id in &ids {
            e.versioned_index_put(idx_entry("c", "f", "v", id, 100))
                .unwrap();
        }
        let got = e
            .versioned_index_lookup_as_of(1, 1, "c", "f", "v", None)
            .unwrap();
        assert_eq!(got.len(), 1500, "no silent truncation");
    }

    #[test]
    fn range_lookup_honors_bounds_and_cut() {
        let (e, _d) = open_temp();
        let (u1, u2, u3, u4) = (key(1), key(2), key(3), key(4));
        e.versioned_index_put(idx_entry("c", "age", "025", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "age", "030", &u2, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "age", "035", &u3, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "age", "040", &u4, 100))
            .unwrap();
        // Row 2 moves from 030 to 045 at 200.
        e.versioned_index_tombstone(idx_entry("c", "age", "030", &u2, 200))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "age", "045", &u2, 200))
            .unwrap();
        let range = |cut: Option<i64>| {
            e.versioned_index_range_as_of(VersionedIndexRange {
                database_id: 1,
                tenant: 1,
                coll: "c",
                field: "age",
                lower: Some("025"),
                upper: Some("036"),
                sys_cutoff: cut,
            })
            .unwrap()
        };
        assert_eq!(
            range(Some(150)),
            vec![
                ("025".to_string(), u1),
                ("030".to_string(), u2),
                ("035".to_string(), u3)
            ]
        );
        assert_eq!(
            range(None),
            vec![("025".to_string(), u1), ("035".to_string(), u3)]
        );
    }

    #[test]
    fn range_upper_is_exclusive_and_prefix_values_stay_in() {
        let (e, _d) = open_temp();
        let (u1, u2) = (key(1), key(2));
        e.versioned_index_put(idx_entry("c", "f", "a", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "f", "a!", &u2, 100))
            .unwrap();
        let got = e
            .versioned_index_range_as_of(VersionedIndexRange {
                database_id: 1,
                tenant: 1,
                coll: "c",
                field: "f",
                lower: None,
                upper: Some("a!"),
                sys_cutoff: None,
            })
            .unwrap();
        assert_eq!(got, vec![("a".to_string(), u1)]);
    }

    #[test]
    fn tombstone_document_hides_every_live_entry_of_the_row() {
        let (e, _d) = open_temp();
        let (u1, u2) = (key(1), key(2));
        e.versioned_index_put(idx_entry("c", "f", "x", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "g", "y", &u1, 100))
            .unwrap();
        e.versioned_index_put(idx_entry("c", "f", "x", &u2, 100))
            .unwrap();
        let txn = e.db.begin_write().unwrap();
        let mut gone = e
            .versioned_index_tombstone_document_in_txn(&txn, 1, 1, "c", &u1, 200)
            .unwrap();
        txn.commit().unwrap();
        gone.sort();
        assert_eq!(
            gone,
            vec![
                ("f".to_string(), "x".to_string()),
                ("g".to_string(), "y".to_string())
            ]
        );
        assert_eq!(
            e.versioned_index_lookup_as_of(1, 1, "c", "f", "x", None)
                .unwrap(),
            vec![u2]
        );
        assert_eq!(
            e.versioned_index_lookup_as_of(1, 1, "c", "f", "x", Some(150))
                .unwrap(),
            vec![u1, u2]
        );
    }
}
