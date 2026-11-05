// SPDX-License-Identifier: BUSL-1.1

//! History purge and hard drops for the versioned document / index tables.
//!
//! The purge at ordinal `horizon` removes every version no read at or above
//! the horizon can see: a version whose successor's `sys_from <= horizon`,
//! and a tombstone at or below the horizon with nothing after it. The
//! version visible at the horizon always stays, so every `AS OF` read at or
//! above the horizon answers as before the purge. The audit-retention purge
//! of a `bitemporal=true` collection runs the same rule at its retention
//! cutoff.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::doc::DOCUMENTS_VERSIONED;
use super::history_bytes::{apply_versioned_delta, remove_counted};
use super::index::INDEXES_VERSIONED;
use super::key::{coll_prefix, coll_prefix_end, tenant_prefix, tenant_prefix_end};
use super::value::TAG_TOMBSTONE;
use crate::engine::sparse::btree::{KeyedTable, SparseEngine, invalid_storage_key_err, redb_err};
use crate::engine::storage_bytes::StorageDelta;

impl SparseEngine {
    /// Hard-drop **every** version of every document + secondary-index entry
    /// for a single `(database_id, tenant, coll)` from the versioned tables.
    ///
    /// This is the DROP-collection counterpart to
    /// [`Self::purge_document_versions_below`]: where the purge keeps the
    /// version visible at its horizon, this path removes the entire
    /// collection prefix unconditionally. Without it, a DROP + re-CREATE
    /// under the same name reuses the same key prefix and the old history
    /// resurrects.
    ///
    /// Returns `(doc_versions_removed, index_versions_removed)`.
    pub fn delete_all_versioned_for_collection(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
    ) -> crate::Result<(usize, usize)> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        self.delete_all_versioned_in_range(&lo, &hi)
    }

    /// Whole-tenant analogue of [`Self::delete_all_versioned_for_collection`]:
    /// hard-drop every versioned document + index row across all collections
    /// belonging to `(database_id, tenant)`.
    pub fn delete_all_versioned_for_tenant(
        &self,
        database_id: u64,
        tenant: u64,
    ) -> crate::Result<(usize, usize)> {
        let lo = tenant_prefix(database_id, tenant);
        let hi = tenant_prefix_end(database_id, tenant);
        self.delete_all_versioned_in_range(&lo, &hi)
    }

    /// Range-delete every row of both versioned tables within `[lo, hi)`.
    /// Keys are collected first (redb's range iterator borrows the table
    /// immutably) then removed under a single write transaction so the
    /// document and index purges commit atomically.
    fn delete_all_versioned_in_range(&self, lo: &str, hi: &str) -> crate::Result<(usize, usize)> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn versioned drop", e))?;

        let docs_removed;
        let idx_removed;
        let mut delta = StorageDelta::default();
        {
            let mut docs = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open docs versioned drop", e))?;
            let mut doc_keys: Vec<String> = Vec::new();
            for r in docs
                .range(lo..hi)
                .map_err(|e| redb_err("range docs versioned drop", e))?
            {
                let (k, _) = r.map_err(|e| redb_err("entry docs versioned drop", e))?;
                doc_keys.push(k.value().to_string());
            }
            docs_removed = doc_keys.len();
            for k in &doc_keys {
                remove_counted(&mut docs, k, &mut delta)?;
            }

            let mut idx = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open idx versioned drop", e))?;
            let mut idx_keys: Vec<String> = Vec::new();
            for r in idx
                .range(lo..hi)
                .map_err(|e| redb_err("range idx versioned drop", e))?
            {
                let (k, _) = r.map_err(|e| redb_err("entry idx versioned drop", e))?;
                idx_keys.push(k.value().to_string());
            }
            idx_removed = idx_keys.len();
            for k in &idx_keys {
                remove_counted(&mut idx, k, &mut delta)?;
            }
        }
        apply_versioned_delta(&txn, delta)?;
        txn.commit()
            .map_err(|e| redb_err("commit versioned drop", e))?;
        Ok((docs_removed, idx_removed))
    }

    /// Audit-retention purge of `(tenant, coll)`. `cutoff_system_ms` is the
    /// retention horizon in milliseconds. It converts to the last ordinal of
    /// that millisecond, as the edge store's purge converts it, and the purge
    /// runs [`Self::purge_document_versions_below`] at that ordinal. Returns
    /// `(doc_versions_deleted, index_entries_deleted)`.
    pub fn purge_superseded_document_versions(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<(usize, usize)> {
        let horizon = nodedb_types::ms_to_ordinal_upper(cutoff_system_ms);
        self.purge_document_versions_below(database_id, tenant, coll, horizon)
    }

    /// Remove every document version and index entry of `(tenant, coll)`
    /// that no read at or above `horizon` can see.
    ///
    /// A version goes when its successor's `sys_from <= horizon`. The
    /// version visible at the horizon always stays. A tombstone at or below
    /// the horizon with nothing after it goes too, and its row leaves the
    /// table entirely. Returns `(doc_versions_deleted,
    /// index_entries_deleted)`.
    pub fn purge_document_versions_below(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        horizon: i64,
    ) -> crate::Result<(usize, usize)> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let doc_victims = self.collect_victims(DOCUMENTS_VERSIONED, &lo, &hi, horizon, coll)?;
        let idx_victims = self.collect_victims(INDEXES_VERSIONED, &lo, &hi, horizon, coll)?;

        if doc_victims.is_empty() && idx_victims.is_empty() {
            return Ok((0, 0));
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn purge", e))?;
        let mut delta = StorageDelta::default();
        {
            let mut docs = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open docs purge", e))?;
            for k in &doc_victims {
                remove_counted(&mut docs, k, &mut delta)?;
            }
            let mut idx = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open idx purge", e))?;
            for k in &idx_victims {
                remove_counted(&mut idx, k, &mut delta)?;
            }
        }
        apply_versioned_delta(&txn, delta)?;
        txn.commit().map_err(|e| redb_err("commit purge", e))?;
        Ok((doc_victims.len(), idx_victims.len()))
    }

    /// The keys of `table_def` in `[lo, hi)` that the purge rule at
    /// `horizon` removes. Both versioned tables key a version as
    /// `{base}\x00{sys_from:020}` and lead its value with the tag byte, so
    /// one walk serves both. Only the tag byte is read.
    fn collect_victims(
        &self,
        table_def: TableDefinition<'static, &'static str, &'static [u8]>,
        lo: &str,
        hi: &str,
        horizon: i64,
        coll: &str,
    ) -> crate::Result<Vec<String>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read txn purge", e))?;
        let t = txn
            .open_table(table_def)
            .map_err(|e| redb_err("open table purge", e))?;
        let range = t.range(lo..hi).map_err(|e| redb_err("range purge", e))?;

        let mut victims: Vec<String> = Vec::new();
        // The previous version of the row being walked: its key, its
        // `sys_from`, and whether it is a tombstone.
        let mut pending: Option<PendingVersion> = None;
        for r in range {
            let (k, v) = r.map_err(|e| redb_err("entry purge", e))?;
            let key = k.value();
            let invalid = || invalid_storage_key_err(KeyedTable::DocumentsVersioned, coll, key);
            let (base, suffix) = key.rsplit_once('\x00').ok_or_else(invalid)?;
            let sys_from = suffix.parse::<i64>().map_err(|_| invalid())?;
            let tombstone = v.value().first().copied().ok_or_else(invalid)? == TAG_TOMBSTONE;
            if let Some(prev) = pending.take() {
                let superseded = prev.base_len == base.len()
                    && prev.key.as_bytes()[..prev.base_len] == *base.as_bytes();
                if superseded {
                    if sys_from <= horizon {
                        victims.push(prev.key);
                    }
                } else if prev.terminal_victim(horizon) {
                    victims.push(prev.key);
                }
            }
            pending = Some(PendingVersion {
                key: key.to_string(),
                base_len: base.len(),
                sys_from,
                tombstone,
            });
        }
        if let Some(prev) = pending
            && prev.terminal_victim(horizon)
        {
            victims.push(prev.key);
        }
        Ok(victims)
    }
}

/// The last version the purge walk read.
struct PendingVersion {
    key: String,
    /// Length of the key's `{base}` part, before `\x00`.
    base_len: usize,
    sys_from: i64,
    tombstone: bool,
}

impl PendingVersion {
    /// Whether this version, the newest of its row, goes: a tombstone at or
    /// below the horizon.
    fn terminal_victim(&self, horizon: i64) -> bool {
        self.tombstone && self.sys_from <= horizon
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::StorageKey;

    use super::super::value::VersionedPut;
    use super::*;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(nodedb_types::Surrogate::new(surrogate))
    }

    fn fresh_engine() -> (tempfile::TempDir, SparseEngine) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.redb");
        let eng = SparseEngine::open(&path).unwrap();
        (dir, eng)
    }

    /// A version stamped at the first ordinal of millisecond `at_ms`.
    fn put_version(eng: &SparseEngine, doc_id: u32, at_ms: i64) {
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll: "c",
            doc_id: &key(doc_id),
            body: b"payload",
            sys_from: at_ms * nodedb_types::NANOS_PER_MS,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
    }

    /// A version superseded inside the horizon millisecond is purged: the
    /// horizon converts to the last ordinal of that millisecond. The version
    /// visible at the horizon stays.
    #[test]
    fn purge_reaches_versions_inside_the_horizon_millisecond() {
        let (_d, eng) = fresh_engine();
        let first_of_150 = 150 * nodedb_types::NANOS_PER_MS;
        let first_of_151 = 151 * nodedb_types::NANOS_PER_MS;
        for sys_from in [first_of_150, first_of_150 + 5, first_of_151] {
            eng.versioned_put(VersionedPut {
                database_id: 1,
                tenant: 1,
                coll: "c",
                doc_id: &key(1),
                body: b"payload",
                sys_from,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            })
            .unwrap();
        }
        let (docs, _idx) = eng
            .purge_superseded_document_versions(1, 1, "c", 150)
            .unwrap();
        assert_eq!(docs, 1, "only the version superseded inside ms 150 goes");
        assert_eq!(count_doc_versions(&eng, 1), 2);
    }

    fn count_doc_versions(eng: &SparseEngine, doc_id: u32) -> usize {
        let lo = super::super::key::doc_prefix(1, 1, "c", &key(doc_id));
        let hi = super::super::key::doc_prefix_end(1, 1, "c", &key(doc_id));
        let txn = eng.db.begin_read().unwrap();
        let t = txn.open_table(DOCUMENTS_VERSIONED).unwrap();
        t.range(lo.as_str()..hi.as_str()).unwrap().count()
    }

    #[test]
    fn purge_drops_superseded_doc_versions_below_cutoff() {
        let (_d, eng) = fresh_engine();
        put_version(&eng, 1, 100);
        put_version(&eng, 1, 200);
        put_version(&eng, 1, 300);
        assert_eq!(count_doc_versions(&eng, 1), 3);

        let (docs, _idx) = eng
            .purge_superseded_document_versions(1, 1, "c", 150)
            .unwrap();
        assert_eq!(docs, 0, "v@100 is the version visible at 150 and stays");
        let (docs, _idx) = eng
            .purge_superseded_document_versions(1, 1, "c", 250)
            .unwrap();
        assert_eq!(docs, 1, "v@100 is superseded at 200 <= 250");
        assert_eq!(count_doc_versions(&eng, 1), 2);
    }

    #[test]
    fn purge_preserves_latest_version_even_below_cutoff() {
        let (_d, eng) = fresh_engine();
        put_version(&eng, 1, 100);
        // Only one version: it's the latest, never deleted.
        let (docs, _idx) = eng
            .purge_superseded_document_versions(1, 1, "c", 10_000)
            .unwrap();
        assert_eq!(docs, 0);
        assert_eq!(count_doc_versions(&eng, 1), 1);
    }

    #[test]
    fn purge_groups_by_doc_id() {
        let (_d, eng) = fresh_engine();
        // Two docs, each with 3 versions at 100/200/300.
        for id in [1u32, 2] {
            put_version(&eng, id, 100);
            put_version(&eng, id, 200);
            put_version(&eng, id, 300);
        }
        let (docs, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 250)
            .unwrap();
        assert_eq!(docs, 2, "one victim per doc");
        assert_eq!(count_doc_versions(&eng, 1), 2);
        assert_eq!(count_doc_versions(&eng, 2), 2);
    }

    #[test]
    fn purge_is_idempotent() {
        let (_d, eng) = fresh_engine();
        put_version(&eng, 1, 100);
        put_version(&eng, 1, 200);
        put_version(&eng, 1, 300);
        let (a, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 250)
            .unwrap();
        let (b, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 250)
            .unwrap();
        assert_eq!(a, 1);
        assert_eq!(b, 0);
    }

    fn count_all_versions(eng: &SparseEngine, database_id: u64, tenant: u64, coll: &str) -> usize {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let txn = eng.db.begin_read().unwrap();
        let t = txn.open_table(DOCUMENTS_VERSIONED).unwrap();
        t.range(lo.as_str()..hi.as_str()).unwrap().count()
    }

    #[test]
    fn delete_all_versioned_for_collection_drops_every_version() {
        let (_d, eng) = fresh_engine();
        // Two docs, three system-time versions each in coll "c".
        for id in [1u32, 2] {
            put_version(&eng, id, 100);
            put_version(&eng, id, 200);
            put_version(&eng, id, 300);
        }
        // Same doc_ids in a sibling collection and a second tenant must survive.
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll: "other",
            doc_id: &key(1),
            body: b"x",
            sys_from: 100,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 2,
            coll: "c",
            doc_id: &key(1),
            body: b"x",
            sys_from: 100,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();

        assert_eq!(count_all_versions(&eng, 1, 1, "c"), 6);
        let (docs, _idx) = eng.delete_all_versioned_for_collection(1, 1, "c").unwrap();
        assert_eq!(docs, 6, "all 6 versions of coll c tenant 1 removed");
        assert_eq!(
            count_all_versions(&eng, 1, 1, "c"),
            0,
            "no history resurrects"
        );
        // Untouched neighbours.
        assert_eq!(count_all_versions(&eng, 1, 1, "other"), 1);
        assert_eq!(count_all_versions(&eng, 1, 2, "c"), 1);

        // Idempotent: a second drop removes nothing and does not error.
        let (docs2, _) = eng.delete_all_versioned_for_collection(1, 1, "c").unwrap();
        assert_eq!(docs2, 0);
    }

    #[test]
    fn delete_all_versioned_for_tenant_drops_every_collection() {
        let (_d, eng) = fresh_engine();
        for coll in ["c", "other"] {
            eng.versioned_put(VersionedPut {
                database_id: 1,
                tenant: 1,
                coll,
                doc_id: &key(1),
                body: b"x",
                sys_from: 100,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            })
            .unwrap();
        }
        // A different tenant must survive.
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 2,
            coll: "c",
            doc_id: &key(1),
            body: b"x",
            sys_from: 100,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();

        let (docs, _idx) = eng.delete_all_versioned_for_tenant(1, 1).unwrap();
        assert_eq!(docs, 2, "one version in each of two collections removed");
        assert_eq!(count_all_versions(&eng, 1, 1, "c"), 0);
        assert_eq!(count_all_versions(&eng, 1, 1, "other"), 0);
        assert_eq!(
            count_all_versions(&eng, 1, 2, "c"),
            1,
            "other tenant intact"
        );
    }

    #[test]
    fn purge_scoped_to_tenant_collection() {
        let (_d, eng) = fresh_engine();
        // same doc_id in another collection / tenant; must not be touched.
        put_version(&eng, 1, 100);
        put_version(&eng, 1, 200);
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll: "other",
            doc_id: &key(1),
            body: b"x",
            sys_from: 100,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll: "other",
            doc_id: &key(1),
            body: b"x",
            sys_from: 200,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 2,
            coll: "c",
            doc_id: &key(1),
            body: b"x",
            sys_from: 100,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 2,
            coll: "c",
            doc_id: &key(1),
            body: b"x",
            sys_from: 200,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();

        let (docs, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 250)
            .unwrap();
        assert_eq!(docs, 1);
        // The other collection and tenant still have all their versions.
        let lo_other = super::super::key::doc_prefix(1, 1, "other", &key(1));
        let hi_other = super::super::key::doc_prefix_end(1, 1, "other", &key(1));
        let txn = eng.db.begin_read().unwrap();
        let t = txn.open_table(DOCUMENTS_VERSIONED).unwrap();
        assert_eq!(
            t.range(lo_other.as_str()..hi_other.as_str())
                .unwrap()
                .count(),
            2
        );
        let lo_t2 = super::super::key::doc_prefix(1, 2, "c", &key(1));
        let hi_t2 = super::super::key::doc_prefix_end(1, 2, "c", &key(1));
        assert_eq!(t.range(lo_t2.as_str()..hi_t2.as_str()).unwrap().count(), 2);
    }

    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let (_d, eng) = fresh_engine();
        for sys_from in [100, 200, 300] {
            eng.put(
                1,
                1,
                "c",
                &key(1),
                format!("v{sys_from}").as_bytes(),
                sys_from,
            )
            .unwrap();
        }
        let (docs, _) = eng.purge_document_versions_below(1, 1, "c", 250).unwrap();
        assert_eq!(docs, 1, "only v@100 goes: its successor 200 <= 250");
        let at = |cut: i64| {
            eng.versioned_get_as_of(1, 1, "c", &key(1), Some(cut), None)
                .unwrap()
        };
        assert_eq!(at(250), Some(b"v200".to_vec()));
        assert_eq!(at(300), Some(b"v300".to_vec()));
    }

    #[test]
    fn purge_removes_a_row_whose_tombstone_is_below_the_horizon() {
        let (_d, eng) = fresh_engine();
        eng.put(1, 1, "c", &key(1), b"v1", 100).unwrap();
        eng.delete(1, 1, "c", &key(1), 200).unwrap();
        eng.put(1, 1, "c", &key(2), b"w1", 100).unwrap();
        eng.delete(1, 1, "c", &key(2), 400).unwrap();
        let (docs, _) = eng.purge_document_versions_below(1, 1, "c", 300).unwrap();
        assert_eq!(
            docs, 2,
            "row 1 leaves the table: its live version and tombstone"
        );
        assert_eq!(count_doc_versions(&eng, 1), 0);
        assert_eq!(
            count_doc_versions(&eng, 2),
            2,
            "row 2's tombstone sits above the horizon"
        );
        assert_eq!(
            eng.versioned_get_as_of(1, 1, "c", &key(2), Some(300), None)
                .unwrap(),
            Some(b"w1".to_vec())
        );
    }

    #[test]
    fn purge_applies_the_rule_to_index_entries() {
        use super::super::value::VersionedIndexEntry;
        let (_d, eng) = fresh_engine();
        let doc1 = key(1);
        let entry = |value: &'static str, sys_from: i64| VersionedIndexEntry {
            database_id: 1,
            tenant: 1,
            coll: "c",
            field: "f",
            value,
            doc_id: &doc1,
            sys_from,
        };
        eng.versioned_index_put(entry("a", 100)).unwrap();
        eng.versioned_index_tombstone(entry("a", 200)).unwrap();
        eng.versioned_index_put(entry("b", 200)).unwrap();
        let (_, idx) = eng.purge_document_versions_below(1, 1, "c", 250).unwrap();
        assert_eq!(idx, 2, "value a's put and its tombstone both go");
        assert_eq!(
            eng.versioned_index_lookup_as_of(1, 1, "c", "f", "b", None)
                .unwrap(),
            vec![doc1]
        );
    }
}
