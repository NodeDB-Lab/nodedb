// SPDX-License-Identifier: BUSL-1.1

//! Audit-retention purge for the versioned document / index tables.
//!
//! Same contract as `edge_store::temporal::purge`: drop *superseded*
//! versions older than `cutoff_system_ms`. The newest version of every
//! `(doc_id)` — live, tombstone, or GDPR-erased — is always preserved so
//! the Ceiling read for any future `AS OF` cutoff still sees the row's
//! terminal state.

use redb::{ReadableDatabase, ReadableTable};

use super::doc::DOCUMENTS_VERSIONED;
use super::index::INDEXES_VERSIONED;
use super::key::{coll_prefix, coll_prefix_end, tenant_prefix, tenant_prefix_end};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

impl SparseEngine {
    /// Hard-drop **every** version of every document + secondary-index entry
    /// for a single `(database_id, tenant, coll)` from the versioned tables.
    ///
    /// This is the DROP-collection counterpart to
    /// [`Self::purge_superseded_document_versions`] (retention GC): where the
    /// GC path keeps the terminal version of every doc_id, this path removes
    /// the entire collection prefix unconditionally. Without it, a
    /// DROP + re-CREATE of a `bitemporal=true` collection under the same name
    /// reuses the same key prefix and the old versioned history resurrects.
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
                docs.remove(k.as_str())
                    .map_err(|e| redb_err("remove doc version drop", e))?;
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
                idx.remove(k.as_str())
                    .map_err(|e| redb_err("remove idx version drop", e))?;
            }
        }
        txn.commit()
            .map_err(|e| redb_err("commit versioned drop", e))?;
        Ok((docs_removed, idx_removed))
    }

    /// Purge superseded document + index versions for `(tenant, coll)`.
    /// Returns `(docs_deleted, index_entries_deleted)`.
    pub fn purge_superseded_document_versions(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<(usize, usize)> {
        let doc_victims = self.collect_doc_victims(database_id, tenant, coll, cutoff_system_ms)?;
        let idx_victims =
            self.collect_index_victims(database_id, tenant, coll, cutoff_system_ms)?;

        if doc_victims.is_empty() && idx_victims.is_empty() {
            return Ok((0, 0));
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn purge", e))?;
        {
            let mut docs = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open docs purge", e))?;
            for k in &doc_victims {
                docs.remove(k.as_str())
                    .map_err(|e| redb_err("remove doc version", e))?;
            }
            let mut idx = txn
                .open_table(INDEXES_VERSIONED)
                .map_err(|e| redb_err("open idx purge", e))?;
            for k in &idx_victims {
                idx.remove(k.as_str())
                    .map_err(|e| redb_err("remove idx version", e))?;
            }
        }
        txn.commit().map_err(|e| redb_err("commit purge", e))?;
        Ok((doc_victims.len(), idx_victims.len()))
    }

    fn collect_doc_victims(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<Vec<String>> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read txn purge", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open docs read purge", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range purge", e))?;

        // Stream pattern: table sorted by (doc_id, sys_from) ascending.
        // Group-by rule: base-group identity is the prefix before `\x00`.
        // Whenever we see a row for a base we've already seen, the previous
        // row in that base is guaranteed to be superseded; it becomes a
        // victim only if also below the cutoff. The last-seen row per base
        // is always kept.
        let mut victims: Vec<String> = Vec::new();
        let mut pending: Option<(String, i64, String)> = None; // (base, sys_from, full_key)

        for r in range {
            let (k, _v) = r.map_err(|e| redb_err("entry purge", e))?;
            let key_str = k.value().to_string();
            let Some((base, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Ok(sys_from) = suffix.parse::<i64>() else {
                continue;
            };
            let base_s = base.to_string();

            match pending.take() {
                None => {
                    pending = Some((base_s, sys_from, key_str));
                }
                Some((prev_base, prev_sf, prev_key)) if prev_base == base_s => {
                    if prev_sf < cutoff_system_ms {
                        victims.push(prev_key);
                    }
                    pending = Some((base_s, sys_from, key_str));
                }
                Some(_prev_latest_for_other_base) => {
                    pending = Some((base_s, sys_from, key_str));
                }
            }
        }
        Ok(victims)
    }

    fn collect_index_victims(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<Vec<String>> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read txn idx purge", e))?;
        let t = txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open idx read purge", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range idx purge", e))?;

        // Same streaming group-by: the base here is
        // `{tenant}:{coll}:{field}:{value}:{doc_id}`, terminated by `\x00`.
        let mut victims: Vec<String> = Vec::new();
        let mut pending: Option<(String, i64, String)> = None;

        for r in range {
            let (k, _v) = r.map_err(|e| redb_err("entry idx purge", e))?;
            let key_str = k.value().to_string();
            let Some((base, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Ok(sys_from) = suffix.parse::<i64>() else {
                continue;
            };
            let base_s = base.to_string();

            match pending.take() {
                None => {
                    pending = Some((base_s, sys_from, key_str));
                }
                Some((prev_base, prev_sf, prev_key)) if prev_base == base_s => {
                    if prev_sf < cutoff_system_ms {
                        victims.push(prev_key);
                    }
                    pending = Some((base_s, sys_from, key_str));
                }
                Some(_) => {
                    pending = Some((base_s, sys_from, key_str));
                }
            }
        }
        Ok(victims)
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

    fn put_version(eng: &SparseEngine, doc_id: u32, sys_from: i64) {
        eng.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll: "c",
            doc_id: &key(doc_id),
            body: b"payload",
            sys_from_ms: sys_from,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
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
        assert_eq!(docs, 1, "only v@100 is below cutoff AND superseded");
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
            .purge_superseded_document_versions(1, 1, "c", 150)
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
            .purge_superseded_document_versions(1, 1, "c", 150)
            .unwrap();
        let (b, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 150)
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
            sys_from_ms: 100,
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
            sys_from_ms: 100,
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
                sys_from_ms: 100,
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
            sys_from_ms: 100,
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
            sys_from_ms: 100,
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
            sys_from_ms: 200,
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
            sys_from_ms: 100,
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
            sys_from_ms: 200,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();

        let (docs, _) = eng
            .purge_superseded_document_versions(1, 1, "c", 150)
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
}
