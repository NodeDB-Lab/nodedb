// SPDX-License-Identifier: BUSL-1.1

//! The stored-bytes counter of the sparse file.
//!
//! The counted tables of the sparse file are the user-data tables keyed by
//! `(database_id, tenant_id, collection, …)`:
//!
//! - `documents_versioned` and `indexes_versioned`: every row version and
//!   index entry version, tombstones and erasures included;
//! - the full-text tables `text.postings`, `text.doc_lengths`,
//!   `text.doc_terms`, `text.doc_fields`, `text.segments`,
//!   `text.live_from`, `text.live_by_from` and `text.ended`.
//!
//! Summary and configuration tables do not count: `column_stats`,
//! `text.stats` and `text.meta` hold per-index aggregates and settings, not
//! stored rows. Internal tables do not count either: `chain_heads`,
//! `crdt_dead_letters`, `write_set_captures`, `document_history_bytes` and
//! `storage_bytes` itself.

use crate::engine::sparse::btree::SparseEngine;
use crate::engine::sparse::btree::redb_err;
use crate::engine::storage_bytes::{
    DroppedScope, StorageBytesRow, retire_storage_bytes, storage_bytes_snapshot,
};

/// The sparse file's name in counter errors and reports.
pub(crate) const SPARSE_COUNTER: &str = "sparse";

impl SparseEngine {
    /// Every collection's stored bytes in this file's counted tables, one
    /// row per collection holding bytes. Cost is the number of counter rows.
    pub fn storage_bytes_snapshot(&self) -> crate::Result<Vec<StorageBytesRow>> {
        storage_bytes_snapshot(&self.db, SPARSE_COUNTER)
    }

    /// Remove the counter rows of `scope` once its document, index and
    /// full-text rows are all dropped. A row left holding bytes is reported
    /// before it goes. Returns the number of rows removed.
    pub fn retire_storage_bytes(&self, scope: DroppedScope<'_>) -> crate::Result<usize> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn retire storage bytes", e))?;
        let removed = retire_storage_bytes(&txn, SPARSE_COUNTER, scope)?;
        txn.commit()
            .map_err(|e| redb_err("commit retire storage bytes", e))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use nodedb_fts::IndexScope;
    use nodedb_fts::backend::FtsBackend;
    use nodedb_types::{StorageKey, Surrogate, TenantId};
    use redb::{ReadableDatabase, ReadableTable};

    use super::*;
    use crate::engine::sparse::btree_versioned::doc::DOCUMENTS_VERSIONED;
    use crate::engine::sparse::btree_versioned::history_bytes::key_scope;
    use crate::engine::sparse::btree_versioned::index::INDEXES_VERSIONED;
    use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
    use crate::engine::sparse::fts_redb::tables::{
        DOC_FIELDS, DOC_LENGTHS, DOC_TERMS, POSTINGS, SEGMENTS, TEXT_ENDED, TEXT_LIVE_BY_FROM,
        TEXT_LIVE_FROM,
    };
    use crate::engine::sparse::inverted::InvertedIndex;
    use crate::engine::sparse::inverted::test_support::body;

    type Totals = BTreeMap<(u64, u64, String), u64>;

    fn add(totals: &mut Totals, scope: (u64, u64, &str), bytes: usize) {
        *totals
            .entry((scope.0, scope.1, scope.2.to_owned()))
            .or_default() += bytes as u64;
    }

    /// The stored bytes of every counted table, by a full scan.
    fn scan(engine: &SparseEngine) -> Totals {
        let txn = engine.db().begin_read().unwrap();
        let mut totals = Totals::new();
        for def in [DOCUMENTS_VERSIONED, INDEXES_VERSIONED] {
            for entry in txn.open_table(def).unwrap().iter().unwrap() {
                let (k, v) = entry.unwrap();
                let scope = key_scope(k.value()).unwrap();
                add(&mut totals, scope, k.value().len() + v.value().len());
            }
        }
        for entry in txn.open_table(POSTINGS).unwrap().iter().unwrap() {
            let (k, v) = entry.unwrap();
            let (d, t, c, f, term, _) = k.value();
            let key = 16 + c.len() + f.len() + term.len() + 4;
            add(&mut totals, (d, t, c), key + v.value().len());
        }
        for def in [DOC_LENGTHS, DOC_TERMS] {
            for entry in txn.open_table(def).unwrap().iter().unwrap() {
                let (k, v) = entry.unwrap();
                let (d, t, c, f, _) = k.value();
                add(
                    &mut totals,
                    (d, t, c),
                    16 + c.len() + f.len() + 4 + v.value().len(),
                );
            }
        }
        for entry in txn.open_table(DOC_FIELDS).unwrap().iter().unwrap() {
            let (k, v) = entry.unwrap();
            let (d, t, c, _) = k.value();
            add(&mut totals, (d, t, c), 16 + c.len() + 4 + v.value().len());
        }
        for entry in txn.open_table(SEGMENTS).unwrap().iter().unwrap() {
            let (k, v) = entry.unwrap();
            let (d, t, c, f, s) = k.value();
            let key = 16 + c.len() + f.len() + s.len();
            add(&mut totals, (d, t, c), key + v.value().len());
        }
        for entry in txn.open_table(TEXT_LIVE_FROM).unwrap().iter().unwrap() {
            let (k, _) = entry.unwrap();
            let (d, t, c, f, _) = k.value();
            add(&mut totals, (d, t, c), 16 + c.len() + f.len() + 4 + 8);
        }
        for entry in txn.open_table(TEXT_LIVE_BY_FROM).unwrap().iter().unwrap() {
            let (k, _) = entry.unwrap();
            let (d, t, c, f, _, _) = k.value();
            add(&mut totals, (d, t, c), 16 + c.len() + f.len() + 8 + 4 + 1);
        }
        for entry in txn.open_table(TEXT_ENDED).unwrap().iter().unwrap() {
            let (k, v) = entry.unwrap();
            let (d, t, c, f, ..) = k.value();
            let key = 16 + c.len() + f.len() + 8 + 4 + 8;
            add(&mut totals, (d, t, c), key + v.value().len());
        }
        totals
    }

    /// The counter rows, in the shape of [`scan`].
    fn counted(engine: &SparseEngine) -> Totals {
        engine
            .storage_bytes_snapshot()
            .unwrap()
            .into_iter()
            .map(|(d, t, c, bytes)| ((d.as_u64(), t.as_u64(), c), bytes))
            .collect()
    }

    fn assert_exact(engine: &SparseEngine) {
        assert_eq!(counted(engine), scan(engine));
    }

    fn open(dir: &Path) -> (SparseEngine, InvertedIndex) {
        let engine = SparseEngine::open(&dir.join("sparse.redb")).unwrap();
        let governor = crate::data::executor::core_loop::test_governor();
        let inverted = InvertedIndex::open(engine.db().clone(), governor).unwrap();
        (engine, inverted)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    /// A live version of `doc` in `0:1:users` at `sys`.
    fn version<'a>(doc: &'a StorageKey, sys: i64, body: &'a [u8]) -> VersionedPut<'a> {
        VersionedPut {
            database_id: 0,
            tenant: 1,
            coll: "users",
            doc_id: doc,
            sys_from: sys,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
            body,
        }
    }

    fn entry<'a>(
        coll: &'a str,
        value: &'a str,
        doc: &'a StorageKey,
        sys: i64,
    ) -> VersionedIndexEntry<'a> {
        VersionedIndexEntry {
            database_id: 0,
            tenant: 1,
            coll,
            field: "email",
            value,
            doc_id: doc,
            sys_from: sys,
        }
    }

    #[test]
    fn document_and_index_writes_match_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _inverted) = open(dir.path());
        let doc = key(1);
        engine.put(0, 1, "users", &doc, b"alice", 10).unwrap();
        assert_exact(&engine);
        assert!(counted(&engine)[&(0, 1, "users".to_owned())] > 0);

        // An overwrite of one version subtracts the bytes it replaces.
        engine
            .versioned_put(version(&doc, 10, b"a much longer body"))
            .unwrap();
        assert_exact(&engine);
        engine.versioned_put(version(&doc, 10, b"x")).unwrap();
        assert_exact(&engine);

        engine.put(0, 1, "users", &doc, b"alice2", 20).unwrap();
        engine
            .versioned_index_put(entry("users", "a@x", &doc, 10))
            .unwrap();
        engine
            .versioned_index_tombstone(entry("users", "a@x", &doc, 20))
            .unwrap();
        assert_exact(&engine);
        let txn = engine.begin_write().unwrap();
        engine
            .versioned_index_remove_in_txn(&txn, entry("users", "a@x", &doc, 20))
            .unwrap();
        txn.commit().unwrap();
        assert_exact(&engine);

        engine.delete(0, 1, "users", &doc, 30).unwrap();
        assert_exact(&engine);
        engine.versioned_gdpr_erase(0, 1, "users", &doc).unwrap();
        assert_exact(&engine);
        engine
            .purge_document_versions_below(0, 1, "users", 25)
            .unwrap();
        assert_exact(&engine);
        engine
            .delete_all_versioned_for_collection(0, 1, "users")
            .unwrap();
        assert_exact(&engine);
        assert!(counted(&engine).is_empty());
    }

    #[test]
    fn text_writes_match_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, inverted) = open(dir.path());
        let t = TenantId::new(1);
        let s = Surrogate::new(7);
        inverted
            .index_document_at(0, t, "docs", s, &body("hello quick world"), 10)
            .unwrap();
        assert_exact(&engine);
        assert!(!counted(&engine).is_empty());
        // A re-index ends the prior version into the history rows.
        inverted
            .index_document_at(0, t, "docs", s, &body("hello slow"), 20)
            .unwrap();
        assert_exact(&engine);
        inverted
            .index_document_at(0, t, "docs", Surrogate::new(8), &body("other words"), 21)
            .unwrap();
        assert_exact(&engine);
        inverted.undo_text_version(0, t, "docs", s, 20).unwrap();
        assert_exact(&engine);
        inverted.remove_document_at(0, t, "docs", s, 30).unwrap();
        assert_exact(&engine);
        inverted.purge_text_versions_below(25).unwrap();
        assert_exact(&engine);
        inverted
            .remove_documents(0, t, "docs", &[Surrogate::new(8)], 40)
            .unwrap();
        assert_exact(&engine);

        let index = IndexScope::document("docs");
        let backend = inverted.backend();
        backend
            .write_segment(0, 1, index, "L0:1", b"segment bytes")
            .unwrap();
        backend.write_segment(0, 1, index, "L0:1", b"seg").unwrap();
        assert_exact(&engine);
        backend.remove_segment(0, 1, index, "L0:1").unwrap();
        assert_exact(&engine);

        inverted
            .index_document_at(0, t, "docs", s, &body("again"), 50)
            .unwrap();
        inverted.purge_collection(0, t, "docs").unwrap();
        assert_exact(&engine);
        assert!(counted(&engine).is_empty());
    }

    #[test]
    fn the_counter_survives_reopening_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let before = {
            let (engine, inverted) = open(dir.path());
            engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
            inverted
                .index_document_at(
                    0,
                    TenantId::new(1),
                    "users",
                    Surrogate::new(1),
                    &body("alice"),
                    10,
                )
                .unwrap();
            counted(&engine)
        };
        assert!(!before.is_empty());
        let (engine, _inverted) = open(dir.path());
        assert_eq!(counted(&engine), before);
        assert_exact(&engine);
    }

    #[test]
    fn a_batch_write_sums_into_one_counter_row() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _inverted) = open(dir.path());
        let docs: Vec<StorageKey> = (1..=5).map(key).collect();
        let mut keys: Vec<String> = docs
            .iter()
            .map(|doc| entry("users", "v", doc, 10).redb_key())
            .collect();
        let expected: u64 = keys.iter().map(|k| (k.len() + 1) as u64).sum();
        let txn = engine.begin_write().unwrap();
        engine
            .versioned_index_put_sorted_in_txn(&txn, &mut keys)
            .unwrap();
        txn.commit().unwrap();
        assert_eq!(counted(&engine)[&(0, 1, "users".to_owned())], expected);
        assert_exact(&engine);

        let txn = engine.begin_write().unwrap();
        for doc in &docs {
            engine
                .versioned_put_in_txn(&txn, version(doc, 10, b"row"))
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(counted(&engine).len(), 1);
        assert_exact(&engine);
    }

    #[test]
    fn tenants_and_databases_stay_separate() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, inverted) = open(dir.path());
        for (d, t) in [(0, 1), (0, 2), (1, 1)] {
            engine.put(d, t, "c", &key(1), b"row", 10).unwrap();
            inverted
                .index_document_at(
                    d,
                    TenantId::new(t),
                    "c",
                    Surrogate::new(1),
                    &body("text"),
                    10,
                )
                .unwrap();
        }
        engine.put(0, 1, "other", &key(1), b"row", 10).unwrap();
        let rows = counted(&engine);
        assert_eq!(rows.len(), 4);
        assert_exact(&engine);

        engine.delete_all_versioned_for_tenant(0, 1).unwrap();
        inverted.purge_tenant(0, TenantId::new(1)).unwrap();
        let tenant = DroppedScope::Tenant {
            database_id: 0,
            tenant_id: 1,
        };
        assert_eq!(engine.retire_storage_bytes(tenant).unwrap(), 0);
        let after = counted(&engine);
        assert_eq!(after.len(), 2);
        assert_eq!(
            after[&(0, 2, "c".to_owned())],
            rows[&(0, 2, "c".to_owned())]
        );
        assert_eq!(
            after[&(1, 1, "c".to_owned())],
            rows[&(1, 1, "c".to_owned())]
        );
        assert_exact(&engine);
    }

    #[test]
    fn retiring_a_scope_removes_only_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _inverted) = open(dir.path());
        engine.put(0, 1, "a", &key(1), b"row", 10).unwrap();
        engine.put(0, 1, "b", &key(1), b"row", 10).unwrap();
        let only_a = DroppedScope::Collection {
            database_id: 0,
            tenant_id: 1,
            collection: "a",
        };
        // The rows of `a` still exist, so the counter row is residue here.
        assert_eq!(engine.retire_storage_bytes(only_a).unwrap(), 1);
        let rows = counted(&engine);
        assert_eq!(rows.len(), 1);
        assert!(rows.contains_key(&(0, 1, "b".to_owned())));
    }

    /// A small deterministic generator, so a failing sequence replays.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % n
        }
    }

    #[test]
    fn a_random_sequence_keeps_the_counter_equal_to_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, inverted) = open(dir.path());
        let words = ["alpha", "beta", "gamma", "delta", "eps"];
        let mut rng = Lcg(0x5eed);
        let mut sys = 1i64;
        for _ in 0..300 {
            sys += 1;
            let tenant = 1 + rng.below(2);
            let coll = ["a", "b"][rng.below(2) as usize];
            let doc_n = rng.below(4) as u32;
            let doc = key(doc_n);
            let tid = TenantId::new(tenant);
            match rng.below(8) {
                0 | 1 => {
                    let len = rng.below(40) as usize;
                    engine
                        .put(0, tenant, coll, &doc, &vec![b'x'; len], sys)
                        .unwrap();
                }
                2 => {
                    engine.delete(0, tenant, coll, &doc, sys).unwrap();
                }
                3 => {
                    let value = words[rng.below(5) as usize];
                    let e = VersionedIndexEntry {
                        database_id: 0,
                        tenant,
                        coll,
                        field: "f",
                        value,
                        doc_id: &doc,
                        sys_from: sys,
                    };
                    engine.versioned_index_put(e).unwrap();
                }
                4 => {
                    let n = 1 + rng.below(4) as usize;
                    let text: Vec<&str> = (0..n).map(|_| words[rng.below(5) as usize]).collect();
                    let surrogate = Surrogate::new(doc_n);
                    inverted
                        .index_document_at(0, tid, coll, surrogate, &body(&text.join(" ")), sys)
                        .unwrap();
                }
                5 => {
                    let surrogate = Surrogate::new(doc_n);
                    inverted
                        .remove_document_at(0, tid, coll, surrogate, sys)
                        .unwrap();
                }
                6 => {
                    engine
                        .purge_document_versions_below(0, tenant, coll, sys / 2)
                        .unwrap();
                    inverted.purge_text_versions_below(sys / 2).unwrap();
                }
                _ => {
                    if rng.below(4) == 0 {
                        engine
                            .delete_all_versioned_for_collection(0, tenant, coll)
                            .unwrap();
                        inverted.purge_collection(0, tid, coll).unwrap();
                    } else {
                        engine.versioned_gdpr_erase(0, tenant, coll, &doc).unwrap();
                    }
                }
            }
            assert_exact(&engine);
        }
    }
}
