// SPDX-License-Identifier: BUSL-1.1

//! `InvertedIndex` struct, lifecycle, backend access, and structural
//! tenant/collection purge. All other concerns (indexing, search,
//! synonyms, compaction) live in sibling modules.

use std::cell::RefCell;
use std::sync::Arc;

use nodedb_mem::MemoryGovernor;
use nodedb_types::TenantId;

use super::errors::into_result_err;
use super::rebuild_journal::FtsJournals;
use crate::engine::durability_gate::GatedDatabase;
use crate::engine::sparse::fts_redb::RedbFtsBackend;
use crate::storage::quarantine::QuarantineRegistry;

/// Full-text inverted index backed by redb via `nodedb-fts`.
pub struct InvertedIndex {
    pub(super) inner: nodedb_fts::index::FtsIndex<RedbFtsBackend>,
    /// Write journals of the collection rebuilds running on this index.
    pub(super) journals: RefCell<FtsJournals>,
}

impl InvertedIndex {
    /// Open or create an inverted index at the given redb database, with
    /// FTS memory budgeted against `governor`.
    pub fn open(db: Arc<GatedDatabase>, governor: Arc<MemoryGovernor>) -> crate::Result<Self> {
        let backend = RedbFtsBackend::open(db)?;
        Ok(Self {
            inner: nodedb_fts::index::FtsIndex::new(backend, governor),
            journals: RefCell::new(FtsJournals::default()),
        })
    }

    /// Install the quarantine registry for corrupt FTS segment detection.
    ///
    /// Called once by the server bootstrap after the registry is created.
    pub fn set_quarantine_registry(&mut self, registry: Arc<QuarantineRegistry>) {
        self.inner.backend_mut().set_quarantine_registry(registry);
    }

    /// Shared access to the underlying redb FTS backend.
    ///
    /// Exposes the raw `FtsBackend` methods for maintenance operations such as
    /// bulk postings snapshot and restore used by concurrent index rebuild.
    pub fn backend(&self) -> &RedbFtsBackend {
        self.inner.backend()
    }

    /// Mutable access to the underlying redb FTS backend.
    pub fn backend_mut(&mut self) -> &mut RedbFtsBackend {
        self.inner.backend_mut()
    }

    /// Purge all inverted index entries for a `(database, tenant)`. Structural
    /// drop via tuple ranges on every FTS table.
    pub fn purge_tenant(&self, database_id: u64, tid: TenantId) -> crate::Result<usize> {
        self.note_purge(database_id, tid.as_u64(), None);
        self.inner
            .purge_tenant(database_id, tid.as_u64())
            .map_err(into_result_err)
    }

    /// Purge all inverted index entries for a single
    /// `(database, tenant, collection)`. Structural drop via tuple ranges on
    /// every FTS table.
    pub fn purge_collection(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<usize> {
        self.note_purge(database_id, tid.as_u64(), Some(collection));
        self.inner
            .purge_collection(database_id, tid.as_u64(), collection)
            .map_err(into_result_err)
    }

    /// Empty every index of a `(database, tenant, collection)` in one write
    /// transaction, keeping the collection's analyzer, language, and fuzzy
    /// configuration. TRUNCATE empties a collection this way.
    pub fn clear_collection(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<usize> {
        self.note_purge(database_id, tid.as_u64(), Some(collection));
        self.inner
            .memtable()
            .drain_collection(database_id, tid.as_u64(), collection);
        self.inner
            .backend()
            .clear_collection_data(database_id, tid.as_u64(), collection)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::sparse::inverted::test_support::body;

    const DB: u64 = 0;

    fn open_temp() -> (InvertedIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test-inverted.redb");
        let db = Arc::new(GatedDatabase::new(redb::Database::create(&path).unwrap()));
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    /// Clearing a collection empties its indexes and keeps its fuzzy
    /// configuration.
    #[test]
    fn clear_collection_empties_indexes_and_keeps_config() {
        let (idx, _dir) = open_temp();
        let t = TenantId::new(1);
        idx.set_collection_fuzzy(DB, t, "docs", true).unwrap();
        idx.index_document(DB, t, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();
        idx.index_document(DB, t, "other", Surrogate::new(1), &body("alpha"))
            .unwrap();

        idx.clear_collection(DB, t, "docs").unwrap();

        let search = |collection: &str| {
            idx.search(
                DB,
                t,
                collection,
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap()
        };
        assert!(
            search("docs").is_empty(),
            "the cleared collection holds no text"
        );
        assert_eq!(
            search("other").len(),
            1,
            "another collection keeps its text"
        );
        assert_eq!(idx.corpus_stats(DB, t, "docs").unwrap().0, 0);
        assert!(
            idx.inner
                .get_collection_fuzzy(DB, t.as_u64(), "docs")
                .unwrap(),
            "the fuzzy configuration survives"
        );
    }

    #[test]
    fn purge_tenant_structurally_drops_data() {
        let (idx, _dir) = open_temp();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        idx.index_document(DB, t1, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();
        idx.index_document(DB, t2, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();

        idx.purge_tenant(DB, t1).unwrap();

        assert!(
            idx.search(
                DB,
                t1,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            !idx.search(
                DB,
                t2,
                "docs",
                FtsSearchParams {
                    query: "alpha",
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None
                }
            )
            .unwrap()
            .is_empty()
        );
    }
}
