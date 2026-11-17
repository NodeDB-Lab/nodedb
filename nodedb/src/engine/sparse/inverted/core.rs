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

    /// Every collection the tenant holds an inverted index of, stored names.
    pub fn tenant_collections(
        &self,
        database_id: u64,
        tid: TenantId,
    ) -> crate::Result<Vec<String>> {
        self.backend().tenant_collections(database_id, tid.as_u64())
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
        let db = Arc::new(GatedDatabase::create(&path).unwrap());
        let idx =
            InvertedIndex::open(db, crate::data::executor::core_loop::test_governor()).unwrap();
        (idx, dir)
    }

    #[test]
    fn purge_collection_keeps_the_other_tenant() {
        let (idx, _dir) = open_temp();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        idx.index_document(DB, t1, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();
        idx.index_document(DB, t2, "docs", Surrogate::new(1), &body("alpha bravo"))
            .unwrap();

        assert_eq!(
            idx.tenant_collections(DB, t1).unwrap(),
            vec!["docs".to_string()]
        );
        idx.purge_collection(DB, t1, "docs").unwrap();
        assert!(idx.tenant_collections(DB, t1).unwrap().is_empty());

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
