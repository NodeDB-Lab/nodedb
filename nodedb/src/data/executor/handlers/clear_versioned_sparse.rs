// SPDX-License-Identifier: BUSL-1.1

//! The sparse step of a collection clear: every versioned document and
//! index entry, and when the caller says so its maintained index set.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::reclaim_retry::retry_reclaim;

/// What a clear does with the collection's maintained index set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum MaintainedSetFate {
    /// The set stays. The collection maintains the same paths over no row.
    Keep,
    /// The set goes in the clear's own transaction, and then from the
    /// mirror. A dropped collection takes this, and so does a snapshot
    /// install, whose snapshot carries the set of the entries it installs.
    Forget,
}

/// How a collection clear treats the collection's definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum CollectionClear {
    /// The collection leaves the core: its definition and its maintained
    /// set go.
    Drop,
    /// The definition stays. The field names the maintained set's fate.
    KeepDefinition(MaintainedSetFate),
}

impl CollectionClear {
    /// Whether the collection's schema and derived metadata stay.
    pub(in crate::data::executor) fn keeps_definition(self) -> bool {
        match self {
            Self::Drop => false,
            Self::KeepDefinition(_) => true,
        }
    }

    /// What the clear does with the maintained set.
    pub(in crate::data::executor) fn maintained_set(self) -> MaintainedSetFate {
        match self {
            Self::Drop => MaintainedSetFate::Forget,
            Self::KeepDefinition(fate) => fate,
        }
    }
}

impl CoreLoop {
    /// Remove every version of every document and index entry of one
    /// collection. Returns `(doc_versions_removed, index_versions_removed)`.
    ///
    /// [`MaintainedSetFate::Forget`] removes the maintained set in the same
    /// transaction as the rows, so no commit keeps a set whose entries
    /// left. The mirror drops it after that commit.
    pub(in crate::data::executor) fn clear_versioned_sparse(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        maintained_set: MaintainedSetFate,
    ) -> crate::Result<(usize, usize)> {
        let removed = retry_reclaim(
            "sparse.delete_all_versioned_for_collection",
            tenant_id,
            collection,
            || match maintained_set {
                MaintainedSetFate::Keep => self.sparse.delete_all_versioned_for_collection(
                    database_id,
                    tenant_id,
                    collection,
                ),
                MaintainedSetFate::Forget => self.sparse.drop_all_versioned_for_collection(
                    database_id,
                    tenant_id,
                    collection,
                ),
            },
        )?;
        match maintained_set {
            MaintainedSetFate::Keep => {}
            MaintainedSetFate::Forget => {
                self.maintained_indexes
                    .remove(database_id, tenant_id, collection);
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::MaintainedSetFate;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::sparse::btree::MaintainedIndexSet;
    use crate::engine::sparse::btree_versioned::VersionedIndexEntry;
    use nodedb_types::{StorageKey, Surrogate};

    const COLLECTION: &str = "cleared";

    #[test]
    fn a_forget_clear_leaves_no_set_row_and_no_versioned_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
        let mut set = MaintainedIndexSet::empty(0, 1, COLLECTION);
        set.top_level = true;
        let doc_id = StorageKey::for_surrogate(Surrogate::new(1));
        let txn = core.sparse.begin_write().expect("txn");
        core.sparse
            .put_maintained_indexes_in_txn(&txn, &set)
            .expect("put set");
        core.sparse
            .versioned_index_put_in_txn(
                &txn,
                VersionedIndexEntry {
                    database_id: 0,
                    tenant: 1,
                    coll: COLLECTION,
                    field: "$.city",
                    value: "oslo",
                    doc_id: &doc_id,
                    sys_from: 1,
                },
            )
            .expect("put entry");
        txn.commit().expect("commit");
        core.maintained_indexes.install(&set).expect("mirror");

        let (_, index_removed) = core
            .clear_versioned_sparse(0, 1, COLLECTION, MaintainedSetFate::Forget)
            .expect("clear");
        assert_eq!(index_removed, 1);
        assert!(
            core.sparse
                .load_maintained_indexes()
                .expect("sets")
                .is_empty()
        );
        assert!(
            core.sparse
                .scan_versioned_indexes_for_tenant(0, 1)
                .expect("entries")
                .is_empty()
        );
        assert!(!core.maintained_indexes.maintains_any(0, 1, COLLECTION));
    }
}
