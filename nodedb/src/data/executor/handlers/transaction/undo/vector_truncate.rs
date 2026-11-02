// SPDX-License-Identifier: BUSL-1.1

//! Undo of a vector-primary `TRUNCATE` a committed redo record installs.
//!
//! A truncate removes the mmap files of the collection's sealed segments, so
//! it cannot be reversed after the fact. Before the truncate runs, the
//! install detaches the collection whole and puts an empty collection with
//! the same configuration in its place; the truncate then empties that one.
//! The install also records the version floor of every sidecar row. The
//! undo puts the detached collection back and removes the tombstone the
//! truncate appended to each sidecar row ([`super::row_version`]). Once the
//! record installed, the detached collection is truncated for good, which
//! removes its segment files.

use nodedb_types::StorageKey;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::VectorCollection;

use super::row_version::{RowVersionFloor, WrittenRows};
use super::{UndoEntry, UndoError};

/// The collection and sidecar rows a truncate would remove.
pub(in crate::data::executor) struct VectorTruncateUndo {
    pub index_key: VectorIndexKey,
    pub tid: u64,
    pub collection: String,
    /// `None` when the index did not exist.
    pub prior: Option<VectorCollection>,
    /// Every live sidecar row and its newest version before the truncate.
    pub sidecars: Vec<RowVersionFloor>,
}

impl CoreLoop {
    /// Detach the collection at `index_key` and record the version floor of
    /// every sidecar row of `collection`, leaving an empty collection with
    /// the same configuration for the truncate to empty.
    pub(in crate::data::executor) fn detach_vector_collection_for_truncate(
        &mut self,
        index_key: &VectorIndexKey,
        tid: u64,
        collection: &str,
    ) -> crate::Result<UndoEntry> {
        let database_id = index_key.0.as_u64();
        let surrogates = self
            .scan_vector_sidecar_matches(database_id, tid, collection, &[])
            .map_err(crate::Error::DataPlane)?;
        let mut sidecars = Vec::with_capacity(surrogates.len());
        for surrogate in surrogates {
            let key = StorageKey::for_surrogate(surrogate);
            sidecars.push(self.row_version_floor(database_id, tid, collection, key)?);
        }
        let prior = match self.vector_collections.get(index_key) {
            Some(coll) => {
                let empty = coll.detached_empty();
                self.vector_collections.insert(index_key.clone(), empty)
            }
            None => None,
        };
        Ok(UndoEntry::VectorTruncate(Box::new(VectorTruncateUndo {
            index_key: index_key.clone(),
            tid,
            collection: collection.to_string(),
            prior,
            sidecars,
        })))
    }

    /// Put the detached collection back and remove the sidecar tombstones the
    /// truncate appended.
    pub(super) fn apply_undo_vector_truncate(
        &mut self,
        entry_index: usize,
        undo: VectorTruncateUndo,
    ) -> Result<(), UndoError> {
        let VectorTruncateUndo {
            index_key,
            tid,
            collection,
            prior,
            sidecars,
        } = undo;
        let database_id = index_key.0.as_u64();
        match prior {
            Some(coll) => {
                self.vector_collections.insert(index_key, coll);
            }
            None => {
                self.vector_collections.remove(&index_key);
            }
        }
        self.remove_written_row_versions(
            WrittenRows {
                entry_index,
                database_id,
                tid,
                collection: &collection,
            },
            &sidecars,
        )
    }

    /// Truncate for good every collection a committed install detached,
    /// removing the mmap files of its sealed segments.
    pub(in crate::data::executor) fn finalize_vector_truncates(
        &mut self,
        undo_log: &mut [UndoEntry],
    ) {
        for entry in undo_log {
            if let UndoEntry::VectorTruncate(undo) = entry
                && let Some(prior) = undo.prior.as_mut()
            {
                prior.truncate();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_types::{Surrogate, Value};

    const TID: u64 = 1;

    /// A rolled-back truncate removes the tombstone it appended to each
    /// sidecar row: the row reads as before at every system time.
    #[test]
    fn a_rolled_back_truncate_removes_its_sidecar_tombstones() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());
        let row = StorageKey::for_surrogate(Surrogate::new(5));
        let body = nodedb_types::value_to_msgpack(&Value::Object(HashMap::from([(
            "tag".to_string(),
            Value::String("a".into()),
        )])))
        .expect("encode sidecar");
        core.sparse
            .put(0, TID, "docs", &row, &body, 100)
            .expect("seed sidecar");

        let undo = core
            .detach_vector_collection_for_truncate(&key, TID, "docs")
            .expect("detach");
        let UndoEntry::VectorTruncate(undo) = undo else {
            panic!("a truncate captures a VectorTruncate undo");
        };
        assert_eq!(undo.sidecars.len(), 1);
        core.sparse
            .delete(0, TID, "docs", &row, 200)
            .expect("truncate the sidecar");
        core.apply_undo_vector_truncate(0, *undo)
            .expect("undo truncate");

        assert_eq!(core.sparse.get(0, TID, "docs", &row).unwrap(), Some(body));
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(0, TID, "docs", &row)
                .unwrap(),
            Some(100),
            "the rolled-back tombstone leaves no history"
        );
    }
}
