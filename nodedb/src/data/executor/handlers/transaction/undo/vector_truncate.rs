// SPDX-License-Identifier: BUSL-1.1

//! Undo of a vector-primary `TRUNCATE` a committed redo record installs.
//!
//! A truncate removes the mmap files of the collection's sealed segments, so
//! it cannot be reversed after the fact. Before the truncate runs, the
//! install detaches the collection whole and puts an empty collection with
//! the same configuration in its place; the truncate then empties that one.
//! The undo puts the detached collection and every sidecar row back. Once
//! the record installed, the detached collection is truncated for good,
//! which removes its segment files.

use nodedb_types::{StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::VectorCollection;

use super::UndoEntry;

/// The collection and sidecar rows a truncate would remove.
pub(in crate::data::executor) struct VectorTruncateUndo {
    pub index_key: VectorIndexKey,
    pub tid: u64,
    pub collection: String,
    /// `None` when the index did not exist.
    pub prior: Option<VectorCollection>,
    pub sidecars: Vec<(Surrogate, Vec<u8>)>,
}

impl CoreLoop {
    /// Detach the collection at `index_key` and capture every sidecar row of
    /// `collection`, leaving an empty collection with the same configuration
    /// for the truncate to empty.
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
            if let Some(bytes) = self.sparse.get(database_id, tid, collection, &key)? {
                sidecars.push((surrogate, bytes));
            }
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

    /// Put the detached collection and every sidecar row back.
    pub(super) fn apply_undo_vector_truncate(
        &mut self,
        entry_index: usize,
        undo: VectorTruncateUndo,
    ) -> Result<(), (usize, String)> {
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
        for (surrogate, bytes) in sidecars {
            let key = StorageKey::for_surrogate(surrogate);
            self.sparse
                .put(database_id, tid, &collection, &key, &bytes)
                .map_err(|e| {
                    (
                        entry_index,
                        format!("restoring the sidecar of {key} in '{collection}': {e}"),
                    )
                })?;
            self.doc_cache
                .invalidate(database_id, tid, &collection, &key);
        }
        Ok(())
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
