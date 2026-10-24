// SPDX-License-Identifier: BUSL-1.1

//! Undo of one sync-ingested spatial write (`SpatialOp::Insert` / `Delete`).
//!
//! Such a write replaces three things at once: the geometry row in the
//! sparse store, the entry in the field's R-tree, and its reverse
//! `spatial_doc_map` record. The pre-image of all three is captured before
//! the write, and the undo puts every one back.

use nodedb_types::{BoundingBox, StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::types::{DatabaseId, TenantId};
use crate::util::fnv1a_hash;

use super::{UndoEntry, UndoError};

/// The pre-image of one spatial row.
pub(in crate::data::executor) struct SpatialRowUndo {
    pub key: SpatialIndexKey,
    pub storage_key: StorageKey,
    /// The sparse-store row before the write, `None` when absent.
    pub prior_row: Option<Vec<u8>>,
    /// The R-tree entry id the write keys on.
    pub entry_id: u64,
    /// The R-tree entry, its reverse-map value and the tree's SRID before the
    /// write, `None` when the field held no entry for the row.
    pub prior_entry: Option<(BoundingBox, String, crate::engine::spatial::TreeSrid)>,
}

impl CoreLoop {
    /// Capture the pre-image of the spatial row `surrogate` in `field` of
    /// `collection`.
    pub(in crate::data::executor) fn capture_spatial_row_undo(
        &self,
        database_id: DatabaseId,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Surrogate,
    ) -> crate::Result<UndoEntry> {
        let storage_key = StorageKey::for_surrogate(surrogate);
        let prior_row = self
            .sparse
            .get(database_id.as_u64(), tid, collection, &storage_key)?;
        let key: SpatialIndexKey = (
            database_id,
            TenantId::new(tid),
            collection.to_string(),
            field.to_string(),
        );
        let entry_id = fnv1a_hash(storage_key.to_string().as_bytes());
        let doc_map_key = (key.0, key.1, key.2.clone(), key.3.clone(), entry_id);
        let prior_entry = self
            .spatial_doc_map
            .get(&doc_map_key)
            .and_then(|document_id| {
                let rtree = self.spatial_indexes.get(&key)?;
                let bbox = rtree
                    .entries()
                    .into_iter()
                    .find(|entry| entry.id == entry_id)
                    .map(|entry| entry.bbox)?;
                Some((bbox, document_id.clone(), rtree.tree_srid()))
            });
        Ok(UndoEntry::SpatialRow(Box::new(SpatialRowUndo {
            key,
            storage_key,
            prior_row,
            entry_id,
            prior_entry,
        })))
    }

    /// Put the row, the R-tree entry and the reverse-map record back.
    pub(super) fn apply_undo_spatial_row(
        &mut self,
        entry_index: usize,
        undo: SpatialRowUndo,
    ) -> Result<(), UndoError> {
        let SpatialRowUndo {
            key,
            storage_key,
            prior_row,
            entry_id,
            prior_entry,
        } = undo;
        let database_id = key.0.as_u64();
        let tid = key.1.as_u64();
        let restored = match &prior_row {
            Some(row) => self
                .sparse
                .put(database_id, tid, &key.2, &storage_key, row)
                .map(drop),
            None => self
                .sparse
                .delete(database_id, tid, &key.2, &storage_key)
                .map(drop),
        };
        restored.map_err(|e| {
            UndoError::failed(
                entry_index,
                format!("restoring spatial row in '{}'", key.2),
                e,
            )
        })?;

        if let Some(rtree) = self.spatial_indexes.get_mut(&key) {
            rtree.delete(entry_id);
        }
        let doc_map_key = (key.0, key.1, key.2.clone(), key.3.clone(), entry_id);
        match prior_entry {
            Some((bbox, document_id, srid)) => {
                let memory = nodedb_mem::ScopedMemory::new(
                    self.governor.clone(),
                    key.0,
                    key.1,
                    nodedb_mem::EngineId::Spatial,
                );
                let (collection, field) = (key.2.clone(), key.3.clone());
                let rtree = self
                    .spatial_indexes
                    .entry(key)
                    .or_insert_with(|| crate::engine::spatial::RTree::new(memory, srid));
                // Undo runs in reverse, so the tree holds no later entry in
                // another SRID and binds back to the row's.
                rtree.bind_srid(srid).map_err(|m| {
                    UndoError::failed(
                        entry_index,
                        format!("restoring spatial entry in '{collection}.{field}'"),
                        crate::Error::MixedSrid {
                            collection: collection.clone(),
                            field: field.clone(),
                            index_srid: m.index.code(),
                            row_srid: m.entry.code(),
                        },
                    )
                })?;
                rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                self.spatial_doc_map.insert(doc_map_key, document_id);
            }
            None => {
                self.spatial_doc_map.remove(&doc_map_key);
            }
        }
        Ok(())
    }
}
