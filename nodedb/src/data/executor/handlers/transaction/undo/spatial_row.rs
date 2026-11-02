// SPDX-License-Identifier: BUSL-1.1

//! Undo of one sync-ingested spatial write (`SpatialOp::Insert` / `Delete`).
//!
//! Such a write replaces three things at once: the geometry row in the
//! sparse store, the entry in the field's R-tree, and its reverse
//! `spatial_doc_map` record. Before the write the undo records the row's
//! version floor and the R-tree entry. The undo removes the row version the
//! write appended ([`super::row_version`]) and puts the R-tree entry back.

use nodedb_types::{BoundingBox, StorageKey, Surrogate};

use super::row_version::{RowVersionFloor, WrittenRows};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::types::{DatabaseId, TenantId};
use crate::util::fnv1a_hash;

use super::{UndoEntry, UndoError};

/// The pre-image of one spatial row.
pub(in crate::data::executor) struct SpatialRowUndo {
    pub key: SpatialIndexKey,
    /// The geometry row and its newest version before the write.
    pub row: RowVersionFloor,
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
        let row = self.row_version_floor(database_id.as_u64(), tid, collection, storage_key)?;
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
            row,
            entry_id,
            prior_entry,
        })))
    }

    /// Remove the row version the write appended, and put the R-tree entry
    /// and the reverse-map record back.
    pub(super) fn apply_undo_spatial_row(
        &mut self,
        entry_index: usize,
        undo: SpatialRowUndo,
    ) -> Result<(), UndoError> {
        let SpatialRowUndo {
            key,
            row,
            entry_id,
            prior_entry,
        } = undo;
        // The ordinal of the version the write appended: the forward write
        // ended the prior R-tree entry there.
        let written_at = self
            .sparse
            .versioned_latest_sys_from(key.0.as_u64(), key.1.as_u64(), &key.2, &row.key)
            .map_err(|e| {
                UndoError::failed(
                    entry_index,
                    format!("reading the written version of {} in '{}'", row.key, key.2),
                    e,
                )
            })?
            .filter(|sys| row.floor.is_none_or(|floor| *sys > floor))
            .unwrap_or(nodedb_spatial::rtree::UNVERSIONED);
        self.remove_written_row_versions(
            WrittenRows {
                entry_index,
                database_id: key.0.as_u64(),
                tid: key.1.as_u64(),
                collection: &key.2,
            },
            std::slice::from_ref(&row),
        )?;

        if let Some(rtree) = self.spatial_indexes.get_mut(&key) {
            rtree.forget(entry_id);
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
                rtree.restore_ended(entry_id, bbox, written_at);
                self.spatial_doc_map.insert(doc_map_key, document_id);
            }
            None => {
                self.spatial_doc_map.remove(&doc_map_key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const TID: u64 = 1;

    /// A rolled-back geometry write removes the row version it appended:
    /// the row reads as before at every system time.
    #[test]
    fn a_rolled_back_spatial_write_leaves_no_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let surrogate = Surrogate::new(3);
        let row = StorageKey::for_surrogate(surrogate);
        core.sparse
            .put(0, TID, "places", &row, b"before", 100)
            .expect("seed row");

        let undo = core
            .capture_spatial_row_undo(DatabaseId::DEFAULT, TID, "places", "geom", surrogate)
            .expect("capture undo");
        let UndoEntry::SpatialRow(undo) = undo else {
            panic!("a spatial write captures a SpatialRow undo");
        };
        core.sparse
            .put(0, TID, "places", &row, b"after", 200)
            .expect("write row");
        core.apply_undo_spatial_row(0, *undo)
            .expect("undo spatial write");

        assert_eq!(
            core.sparse.get(0, TID, "places", &row).unwrap(),
            Some(b"before".to_vec())
        );
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(0, TID, "places", &row)
                .unwrap(),
            Some(100),
            "the rolled-back version leaves no history"
        );
    }
}
