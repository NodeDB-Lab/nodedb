// SPDX-License-Identifier: BUSL-1.1

//! Spatial-engine undo entry application logic.
//!
//! Spatial index mutations are IN-MEMORY (the per-field R-tree in
//! `spatial_indexes` plus the reverse `spatial_doc_map`), so an aborted redb
//! write transaction does NOT reverse them — they require explicit undo. This
//! mirrors the vector-index undo path (`apply_undo_vector`).
//!
//! Returns an [`UndoError`] on fatal failure, and the caller escalates it to
//! a typed `RollbackFailed` response.

use crate::data::executor::core_loop::CoreLoop;

use super::{UndoEntry, UndoError};

impl CoreLoop {
    pub(super) fn apply_undo_spatial(
        &mut self,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), UndoError> {
        match entry {
            UndoEntry::SpatialInsert { key, entry_id } => {
                // Reverse a forward spatial insert: drop the R-tree entry and
                // its reverse map record. A missing index means the entry was
                // never created (nothing to undo) — safe no-op.
                if let Some(rtree) = self.spatial_indexes.get_mut(&key) {
                    rtree.delete(entry_id);
                }
                self.spatial_doc_map
                    .remove(&(key.0, key.1, key.2, key.3, entry_id));
                Ok(())
            }
            UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
                srid,
            } => {
                // Reverse a forward spatial removal: re-insert the entry with
                // its captured bbox and re-populate the reverse map, matching
                // the forward `apply_point_put_spatial` insert shape. Undo
                // runs in reverse, so every later insert into this tree is
                // already gone and the tree binds back to `srid`.
                let memory = nodedb_mem::ScopedMemory::new(
                    self.governor.clone(),
                    key.0,
                    key.1,
                    nodedb_mem::EngineId::Spatial,
                );
                let rtree = self
                    .spatial_indexes
                    .entry(key.clone())
                    .or_insert_with(|| crate::engine::spatial::RTree::new(memory, srid));
                rtree.bind_srid(srid).map_err(|m| {
                    UndoError::failed(
                        entry_index,
                        format!("re-inserting a spatial entry into '{}.{}'", key.2, key.3),
                        crate::Error::MixedSrid {
                            collection: key.2.clone(),
                            field: key.3.clone(),
                            index_srid: m.index.code(),
                            row_srid: m.entry.code(),
                        },
                    )
                })?;
                rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                self.spatial_doc_map
                    .insert((key.0, key.1, key.2, key.3, entry_id), document_id);
                Ok(())
            }
            _ => unreachable!("apply_undo_spatial called with non-spatial entry"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::TenantId;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn spatial_key() -> (nodedb_types::DatabaseId, TenantId, String, String) {
        (
            nodedb_types::DatabaseId::new(DB),
            TenantId::new(TID),
            "c".to_string(),
            "geom".to_string(),
        )
    }

    fn rtree_has(core: &crate::data::executor::core_loop::CoreLoop, entry_id: u64) -> bool {
        core.spatial_indexes
            .get(&spatial_key())
            .map(|rt| rt.entries().into_iter().any(|e| e.id == entry_id))
            .unwrap_or(false)
    }

    #[test]
    fn spatial_insert_undo_removes_entry_and_reverse_map() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let key = spatial_key();
        let entry_id: u64 = 42;
        let bbox = nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0);

        // Seed as though a forward spatial insert had run.
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            key.0,
            key.1,
            nodedb_mem::EngineId::Spatial,
        );
        let rtree = core.spatial_indexes.entry(key.clone()).or_insert_with(|| {
            crate::engine::spatial::RTree::new(memory, crate::engine::spatial::TreeSrid::WGS84)
        });
        rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
        core.spatial_doc_map.insert(
            (key.0, key.1, key.2.clone(), key.3.clone(), entry_id),
            "d1".to_string(),
        );
        assert!(rtree_has(&core, entry_id));

        let undo = UndoEntry::SpatialInsert {
            key: key.clone(),
            entry_id,
        };
        core.apply_undo_spatial(0, undo).unwrap();

        assert!(!rtree_has(&core, entry_id), "R-tree entry must be removed");
        assert!(
            !core
                .spatial_doc_map
                .contains_key(&(key.0, key.1, key.2, key.3, entry_id)),
            "reverse map record must be removed"
        );
    }

    #[test]
    fn spatial_delete_undo_reinserts_entry_with_bbox() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let key = spatial_key();
        let entry_id: u64 = 7;
        let bbox = nodedb_types::BoundingBox::new(10.0, 20.0, 30.0, 40.0);

        // R-tree starts empty (the forward op removed the entry).
        assert!(!rtree_has(&core, entry_id));

        let undo = UndoEntry::SpatialDelete {
            key: key.clone(),
            entry_id,
            bbox,
            document_id: "d1".to_string(),
            srid: crate::engine::spatial::TreeSrid {
                srid: nodedb_types::geometry::Srid::new(32648).expect("srid"),
                space: nodedb_types::BboxSpace::Planar,
            },
        };
        core.apply_undo_spatial(0, undo).unwrap();

        let restored = core
            .spatial_indexes
            .get(&key)
            .and_then(|rt| rt.entries().into_iter().find(|e| e.id == entry_id).cloned());
        let restored = restored.expect("R-tree entry must be re-inserted");
        assert_eq!(
            restored.bbox, bbox,
            "restored bbox must match captured bbox"
        );
        assert_eq!(
            core.spatial_indexes.get(&key).map(|rt| rt.srid()),
            Some(nodedb_types::geometry::Srid::new(32648).expect("srid")),
            "the re-created tree takes the captured SRID"
        );
        assert_eq!(
            core.spatial_doc_map
                .get(&(key.0, key.1, key.2, key.3, entry_id))
                .map(String::as_str),
            Some("d1"),
            "reverse map record must be restored"
        );
    }
}
