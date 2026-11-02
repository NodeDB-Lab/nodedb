// SPDX-License-Identifier: BUSL-1.1

//! Install of the index state a snapshot carries beside its rows: each
//! vector index's history, each sparse vector index, and each spatial
//! R-tree. Every index keeps its system-time history, so a follower that
//! installed the snapshot answers a read at any retained cut as the source
//! does. An image replaces the local index of its collection and field.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::spatial::RTree;
use crate::engine::vector::sparse::SparseInvertedIndex;
use crate::types::{DatabaseId, FieldIndexImage, TenantDataSnapshot, TenantId, VectorIndexHistory};

/// The error for a snapshot index section that does not decode.
fn undecodable(section: &str, key: &str, detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore: {section} '{key}' does not decode: {detail}"),
    }
}

/// The history of the vector index the snapshot keys `key`. An index the
/// snapshot carries no history for has none: every live row is visible at
/// every cut.
pub(super) fn vector_history_of(
    snap: &TenantDataSnapshot,
    key: &str,
) -> crate::Result<VectorIndexHistory> {
    match snap.vector_versions.iter().find(|(k, _)| k == key) {
        Some((_, bytes)) => {
            zerompk::from_msgpack(bytes).map_err(|e| undecodable("vector history", key, e))
        }
        None => Ok(VectorIndexHistory::default()),
    }
}

impl CoreLoop {
    /// Install every sparse vector index and spatial R-tree `snap` carries.
    /// The first image that does not decode fails the restore.
    pub(super) fn install_index_images(&mut self, snap: &TenantDataSnapshot) -> crate::Result<()> {
        for (key, bytes) in &snap.sparse_vectors {
            let image: FieldIndexImage = zerompk::from_msgpack(bytes)
                .map_err(|e| undecodable("sparse vector index", key, e))?;
            let index = SparseInvertedIndex::from_checkpoint(&image.index).ok_or_else(|| {
                undecodable(
                    "sparse vector index",
                    key,
                    format_args!("field '{}' holds no index", image.field),
                )
            })?;
            // A direct sparse write has no document version, so its ordinals
            // fold into the shared clock here.
            if let Some(newest) = index.history().newest_ordinal() {
                self.observe_bitemporal_ordinal(newest);
            }
            self.sparse_vector_indexes.insert(
                (
                    DatabaseId::new(image.database_id),
                    TenantId::new(image.tenant_id),
                    image.collection,
                    image.field,
                ),
                index,
            );
            self.checkpoint_coordinator.mark_dirty("vector", 1);
        }
        for (key, bytes) in &snap.spatial_indexes {
            let image: FieldIndexImage =
                zerompk::from_msgpack(bytes).map_err(|e| undecodable("spatial index", key, e))?;
            let db = DatabaseId::new(image.database_id);
            let tid = TenantId::new(image.tenant_id);
            let memory = nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                db,
                tid,
                nodedb_mem::EngineId::Spatial,
            );
            let rtree = RTree::from_checkpoint(&image.index, None, memory)?;
            let FieldIndexImage {
                collection,
                field,
                doc_entries,
                ..
            } = image;
            self.spatial_doc_map.retain(|(d, t, c, f, _), _| {
                !(*d == db && *t == tid && *c == collection && *f == field)
            });
            for (entry_id, doc_id) in doc_entries {
                self.spatial_doc_map.insert(
                    (db, tid, collection.clone(), field.clone(), entry_id),
                    doc_id,
                );
            }
            self.spatial_indexes
                .insert((db, tid, collection, field), rtree);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{SparseVector, Surrogate};
    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::engine::spatial::{RTreeEntry, TreeSrid};
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::MetaOp;

    const TID: u64 = 1;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx);
        drop(resp_rx);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    fn sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).expect("sparse vector")
    }

    fn rtree(core: &CoreLoop) -> RTree {
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            nodedb_mem::EngineId::Spatial,
        );
        let mut rtree = RTree::new(
            memory,
            TreeSrid {
                srid: nodedb_types::geometry::Srid::new(32647).expect("srid"),
                space: nodedb_types::BboxSpace::Planar,
            },
        );
        rtree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: nodedb_types::BoundingBox::new(5.0, 5.0, 6.0, 6.0),
            },
            10,
        );
        rtree.end_at(1, "0000000a", 20);
        rtree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
            },
            20,
        );
        rtree
    }

    /// A source core holds a vector index, a sparse vector index and an
    /// R-tree, each with a replaced version. A follower installs the source's
    /// snapshot through the Raft install path and answers a read at a cut
    /// below every replacement as the source does.
    #[test]
    fn a_snapshot_install_answers_a_past_cut_as_the_source() {
        let source_dir = TempDir::new().expect("tempdir");
        let mut source = open_core(source_dir.path());

        let vector_key = CoreLoop::vector_index_key(0, TID, "docs", "emb");
        let row = Surrogate::new(10);
        let mut coll = VectorCollection::new(2, HnswParams::default());
        coll.insert_at(vec![1.0, 0.0], row, 10).expect("insert");
        coll.insert_at(vec![0.0, 1.0], row, 20).expect("overwrite");
        source.vector_collections.insert(vector_key.clone(), coll);

        // A multi-vector document rewritten at 20: two vectors before, one
        // after.
        let multi_key = CoreLoop::vector_index_key(0, TID, "docs", "tokens");
        let document = Surrogate::new(11);
        let mut multi = VectorCollection::new(2, HnswParams::default());
        multi
            .insert_multi_vector_at(&[&[1.0, 0.0], &[0.9, 0.1]], document, 10)
            .expect("multi insert");
        multi
            .insert_multi_vector_at(&[&[0.0, 1.0]], document, 20)
            .expect("multi rewrite");
        source.vector_collections.insert(multi_key.clone(), multi);

        let sparse_key = CoreLoop::sparse_index_key(0, TID, "docs", "terms");
        let mut sparse = SparseInvertedIndex::new();
        sparse.insert_at("0000000a", &sv(&[(3, 1.0)]), 10);
        sparse.insert_at("0000000a", &sv(&[(3, 2.0)]), 20);
        sparse.insert_at("0000000b", &sv(&[(3, 5.0)]), 12);
        sparse.delete_at("0000000b", 25);
        source
            .sparse_vector_indexes
            .insert(sparse_key.clone(), sparse);

        let spatial_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "docs".to_string(),
            "geom".to_string(),
        );
        let tree = rtree(&source);
        let history = tree.history_rows();
        source.spatial_indexes.insert(spatial_key.clone(), tree);
        source.spatial_doc_map.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                "docs".to_string(),
                "geom".to_string(),
                1,
            ),
            "0000000a".to_string(),
        );

        let mut snap = TenantDataSnapshot::default();
        source
            .capture_memory_engines(DatabaseId::DEFAULT, TenantId::new(TID), &mut snap)
            .expect("capture");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode");

        let follower_dir = TempDir::new().expect("tempdir");
        let mut follower = open_core(follower_dir.path());
        let task = CoreLoop::replay_vector_task(
            TenantId::new(0),
            DatabaseId::DEFAULT,
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs").vshard(),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            None,
        );
        let response = follower.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &[]);
        assert_eq!(response.status, Status::Ok, "install: {response:?}");

        let coll = follower
            .vector_collections
            .get(&vector_key)
            .expect("vector index installed");
        assert_eq!(coll.vector_for_surrogate_at(row, 5), None);
        assert_eq!(coll.vector_for_surrogate_at(row, 15), Some(vec![1.0, 0.0]));
        assert_eq!(coll.vector_for_surrogate_at(row, 25), Some(vec![0.0, 1.0]));
        assert_eq!(coll.live_count(), 1);

        let multi = follower
            .vector_collections
            .get(&multi_key)
            .expect("multi-vector index installed");
        assert_eq!(multi.multi_vector_documents(), vec![document]);
        assert_eq!(multi.live_count(), 1);
        let at = |cut: i64| {
            multi
                .search_at(
                    &[1.0, 0.0],
                    10,
                    32,
                    crate::engine::vector::distance::DistanceMetric::L2,
                    None,
                    cut,
                )
                .expect("search")
                .into_iter()
                .filter(|hit| multi.get_surrogate(hit.id) == Some(document))
                .count()
        };
        assert_eq!(at(5), 0);
        assert_eq!(at(15), 2, "the first version's two vectors");
        assert_eq!(at(25), 1, "the rewrite's one vector");

        let sparse = follower
            .sparse_vector_indexes
            .get(&sparse_key)
            .expect("sparse index installed");
        let query = sv(&[(3, 1.0)]);
        let scores = |at: i64| -> Vec<f32> {
            crate::engine::vector::sparse::search::dot_product_topk_at(sparse, &query, 10, at)
                .into_iter()
                .map(|r| r.score)
                .collect()
        };
        assert_eq!(scores(15), vec![5.0, 1.0]);
        assert_eq!(scores(22), vec![5.0, 2.0]);
        assert_eq!(scores(26), vec![2.0], "deleted at 25");

        let tree = follower
            .spatial_indexes
            .get(&spatial_key)
            .expect("R-tree installed");
        assert_eq!(tree.history_rows(), history);
        assert_eq!(
            follower.spatial_doc_map.get(&(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                "docs".to_string(),
                "geom".to_string(),
                1,
            )),
            Some(&"0000000a".to_string())
        );

        // The install carries no `vector_doc_map` entry. A delete of the row
        // on the follower still ends its node, through the surrogate binding.
        follower
            .vector_params
            .insert(vector_key.clone(), HnswParams::default());
        let deltas = follower.remove_document_vector_indexes(
            0,
            TID,
            "docs",
            nodedb_types::StorageKey::for_surrogate(row),
            30,
        );
        assert_eq!(deltas.len(), 1, "the delete finds the installed node");
        let coll = follower
            .vector_collections
            .get(&vector_key)
            .expect("vector index installed");
        assert_eq!(coll.live_count(), 0, "the delete ends the node");
        assert_eq!(coll.vector_for_surrogate_at(row, 25), Some(vec![0.0, 1.0]));
    }
}
