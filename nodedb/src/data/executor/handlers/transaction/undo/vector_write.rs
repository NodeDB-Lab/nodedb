// SPDX-License-Identifier: BUSL-1.1

//! Undo of one vector write a committed redo record installs: an HNSW or
//! IVF-PQ insert, a delete by node or by surrogate, a multi-vector write, or a
//! vector-primary row write.
//!
//! The pre-image is taken before the write: the collection's write mark
//! (`VectorCollection::write_mark`), the IVF-PQ add counter, and for a
//! vector-primary collection the sidecar row and payload bitmap entries of
//! every named row. The undo withdraws every node the write inserted, puts
//! every binding and tombstone back, and restores the sidecars and bitmap
//! entries. A collection the write created is removed again.

use std::collections::HashMap;

use nodedb_types::{StorageKey, Surrogate, Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::VectorWriteMark;

use super::UndoEntry;

/// The IVF-PQ state of a collection before a write.
pub(in crate::data::executor) struct IvfMark {
    /// Vectors the index held.
    pub count: u32,
    pub trained: bool,
}

/// The pre-image of one vector write.
pub(in crate::data::executor) struct VectorWriteUndo {
    pub index_key: VectorIndexKey,
    pub tid: u64,
    /// The collection name the sidecars are stored under.
    pub collection: String,
    /// `None` when the collection did not exist before the write.
    pub mark: Option<VectorWriteMark>,
    /// Whether the write found no `vector_params` entry for the key.
    pub params_absent: bool,
    /// `None` when the collection had no IVF-PQ index.
    pub ivf: Option<IvfMark>,
    /// Sidecar bytes of every named surrogate, `None` when absent. Empty for
    /// a write that stores no sidecar.
    pub sidecars: Vec<(Surrogate, Option<Vec<u8>>)>,
    /// Payload bitmap rows of the named nodes that were live.
    pub payload_rows: Vec<(u32, HashMap<String, Value>)>,
}

/// What one vector write names.
pub(in crate::data::executor) struct VectorWriteTarget<'a> {
    pub index_key: &'a VectorIndexKey,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogates: &'a [Surrogate],
    pub ids: &'a [u32],
    /// Whether the write stores sidecar rows (a vector-primary write).
    pub sidecars: bool,
}

impl CoreLoop {
    /// Capture the pre-image of the vector write `target` names.
    pub(in crate::data::executor) fn capture_vector_write_undo(
        &self,
        target: VectorWriteTarget<'_>,
    ) -> crate::Result<UndoEntry> {
        let VectorWriteTarget {
            index_key,
            tid,
            collection,
            surrogates,
            ids,
            sidecars,
        } = target;
        let database_id = index_key.0.as_u64();
        let coll = self.vector_collections.get(index_key);
        let mut sidecar_rows = Vec::new();
        if sidecars {
            for &surrogate in surrogates {
                let key = StorageKey::for_surrogate(surrogate);
                let bytes = self.sparse.get(database_id, tid, collection, &key)?;
                sidecar_rows.push((surrogate, bytes));
            }
        }
        let mut payload_rows = Vec::new();
        if let Some(coll) = coll.filter(|coll| !coll.payload.is_empty()) {
            let named = surrogates
                .iter()
                .filter_map(|s| coll.local_for_surrogate(*s).map(|id| (id, *s)))
                .chain(
                    ids.iter()
                        .filter_map(|id| coll.get_surrogate(*id).map(|s| (*id, s))),
                );
            for (id, surrogate) in named {
                if !coll.is_live(id) {
                    continue;
                }
                let key = StorageKey::for_surrogate(surrogate);
                if let Some(bytes) = self.sparse.get(database_id, tid, collection, &key)? {
                    let fields =
                        crate::data::executor::handlers::vector_upsert::decode_payload_lowercased(
                            &bytes,
                        )
                        .map_err(|e| crate::Error::Internal {
                            detail: format!("vector sidecar of {key} does not decode: {e}"),
                        })?;
                    payload_rows.push((id, fields));
                }
            }
        }
        Ok(UndoEntry::VectorWrite(Box::new(VectorWriteUndo {
            index_key: index_key.clone(),
            tid,
            collection: collection.to_string(),
            mark: coll.map(|coll| coll.write_mark(surrogates, ids)),
            params_absent: !self.vector_params.contains_key(index_key),
            ivf: self.ivf_indexes.get(index_key).map(|ivf| IvfMark {
                count: ivf.len() as u32,
                trained: ivf.is_trained(),
            }),
            sidecars: sidecar_rows,
            payload_rows,
        })))
    }

    /// Reverse one vector write.
    pub(super) fn apply_undo_vector_write(
        &mut self,
        entry_index: usize,
        undo: VectorWriteUndo,
    ) -> Result<(), (usize, String)> {
        let VectorWriteUndo {
            index_key,
            tid,
            collection,
            mark,
            params_absent,
            ivf,
            sidecars,
            payload_rows,
        } = undo;
        let database_id = index_key.0.as_u64();
        let fail = |detail: String| (entry_index, detail);

        // The bitmap entries of the rows the write left behind go first: their
        // fields sit in the sidecars the write stored.
        for (surrogate, _) in &sidecars {
            let Some(coll) = self.vector_collections.get(&index_key) else {
                break;
            };
            let Some(id) = coll.local_for_surrogate(*surrogate) else {
                continue;
            };
            let key = StorageKey::for_surrogate(*surrogate);
            let current = self
                .sparse
                .get(database_id, tid, &collection, &key)
                .map_err(|e| fail(format!("reading the sidecar of {key}: {e}")))?;
            if let Some(bytes) = current
                && let Ok(fields) =
                    crate::data::executor::handlers::vector_upsert::decode_payload_lowercased(
                        &bytes,
                    )
                && let Some(coll) = self.vector_collections.get_mut(&index_key)
            {
                coll.payload.delete_row(id, &fields);
            }
        }

        match mark {
            Some(mark) => {
                let Some(coll) = self.vector_collections.get_mut(&index_key) else {
                    return Err(fail(format!(
                        "vector index {:?} vanished before its write was rolled back",
                        index_key
                    )));
                };
                if !coll.roll_back_to(mark) {
                    return Err(fail(format!(
                        "vector index {:?} sealed the nodes a rolled-back write inserted",
                        index_key
                    )));
                }
                for (id, fields) in &payload_rows {
                    coll.payload.insert_row(*id, fields);
                }
            }
            None => {
                self.vector_collections.remove(&index_key);
            }
        }
        if params_absent {
            self.vector_params.remove(&index_key);
        }
        match ivf {
            Some(IvfMark { count, trained }) => {
                if let Some(index) = self.ivf_indexes.get_mut(&index_key) {
                    index.roll_back_to(count, trained);
                }
            }
            None => {
                self.ivf_indexes.remove(&index_key);
            }
        }

        for (surrogate, prior) in sidecars {
            let key = StorageKey::for_surrogate(surrogate);
            let restored = match &prior {
                Some(bytes) => self
                    .sparse
                    .put(database_id, tid, &collection, &key, bytes)
                    .map(drop),
                None => self
                    .sparse
                    .delete(database_id, tid, &collection, &key)
                    .map(drop),
            };
            restored.map_err(|e| fail(format!("restoring the sidecar of {key}: {e}")))?;
            self.doc_cache
                .invalidate(database_id, tid, &collection, &key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::ivf::{IvfPqIndex, IvfPqParams};
    use crate::types::{DatabaseId, TenantId};

    const TID: u64 = 1;

    fn params() -> IvfPqParams {
        IvfPqParams {
            n_cells: 2,
            pq_m: 2,
            pq_k: 4,
            nprobe: 2,
            metric: nodedb_vector::DistanceMetric::L2,
        }
    }

    fn vectors() -> Vec<Vec<f32>> {
        (0..8)
            .map(|i| vec![i as f32, (i * 2) as f32, 1.0, 0.5])
            .collect()
    }

    /// The IVF-PQ adds of a rolled-back write leave the index: it holds the
    /// vectors and the training it held before the write.
    #[test]
    fn a_rolled_back_write_withdraws_its_ivf_adds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());
        let vecs = vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut index = IvfPqIndex::new(4, params());
        index.train(
            &refs,
            nodedb_mem::ScopedMemory::new(
                crate::data::executor::core_loop::test_governor(),
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                nodedb_mem::EngineId::Vector,
            ),
        );
        index.add_batch(&refs[..4]);
        core.ivf_indexes.insert(key.clone(), index);

        let undo = core
            .capture_vector_write_undo(VectorWriteTarget {
                index_key: &key,
                tid: TID,
                collection: "docs",
                surrogates: &[],
                ids: &[],
                sidecars: false,
            })
            .expect("capture undo");
        if let Some(index) = core.ivf_indexes.get_mut(&key) {
            index.add_batch(&refs[4..]);
        }
        let UndoEntry::VectorWrite(undo) = undo else {
            panic!("a vector write captures a VectorWrite undo");
        };
        core.apply_undo_vector_write(0, *undo)
            .expect("undo vector write");

        let index = core.ivf_indexes.get(&key).expect("index stays");
        assert_eq!(index.len(), 4);
        assert!(index.is_trained());
        assert!(
            index.search(&vecs[6], 8).iter().all(|r| r.id < 4),
            "no vector the write added is found"
        );
    }

    /// An IVF-PQ index the rolled-back write created is removed.
    #[test]
    fn a_rolled_back_write_removes_the_ivf_index_it_created() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());

        let undo = core
            .capture_vector_write_undo(VectorWriteTarget {
                index_key: &key,
                tid: TID,
                collection: "docs",
                surrogates: &[],
                ids: &[],
                sidecars: false,
            })
            .expect("capture undo");
        core.ivf_indexes
            .insert(key.clone(), IvfPqIndex::new(4, params()));
        let UndoEntry::VectorWrite(undo) = undo else {
            panic!("a vector write captures a VectorWrite undo");
        };
        core.apply_undo_vector_write(0, *undo)
            .expect("undo vector write");

        assert!(!core.ivf_indexes.contains_key(&key));
    }
}
