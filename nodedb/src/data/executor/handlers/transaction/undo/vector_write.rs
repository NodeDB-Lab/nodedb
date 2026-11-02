// SPDX-License-Identifier: BUSL-1.1

//! Undo of one vector write a committed redo record installs: an HNSW or
//! IVF-PQ insert, a delete by node or by surrogate, a multi-vector write, or a
//! vector-primary row write.
//!
//! The pre-image is taken before the write: the collection's write mark
//! (`VectorCollection::write_mark`, which covers a trained IVF-PQ index too),
//! and for a vector-primary collection the version floor of every named
//! sidecar row and the payload bitmap entries of every named row. The undo
//! withdraws every node the write inserted, puts every binding and tombstone
//! back, removes the sidecar version the write appended
//! ([`super::row_version`]) and restores the bitmap entries. A collection
//! the write created is removed again.

use std::collections::HashMap;

use nodedb_types::{StorageKey, Surrogate, Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::engine::vector::collection::VectorWriteMark;

use super::row_version::{RowVersionFloor, WrittenRows};
use super::{UndoEntry, UndoError};

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
    /// The sidecar row of every named surrogate and its newest version
    /// before the write. Empty for a write that stores no sidecar.
    pub sidecars: Vec<RowVersionFloor>,
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
                sidecar_rows.push(self.row_version_floor(database_id, tid, collection, key)?);
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
            sidecars: sidecar_rows,
            payload_rows,
        })))
    }

    /// Reverse one vector write.
    pub(super) fn apply_undo_vector_write(
        &mut self,
        entry_index: usize,
        undo: VectorWriteUndo,
    ) -> Result<(), UndoError> {
        let VectorWriteUndo {
            index_key,
            tid,
            collection,
            mark,
            params_absent,
            sidecars,
            payload_rows,
        } = undo;
        let database_id = index_key.0.as_u64();

        // The bitmap entries of the rows the write left behind go first: their
        // fields sit in the sidecars the write stored.
        for RowVersionFloor { key, .. } in &sidecars {
            let Some(coll) = self.vector_collections.get(&index_key) else {
                break;
            };
            let Some(id) = coll.local_for_surrogate(key.surrogate()) else {
                continue;
            };
            let current = self
                .sparse
                .get(database_id, tid, &collection, key)
                .map_err(|e| {
                    UndoError::failed(entry_index, format!("reading the sidecar of {key}"), e)
                })?;
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
                    return Err(UndoError::mismatch(
                        entry_index,
                        format!(
                            "vector index {index_key:?} vanished before its write was rolled back"
                        ),
                    ));
                };
                if !coll.roll_back_to(mark) {
                    return Err(UndoError::mismatch(
                        entry_index,
                        format!(
                            "vector index {index_key:?} sealed or trained away the nodes a \
                             rolled-back write inserted"
                        ),
                    ));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::index_config::{IndexConfig, IndexType};
    use crate::types::{DatabaseId, TenantId};

    const TID: u64 = 1;
    const DIM: usize = 4;

    /// Distinct per `i`: the first component is `i + 1`.
    fn vector(i: usize) -> Vec<f32> {
        vec![
            (i + 1) as f32,
            (i % 7 + 1) as f32,
            (i % 11 + 1) as f32,
            (i % 13 + 1) as f32,
        ]
    }

    fn ivf_collection() -> VectorCollection {
        VectorCollection::with_index_config(
            DIM,
            IndexConfig {
                index_type: IndexType::IvfPq,
                pq_m: 2,
                ivf_cells: 2,
                ivf_nprobe: 2,
                ..IndexConfig::default()
            },
        )
    }

    fn memory() -> nodedb_mem::ScopedMemory {
        nodedb_mem::ScopedMemory::new(
            crate::data::executor::core_loop::test_governor(),
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            nodedb_mem::EngineId::Vector,
        )
    }

    fn capture(core: &CoreLoop, key: &VectorIndexKey) -> VectorWriteUndo {
        let undo = core
            .capture_vector_write_undo(VectorWriteTarget {
                index_key: key,
                tid: TID,
                collection: "docs",
                surrogates: &[],
                ids: &[],
                sidecars: false,
            })
            .expect("capture undo");
        let UndoEntry::VectorWrite(undo) = undo else {
            panic!("a vector write captures a VectorWrite undo");
        };
        *undo
    }

    /// The inserts of a rolled-back write leave a trained IVF-PQ index: it
    /// holds the vectors and the training it held before the write.
    #[test]
    fn a_rolled_back_write_withdraws_its_ivf_inserts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());
        let mut coll = ivf_collection();
        for i in 0..256 {
            coll.insert(vector(i)).unwrap();
        }
        coll.train_ivf(memory(), 1).unwrap();
        core.vector_collections.insert(key.clone(), coll);

        let undo = capture(&core, &key);
        if let Some(coll) = core.vector_collections.get_mut(&key) {
            for i in 256..260 {
                coll.insert(vector(i)).unwrap();
            }
        }
        core.apply_undo_vector_write(0, undo)
            .expect("undo vector write");

        let coll = core
            .vector_collections
            .get_mut(&key)
            .expect("collection stays");
        assert_eq!(coll.live_count(), 256);
        assert!(coll.ivf_index().is_some_and(|ivf| ivf.is_trained()));
        assert!(
            coll.search(&vector(258), 300, 64)
                .unwrap()
                .iter()
                .all(|r| r.id < 256),
            "no vector the write inserted is found"
        );
        assert_eq!(
            coll.insert(vector(300)).unwrap(),
            256,
            "the id counter is back"
        );
    }

    /// A rolled-back vector-primary write removes the sidecar version it
    /// appended: the row reads as before at every system time.
    #[test]
    fn a_rolled_back_write_removes_its_sidecar_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());
        let surrogate = Surrogate::new(9);
        let row = StorageKey::for_surrogate(surrogate);
        core.sparse
            .put(0, TID, "docs", &row, b"before", 100)
            .expect("seed sidecar");

        let undo = core
            .capture_vector_write_undo(VectorWriteTarget {
                index_key: &key,
                tid: TID,
                collection: "docs",
                surrogates: &[surrogate],
                ids: &[],
                sidecars: true,
            })
            .expect("capture undo");
        let UndoEntry::VectorWrite(undo) = undo else {
            panic!("a vector write captures a VectorWrite undo");
        };
        core.sparse
            .put(0, TID, "docs", &row, b"after", 200)
            .expect("write sidecar");
        core.apply_undo_vector_write(0, *undo)
            .expect("undo vector write");

        assert_eq!(
            core.sparse.get(0, TID, "docs", &row).unwrap(),
            Some(b"before".to_vec())
        );
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(0, TID, "docs", &row)
                .unwrap(),
            Some(100),
            "the rolled-back version leaves no history"
        );
    }

    /// An IVF-PQ collection the rolled-back write created is removed.
    #[test]
    fn a_rolled_back_write_removes_the_ivf_collection_it_created() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: VectorIndexKey = (DatabaseId::DEFAULT, TenantId::new(TID), "docs:".into());

        let undo = capture(&core, &key);
        let mut coll = ivf_collection();
        coll.insert(vector(0)).unwrap();
        core.vector_collections.insert(key.clone(), coll);
        core.apply_undo_vector_write(0, undo)
            .expect("undo vector write");

        assert!(!core.vector_collections.contains_key(&key));
    }
}
