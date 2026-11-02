// SPDX-License-Identifier: BUSL-1.1

//! Row primitives shared by the vector-primary direct write handlers.
//!
//! A vector-primary row lives in three places: an HNSW node bound to the
//! row's surrogate, the payload bitmap entries of that node, and the
//! payload sidecar in the sparse store under `StorageKey::for_surrogate`.
//! Every direct write moves all three together through these helpers, so
//! an insert, a replace, a patch, and a delete cannot drift apart.

use std::collections::HashMap;

use nodedb_types::{StorageKey, Surrogate, Value};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::vector_upsert::decode_payload_lowercased;

/// In-memory key of one vector index: `(database, tenant, "coll:field")`.
pub(in crate::data::executor) type VectorIndexKey =
    (crate::types::DatabaseId, crate::types::TenantId, String);

/// The index a direct write lands in, with the settings a first write
/// registers on a new index.
pub(in crate::data::executor) struct VectorDirectIndexSpec<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub dim: usize,
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

/// One row to store, for [`CoreLoop::write_vector_direct_row`].
pub(in crate::data::executor) struct VectorDirectRowWrite<'a> {
    pub index_key: &'a VectorIndexKey,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: Surrogate,
    pub vector: &'a [f32],
    pub fields: &'a HashMap<String, Value>,
    pub sidecar: &'a [u8],
}

/// The sidecar stored for one row: its raw bytes and the decoded,
/// lower-cased field map the bitmap indexes were built from.
pub(in crate::data::executor) struct VectorSidecarRow {
    pub bytes: Vec<u8>,
    pub fields: HashMap<String, Value>,
}

/// Encode a payload map into the sidecar's `zerompk` TAGGED form.
pub(in crate::data::executor) fn encode_sidecar(
    fields: &HashMap<String, Value>,
) -> Result<Vec<u8>, ErrorCode> {
    zerompk::to_msgpack_vec(fields).map_err(|e| ErrorCode::Internal {
        detail: format!("vector-primary sidecar encode failed: {e}"),
    })
}

impl CoreLoop {
    /// Check the vector width and storage dtype of a direct write against
    /// the index it lands in, when that index exists. `Ok(None)` means no
    /// index exists yet. Reads only, so the resolve pass can run the same
    /// check the write runs.
    pub(in crate::data::executor) fn check_vector_direct_index(
        &self,
        database_id: u64,
        tid: u64,
        spec: &VectorDirectIndexSpec<'_>,
    ) -> Result<Option<VectorIndexKey>, ErrorCode> {
        let index_key = CoreLoop::vector_index_key(database_id, tid, spec.collection, spec.field);
        let Some(existing) = self.vector_collections.get(&index_key) else {
            return Ok(None);
        };
        if existing.dim() != spec.dim {
            return Err(super::vector::dimension_mismatch(existing.dim(), spec.dim));
        }
        let existing_dtype = existing.params().dtype;
        if existing_dtype != spec.storage_dtype {
            return Err(ErrorCode::DataException {
                detail: format!(
                    "vector storage_dtype mismatch: index has {existing_dtype}, got {}; \
                     dtype is immutable after collection creation",
                    spec.storage_dtype
                ),
            });
        }
        Ok(Some(index_key))
    }

    /// Resolve the HNSW index a direct write lands in, creating it on the
    /// first write. Checks the vector width and storage dtype against an
    /// existing index: the dtype is baked into segment layout at creation.
    pub(in crate::data::executor) fn vector_direct_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        spec: &VectorDirectIndexSpec<'_>,
    ) -> Result<VectorIndexKey, ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        if let Some(index_key) = self.check_vector_direct_index(database_id, tid, spec)? {
            return Ok(index_key);
        }
        let index_key = CoreLoop::vector_index_key(database_id, tid, spec.collection, spec.field);

        // A new vector-primary index: seed the storage dtype so the graph is
        // built with the right `NodeStorage` variant, and give it a dedicated
        // jemalloc arena so its allocations stay apart from document work.
        let core_id = self.core_id;
        let params = self.vector_params.entry(index_key.clone()).or_default();
        params.dtype = spec.storage_dtype;
        let arena_handle = self.collection_arena_registry.clone().and_then(|reg| {
            match reg.get_or_create(tid, spec.collection) {
                Ok(handle) => Some(handle),
                Err(e) => {
                    tracing::debug!(
                        core = core_id,
                        collection = %spec.collection,
                        error = %e,
                        "per-collection arena allocation failed; using global allocator"
                    );
                    None
                }
            }
        });
        let coll = self.get_or_create_vector_index(
            database_id,
            tid,
            spec.collection,
            spec.dim,
            spec.field,
        )?;
        if let Some(handle) = arena_handle {
            coll.arena_index = handle.arena_index();
        }
        coll.set_quantization(spec.quantization);
        for (f, kind) in spec.payload_indexes {
            coll.payload.add_index(f.to_ascii_lowercase(), *kind);
        }
        Ok(index_key)
    }

    /// The HNSW node bound to `surrogate` in `index_key`, if any.
    pub(in crate::data::executor) fn vector_direct_node(
        &self,
        index_key: &VectorIndexKey,
        surrogate: Surrogate,
    ) -> Option<u32> {
        self.vector_collections
            .get(index_key)
            .and_then(|c| c.local_for_surrogate(surrogate))
    }

    /// Read the raw sidecar bytes stored for `surrogate`, undecoded.
    pub(in crate::data::executor) fn vector_sidecar_bytes(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let key = StorageKey::for_surrogate(surrogate);
        self.sparse
            .get(database_id, tid, collection, &key)
            .map_err(ErrorCode::from)
    }

    /// Read and decode the sidecar stored for `surrogate`.
    pub(in crate::data::executor) fn vector_sidecar_row(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Result<Option<VectorSidecarRow>, ErrorCode> {
        let Some(bytes) = self.vector_sidecar_bytes(database_id, tid, collection, surrogate)?
        else {
            return Ok(None);
        };
        let key = StorageKey::for_surrogate(surrogate);
        let fields = decode_payload_lowercased(&bytes).map_err(|e| ErrorCode::Internal {
            detail: format!("vector-primary sidecar decode failed for {key}: {e}"),
        })?;
        Ok(Some(VectorSidecarRow { bytes, fields }))
    }

    /// Remove every trace of `surrogate`'s row: bitmap entries, HNSW node,
    /// sidecar row, and the cached sidecar. Returns the sidecar that was
    /// stored, or `None` when no node was bound to the surrogate.
    ///
    /// The sidecar delete is the only fallible step, so it runs first. A
    /// failure leaves the row fully present. A crash after it leaves a node
    /// with no sidecar, which replay of the delete record removes.
    pub(in crate::data::executor) fn remove_vector_direct_row(
        &mut self,
        index_key: &VectorIndexKey,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Result<Option<VectorSidecarRow>, ErrorCode> {
        let database_id = index_key.0.as_u64();
        let Some(node_id) = self.vector_direct_node(index_key, surrogate) else {
            return Ok(None);
        };
        let sidecar = self.vector_sidecar_row(database_id, tid, collection, surrogate)?;
        let key = StorageKey::for_surrogate(surrogate);
        crate::fail_point_err!(
            self.fail_scope,
            "vector_direct::sidecar_delete",
            |detail: String| ErrorCode::Internal { detail }
        );
        let sys_from = self
            .row_version_ordinal(database_id, tid, collection, &key)
            .map_err(ErrorCode::from)?;
        self.sparse
            .delete(database_id, tid, collection, &key, sys_from)
            .map_err(ErrorCode::from)?;
        self.doc_cache
            .invalidate(database_id, tid, collection, &key);
        if let Some(coll) = self.vector_collections.get_mut(index_key) {
            if let Some(row) = &sidecar {
                coll.payload.delete_row(node_id, &row.fields);
            }
            // The node ends with the row's tombstone: a read at an earlier
            // cut still finds the vector.
            coll.end_at(node_id, sys_from);
        }
        Ok(Some(sidecar.unwrap_or_else(|| VectorSidecarRow {
            bytes: Vec::new(),
            fields: HashMap::new(),
        })))
    }

    /// Store one row: HNSW node bound to `surrogate`, bitmap entries for
    /// `fields`, and `sidecar` in the sparse store. A failed sidecar write
    /// rolls the node and its bitmap entries back, so no phantom node can
    /// answer a search with no row behind it.
    pub(in crate::data::executor) fn write_vector_direct_row(
        &mut self,
        row: VectorDirectRowWrite<'_>,
    ) -> Result<(), ErrorCode> {
        let VectorDirectRowWrite {
            index_key,
            tid,
            collection,
            surrogate,
            vector,
            fields,
            sidecar,
        } = row;
        let database_id = index_key.0.as_u64();
        let key = StorageKey::for_surrogate(surrogate);
        // The node starts at the row version's system time, so a read at an
        // earlier cut does not see it.
        let sys_from = self
            .row_version_ordinal(database_id, tid, collection, &key)
            .map_err(ErrorCode::from)?;
        let Some(coll) = self.vector_collections.get_mut(index_key) else {
            return Err(ErrorCode::Internal {
                detail: format!("vector index for '{collection}' vanished during a direct write"),
            });
        };
        let displaced = coll.local_for_surrogate(surrogate);
        let node_id = coll
            .insert_at(vector.to_vec(), surrogate, sys_from)
            .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
        coll.payload.insert_row(node_id, fields);

        let written = self
            .sparse
            .put(database_id, tid, collection, &key, sidecar, sys_from);
        if let Err(e) = written {
            if let Some(coll) = self.vector_collections.get_mut(index_key) {
                coll.payload.delete_row(node_id, fields);
                coll.delete(node_id);
                if let Some(displaced) = displaced {
                    coll.undelete_bound(displaced, surrogate);
                }
            }
            return Err(ErrorCode::Internal {
                detail: format!("vector-primary payload sparse write failed: {e}"),
            });
        }
        self.doc_cache
            .invalidate(database_id, tid, collection, &key);
        Ok(())
    }

    /// Bookkeeping every completed direct write runs once: seal a full growing
    /// segment or train an IVF-PQ collection at its threshold, mark the checkpoint dirty, and record each
    /// touched surrogate's write.
    pub(in crate::data::executor) fn finish_vector_direct_write(
        &mut self,
        task: &ExecutionTask,
        index_key: &VectorIndexKey,
        tid: u64,
        collection: &str,
        surrogates: &[Surrogate],
    ) {
        // A committed-redo install seals once the whole record landed, so a
        // rollback finds its inserts in the growing segment.
        if !self.recording_redo_undo() {
            self.settle_vector_collection(index_key);
        }
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        for surrogate in surrogates {
            self.note_surrogate_write(task, tid, collection, surrogate.as_u32());
        }
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use nodedb_physical::physical_plan::VectorWriteTargets;
    use nodedb_types::fail_point::FailGuard;
    use nodedb_types::{RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::vector_direct_delete::VectorDirectDeleteParams;
    use crate::data::executor::handlers::vector_direct_test_rows::{
        COLLECTION, FIELD, TID, has_node, has_sidecar, seed,
    };

    const SIDECAR_DELETE: &str = "vector_direct::sidecar_delete";

    fn delete(core: &mut CoreLoop, surrogate: Surrogate) -> Status {
        let task = make_default_task();
        let targets = VectorWriteTargets::Surrogates(vec![surrogate]);
        core.execute_vector_direct_delete(VectorDirectDeleteParams {
            task: &task,
            tid: TID,
            collection: COLLECTION,
            field: FIELD,
            targets: &targets,
            returning: None,
            rls_filters: &[],
            rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
        })
        .status
    }

    #[test]
    fn a_failed_sidecar_delete_leaves_the_row_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let row = Surrogate::new(5);
        seed(&mut core, row);

        {
            let _fail = FailGuard::fail(SIDECAR_DELETE, "disk full");
            assert_ne!(delete(&mut core, row), Status::Ok);
            assert!(has_node(&core, row), "a refused delete keeps the node");
            assert!(
                has_sidecar(&core, row),
                "a refused delete keeps the sidecar"
            );
        }

        assert_eq!(delete(&mut core, row), Status::Ok);
        assert!(!has_node(&core, row));
        assert!(!has_sidecar(&core, row));
    }

    #[test]
    fn a_failed_sidecar_delete_leaves_a_truncated_row_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let row = Surrogate::new(9);
        seed(&mut core, row);

        let _fail = FailGuard::fail(SIDECAR_DELETE, "disk full");
        let task = make_default_task();
        let resp = core.execute_vector_direct_truncate(&task, TID, COLLECTION, FIELD);
        assert_ne!(resp.status, Status::Ok);
        assert!(has_node(&core, row), "a refused truncate keeps the node");
        assert!(
            has_sidecar(&core, row),
            "a refused truncate keeps the sidecar"
        );
    }
}
