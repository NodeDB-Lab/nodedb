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
    pub task: &'a ExecutionTask,
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
            return Err(ErrorCode::RejectedConstraint {
                detail: String::new(),
                constraint: format!(
                    "vector dimension mismatch: index has {}, got {}",
                    existing.dim(),
                    spec.dim
                ),
            });
        }
        let existing_dtype = existing.params().dtype;
        if existing_dtype != spec.storage_dtype {
            return Err(ErrorCode::RejectedConstraint {
                detail: String::new(),
                constraint: format!(
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
        let Some(coll) = self.vector_collections.get_mut(index_key) else {
            return Ok(None);
        };
        if let Some(row) = &sidecar {
            coll.payload.delete_row(node_id, &row.fields);
        }
        coll.delete(node_id);
        let key = StorageKey::for_surrogate(surrogate);
        self.sparse
            .delete(database_id, tid, collection, &key)
            .map_err(ErrorCode::from)?;
        self.doc_cache
            .invalidate(database_id, tid, collection, &key);
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
            task,
            index_key,
            tid,
            collection,
            surrogate,
            vector,
            fields,
            sidecar,
        } = row;
        let database_id = index_key.0.as_u64();
        let Some(coll) = self.vector_collections.get_mut(index_key) else {
            return Err(ErrorCode::Internal {
                detail: format!("vector index for '{collection}' vanished during a direct write"),
            });
        };
        let node_id = coll.insert_with_surrogate(vector.to_vec(), surrogate);
        // Advance the checkpoint watermark so a later vector checkpoint records
        // this write as absorbed; startup replay then skips the straddling WAL
        // record instead of appending a duplicate node.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }
        coll.payload.insert_row(node_id, fields);

        let key = StorageKey::for_surrogate(surrogate);
        if let Err(e) = self.sparse.put(database_id, tid, collection, &key, sidecar) {
            if let Some(coll) = self.vector_collections.get_mut(index_key) {
                coll.payload.delete_row(node_id, fields);
                coll.delete(node_id);
            }
            return Err(ErrorCode::Internal {
                detail: format!("vector-primary payload sparse write failed: {e}"),
            });
        }
        self.doc_cache
            .invalidate(database_id, tid, collection, &key);
        Ok(())
    }

    /// Bookkeeping every completed direct write runs once: seal the growing
    /// segment when it is full, mark the checkpoint dirty, and record each
    /// touched surrogate's write version for cross-shard OCC validation.
    pub(in crate::data::executor) fn finish_vector_direct_write(
        &mut self,
        task: &ExecutionTask,
        index_key: &VectorIndexKey,
        tid: u64,
        collection: &str,
        surrogates: &[Surrogate],
    ) {
        let seal_key = CoreLoop::vector_build_key(index_key);
        // A committed-redo install seals once the whole record landed, so a
        // rollback finds its inserts in the growing segment.
        if !self.recording_redo_undo()
            && let Some(coll) = self.vector_collections.get_mut(index_key)
            && coll.needs_seal()
            && let Some(req) = coll.seal(&seal_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send HNSW build request"
            );
        }
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        for surrogate in surrogates {
            self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());
        }
    }
}
