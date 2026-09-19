// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp::DirectUpdate`: rewrite rows of a vector-primary collection.
//!
//! Per targeted surrogate: the payload patch is merged into the stored
//! sidecar, the write policy decides the merged image, then the row is
//! rewritten. A new vector rebuilds the HNSW node under the same surrogate.
//! A payload-only update keeps the node and moves its bitmap entries.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{ReturningSpec, UpdateValue, VectorWriteTargets};
use nodedb_types::{RlsWriteCheck, StorageKey, Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::rls_write_gate;
use super::upsert::apply_on_conflict_updates;
use super::vector_direct_row::{
    VectorDirectIndexSpec, VectorDirectRowWrite, VectorIndexKey, VectorSidecarRow, encode_sidecar,
};

/// Parameters for [`CoreLoop::execute_vector_direct_update`].
pub(in crate::data::executor) struct VectorDirectUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub targets: &'a VectorWriteTargets,
    pub new_vector: Option<&'a [f32]>,
    pub payload_patch: &'a [(String, UpdateValue)],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    /// When `Some`, project each row's stored post-image instead of a count.
    pub returning: Option<&'a ReturningSpec>,
    /// Read policy bounding which rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Write policy decided against each row's merged post-image.
    pub rls_write_check: &'a RlsWriteCheck,
}

/// The statement-wide inputs one row's payload merge reads.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct VectorPayloadPatch<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub payload_patch: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a RlsWriteCheck,
}

/// One row's planned rewrite: the surrogate, the merged payload, the sidecar
/// bytes it encodes to, and the sidecar bytes it replaces.
pub(in crate::data::executor) struct VectorPlannedRow {
    pub surrogate: Surrogate,
    pub fields: HashMap<String, Value>,
    pub sidecar: Vec<u8>,
    /// The stored sidecar the merge read — the resolve pass ships it as the
    /// mutation's drift check.
    pub old_sidecar: Vec<u8>,
}

impl CoreLoop {
    /// Handle `VectorOp::DirectUpdate`.
    pub(in crate::data::executor) fn execute_vector_direct_update(
        &mut self,
        params: VectorDirectUpdateParams<'_>,
    ) -> Response {
        let VectorDirectUpdateParams {
            task,
            tid,
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            returning,
            rls_filters,
            rls_write_check,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            re_embed = new_vector.is_some(),
            "vector direct update"
        );
        let database_id = task.request.database_id.as_u64();

        // No index means no row was ever written: nothing to rewrite.
        let probe_key = CoreLoop::vector_index_key(database_id, tid, collection, field);
        if !self.vector_collections.contains_key(&probe_key) {
            if let Some(spec) = returning {
                return self.vector_stored_returning_response(task, spec, rls_filters, &[]);
            }
            return self.response_affected(task, 0);
        }
        // A re-embed must fit the index the rows live in.
        let index_key = match new_vector {
            Some(vector) => match self.vector_direct_index(
                task,
                tid,
                &VectorDirectIndexSpec {
                    collection,
                    field,
                    dim: vector.len(),
                    quantization,
                    storage_dtype,
                    payload_indexes,
                },
            ) {
                Ok(key) => key,
                Err(e) => return self.response_error(task, e),
            },
            None => probe_key,
        };

        let surrogates: Vec<Surrogate> =
            match self.resolve_vector_direct_targets(database_id, tid, collection, targets) {
                Ok(s) => s
                    .into_iter()
                    .filter(|s| self.vector_direct_node(&index_key, *s).is_some())
                    .collect(),
                Err(e) => return self.response_error(task, e),
            };

        // Every post-image is computed and decided before the first row is
        // written, so one rejected row leaves the statement without effect.
        let patch = VectorPayloadPatch {
            database_id,
            tid,
            collection,
            field,
            payload_patch,
            rls_write_check,
        };
        let mut planned: Vec<VectorPlannedRow> = Vec::with_capacity(surrogates.len());
        for surrogate in surrogates {
            match self.plan_vector_direct_update_row(&patch, surrogate) {
                Ok(Some(row)) => planned.push(row),
                Ok(None) => {}
                Err(e) => return self.response_error(task, e),
            }
        }

        let mut written: Vec<(StorageKey, Vec<u8>)> = Vec::with_capacity(planned.len());
        for row in planned {
            if let Err(e) = self
                .apply_vector_direct_update_row(task, &index_key, tid, collection, &row, new_vector)
            {
                return self.response_error(task, e);
            }
            written.push((StorageKey::for_surrogate(row.surrogate), row.sidecar));
        }
        if !written.is_empty() {
            let touched: Vec<Surrogate> = written.iter().map(|(k, _)| k.surrogate()).collect();
            self.finish_vector_direct_write(task, &index_key, tid, collection, &touched);
        }

        if let Some(spec) = returning {
            let rows: Vec<(&StorageKey, &[u8])> =
                written.iter().map(|(k, b)| (k, b.as_slice())).collect();
            return self.vector_stored_returning_response(task, spec, rls_filters, &rows);
        }
        self.response_affected(task, written.len() as u64)
    }

    /// Merge the patch into `surrogate`'s stored sidecar and decide the
    /// write policy on the result. `None` when the sidecar is gone.
    pub(in crate::data::executor) fn plan_vector_direct_update_row(
        &self,
        patch: &VectorPayloadPatch<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<VectorPlannedRow>, ErrorCode> {
        let VectorPayloadPatch {
            database_id,
            tid,
            collection,
            field,
            payload_patch,
            rls_write_check,
        } = *patch;
        let Some(VectorSidecarRow {
            fields,
            bytes: old_sidecar,
        }) = self.vector_sidecar_row(database_id, tid, collection, surrogate)?
        else {
            return Ok(None);
        };
        let excluded = Value::Object(HashMap::new());
        let merged =
            match apply_on_conflict_updates(Value::Object(fields), &excluded, payload_patch)? {
                Value::Object(map) => map,
                other => {
                    return Err(ErrorCode::Internal {
                        detail: format!(
                            "UPDATE on '{collection}' produced a non-object row: {other:?}"
                        ),
                    });
                }
            };
        let image = Value::Object(merged.clone());
        rls_write_gate::admit_document_value(rls_write_check, &image, tid, collection)?;
        // The vector column never lives in the sidecar.
        let fields: HashMap<String, Value> = merged
            .into_iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case(field))
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect();
        let sidecar = encode_sidecar(&fields)?;
        Ok(Some(VectorPlannedRow {
            surrogate,
            fields,
            sidecar,
            old_sidecar,
        }))
    }

    /// Rewrite one row. With `new_vector` the old row is removed and a fresh
    /// node bound under the same surrogate; without it the node stays and
    /// only the bitmap entries and sidecar move.
    pub(in crate::data::executor) fn apply_vector_direct_update_row(
        &mut self,
        task: &ExecutionTask,
        index_key: &VectorIndexKey,
        tid: u64,
        collection: &str,
        row: &VectorPlannedRow,
        new_vector: Option<&[f32]>,
    ) -> Result<(), ErrorCode> {
        let database_id = index_key.0.as_u64();
        if let Some(vector) = new_vector {
            self.remove_vector_direct_row(index_key, tid, collection, row.surrogate)?;
            return self.write_vector_direct_row(VectorDirectRowWrite {
                task,
                index_key,
                tid,
                collection,
                surrogate: row.surrogate,
                vector,
                fields: &row.fields,
                sidecar: &row.sidecar,
            });
        }
        let Some(node_id) = self.vector_direct_node(index_key, row.surrogate) else {
            return Ok(());
        };
        let old = self
            .vector_sidecar_row(database_id, tid, collection, row.surrogate)?
            .map(|r| r.fields)
            .unwrap_or_default();
        let Some(coll) = self.vector_collections.get_mut(index_key) else {
            return Err(ErrorCode::Internal {
                detail: format!("vector index for '{collection}' vanished during an update"),
            });
        };
        coll.payload.delete_row(node_id, &old);
        coll.payload.insert_row(node_id, &row.fields);
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }
        let key = StorageKey::for_surrogate(row.surrogate);
        if let Err(e) = self
            .sparse
            .put(database_id, tid, collection, &key, &row.sidecar)
        {
            // Put the bitmap entries back so the index still describes the
            // sidecar that is actually stored.
            if let Some(coll) = self.vector_collections.get_mut(index_key) {
                coll.payload.delete_row(node_id, &row.fields);
                coll.payload.insert_row(node_id, &old);
            }
            return Err(ErrorCode::Internal {
                detail: format!("vector-primary payload sparse write failed: {e}"),
            });
        }
        self.doc_cache
            .invalidate(database_id, tid, collection, &key);
        Ok(())
    }
}
