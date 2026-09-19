// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for vector-primary `DirectDelete` and
//! `DirectUpdate`.
//!
//! Targets resolve against BASE ∪ OVERLAY: a point target counts when the
//! row is live under the overlay (a staged put, or a base node with no
//! staged tombstone); a predicate target is every base sidecar the filters
//! match that the overlay has not superseded, plus every staged put whose
//! sidecar matches. Deletes stage tombstones. Updates merge through the
//! live handler's merge and stage the post-image with the row's vector,
//! so a later same-transaction search ranks the row where COMMIT will.

use std::collections::HashSet;

use nodedb_physical::physical_plan::{UpdateValue, VectorWriteTargets};
use nodedb_types::{RlsWriteCheck, StorageKey, Surrogate, Value};

use super::context::StageCtx;
use super::stage_vector::{VectorCurrentRow, encode_staged_vector_row, staged_vector_row_identity};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::handlers::vector_direct_row::{VectorDirectIndexSpec, VectorIndexKey};
use crate::data::executor::handlers::vector_direct_targets::{
    decode_vector_write_filters, vector_sidecar_matches,
};
use crate::data::executor::handlers::vector_direct_update::{
    VectorPayloadPatch, merge_vector_direct_update_row,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

/// Inputs of one staged `DirectDelete`.
pub(super) struct StageVectorDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub field: &'a str,
    pub targets: &'a VectorWriteTargets,
    pub rls_write_check: &'a RlsWriteCheck,
}

/// Inputs of one staged `DirectUpdate`.
pub(super) struct StageVectorUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub field: &'a str,
    pub targets: &'a VectorWriteTargets,
    pub new_vector: Option<&'a [f32]>,
    pub payload_patch: &'a [(String, UpdateValue)],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub rls_write_check: &'a RlsWriteCheck,
}

impl CoreLoop {
    /// The rows `targets` names that are live under BASE ∪ OVERLAY, with
    /// each row's current image, in ascending surrogate order.
    fn stage_vector_targets(
        &self,
        ctx: &StageCtx<'_>,
        index_key: &VectorIndexKey,
        targets: &VectorWriteTargets,
    ) -> Result<Vec<(Surrogate, VectorCurrentRow)>, ErrorCode> {
        let mut out: Vec<(Surrogate, VectorCurrentRow)> = Vec::new();
        match targets {
            VectorWriteTargets::Surrogates(surrogates) => {
                let mut seen = HashSet::with_capacity(surrogates.len());
                for surrogate in surrogates {
                    if *surrogate == Surrogate::ZERO || !seen.insert(*surrogate) {
                        continue;
                    }
                    if let Some(row) = self.stage_vector_current_row(ctx, index_key, *surrogate)? {
                        out.push((*surrogate, row));
                    }
                }
            }
            VectorWriteTargets::Predicate(filter_bytes) => {
                let filters = decode_vector_write_filters(ctx.collection, filter_bytes)?;
                let overlay = self.txn_overlays.get(&ctx.txn_id);
                // Base rows the filters match, minus every row the overlay
                // superseded: those are decided from their staged image below.
                for surrogate in self.scan_vector_sidecar_matches(
                    ctx.database_id,
                    ctx.tid,
                    ctx.collection,
                    &filters,
                )? {
                    let superseded =
                        overlay.is_some_and(|o| o.get(&ctx.coll_key, surrogate.0).is_some());
                    if superseded {
                        continue;
                    }
                    if let Some(row) = self.stage_vector_current_row(ctx, index_key, surrogate)? {
                        out.push((surrogate, row));
                    }
                }
                // Staged puts whose sidecar matches; a staged tombstone is a
                // row that is gone.
                if let Some(overlay) = overlay {
                    for (surrogate, staged) in overlay.iter_for_collection(&ctx.coll_key) {
                        let Staged::Put(_) = staged else {
                            continue;
                        };
                        let surrogate = Surrogate::new(surrogate);
                        let Some(row) = self.stage_vector_current_row(ctx, index_key, surrogate)?
                        else {
                            continue;
                        };
                        let key = StorageKey::for_surrogate(surrogate);
                        if vector_sidecar_matches(&key, &row.sidecar.bytes, &filters)? {
                            out.push((surrogate, row));
                        }
                    }
                }
            }
        }
        out.sort_by_key(|(surrogate, _)| surrogate.0);
        Ok(out)
    }

    /// Stage `DirectDelete`: a tombstone per live target.
    pub(super) fn stage_vector_delete(&mut self, params: StageVectorDeleteParams<'_>) -> Response {
        let StageVectorDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            field,
            targets,
            rls_write_check,
        } = params;
        let ctx = StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            StorageKey::for_surrogate(Surrogate::ZERO).to_identity(),
            Surrogate::ZERO,
        );
        let index_key = CoreLoop::vector_index_key(ctx.database_id, tid, collection, field);
        let rows = match self.stage_vector_targets(&ctx, &index_key, targets) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        // Gate every row on the write policy BEFORE any tombstone, so a
        // rejected row cannot leave the rows ahead of it already staged. The
        // sidecar image is the only image a delete has.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for (_, row) in &rows {
                let image = Value::Object(row.sidecar.fields.clone());
                if let Err(e) =
                    rls_write_gate::admit_document_value(rls_write_check, &image, tid, collection)
                {
                    return self.response_error(task, e);
                }
            }
        }

        let count = rows.len();
        for (surrogate, row) in rows {
            let key = StorageKey::for_surrogate(surrogate);
            let identity = staged_vector_row_identity(&row.sidecar.bytes, key);
            self.txn_overlay_mut(txn_id).insert_tombstone(
                ctx.coll_key.clone(),
                surrogate.0,
                &identity,
            );
        }
        self.stage_count_response(task, count)
    }

    /// Stage `DirectUpdate`: the merged post-image per live target, with
    /// the new vector or the row's current one.
    pub(super) fn stage_vector_update(&mut self, params: StageVectorUpdateParams<'_>) -> Response {
        let StageVectorUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            rls_write_check,
        } = params;
        let ctx = StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            StorageKey::for_surrogate(Surrogate::ZERO).to_identity(),
            Surrogate::ZERO,
        );
        // A re-embed must fit the index the rows live in.
        let index_key = match new_vector {
            Some(vector) => match self.stage_vector_index_key(
                &ctx,
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
            None => CoreLoop::vector_index_key(ctx.database_id, tid, collection, field),
        };
        let rows = match self.stage_vector_targets(&ctx, &index_key, targets) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        // Every post-image is merged and decided before the first row is
        // staged, so one rejected row leaves the statement without effect.
        let patch = VectorPayloadPatch {
            database_id: ctx.database_id,
            tid,
            collection,
            field,
            payload_patch,
            rls_write_check,
        };
        // Per row: its surrogate, the staged body, and the sidecar it carries.
        let mut planned: Vec<(Surrogate, Vec<u8>, Vec<u8>)> = Vec::with_capacity(rows.len());
        for (surrogate, row) in rows {
            let VectorCurrentRow { vector, sidecar } = row;
            let merged = match merge_vector_direct_update_row(&patch, surrogate, sidecar) {
                Ok(merged) => merged,
                Err(e) => return self.response_error(task, e),
            };
            let vector = new_vector.map_or(vector, <[f32]>::to_vec);
            let (staged, sidecar) = match encode_staged_vector_row(&vector, &merged.fields) {
                Ok(encoded) => encoded,
                Err(e) => return self.response_error(task, e),
            };
            planned.push((surrogate, staged, sidecar));
        }

        let count = planned.len();
        for (surrogate, staged, sidecar) in planned {
            let key = StorageKey::for_surrogate(surrogate);
            let row_ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                staged_vector_row_identity(&sidecar, key),
                surrogate,
            );
            if let Err(e) = self.stage_put_capped(&row_ctx, staged) {
                return self.response_error(task, e);
            }
        }
        self.stage_count_response(task, count)
    }
}
