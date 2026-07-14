// SPDX-License-Identifier: BUSL-1.1

//! `StageWrite` dispatch: route a point-write plan to the matching staging
//! path, compute its real affected-row count, and record it in the overlay.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, GraphOp, SpatialOp, TimeseriesOp};

use super::constraint::OverlayPk;
use super::context::StageCtx;
use super::{
    StageBulkDeleteParams, StageBulkUpdateParams, StageColumnarDeleteParams,
    StageColumnarInsertParams, StageColumnarUpdateParams, StageSpatialInsertParams,
    StageTimeseriesInsertParams,
};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{MAX_TXN_OVERLAY_BYTES, Staged};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a `MetaOp::StageWrite` for an in-transaction point write.
    ///
    /// Only point-write `DocumentOp`s are valid here (the Control Plane only
    /// builds `StageWrite` for those); anything else is an internal error.
    pub(in crate::data::executor) fn execute_stage_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
    ) -> Response {
        let Some(txn_id) = task.request.txn_id else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "StageWrite dispatched without a txn_id".into(),
                },
            );
        };

        let doc_op = match plan {
            PhysicalPlan::Document(op) => op,
            PhysicalPlan::Kv(op) => return self.execute_stage_kv(task, tid, txn_id, op),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                surrogates,
                schema_bytes,
                ..
            }) => {
                return self.stage_columnar_insert(StageColumnarInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    payload,
                    surrogates,
                    schema_bytes,
                });
            }
            // Predicate DELETE / UPDATE on a columnar collection staged at
            // statement time — the affected surrogate set is resolved against
            // the current BASE ∪ OVERLAY view and tombstoned (delete) or
            // superseded (update). COMMIT replay of the buffered plan remains
            // the sole durable apply.
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection,
                filters,
            }) => {
                return self.stage_columnar_delete(StageColumnarDeleteParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    filter_bytes: filters,
                });
            }
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection,
                filters,
                updates,
            }) => {
                return self.stage_columnar_update(StageColumnarUpdateParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    filter_bytes: filters,
                    updates,
                });
            }
            PhysicalPlan::Columnar(
                ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. },
            ) => return self.stage_not_point_write(task),
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection,
                field,
                surrogate,
                geometry,
                provenance: _,
            }) => {
                return self.stage_spatial_insert(StageSpatialInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    field,
                    surrogate: *surrogate,
                    geometry,
                });
            }
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection,
                surrogate,
                field: _,
                provenance: _,
            }) => return self.stage_spatial_delete(task, tid, txn_id, collection, *surrogate),
            PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => {
                return self.stage_not_point_write(task);
            }
            PhysicalPlan::Graph(
                op @ (GraphOp::EdgePut { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }),
            ) => return self.execute_stage_graph(task, tid, txn_id, op),
            PhysicalPlan::Graph(
                GraphOp::Hop { .. }
                | GraphOp::Neighbors { .. }
                | GraphOp::NeighborsMulti { .. }
                | GraphOp::Path { .. }
                | GraphOp::Subgraph { .. }
                | GraphOp::RagFusion { .. }
                | GraphOp::Algo { .. }
                | GraphOp::Match { .. }
                | GraphOp::MatchContinuation { .. }
                | GraphOp::MatchVarLenResume { .. }
                | GraphOp::BspSuperstep(_)
                | GraphOp::WccSuperstep(_)
                | GraphOp::TemporalNeighbors { .. }
                | GraphOp::TemporalAlgorithm { .. }
                | GraphOp::Stats { .. },
            ) => return self.stage_not_point_write(task),
            PhysicalPlan::Vector(_) => return self.stage_not_point_write(task),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                surrogates,
                ..
            }) => {
                return self.stage_timeseries_insert(StageTimeseriesInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection,
                    payload,
                    surrogates,
                });
            }
            PhysicalPlan::Timeseries(TimeseriesOp::Scan { .. }) => {
                return self.stage_not_point_write(task);
            }
            PhysicalPlan::Text(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_) => return self.stage_not_point_write(task),
        };

        match doc_op {
            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_insert(&ctx, value, *if_absent)
            }
            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_put(&ctx, value)
            }
            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_delete(&ctx)
            }
            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                updates,
                ..
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_point_update(&ctx, updates)
            }
            // Predicate UPDATE staged at statement time — same treatment as a
            // point update, resolved against the BASE ∪ OVERLAY matching set.
            // A `RETURNING` clause does not change staging: the matched rows'
            // post-images are staged identically; the clause only governs the
            // client response shape, which the in-transaction path renders as
            // an affected-count tag either way.
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning: _,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
            } => self.stage_bulk_update(StageBulkUpdateParams {
                task,
                tid,
                txn_id,
                collection,
                filter_bytes: filters,
                updates,
            }),

            // Predicate DELETE staged at statement time — same treatment as a
            // point delete, resolved against the BASE ∪ OVERLAY matching set.
            // As with `BulkUpdate`, a `RETURNING` clause does not change what
            // is staged.
            DocumentOp::BulkDelete {
                collection,
                filters,
                returning: _,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
            } => self.stage_bulk_delete(StageBulkDeleteParams {
                task,
                tid,
                txn_id,
                collection,
                filter_bytes: filters,
            }),

            // `UPSERT INTO` staged at statement time -- resolve the current
            // body under BASE ∪ OVERLAY and either insert or merge/apply
            // `ON CONFLICT DO UPDATE SET`, mirroring the autocommit
            // `execute_upsert` handler exactly. `Upsert` has no `RETURNING`
            // variant, so it is always stageable (see `is_point_write`).
            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
            } => {
                let ctx = StageCtx::new(task, tid, txn_id, collection, document_id, *surrogate);
                self.stage_document_upsert(&ctx, value, on_conflict_updates)
            }

            // `INSERT ... SELECT` is resolved + staged as concrete
            // fresh-surrogate `PointInsert` ops at STATEMENT time on the
            // Control Plane (`session::expander_stage` →
            // `resolve_and_emit_insert_select_ops`); a raw `InsertSelect`
            // plan never reaches `StageWrite`, so it is not a point write here.
            DocumentOp::InsertSelect { .. }
            | DocumentOp::PointGet { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::MaterializeScan { .. } => self.stage_not_point_write(task),
        }
    }

    pub(super) fn stage_not_point_write(&self, task: &ExecutionTask) -> Response {
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "StageWrite is only valid for point-write document operations".into(),
            },
        )
    }

    // ── Shared helpers ──────────────────────────────────────────────────────
    //
    // The Document point-write staging methods (`stage_point_insert` /
    // `stage_point_put` / `stage_point_delete` / `stage_point_update`) live in
    // the sibling `stage_point_document` module; they call the `pub(super)`
    // helpers below.

    pub(super) fn stage_overlay_pk(&self, ctx: &StageCtx<'_>) -> OverlayPk {
        match self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
        {
            Some(Staged::Put(_)) => OverlayPk::Present,
            Some(Staged::Tombstone) => OverlayPk::Absent,
            None => OverlayPk::Unstaged,
        }
    }

    /// Stage a put after enforcing the per-transaction overlay memory cap.
    pub(super) fn stage_put_capped(
        &mut self,
        ctx: &StageCtx<'_>,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        let current = self
            .txn_overlays
            .get(&ctx.txn_id)
            .map(|o| o.memory_size_estimate())
            .unwrap_or(0);
        if current.saturating_add(body.len()) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        self.txn_overlay_mut(ctx.txn_id).insert_put(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
            body,
        );
        Ok(())
    }

    pub(super) fn stage_count_response(&self, task: &ExecutionTask, affected: usize) -> Response {
        match response_codec::encode_count("affected", affected) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}
