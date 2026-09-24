// SPDX-License-Identifier: BUSL-1.1

//! Stage a Calvin write plan into the shared per-core `txn_overlays` (and
//! `graph_txn_overlays` / `array_txn_overlays`), keyed by a synthetic `TxnId`
//! (see `calvin_txn_id.rs`).
//!
//! Staging is the producer side for `CalvinResolve`, which reads the overlay
//! the same way `MetaOp::ResolveTxn` does for session transactions
//! (`resolve/entry.rs`). The redo record it builds is what the flush
//! installs.

use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, PhysicalPlan, TimeseriesOp, VectorOp,
};
use nodedb_types::RowIdentity;

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::stage_write::{
    StageBalanceDeltaParams, StageBatchInsertParams, StageCtx, StageTimeseriesInsertParams,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

use super::calvin_txn_id::calvin_synthetic_txn_id;

impl CoreLoop {
    /// Stage one Calvin write plan into the transaction overlay under the
    /// synthetic `txn_id`, reusing the statement-time staging handlers a
    /// session `BEGIN..COMMIT` write uses, and return the handler's reply.
    ///
    /// Staged here: the Document point family (`PointInsert` / `PointPut` /
    /// `PointDelete` / `PointUpdate` / `Upsert`), `BatchInsert`,
    /// `ApplyBalanceDelta`, `Truncate`, KV ops, GRAPH edge/label ops,
    /// `TimeseriesOp::Ingest`, every columnar write, the vector-primary direct
    /// writes, CRDT row writes, and ARRAY cell writes. `MERGE` /
    /// `UPDATE ... FROM` / `INSERT ... SELECT` are resolved into point writes
    /// on the Control Plane before dispatch, so one reaching here is refused.
    ///
    /// The remaining write families stage nothing because resolve serializes
    /// them from the plan node itself: the other vector writes, CRDT deltas,
    /// FTS and spatial writes, timeseries truncates and array flushes.
    ///
    /// `DocumentOp::BulkUpdate` / `BulkDelete` (predicate DML) are staged via
    /// the predicted-surrogate-set primitives in
    /// [`calvin_overlay_stage_bulk`][super::calvin_overlay_stage_bulk] rather
    /// than a live predicate rescan. See that module's docs for the
    /// determinism rationale. Staging runs under the epoch's time anchor, so
    /// every replica stages the same images. Spatial and text writes carry
    /// their absolute post-image on the plan node, stay unstaged, and answer
    /// an empty reply.
    pub(in crate::data::executor) fn stage_calvin_overlay(
        &mut self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
    ) -> Result<Vec<u8>, ErrorCode> {
        let tid = tenant_id.as_u64();
        match plan {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
                ..
            }) => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                let resp = self.stage_point_insert(&ctx, value, *if_absent);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            }) => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                let resp = self.stage_point_put(&ctx, value);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                rls_write_check,
                ..
            }) => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                let resp = self.stage_point_delete(&ctx, rls_write_check);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                updates,
                rls_write_check,
                declared_primary_key,
                ..
            }) => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                let resp = self.stage_point_update(
                    &ctx,
                    updates,
                    rls_write_check,
                    declared_primary_key.as_deref(),
                );
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
                rls_write_check,
                ..
            }) => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                let resp =
                    self.stage_document_upsert(&ctx, value, on_conflict_updates, rls_write_check);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection,
                ollp_predicted_surrogates,
                rls_write_check,
                declared_primary_key,
                ..
            }) => self
                .stage_calvin_bulk_delete(super::calvin_overlay_stage_bulk::CalvinBulkDeleteStage {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    ollp_predicted_surrogates: ollp_predicted_surrogates.as_deref(),
                    rls_write_check,
                    declared_primary_key: declared_primary_key.as_deref(),
                })
                .and_then(affected_reply)
                .map_err(ErrorCode::from),
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection,
                updates,
                ollp_predicted_surrogates,
                rls_write_check,
                declared_primary_key,
                ..
            }) => self
                .stage_calvin_bulk_update(super::calvin_overlay_stage_bulk::CalvinBulkUpdateStage {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    updates,
                    ollp_predicted_surrogates: ollp_predicted_surrogates.as_deref(),
                    rls_write_check,
                    declared_primary_key: declared_primary_key.as_deref(),
                })
                .and_then(affected_reply)
                .map_err(ErrorCode::from),
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection,
                documents,
                surrogates,
                ..
            }) => {
                let resp = self.stage_document_batch_insert(StageBatchInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    documents,
                    surrogates,
                });
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
                collection,
                document_id,
                surrogate,
                column,
                delta,
                join_column,
                join_value,
                declared_primary_key,
            }) => {
                let resp = self.stage_apply_balance_delta(StageBalanceDeltaParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    column,
                    delta,
                    join_column,
                    join_value,
                    declared_primary_key: declared_primary_key.as_deref(),
                });
                Self::stage_result(&resp)
            }
            PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
                let resp = self.stage_collection_truncate(task, tid, txn_id, collection.as_str());
                Self::stage_result(&resp)
            }
            // The Control Plane resolves these into point writes, or proposes
            // them through Raft, before any Calvin dispatch. Staging one would
            // install nothing, so it is refused.
            PhysicalPlan::Document(
                DocumentOp::InsertSelect { .. }
                | DocumentOp::UpdateFromJoin { .. }
                | DocumentOp::Merge { .. }
                | DocumentOp::ResolvedWrite { .. },
            ) => Err(ErrorCode::Internal {
                detail: "a cross-collection or pre-resolved document write reached Calvin \
                          staging; the Control Plane resolves it before dispatch"
                    .into(),
            }),
            PhysicalPlan::Kv(op) => {
                let resp = self.execute_stage_kv(task, tid, txn_id, op);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                surrogates,
                rls_write_check,
                ..
            }) => {
                let response = self.stage_timeseries_insert(StageTimeseriesInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    payload,
                    format,
                    surrogates,
                    rls_write_check,
                });
                Self::stage_result(&response)
            }
            // Every columnar write stages its post-image the way a session
            // write does, so the transaction's redo carries the rows the
            // flush installs: an ON CONFLICT merge and a predicate DML's
            // matched rows are resolved once, here, against the state this
            // position observes.
            PhysicalPlan::Columnar(
                op @ (ColumnarOp::Insert { .. }
                | ColumnarOp::Update { .. }
                | ColumnarOp::Delete { .. }
                | ColumnarOp::ResolvedUpdate { .. }
                | ColumnarOp::ResolvedDelete { .. }
                | ColumnarOp::Truncate { .. }),
            ) => {
                let resp = self.execute_stage_columnar(task, tid, txn_id, op);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Graph(
                op @ (GraphOp::EdgePut { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }),
            ) => {
                let resp = self.execute_stage_graph(task, tid, txn_id, op);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Vector(
                op @ (VectorOp::DirectInsert { .. }
                | VectorOp::DirectInsertIfAbsent { .. }
                | VectorOp::DirectUpsert { .. }
                | VectorOp::DirectUpdate { .. }
                | VectorOp::DirectDelete { .. }
                | VectorOp::DirectTruncate { .. }),
            ) => {
                let resp = self.execute_stage_vector(task, tid, txn_id, op);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Crdt(op @ (CrdtOp::DocUpsert { .. } | CrdtOp::DocDelete { .. })) => {
                let resp = self.execute_stage_crdt(task, tid, txn_id, op);
                Self::stage_result(&resp)
            }
            PhysicalPlan::Array(op @ (ArrayOp::Put { .. } | ArrayOp::Delete { .. })) => {
                let resp = self.execute_stage_array(task, txn_id, op);
                Self::stage_result(&resp)
            }
            // Document reads and index DDL write no row.
            PhysicalPlan::Document(
                DocumentOp::ResolveWrite(_)
                | DocumentOp::PointGet { .. }
                | DocumentOp::Scan { .. }
                | DocumentOp::RangeScan { .. }
                | DocumentOp::IndexLookup { .. }
                | DocumentOp::IndexedFetch { .. }
                | DocumentOp::EstimateCount { .. }
                | DocumentOp::MaterializeScan { .. }
                | DocumentOp::Register { .. }
                | DocumentOp::DropIndex { .. }
                | DocumentOp::BackfillIndex { .. },
            ) => Ok(Vec::new()),
            // Resolve serializes these writes from the plan node, and the
            // rest of these families write nothing.
            PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => Ok(Vec::new()),
        }
    }

    /// Turn a staging handler's `Response` into its reply, so a staging
    /// failure propagates loudly to the Calvin caller instead of being
    /// silently swallowed.
    fn stage_result(resp: &Response) -> Result<Vec<u8>, ErrorCode> {
        if resp.status == Status::Error {
            return Err(resp.error_code.as_deref().cloned().unwrap_or_else(|| {
                ErrorCode::Internal {
                    detail: "calvin overlay staging failed without an error code".into(),
                }
            }));
        }
        Ok(resp.payload.as_bytes().to_vec())
    }

    /// Discard the synthetic-`TxnId` overlay entries staged for
    /// `(epoch, position, vshard)`, if any -- `txn_overlays` (Document /
    /// KV), `graph_txn_overlays` (GRAPH edge/label ops route into their
    /// own parallel overlay, same as a session transaction's
    /// `execute_stage_graph`), and `array_txn_overlays` (ARRAY cell ops,
    /// same as `execute_stage_array`). Called from both
    /// [`CoreLoop::execute_calvin_flush`] and [`CoreLoop::execute_calvin_drop`]
    /// so neither overlay outlives the `commit_pending` entry it shadows.
    /// Idempotent: a missing key (already removed, or the id derivation
    /// itself failing) is a silent no-op — the same shape as the
    /// `commit_pending` removal it accompanies.
    ///
    /// Runs the same teardown as `MetaOp::DropTxnOverlay`
    /// (`drop_overlay_entry`), so the `active_txn_overlays` gauge stays exact.
    pub(in crate::data::executor) fn drop_calvin_synthetic_overlay(
        &mut self,
        epoch: u64,
        position: u32,
        vshard: u32,
    ) {
        if let Ok(synthetic_txn_id) = calvin_synthetic_txn_id(epoch, position, vshard) {
            // The shared teardown also drops a columnar engine staging
            // auto-created that the flush left empty.
            self.drop_overlay_entry(synthetic_txn_id);
        }
    }
}

/// The `{"affected": n}` reply of a Calvin bulk stage.
fn affected_reply(affected: usize) -> crate::Result<Vec<u8>> {
    response_codec::encode_count("affected", affected)
}
