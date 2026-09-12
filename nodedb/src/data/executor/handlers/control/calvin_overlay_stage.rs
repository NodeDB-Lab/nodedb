// SPDX-License-Identifier: BUSL-1.1

//! Stage a Calvin static-execute write plan into the shared per-core
//! `txn_overlays` (and `graph_txn_overlays`), keyed by a synthetic `TxnId`
//! (see `calvin_txn_id.rs`).
//!
//! This is purely additive to
//! [`CoreLoop::execute_calvin_execute_static`]'s existing `commit_pending`
//! raw-plan buffering, which remains untouched and still drives the base
//! install at flush time. Staging here is the producer side for a later
//! `CalvinResolve` op that reads the overlay the same way
//! `MetaOp::ResolveTxn` already does for session transactions
//! (`resolve/entry.rs`).

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan, TimeseriesOp};
use nodedb_types::RowIdentity;

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::stage_write::{
    StageCtx, StageTimeseriesInsertParams,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

use super::calvin_txn_id::calvin_synthetic_txn_id;

impl CoreLoop {
    /// Stage one Calvin write plan into the transaction overlay under the
    /// synthetic `txn_id`, reusing the exact same statement-time staging
    /// handlers a session `BEGIN..COMMIT` point write uses.
    ///
    /// Concrete, surrogate-carrying point ops are staged here: the Document
    /// point family (`PointInsert` / `PointPut` / `PointDelete` /
    /// `PointUpdate` / `Upsert`), KV point ops, and GRAPH edge/label ops.
    /// These are also the only op shapes that reach Calvin buffering for the
    /// Document family in the first place — `MERGE` / `UPDATE ... FROM` /
    /// `INSERT ... SELECT` are already expanded to concrete point ops at
    /// statement time before Calvin buffering (`commit.rs`).
    ///
    /// `DocumentOp::BulkUpdate` / `BulkDelete` (predicate DML) are also
    /// staged, but via the predicted-surrogate-set primitives in
    /// [`calvin_overlay_stage_bulk`][super::calvin_overlay_stage_bulk] rather
    /// than a live predicate rescan — see that module's docs for the
    /// determinism rationale. `TimeseriesOp::Ingest` is staged through the
    /// same canonical row decoder as session writes; its per-row tokens are
    /// overlay-local and never become base-storage identities. Columnar and
    /// spatial predicate writes remain unstaged because they have no
    /// deterministic post-image overlay representation.
    pub(in crate::data::executor) fn stage_calvin_overlay(
        &mut self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
    ) -> Result<(), ErrorCode> {
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
                .map_err(ErrorCode::from),
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
            _ => Ok(()),
        }
    }

    /// Turn a staging handler's `Response` into a `Result`, so a staging
    /// failure propagates loudly to the Calvin caller instead of being
    /// silently swallowed.
    fn stage_result(resp: &Response) -> Result<(), ErrorCode> {
        if resp.status == Status::Error {
            return Err(resp.error_code.as_deref().cloned().unwrap_or_else(|| {
                ErrorCode::Internal {
                    detail: "calvin overlay staging failed without an error code".into(),
                }
            }));
        }
        Ok(())
    }

    /// Discard the synthetic-`TxnId` overlay entries staged for
    /// `(epoch, position, vshard)`, if any -- both `txn_overlays` (Document /
    /// KV) and `graph_txn_overlays` (GRAPH edge/label ops route into their
    /// own parallel overlay, same as a session transaction's
    /// `execute_stage_graph`). Called from both
    /// [`CoreLoop::execute_calvin_flush`] and [`CoreLoop::execute_calvin_drop`]
    /// so neither overlay outlives the `commit_pending` entry it shadows.
    /// Idempotent: a missing key (already removed, or the id derivation
    /// itself failing) is a silent no-op — the same shape as the
    /// `commit_pending` removal it accompanies.
    ///
    /// Mirrors `MetaOp::DropTxnOverlay`'s gauge accounting exactly: both maps
    /// were populated (if at all) via the `txn_overlay_mut` /
    /// `graph_txn_overlay_mut` choke points, which bump `active_txn_overlays`
    /// on first creation, so removal here must decrement by the same count
    /// or the gauge drifts upward forever on every Calvin-staged transaction.
    pub(in crate::data::executor) fn drop_calvin_synthetic_overlay(
        &mut self,
        epoch: u64,
        position: u32,
        vshard: u32,
    ) {
        if let Ok(synthetic_txn_id) = calvin_synthetic_txn_id(epoch, position, vshard) {
            let removed = u64::from(self.txn_overlays.remove(&synthetic_txn_id).is_some())
                + u64::from(self.graph_txn_overlays.remove(&synthetic_txn_id).is_some());
            if removed > 0
                && let Some(m) = &self.metrics
            {
                m.active_txn_overlays
                    .fetch_sub(removed, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}
