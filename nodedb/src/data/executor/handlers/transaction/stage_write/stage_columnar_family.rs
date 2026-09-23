// SPDX-License-Identifier: BUSL-1.1

//! `StageWrite` routing for the columnar-storage family (`ColumnarOp`,
//! `TimeseriesOp`): the columnar arm of `dispatch.rs`, exhaustive over each
//! op enum so a new write variant cannot silently become unstageable.

use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};

use super::{
    StageColumnarDeleteParams, StageColumnarInsertParams, StageColumnarResolvedDeleteParams,
    StageColumnarResolvedUpdateParams, StageColumnarUpdateParams, StageTimeseriesInsertParams,
};
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

impl CoreLoop {
    /// Stage a `ColumnarOp` write into `txn_id`'s overlay.
    pub(in crate::data::executor) fn execute_stage_columnar(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &ColumnarOp,
    ) -> Response {
        match op {
            ColumnarOp::Insert {
                collection,
                payload,
                surrogates,
                schema_bytes,
                intent,
                on_conflict_updates,
                rls_write_check,
                ..
            } => self.stage_columnar_insert(StageColumnarInsertParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                payload,
                surrogates,
                schema_bytes,
                intent: *intent,
                on_conflict_updates,
                rls_write_check,
            }),
            // Predicate DELETE/UPDATE staged at statement time: the affected
            // set resolves against base ∪ overlay; commit replay durably applies.
            ColumnarOp::Delete {
                collection,
                filters,
                rls_write_check,
            } => self.stage_columnar_delete(StageColumnarDeleteParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                filter_bytes: filters,
                rls_write_check,
            }),
            ColumnarOp::Update {
                collection,
                filters,
                updates,
                rls_write_check,
            } => self.stage_columnar_update(StageColumnarUpdateParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                filter_bytes: filters,
                updates,
                rls_write_check,
            }),
            // Resolved-row-set UPDATE/DELETE: staging locates each shipped PK
            // in base ∪ overlay rather than re-evaluating a predicate.
            ColumnarOp::ResolvedUpdate {
                collection,
                rows,
                rls_write_check,
            } => self.stage_columnar_resolved_update(StageColumnarResolvedUpdateParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                rows,
                rls_write_check,
            }),
            ColumnarOp::ResolvedDelete {
                collection,
                pks,
                rls_write_check: _,
            } => self.stage_columnar_resolved_delete(StageColumnarResolvedDeleteParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                pks,
            }),
            // An overlay marker hides every base row; COMMIT replays the
            // live truncate.
            ColumnarOp::Truncate {
                collection,
                restart_identity: _,
            } => self.stage_collection_truncate(task, tid, txn_id, collection.as_str()),
            ColumnarOp::Scan { .. }
            | ColumnarOp::MaterializeScan { .. }
            | ColumnarOp::ResolveDml { .. } => self.stage_not_point_write(task),
        }
    }

    /// Stage a `TimeseriesOp` write into `txn_id`'s overlay.
    pub(super) fn execute_stage_timeseries(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &TimeseriesOp,
    ) -> Response {
        match op {
            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                surrogates,
                rls_write_check,
                ..
            } => self.stage_timeseries_insert(StageTimeseriesInsertParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                payload,
                format,
                surrogates,
                rls_write_check,
            }),
            // An overlay marker hides every base row; COMMIT replays the
            // live truncate.
            TimeseriesOp::Truncate {
                collection,
                restart_identity: _,
            } => self.stage_collection_truncate(task, tid, txn_id, collection.as_str()),
            TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => {
                self.stage_not_point_write(task)
            }
        }
    }
}
