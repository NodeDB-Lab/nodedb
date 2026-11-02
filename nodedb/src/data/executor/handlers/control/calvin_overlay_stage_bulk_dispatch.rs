// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of one Calvin predicate plan (`DocumentOp::BulkDelete` /
//! `BulkUpdate`) to its stage in
//! [`calvin_overlay_stage_bulk`][super::calvin_overlay_stage_bulk], with the
//! prediction the plan carries.

use nodedb_physical::physical_plan::DocumentOp;

use super::calvin_overlay_stage_bulk::{
    CalvinBulkCheck, CalvinBulkDeleteStage, CalvinBulkUpdateStage,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::TxnPrediction;
use crate::data::executor::handlers::write_match_cut::write_match_cut;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

impl CoreLoop {
    /// Stage one Calvin predicate plan and answer its `{"affected": n}`
    /// reply. Any other document op is refused: it never routes here.
    pub(in crate::data::executor) fn stage_calvin_bulk_plan(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &DocumentOp,
    ) -> Result<Vec<u8>, ErrorCode> {
        let staged = match op {
            DocumentOp::BulkDelete {
                collection,
                filters,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                system_time,
                ..
            } => self.stage_calvin_bulk_delete(CalvinBulkDeleteStage {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                check: CalvinBulkCheck {
                    filters,
                    system_as_of: write_match_cut(system_time, "a Calvin predicate DELETE")?,
                    prediction: TxnPrediction::of(ollp_predicted_surrogates, ollp_predicted_edges),
                    resolved_sum_targets,
                },
                rls_write_check,
                declared_primary_key: declared_primary_key.as_deref(),
            }),
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                system_time,
                ..
            } => self.stage_calvin_bulk_update(CalvinBulkUpdateStage {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                updates,
                check: CalvinBulkCheck {
                    filters,
                    system_as_of: write_match_cut(system_time, "a Calvin predicate UPDATE")?,
                    prediction: TxnPrediction::of(ollp_predicted_surrogates, ollp_predicted_edges),
                    resolved_sum_targets,
                },
                rls_write_check,
                declared_primary_key: declared_primary_key.as_deref(),
            }),
            DocumentOp::PointGet { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => {
                return Err(ErrorCode::Internal {
                    detail: "a document op that is not a predicate write reached Calvin \
                             predicate staging"
                        .into(),
                });
            }
        };
        staged
            .and_then(|affected| response_codec::encode_count("affected", affected))
            .map_err(ErrorCode::from)
    }
}
