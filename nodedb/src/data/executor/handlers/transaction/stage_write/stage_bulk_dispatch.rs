// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging dispatch for a predicate `UPDATE` or `DELETE`
//! (`DocumentOp::BulkUpdate` / `BulkDelete`).
//!
//! The plan carries the prediction the Control Plane read at this statement,
//! and the scope it matched at: the block's snapshot, with the staged writes
//! on top.
//! The stage checks it against the rows it matches, and refuses with
//! `OllpRetryRequired` before staging anything when they differ.

use nodedb_physical::physical_plan::DocumentOp;

use super::stage_returning::StageReturning;
use super::{StageBulkDeleteParams, StageBulkUpdateParams};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::TxnPrediction;
use crate::data::executor::handlers::write_match_cut::write_match_cut;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

impl CoreLoop {
    /// Stage one predicate write. Any other document op is refused: it never
    /// routes here.
    pub(super) fn stage_bulk_document_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &DocumentOp,
    ) -> Response {
        match op {
            // Staged like a point update. A RETURNING spec projects the staged
            // post-images.
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_filters,
                rls_write_check,
                // Staged post-images become concrete point ops at commit.
                resolved_sum_targets: _,
                declared_primary_key,
                system_time,
            } => {
                let system_as_of = match write_match_cut(system_time, "a predicate UPDATE") {
                    Ok(cut) => cut,
                    Err(e) => return self.response_error(task, e),
                };
                self.stage_bulk_update(StageBulkUpdateParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    updates,
                    rls_write_check,
                    declared_primary_key: declared_primary_key.as_deref(),
                    returning: StageReturning::of(returning, rls_filters),
                    prediction: TxnPrediction::of(ollp_predicted_surrogates, ollp_predicted_edges),
                    system_as_of,
                })
            }
            // Staged like a point delete. A RETURNING spec projects the
            // removed rows.
            DocumentOp::BulkDelete {
                collection,
                filters,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_filters,
                rls_write_check,
                // See the `BulkUpdate` arm: staging carries no delta.
                resolved_sum_targets: _,
                declared_primary_key,
                system_time,
            } => {
                let system_as_of = match write_match_cut(system_time, "a predicate DELETE") {
                    Ok(cut) => cut,
                    Err(e) => return self.response_error(task, e),
                };
                self.stage_bulk_delete(StageBulkDeleteParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    rls_write_check,
                    declared_primary_key: declared_primary_key.as_deref(),
                    returning: StageReturning::of(returning, rls_filters),
                    prediction: TxnPrediction::of(ollp_predicted_surrogates, ollp_predicted_edges),
                    system_as_of,
                })
            }
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
            | DocumentOp::ApplyBalanceDelta { .. } => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "a document op that is not a predicate write reached predicate \
                             staging"
                        .into(),
                },
            ),
        }
    }
}
