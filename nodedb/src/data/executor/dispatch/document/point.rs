// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for the single-row document ops: point get, put, insert, delete,
//! and update.

use nodedb_physical::physical_plan::DocumentOp;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::dispatch::document_admit::{
    all_versions_refused, merges_overlay, point_cut,
};
use crate::data::executor::handlers::point;
use crate::data::executor::task::ExecutionTask;

use super::entry::misrouted;

impl CoreLoop {
    /// Dispatch one point op. Any other op is a routing error.
    pub(super) fn dispatch_document_point(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
    ) -> Response {
        match op {
            DocumentOp::PointGet {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                rls_filters,
                system_time,
                valid_at_ms,
            } => {
                let Some(system_as_of) = point_cut(system_time) else {
                    return self.response_error(task, all_versions_refused("point gets"));
                };
                self.execute_point_get(
                    task,
                    point::get::PointGetParams {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        rls_filters,
                        system_as_of,
                        valid_at_ms: *valid_at_ms,
                        merge_overlay: merges_overlay(system_time, *valid_at_ms),
                    },
                )
            }

            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                pk_bytes: _,
                returning,
                rls_filters,
                resolved_sum_targets,
            } => self.execute_point_put(
                task,
                point::put::PointPutExec {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    value,
                    returning: returning.as_ref(),
                    rls_filters,
                    resolved_sum_targets,
                },
            ),

            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
                returning,
                rls_filters,
                resolved_sum_targets,
                deferred_sum_targets,
            } => self.execute_point_insert(point::insert::PointInsertParams {
                task,
                tid,
                collection: collection.as_str(),
                document_id,
                surrogate: *surrogate,
                value,
                if_absent: *if_absent,
                returning: returning.as_ref(),
                rls_filters,
                resolved_sum_targets,
                deferred_sum_targets,
            }),

            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                ..
            } => self.execute_point_delete(
                task,
                point::delete::PointDeleteExec {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                },
            ),

            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                updates,
                returning,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
            } => self.execute_point_update(
                task,
                point::update::PointUpdateParams {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    updates,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),

            DocumentOp::Scan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => {
                self.response_error(task, misrouted("a point op"))
            }
        }
    }
}
