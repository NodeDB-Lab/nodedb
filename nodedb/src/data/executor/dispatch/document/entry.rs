// SPDX-License-Identifier: BUSL-1.1

//! The document dispatch entry: the engine-pressure gate, the history
//! recharge, and the match that routes each op to its handler.

use nodedb_physical::physical_plan::DocumentOp;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::dispatch::document_admit::{
    DOCUMENT_WRITE_ENGINES, changes_document_history, is_document_write,
};
use crate::data::executor::handlers::bulk_dml::{
    BulkDeleteParams, BulkUpdateParams, OllpPrediction,
};
use crate::data::executor::handlers::write_match_cut::autocommit_match_cut;
use crate::data::executor::handlers::{document, truncate, upsert};
use crate::data::executor::task::ExecutionTask;

/// The error for an op that reached a dispatch group it does not belong to.
/// `group` names the group.
pub(super) fn misrouted(group: &str) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!("document dispatch routed an op that is not {group} to {group}'s handler"),
    }
}

impl CoreLoop {
    /// Dispatch one document op. An op that changes stored rows or index
    /// entries then recharges its tenant's document history to the engine
    /// budget, so the write gate of the next write sees the history this one
    /// added.
    pub(in crate::data::executor::dispatch) fn dispatch_document(
        &mut self,
        task: &ExecutionTask,
        op: &DocumentOp,
    ) -> Response {
        let response = self.dispatch_document_op(task, op);
        if changes_document_history(op)
            && let Err(e) =
                self.recharge_document_history(task.request.database_id, task.request.tenant_id)
        {
            // The write landed. Its response stands, and the charge keeps
            // its prior value until the next write recharges it.
            crate::diag::history_charge_stale(&e, "document write");
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "document history recharge failed; the prior charge stays"
            );
        }
        response
    }

    fn dispatch_document_op(&mut self, task: &ExecutionTask, op: &DocumentOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        // Pressure guard for write operations.
        if is_document_write(op) {
            for engine in DOCUMENT_WRITE_ENGINES {
                if let Some(r) = self.check_engine_pressure(task, engine) {
                    return r;
                }
            }
        }
        match op {
            DocumentOp::PointGet { .. }
            | DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::PointUpdate { .. } => self.dispatch_document_point(task, tid, op),

            DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::MaterializeScan { .. } => self.dispatch_document_read(task, tid, op),

            DocumentOp::BatchInsert {
                collection,
                documents,
                if_absent,
                surrogates,
                returning,
                rls_filters,
                resolved_sum_targets,
                deferred_sum_targets,
            } => self.execute_document_batch_insert(
                task,
                document::write::DocumentBatchInsertParams {
                    tid,
                    collection: collection.as_str(),
                    documents,
                    surrogates,
                    if_absent: *if_absent,
                    returning: returning.as_ref(),
                    rls_filters,
                    resolved_sum_targets,
                    deferred_sum_targets,
                },
            ),

            DocumentOp::UpdateFromJoin { .. } => {
                self.dispatch_update_from_join(task, tid, op, false)
            }

            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                system_time,
            } => {
                if let Err(e) = autocommit_match_cut(system_time, "a predicate UPDATE") {
                    return self.response_error(task, e);
                }
                self.execute_bulk_update(
                    task,
                    tid,
                    BulkUpdateParams {
                        collection: collection.as_str(),
                        filter_bytes: filters,
                        updates,
                        returning: returning.as_ref(),
                        ollp_predicted_surrogates: ollp_predicted_surrogates.as_deref(),
                        ollp_predicted_edges: ollp_predicted_edges.as_deref(),
                        rls_filters,
                        rls_write_check,
                        resolved_sum_targets,
                        declared_primary_key: declared_primary_key.as_deref(),
                    },
                )
            }

            DocumentOp::BulkDelete {
                collection,
                filters,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                rls_filters,
                rls_write_check,
                resolved_sum_targets,
                declared_primary_key,
                system_time,
            } => {
                if let Err(e) = autocommit_match_cut(system_time, "a predicate DELETE") {
                    return self.response_error(task, e);
                }
                self.execute_bulk_delete(
                    task,
                    tid,
                    BulkDeleteParams {
                        collection: collection.as_str(),
                        filter_bytes: filters,
                        returning: returning.as_ref(),
                        rls_filters,
                        rls_write_check,
                        resolved_sum_targets,
                        ollp: OllpPrediction {
                            surrogates: ollp_predicted_surrogates.as_deref(),
                            edges: ollp_predicted_edges.as_deref(),
                        },
                        declared_primary_key: declared_primary_key.as_deref(),
                    },
                )
            }

            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
                resolved_sum_targets,
            } => self.execute_upsert(
                task,
                upsert::UpsertParams {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    value,
                    on_conflict_updates,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                    resolved_sum_targets,
                },
            ),

            DocumentOp::BatchUpsert { .. } => self.dispatch_upsert_batch(task, tid, op),

            DocumentOp::Truncate {
                collection,
                restart_identity: _,
                resolved_sum_targets,
                declared_primary_key,
            } => self.execute_truncate(
                task,
                tid,
                truncate::TruncateParams {
                    collection: collection.as_str(),
                    resolved_sum_targets,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),

            DocumentOp::InsertSelect { .. } => {
                // `INSERT ... SELECT` is resolved entirely on the Control Plane
                // (autocommit orchestrator or statement-time expander). Reaching
                // this arm is a routing bug, surfaced loudly, not silently mis-copied.
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "InsertSelect must be resolved on the Control Plane \
                                 (autocommit orchestrator / statement-time expander); \
                                 it must never reach Data-Plane dispatch"
                            .into(),
                    },
                )
            }

            DocumentOp::Register { .. } => self.dispatch_register(task, tid, op),

            DocumentOp::DropIndex { collection, field } => {
                self.execute_drop_document_index(task, tid, collection.as_str(), field)
            }

            DocumentOp::Merge { .. } => self.dispatch_merge(task, tid, op, false),

            DocumentOp::ResolveWrite(inner) => {
                self.dispatch_document_resolve_write(task, tid, inner)
            }

            DocumentOp::ResolvedWrite {
                mutations,
                response_payload,
                rls_write_check,
            } => self.execute_document_resolved_write(
                task,
                tid,
                mutations,
                response_payload,
                rls_write_check,
            ),

            DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
            } => self.execute_backfill_index(
                task,
                document::index_maintenance::BackfillIndexParams {
                    tid,
                    collection: collection.as_str(),
                    path,
                    is_array: *is_array,
                    unique: *unique,
                    case_insensitive: *case_insensitive,
                    predicate: predicate.as_deref(),
                },
            ),

            DocumentOp::ApplyBalanceDelta {
                collection,
                document_id,
                surrogate,
                column,
                delta,
                join_column,
                join_value,
                declared_primary_key,
            } => self.execute_apply_balance_delta(
                task,
                document::apply_balance_delta::ApplyBalanceDeltaParams {
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    column,
                    delta,
                    join_column,
                    join_value,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),
        }
    }
}
