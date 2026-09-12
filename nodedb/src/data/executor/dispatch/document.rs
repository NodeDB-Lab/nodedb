// SPDX-License-Identifier: BUSL-1.1

//! Document operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::SystemTimeScope;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::document_admit::{doc_scan_mode, is_document_write};

impl CoreLoop {
    pub(super) fn dispatch_document(&mut self, task: &ExecutionTask, op: &DocumentOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        // Pressure guard for write operations.
        if is_document_write(op) {
            if let Some(r) =
                self.check_engine_pressure(task, nodedb_mem::EngineId::DocumentSchemaless)
            {
                return r;
            }
            // FTS indexing is a side effect of every document write.
            if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Fts) {
                return r;
            }
        }
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
                let system_as_of_ms = match system_time {
                    SystemTimeScope::Current => None,
                    SystemTimeScope::AsOf(ms) => Some(*ms),
                    SystemTimeScope::AllVersions => {
                        return self.response_error(
                            task,
                            crate::bridge::envelope::ErrorCode::Unsupported {
                                detail: "AS OF SYSTEM TIME NULL (all-versions) is not \
                                         supported on point gets; use a table scan"
                                    .into(),
                            },
                        );
                    }
                };
                self.execute_point_get(
                    task,
                    super::super::handlers::point::get::PointGetParams {
                        tid,
                        collection: collection.as_str(),
                        document_id,
                        surrogate: *surrogate,
                        rls_filters,
                        system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
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
                crate::data::executor::handlers::point::put::PointPutExec {
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
            } => self.execute_point_insert(
                crate::data::executor::handlers::point::insert::PointInsertParams {
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
                },
            ),

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
                crate::data::executor::handlers::point::delete::PointDeleteExec {
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
                crate::data::executor::handlers::point::update::PointUpdateParams {
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

            DocumentOp::Scan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
                distinct,
                projection,
                computed_columns,
                window_functions,
                system_time,
                valid_at_ms,
                prefilter,
            } => {
                let mode = doc_scan_mode(system_time, *valid_at_ms);
                self.execute_document_scan(
                    task,
                    crate::data::executor::handlers::document::read::scan::DocumentScanParams {
                        tid,
                        collection: collection.as_str(),
                        limit: *limit,
                        offset: *offset,
                        sort_keys,
                        filters,
                        distinct: *distinct,
                        projection,
                        computed_columns_bytes: computed_columns,
                        window_functions_bytes: window_functions,
                        mode,
                        prefilter: prefilter.as_ref(),
                    },
                )
            }

            DocumentOp::BatchInsert {
                collection,
                documents,
                surrogates,
                returning,
                rls_filters,
                resolved_sum_targets,
                deferred_sum_targets,
            } => self.execute_document_batch_insert(
                task,
                crate::data::executor::handlers::document::write::DocumentBatchInsertParams {
                    tid,
                    collection: collection.as_str(),
                    documents,
                    surrogates,
                    returning: returning.as_ref(),
                    rls_filters,
                    resolved_sum_targets,
                    deferred_sum_targets,
                },
            ),

            DocumentOp::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
                rls_filters,
            } => self.execute_range_scan(
                task,
                super::super::handlers::control::snapshot::RangeScanArgs {
                    tid,
                    collection: collection.as_str(),
                    field: field.as_str(),
                    lower: lower.as_deref(),
                    upper: upper.as_deref(),
                    limit: *limit,
                    rls_filters,
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
            } => self.execute_bulk_update(
                task,
                tid,
                super::super::handlers::bulk_dml::BulkUpdateParams {
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
            ),

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
            } => self.execute_bulk_delete(
                task,
                tid,
                super::super::handlers::bulk_dml::BulkDeleteParams {
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    returning: returning.as_ref(),
                    rls_filters,
                    rls_write_check,
                    resolved_sum_targets,
                    ollp: crate::data::executor::handlers::bulk_dml::OllpPrediction {
                        surrogates: ollp_predicted_surrogates.as_deref(),
                        edges: ollp_predicted_edges.as_deref(),
                    },
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),

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
                crate::data::executor::handlers::upsert::UpsertParams {
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

            DocumentOp::Truncate {
                collection,
                restart_identity: _,
                resolved_sum_targets,
                declared_primary_key,
            } => self.execute_truncate(
                task,
                tid,
                super::super::handlers::truncate::TruncateParams {
                    collection: collection.as_str(),
                    resolved_sum_targets,
                    declared_primary_key: declared_primary_key.as_deref(),
                },
            ),

            DocumentOp::EstimateCount { collection, field } => {
                self.execute_estimate_count(task, tid, collection.as_str(), field)
            }

            DocumentOp::InsertSelect { .. } => {
                // `INSERT ... SELECT` is resolved entirely on the Control Plane
                // (autocommit orchestrator or statement-time expander). Reaching
                // this arm is a routing bug, surfaced loudly, not silently mis-copied.
                self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: "InsertSelect must be resolved on the Control Plane \
                                 (autocommit orchestrator / statement-time expander); \
                                 it must never reach Data-Plane dispatch"
                            .into(),
                    },
                )
            }

            DocumentOp::Register {
                collection,
                indexes,
                crdt_enabled,
                storage_mode,
                enforcement,
                bitemporal,
                conflict_policy,
                timeseries,
                vector_primary,
            } => self.execute_register_document_collection(
                task,
                super::super::handlers::document::write::RegisterDocumentCollectionParams {
                    tid,
                    collection: collection.as_str(),
                    indexes,
                    crdt_enabled: *crdt_enabled,
                    storage_mode,
                    enforcement,
                    bitemporal: *bitemporal,
                    conflict_policy: conflict_policy.as_deref(),
                    timeseries: timeseries.as_deref(),
                    vector_primary: vector_primary.as_deref(),
                },
            ),

            DocumentOp::IndexLookup {
                collection,
                path,
                value,
            } => self.execute_document_index_lookup(task, tid, collection.as_str(), path, value),

            DocumentOp::IndexedFetch {
                collection,
                path,
                value,
                filters,
                projection,
                limit,
                offset,
            } => self.execute_document_indexed_fetch(
                task,
                super::super::handlers::document::index_fetch::IndexedFetchParams {
                    tid,
                    collection: collection.as_str(),
                    path,
                    value,
                    filters,
                    projection,
                    limit: *limit,
                    offset: *offset,
                },
            ),

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
                super::super::handlers::document::index_maintenance::BackfillIndexParams {
                    tid,
                    collection: collection.as_str(),
                    path,
                    is_array: *is_array,
                    unique: *unique,
                    case_insensitive: *case_insensitive,
                    predicate: predicate.as_deref(),
                },
            ),

            DocumentOp::MaterializeScan {
                collection,
                cursor,
                count,
                system_as_of_ms,
            } => self.execute_document_materialize_scan(
                task,
                tid,
                collection.as_str(),
                cursor,
                *count,
                *system_as_of_ms,
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
                super::super::handlers::document::apply_balance_delta::ApplyBalanceDeltaParams {
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
