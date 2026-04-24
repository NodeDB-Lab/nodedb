//! Dispatch for Kv, Meta, Query, Columnar, Timeseries, and Spatial operations.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::{
    ColumnarOp, MetaOp, PhysicalPlan, QueryOp, SpatialOp, TimeseriesOp,
};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::join::{
    BroadcastJoinParams, HashJoinParams, InlineHashJoinParams, JoinParams,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    #[allow(clippy::too_many_lines)]
    pub(super) fn dispatch_other(&mut self, task: &ExecutionTask, plan: &PhysicalPlan) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match plan {
            PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
            }) => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                having,
                *limit,
                sub_group_by,
                sub_aggregates,
            ),

            PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                post_filters,
                inline_left,
                inline_right,
                ..
            }) => self.execute_hash_join(HashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                tid,
                left_collection,
                right_collection,
                left_alias: left_alias.as_deref(),
                right_alias: right_alias.as_deref(),
                inline_left: inline_left.as_deref(),
                inline_right: inline_right.as_deref(),
            }),

            PhysicalPlan::Query(QueryOp::InlineHashJoin {
                left_data,
                right_data,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                post_filters,
            }) => self.execute_inline_hash_join(InlineHashJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                left_data,
                right_data,
                right_alias: right_alias.as_deref(),
            }),

            PhysicalPlan::Meta(MetaOp::WalAppend { payload }) => {
                self.execute_wal_append(task, payload)
            }

            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                self.execute_cancel(task, *target_request_id)
            }

            PhysicalPlan::Query(QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                condition,
                join_type,
                limit,
            }) => self.execute_nested_loop_join(
                task,
                tid,
                left_collection,
                right_collection,
                condition,
                join_type,
                *limit,
            ),

            PhysicalPlan::Query(QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                pre_sorted,
            }) => self.execute_sort_merge_join(
                task,
                tid,
                left_collection,
                right_collection,
                on,
                join_type,
                *limit,
                *pre_sorted,
            ),

            PhysicalPlan::Query(QueryOp::RecursiveScan {
                collection,
                base_filters,
                recursive_filters,
                join_link,
                max_iterations,
                distinct,
                limit,
            }) => self.execute_recursive_scan(
                task,
                tid,
                collection,
                base_filters,
                recursive_filters,
                join_link.as_ref(),
                *max_iterations,
                *distinct,
                *limit,
            ),

            PhysicalPlan::Meta(MetaOp::TransactionBatch { plans }) => {
                self.execute_transaction_batch(task, tid, plans)
            }

            PhysicalPlan::Query(QueryOp::FacetCounts {
                collection,
                filters,
                fields,
                limit_per_facet,
            }) => {
                self.execute_facet_counts(task, tid, collection, filters, fields, *limit_per_facet)
            }

            PhysicalPlan::Query(QueryOp::PartialAggregate {
                collection,
                group_by,
                aggregates,
                filters,
            }) => self.execute_aggregate(
                task,
                tid,
                collection,
                group_by,
                aggregates,
                filters,
                &[],
                usize::MAX,
                &[],
                &[],
            ),

            PhysicalPlan::Query(QueryOp::BroadcastJoin {
                large_collection,
                small_collection,
                large_alias,
                small_alias,
                broadcast_data,
                on,
                join_type,
                limit,
                projection,
                post_filters,
                ..
            }) => self.execute_broadcast_join(BroadcastJoinParams {
                join: JoinParams {
                    task,
                    on,
                    join_type,
                    limit: *limit,
                    projection,
                    post_filter_bytes: post_filters,
                },
                tid,
                large_collection,
                small_collection,
                large_alias: large_alias.as_deref(),
                small_alias: small_alias.as_deref(),
                broadcast_data,
            }),

            PhysicalPlan::Query(QueryOp::ShuffleJoin {
                left_collection,
                right_collection,
                on,
                join_type,
                limit,
                ..
            }) => {
                // ShuffleJoin executes as a local hash join on the target core.
                self.execute_hash_join(HashJoinParams {
                    join: JoinParams {
                        task,
                        on,
                        join_type,
                        limit: *limit,
                        projection: &[],
                        post_filter_bytes: &[],
                    },
                    tid,
                    left_collection,
                    right_collection,
                    left_alias: None,
                    right_alias: None,
                    inline_left: None,
                    inline_right: None,
                })
            }

            PhysicalPlan::Meta(MetaOp::CreateSnapshot) => self.execute_create_snapshot(task),

            PhysicalPlan::Meta(MetaOp::Compact) => self.execute_compact(task),

            PhysicalPlan::Meta(MetaOp::Checkpoint) => self.execute_checkpoint(task),

            PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate { def }) => {
                self.continuous_agg_mgr.register(def.clone());
                tracing::info!(
                    name = def.name,
                    source = def.source,
                    interval = def.bucket_interval,
                    "continuous aggregate registered"
                );
                self.response_ok(task)
            }

            PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name }) => {
                self.continuous_agg_mgr.unregister(name);
                tracing::info!(name, "continuous aggregate unregistered");
                self.response_ok(task)
            }

            PhysicalPlan::Meta(MetaOp::ListContinuousAggregates) => {
                let infos = self.continuous_agg_mgr.list_aggregates();
                match response_codec::encode_serde(&infos) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { tenant_id }) => {
                self.execute_create_tenant_snapshot(task, *tenant_id)
            }

            PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
                tenant_id,
                snapshot,
            }) => self.execute_restore_tenant_snapshot(task, *tenant_id, snapshot),

            PhysicalPlan::Meta(MetaOp::ConvertCollection {
                collection,
                target_type,
                schema_json,
            }) => self.execute_convert_collection(task, tid, collection, target_type, schema_json),

            PhysicalPlan::Meta(MetaOp::PurgeTenant { tenant_id }) => {
                self.execute_purge_tenant(task, *tenant_id)
            }

            PhysicalPlan::Meta(MetaOp::UnregisterCollection {
                tenant_id,
                name,
                purge_lsn,
            }) => self.execute_unregister_collection(task, *tenant_id, name, *purge_lsn),

            PhysicalPlan::Meta(MetaOp::UnregisterMaterializedView { tenant_id, name }) => {
                self.execute_unregister_materialized_view(task, *tenant_id, name)
            }

            PhysicalPlan::Meta(MetaOp::QueryCollectionSize { tenant_id, name }) => {
                self.execute_query_collection_size(task, *tenant_id, name)
            }

            // Retention / purge / continuous-agg / last-value bodies live in
            // `dispatch/meta_retention/` to keep this file within the size
            // budget; the arms below are one-line delegations so the Meta
            // match stays exhaustive.
            PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                collection,
                max_age_ms,
            }) => self.meta_enforce_timeseries_retention(task, collection, *max_age_ms),
            PhysicalPlan::Meta(MetaOp::ApplyContinuousAggRetention) => {
                self.meta_apply_continuous_agg_retention(task)
            }
            PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark { aggregate_name }) => {
                self.meta_query_aggregate_watermark(task, aggregate_name)
            }
            PhysicalPlan::Meta(MetaOp::QueryLastValues { collection }) => {
                self.meta_query_last_values(task, collection)
            }
            PhysicalPlan::Meta(MetaOp::QueryLastValue {
                collection,
                series_id,
            }) => self.meta_query_last_value(task, collection, *series_id),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeEdgeStore {
                tenant_id,
                collection,
                cutoff_system_ms,
            }) => {
                self.meta_temporal_purge_edge_store(task, *tenant_id, collection, *cutoff_system_ms)
            }
            PhysicalPlan::Meta(MetaOp::TemporalPurgeDocumentStrict {
                tenant_id,
                collection,
                cutoff_system_ms,
            }) => self.meta_temporal_purge_document_strict(
                task,
                *tenant_id,
                collection,
                *cutoff_system_ms,
            ),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                tenant_id,
                collection,
                cutoff_system_ms,
            }) => {
                self.meta_temporal_purge_columnar(task, *tenant_id, collection, *cutoff_system_ms)
            }

            PhysicalPlan::Meta(MetaOp::RawResponse { payload }) => {
                self.response_with_payload(task, payload.clone())
            }

            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection,
                projection,
                limit,
                filters,
                rls_filters,
                sort_keys,
                system_as_of_ms,
                valid_at_ms,
            }) => self.execute_columnar_scan(
                task,
                super::super::handlers::columnar_read::ColumnarScanParams {
                    collection,
                    projection,
                    limit: *limit,
                    filters,
                    rls_filters,
                    sort_keys,
                    system_as_of_ms: *system_as_of_ms,
                    valid_at_ms: *valid_at_ms,
                },
            ),

            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
            }) => self.execute_columnar_insert(
                task,
                collection,
                payload,
                format,
                *intent,
                on_conflict_updates,
            ),

            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection,
                filters,
                updates,
            }) => self.execute_columnar_update(task, collection, filters, updates),

            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection,
                filters,
            }) => self.execute_columnar_delete(task, collection, filters),

            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection,
                time_range,
                limit,
                filters,
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                computed_columns,
                system_as_of_ms,
                valid_at_ms,
                ..
            }) => self.execute_timeseries_scan(
                super::super::handlers::timeseries::TimeseriesScanParams {
                    task,
                    tid: task.request.tenant_id,
                    collection,
                    time_range: *time_range,
                    limit: *limit,
                    filters,
                    bucket_interval_ms: *bucket_interval_ms,
                    group_by,
                    aggregates,
                    gap_fill,
                    computed_columns,
                    system_as_of_ms: *system_as_of_ms,
                    valid_at_ms: *valid_at_ms,
                },
            ),

            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
            }) => self.execute_timeseries_ingest(
                task,
                task.request.tenant_id,
                collection,
                payload,
                format,
                *wal_lsn,
            ),

            PhysicalPlan::Spatial(SpatialOp::Scan {
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters,
                attribute_filters,
                limit,
                projection,
                rls_filters,
            }) => self.execute_spatial_scan(
                task,
                tid,
                collection,
                field,
                predicate,
                query_geometry,
                *distance_meters,
                attribute_filters,
                *limit,
                projection,
                rls_filters,
            ),

            PhysicalPlan::Kv(kv_op) => self.execute_kv(task, tid, kv_op),

            // These variants are dispatched by their dedicated dispatchers in mod.rs.
            // They should never reach dispatch_other.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Text(_) => {
                unreachable!("dispatch_other received plan variant that has its own dispatcher")
            }
        }
    }
}
