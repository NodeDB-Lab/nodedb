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
                documents,
                indexes,
            }) => self.execute_restore_tenant_snapshot(task, *tenant_id, documents, indexes),

            PhysicalPlan::Meta(MetaOp::ConvertCollection {
                collection,
                target_type,
                schema_json,
            }) => self.execute_convert_collection(task, tid, collection, target_type, schema_json),

            PhysicalPlan::Meta(MetaOp::RefreshMaterializedView {
                view_name,
                source_collection,
            }) => self.execute_refresh_materialized_view(task, tid, view_name, source_collection),

            PhysicalPlan::Meta(MetaOp::PurgeTenant { tenant_id }) => {
                self.execute_purge_tenant(task, *tenant_id)
            }

            PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                collection,
                max_age_ms,
            }) => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_else(|e| {
                        tracing::warn!("system clock before UNIX_EPOCH: {e}; using epoch as now");
                        std::time::Duration::ZERO
                    })
                    .as_millis() as i64;
                let cutoff = now_ms - *max_age_ms;
                let mut deleted = 0usize;
                let ts_base = self.data_dir.join("ts").join(collection.as_str());

                if let Some(registry) = self.ts_registries.get_mut(collection.as_str()) {
                    // Find partitions older than cutoff.
                    let expired: Vec<(i64, String)> = registry
                        .iter()
                        .filter(|(_, e)| {
                            e.meta.max_ts < cutoff
                                && e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                        })
                        .map(|(&start, e)| (start, e.dir_name.clone()))
                        .collect();

                    for (start_ts, dir_name) in expired {
                        let partition_path = ts_base.join(&dir_name);
                        if partition_path.exists()
                            && let Err(e) = std::fs::remove_dir_all(&partition_path)
                        {
                            tracing::warn!(
                                path = %partition_path.display(),
                                error = %e,
                                "failed to delete expired partition"
                            );
                            continue;
                        }
                        registry.mark_deleted(start_ts);
                        deleted += 1;
                    }

                    if deleted > 0 {
                        tracing::info!(
                            collection,
                            deleted,
                            max_age_ms,
                            "retention enforcement complete"
                        );
                    }
                }

                // Evict LVC entries older than the cutoff.
                let scoped = format!("{tid}:{collection}");
                if let Some(lvc) = self.ts_last_value_caches.get_mut(&scoped) {
                    let evicted = lvc.evict_older_than(cutoff);
                    if evicted > 0 {
                        tracing::debug!(collection, evicted, "evicted stale LVC entries");
                    }
                }

                let payload = (deleted as u64).to_le_bytes().to_vec();
                self.response_with_payload(task, payload)
            }

            PhysicalPlan::Meta(MetaOp::ApplyContinuousAggRetention) => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_else(|e| {
                        tracing::warn!("system clock before UNIX_EPOCH: {e}; using epoch as now");
                        std::time::Duration::ZERO
                    })
                    .as_millis() as i64;
                let removed = self.continuous_agg_mgr.apply_retention(now_ms);
                tracing::debug!(removed, "continuous aggregate retention applied");
                self.response_ok(task)
            }

            PhysicalPlan::Meta(MetaOp::QueryAggregateWatermark { aggregate_name }) => {
                let wm = self
                    .continuous_agg_mgr
                    .get_watermark(aggregate_name)
                    .cloned()
                    .unwrap_or_default();
                match response_codec::encode_serde(&wm) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::Meta(MetaOp::QueryLastValues { collection }) => {
                let scoped = format!("{tid}:{collection}");

                // LVC is populated on ingest via ingest_batch_with_lvc().
                // After crash recovery, it rebuilds as new data flows in.
                // If no new data has arrived since restart, the cache is empty.
                let entries: Vec<(u64, i64, f64)> =
                    if let Some(lvc) = self.ts_last_value_caches.get(&scoped) {
                        lvc.all().map(|(id, e)| (id, e.ts, e.value)).collect()
                    } else {
                        Vec::new()
                    };
                match response_codec::encode(&entries) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::Meta(MetaOp::RawResponse { payload }) => {
                self.response_with_payload(task, payload.clone())
            }

            PhysicalPlan::Meta(MetaOp::QueryLastValue {
                collection,
                series_id,
            }) => {
                let scoped = format!("{tid}:{collection}");
                let entry: Option<(i64, f64)> = self
                    .ts_last_value_caches
                    .get(&scoped)
                    .and_then(|lvc| lvc.get(*series_id))
                    .map(|e| (e.ts, e.value));
                match response_codec::encode(&entry) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection,
                projection,
                limit,
                filters,
                rls_filters,
            }) => self.execute_columnar_scan(
                task,
                collection,
                projection,
                *limit,
                filters,
                rls_filters,
            ),

            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                format,
            }) => self.execute_columnar_insert(task, collection, payload, format),

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
                ..
            }) => {
                let scoped_coll = format!("{tid}:{collection}");
                self.execute_timeseries_scan(
                    super::super::handlers::timeseries::TimeseriesScanParams {
                        task,
                        collection: &scoped_coll,
                        time_range: *time_range,
                        limit: *limit,
                        filters,
                        bucket_interval_ms: *bucket_interval_ms,
                        group_by,
                        aggregates,
                        gap_fill,
                        computed_columns,
                    },
                )
            }

            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
            }) => {
                let scoped_coll = format!("{tid}:{collection}");
                self.execute_timeseries_ingest(task, &scoped_coll, payload, format, *wal_lsn)
            }

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
