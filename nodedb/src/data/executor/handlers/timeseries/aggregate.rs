// SPDX-License-Identifier: BUSL-1.1

//! Aggregate mode: GROUP BY / time-bucket across memtable + partitions.
//!
//! Uses the grouped_scan engine with SIMD bitmask filters, direct-index Vec
//! for low-cardinality symbol GROUP BY, parallel partition processing via
//! std::thread::scope, sparse index block-level skip, and single metadata
//! read per partition.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::grouped_scan::{
    GroupedAggResult, PartitionAggParams, aggregate_memtable, aggregate_partition,
};

/// Borrowed inputs to [`CoreLoop::execute_ts_aggregate`]: the target
/// collection, time range, GROUP BY / aggregate / filter specs, and gap-fill
/// / projection settings for one timeseries aggregate dispatch.
pub(in crate::data::executor) struct TsAggregateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filter_predicates: &'a [crate::bridge::scan_filter::ScanFilter],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub gap_fill: &'a str,
    pub needed_columns: &'a [String],
    /// `ORDER BY` keys as `(output column, ascending)`, applied to the encoded
    /// group rows before `limit`.
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
}

impl CoreLoop {
    /// Aggregate mode: GROUP BY / time-bucket across memtable + partitions.
    pub(in crate::data::executor) fn execute_ts_aggregate(
        &mut self,
        params: TsAggregateParams<'_>,
    ) -> Response {
        let TsAggregateParams {
            task,
            tid,
            collection,
            time_range,
            limit,
            filter_predicates,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            needed_columns,
            sort_keys,
        } = params;

        let key = (task.request.database_id, tid, collection.to_string());
        let num_aggs = aggregates.len();

        // Phase 1: Aggregate memtable (on TPC core).
        let mut merged = if let Some(mt) = self.columnar_memtables.get(&key)
            && !mt.is_empty()
        {
            aggregate_memtable(
                mt,
                group_by,
                aggregates,
                filter_predicates,
                time_range,
                bucket_interval_ms,
            )
            .unwrap_or_else(|| GroupedAggResult::new(num_aggs))
        } else {
            GroupedAggResult::new(num_aggs)
        };

        // Phase 2: Aggregate disk partitions (parallel).
        if let Some(registry) = self.ts_registries.get(&key) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let entries: Vec<_> = registry.query_partitions(&query_range);

            if !entries.is_empty() {
                let data_dir = &self.data_dir;
                let db_id = task.request.database_id.as_u64();
                let tenant = tid.as_u64();
                let partition_dirs: Vec<std::path::PathBuf> = entries
                    .iter()
                    .map(|e| {
                        super::paths::ts_collection_dir(data_dir, db_id, tenant, collection)
                            .join(&e.dir_name)
                    })
                    .filter(|p| p.exists())
                    .collect();

                if partition_dirs.len() <= 1 {
                    // Single partition — use io_uring batched reads on TPC core.
                    let io_priority = Some(task.request.priority);
                    let io_metrics: &crate::data::io::IoMetrics = &self.io_metrics;
                    for dir in &partition_dirs {
                        if let Some(part_result) = aggregate_partition(PartitionAggParams {
                            partition_dir: dir,
                            group_by,
                            aggregates,
                            filters: filter_predicates,
                            time_range,
                            needed_columns,
                            bucket_interval_ms,
                            uring_reader: self.uring_reader.as_mut(),
                            io_priority,
                            io_metrics: Some(io_metrics),
                        }) {
                            merged.merge(&part_result);
                        }
                    }
                } else {
                    // Multiple partitions — parallel via std::thread::scope.
                    let group_by_owned: Vec<String> = group_by.to_vec();
                    let agg_owned: Vec<(String, String)> = aggregates.to_vec();
                    let filters_owned: Vec<crate::bridge::scan_filter::ScanFilter> =
                        filter_predicates.to_vec();
                    let needed_owned: Vec<String> = needed_columns.to_vec();

                    let available = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1);
                    let thread_count = available.min(partition_dirs.len()).min(8);
                    let chunk_size = partition_dirs.len().div_ceil(thread_count);

                    let partition_results: Vec<GroupedAggResult> = std::thread::scope(|s| {
                        let handles: Vec<_> = partition_dirs
                            .chunks(chunk_size)
                            .map(|chunk| {
                                let gb = &group_by_owned;
                                let ag = &agg_owned;
                                let fl = &filters_owned;
                                let nc = &needed_owned;
                                s.spawn(move || {
                                    let mut local = GroupedAggResult::new(ag.len());
                                    for dir in chunk {
                                        // Parallel threads: no io_uring (fadvise fallback).
                                        if let Some(r) = aggregate_partition(PartitionAggParams {
                                            partition_dir: dir,
                                            group_by: gb,
                                            aggregates: ag,
                                            filters: fl,
                                            time_range,
                                            needed_columns: nc,
                                            bucket_interval_ms,
                                            uring_reader: None,
                                            io_priority: None,
                                            io_metrics: None,
                                        }) {
                                            local.merge(&r);
                                        }
                                    }
                                    local
                                })
                            })
                            .collect();

                        handles.into_iter().filter_map(|h| h.join().ok()).collect()
                    });

                    for r in &partition_results {
                        merged.merge(r);
                    }
                }
            }
        }

        // Phase 3: Apply gap-fill if requested (bucketed results only).
        let merged = if !gap_fill.is_empty() && bucket_interval_ms > 0 {
            super::super::timeseries_gap_fill::apply_gap_fill_to_grouped(
                merged,
                time_range,
                bucket_interval_ms,
                gap_fill,
                group_by,
                aggregates,
            )
        } else {
            merged
        };

        // Phase 4: Encode response (MessagePack, no serde_json intermediate).
        // A grouped column carries the type it carries ungrouped, so the
        // encoder is told each key column's declared shape.
        let group_key_kinds =
            self.ts_group_key_kinds(task.request.database_id, tid, collection, group_by);
        let payload = match super::encode::encode_grouped_results(
            &merged,
            group_by,
            aggregates,
            limit,
            bucket_interval_ms,
            sort_keys,
            &group_key_kinds,
        ) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        self.response_with_payload(task, payload)
    }
}
