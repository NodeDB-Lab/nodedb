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
    GroupedAggResult, aggregate_memtable, aggregate_partition,
};

impl CoreLoop {
    /// Aggregate mode: GROUP BY / time-bucket across memtable + partitions.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_ts_aggregate(
        &self,
        task: &ExecutionTask,
        collection: &str,
        time_range: (i64, i64),
        limit: usize,
        filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
        bucket_interval_ms: i64,
        group_by: &[String],
        aggregates: &[(String, String)],
        gap_fill: &str,
        needed_columns: &[String],
    ) -> Response {
        let num_aggs = aggregates.len();

        // Phase 1: Aggregate memtable (on TPC core).
        let mut merged = if let Some(mt) = self.columnar_memtables.get(collection)
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
        if let Some(registry) = self.ts_registries.get(collection) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let entries: Vec<_> = registry.query_partitions(&query_range);

            if !entries.is_empty() {
                let data_dir = &self.data_dir;
                let partition_dirs: Vec<std::path::PathBuf> = entries
                    .iter()
                    .map(|e| data_dir.join("ts").join(collection).join(&e.dir_name))
                    .filter(|p| p.exists())
                    .collect();

                if partition_dirs.len() <= 1 {
                    for dir in &partition_dirs {
                        if let Some(part_result) = aggregate_partition(
                            dir,
                            group_by,
                            aggregates,
                            filter_predicates,
                            time_range,
                            needed_columns,
                            bucket_interval_ms,
                        ) {
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
                                        if let Some(r) = aggregate_partition(
                                            dir,
                                            gb,
                                            ag,
                                            fl,
                                            time_range,
                                            nc,
                                            bucket_interval_ms,
                                        ) {
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
        let payload = super::encode::encode_grouped_results(
            &merged,
            group_by,
            aggregates,
            limit,
            bucket_interval_ms,
        );
        self.response_with_payload(task, payload)
    }
}
