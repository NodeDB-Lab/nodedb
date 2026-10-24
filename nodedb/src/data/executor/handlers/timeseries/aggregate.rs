// SPDX-License-Identifier: BUSL-1.1

//! Aggregate mode: GROUP BY / time-bucket across memtable, partitions,
//! staged rows, and AUTO_TIER downsample buckets.
//!
//! Uses the grouped_scan engine with SIMD bitmask filters, direct-index Vec
//! for low-cardinality symbol GROUP BY, parallel partition processing via
//! std::thread::scope, sparse index block-level skip, and single metadata
//! read per partition.
//!
//! The caller's read policy travels as part of `filter_predicates`: the
//! grouped scan lowers every predicate onto typed column vectors before any
//! row is aggregated, and a predicate it cannot lower fails the statement.
//! An unlowerable policy therefore never aggregates the rows it governs.

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
    /// The query's WHERE predicates followed by the caller's read policy.
    /// Every predicate is pushed into the grouped scan, so the policy
    /// excludes rows before they reach an accumulator.
    pub filter_predicates: &'a [crate::bridge::scan_filter::ScanFilter],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub gap_fill: &'a str,
    pub needed_columns: &'a [String],
    /// `ORDER BY` keys as `(output column, ascending)`, applied to the encoded
    /// group rows before `limit`.
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    /// HAVING predicates over the finalized group rows.
    pub having: &'a [crate::bridge::scan_filter::ScanFilter],
    /// The transaction's staged rows, folded into the groups so the
    /// aggregate reads its own writes. `None` outside a transaction.
    pub staged: Option<crate::data::executor::handlers::transaction::overlay::StagedTsRows>,
    /// The part of `time_range` flushed partitions serve. `None` when
    /// downsample tiers serve all of it.
    pub raw_range: Option<(i64, i64)>,
    /// AUTO_TIER buckets of the part of `time_range` raw partitions no
    /// longer serve, already grouped like the result.
    pub tier_partials: Option<GroupedAggResult>,
    /// `(encoded key, output name)` of each output column, in SELECT-list
    /// order. Empty keeps the encoded keys.
    pub output_columns: &'a [(String, String)],
}

impl CoreLoop {
    /// Aggregate mode: GROUP BY / time-bucket across memtable + partitions.
    pub(in crate::data::executor) fn execute_ts_aggregate(
        &mut self,
        params: TsAggregateParams<'_>,
    ) -> Response {
        let crs = self.crs_context();
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
            having,
            staged,
            raw_range,
            tier_partials,
            output_columns,
        } = params;

        let key = (task.request.database_id, tid, collection.to_string());
        let num_aggs = aggregates.len();
        // A transaction that truncated the collection sees no committed row.
        let committed_visible = !staged.as_ref().is_some_and(|s| s.truncated);

        // Phase 1: Aggregate memtable (on TPC core).
        let mut merged = if committed_visible
            && let Some(mt) = self.columnar_memtables.get(&key)
            && !mt.is_empty()
        {
            match aggregate_memtable(
                mt,
                group_by,
                aggregates,
                filter_predicates,
                time_range,
                bucket_interval_ms,
            ) {
                Ok(result) => result.unwrap_or_else(|| GroupedAggResult::new(num_aggs)),
                Err(e) => return self.response_error(task, crate::Error::from(e)),
            }
        } else {
            GroupedAggResult::new(num_aggs)
        };

        // Phase 2: Aggregate disk partitions (parallel), over the raw part
        // of the range only.
        if committed_visible
            && let Some(time_range) = raw_range
            && let Some(registry) = self.ts_registries.get(&key)
        {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let entries: Vec<_> = registry.query_partitions(&query_range);

            if !entries.is_empty() {
                let data_dir = &self.data_dir;
                let db_id = task.request.database_id.as_u64();
                let tenant = tid.as_u64();
                // Every listed partition is read. A missing directory refuses
                // the aggregate in `aggregate_partition`.
                let partition_dirs: Vec<std::path::PathBuf> = entries
                    .iter()
                    .map(|e| {
                        super::paths::ts_collection_dir(data_dir, db_id, tenant, collection)
                            .join(&e.dir_name)
                    })
                    .collect();

                if partition_dirs.len() <= 1 {
                    // Single partition — use io_uring batched reads on TPC core.
                    let io_priority = Some(task.request.priority);
                    let io_metrics: &crate::data::io::IoMetrics = &self.io_metrics;
                    for dir in &partition_dirs {
                        match aggregate_partition(PartitionAggParams {
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
                            kek: self.segment_keks.ts_segment_kek.as_ref(),
                        }) {
                            Ok(Some(part_result)) => merged.merge(&part_result),
                            Ok(None) => {}
                            Err(e) => return self.response_error(task, e),
                        }
                    }
                } else {
                    // Multiple partitions — parallel via std::thread::scope.
                    let group_by_owned: Vec<String> = group_by.to_vec();
                    let agg_owned: Vec<(String, String)> = aggregates.to_vec();
                    let filters_owned: Vec<crate::bridge::scan_filter::ScanFilter> =
                        filter_predicates.to_vec();
                    let needed_owned: Vec<String> = needed_columns.to_vec();
                    let kek = self.segment_keks.ts_segment_kek.as_ref();

                    let available = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1);
                    let thread_count = available.min(partition_dirs.len()).min(8);
                    let chunk_size = partition_dirs.len().div_ceil(thread_count);

                    let partition_results: crate::Result<Vec<GroupedAggResult>> =
                        std::thread::scope(|s| {
                            let handles: Vec<_> = partition_dirs
                                .chunks(chunk_size)
                                .map(|chunk| {
                                    let gb = &group_by_owned;
                                    let ag = &agg_owned;
                                    let fl = &filters_owned;
                                    let nc = &needed_owned;
                                    s.spawn(move || -> crate::Result<_> {
                                        let mut local = GroupedAggResult::new(ag.len());
                                        for dir in chunk {
                                            // Parallel threads: no io_uring (fadvise fallback).
                                            if let Some(r) =
                                                aggregate_partition(PartitionAggParams {
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
                                                    kek,
                                                })?
                                            {
                                                local.merge(&r);
                                            }
                                        }
                                        Ok(local)
                                    })
                                })
                                .collect();

                            // A worker that panicked aggregated none of its
                            // partitions: the aggregate is refused, never
                            // answered without them.
                            handles
                                .into_iter()
                                .map(|h| {
                                    h.join().map_err(|_| crate::Error::Internal {
                                        detail: format!(
                                            "timeseries aggregate on '{collection}': a partition \
                                             worker panicked"
                                        ),
                                    })?
                                })
                                .collect()
                        });

                    let partition_results = match partition_results {
                        Ok(results) => results,
                        Err(e) => return self.response_error(task, e),
                    };
                    for r in &partition_results {
                        merged.merge(r);
                    }
                }
            }
        }

        // Phase 2b: Fold the transaction's staged rows.
        if let Some(staged) = &staged
            && let Err(e) = self.fold_staged_ts_rows(
                &mut merged,
                super::staged_fold::StagedFold {
                    database_id: task.request.database_id,
                    tid,
                    collection,
                    rows: &staged.rows,
                    group_by,
                    aggregates,
                    filter_predicates,
                    time_range,
                    bucket_interval_ms,
                },
            )
        {
            return self.response_error(task, e);
        }

        // Phase 2c: Merge the downsample tiers' buckets.
        if committed_visible && let Some(tiers) = &tier_partials {
            merged.merge(tiers);
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
        // The column each aggregate reads, so an extreme over an instant
        // column renders as an instant. `*` names no column and reads Text.
        let aggregate_fields: Vec<String> =
            aggregates.iter().map(|(_, field)| field.clone()).collect();
        let aggregate_kinds =
            self.ts_group_key_kinds(task.request.database_id, tid, collection, &aggregate_fields);
        let bucket_kind = self.ts_time_key_kind(task.request.database_id, tid, collection);
        let payload = match super::encode::encode_grouped_results(
            &merged,
            group_by,
            aggregates,
            super::encode::GroupedOutputShape {
                limit,
                bucket_interval_ms,
                sort_keys,
                having,
                output_columns,
            },
            super::encode::GroupedKeyTypes {
                group_key_kinds: &group_key_kinds,
                aggregate_kinds: &aggregate_kinds,
                bucket_kind,
            },
            &crs,
        ) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        self.response_with_payload(task, payload)
    }
}
