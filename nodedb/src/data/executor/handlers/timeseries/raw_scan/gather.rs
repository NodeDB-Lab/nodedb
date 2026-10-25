// SPDX-License-Identifier: BUSL-1.1

//! The committed timeseries row source: the live memtable, then every
//! flushed partition in the time range. Shared by the raw scan and by the
//! generic collection scan (`scan_collection`), so both read the same rows.

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::types::{DatabaseId, TenantId};

use super::partition_scan::{PartitionScan, scan_partitions_parallel};
use super::row_emit::{emit_memtable_row, row_admitted_by_policy};

/// Inputs to [`CoreLoop::gather_ts_rows`].
pub(in crate::data::executor) struct TsGatherParams<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    /// Inclusive `(min_ms, max_ms)` bounds on the time key.
    pub time_range: (i64, i64),
    /// WHERE predicates; empty admits every row.
    pub filter_predicates: &'a [ScanFilter],
    /// The caller's read policy; empty admits every row.
    pub rls_predicates: &'a [ScanFilter],
    /// Row ceiling.
    pub limit: usize,
}

impl CoreLoop {
    /// Committed rows of a timeseries collection: memtable rows, then rows
    /// of every flushed partition in the time range, each admitted by the
    /// WHERE predicates and then the read policy, up to `limit`.
    ///
    /// A registered partition whose directory is missing is an error: the
    /// rows it holds are part of the answer.
    pub(in crate::data::executor) fn gather_ts_rows(
        &self,
        p: TsGatherParams<'_>,
    ) -> crate::Result<Vec<rmpv::Value>> {
        let crs = self.crs_context();
        let TsGatherParams {
            database_id,
            tid,
            collection,
            time_range,
            filter_predicates,
            rls_predicates,
            limit,
        } = p;
        let has_filters = !filter_predicates.is_empty();
        let key = (database_id, tid, collection.to_string());
        let mut results: Vec<rmpv::Value> = Vec::new();

        // 1. Live memtable.
        if let Some(mt) = self.columnar_memtables.get(&key)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let timestamps = mt.column(schema.timestamp_idx).as_timestamps();
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            let (filtered_indices, need_row_filter) = if has_filters {
                let row_count = mt.row_count() as usize;
                if let Some(bitmask) =
                    crate::data::executor::handlers::columnar_filter::eval_filters_bitmask(
                        mt,
                        filter_predicates,
                        row_count,
                    )
                {
                    (
                        nodedb_query::simd_filter::bitmask_to_indices(&bitmask),
                        false,
                    )
                } else {
                    match crate::data::executor::handlers::columnar_filter::eval_filters_sparse(
                        mt,
                        filter_predicates,
                        &indices,
                    ) {
                        Some(mask) => (
                            crate::data::executor::handlers::columnar_filter::apply_mask(
                                &indices, &mask,
                            ),
                            false,
                        ),
                        None => (indices, true),
                    }
                }
            } else {
                (indices, false)
            };

            let columns: Vec<_> = schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| (i, name, ty, mt.column(i)))
                .collect();
            for &idx in &filtered_indices {
                if results.len() >= limit {
                    break;
                }
                let row = emit_memtable_row(mt, &columns, idx as usize)?;
                if need_row_filter {
                    let mut buf = Vec::new();
                    rmpv::encode::write_value(&mut buf, &row).map_err(|e| crate::Error::Codec {
                        detail: format!("timeseries row encode: {e}"),
                    })?;
                    if !ScanFilter::all_match_binary(filter_predicates, &buf, &crs)? {
                        continue;
                    }
                }
                // The read policy, after the WHERE predicates and before the
                // row takes a limit slot.
                if !row_admitted_by_policy(&row, rls_predicates, &crs)? {
                    continue;
                }
                results.push(row);
            }
        }

        // 2. Flushed partitions.
        if let Some(registry) = self.ts_registries.get(&key) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let partition_dirs: Vec<std::path::PathBuf> = registry
                .query_partitions(&query_range)
                .iter()
                .map(|entry| {
                    super::super::paths::ts_collection_dir(
                        &self.data_dir,
                        database_id.as_u64(),
                        tid.as_u64(),
                        collection,
                    )
                    .join(&entry.dir_name)
                })
                .collect();
            if let Some(missing) = partition_dirs.iter().find(|dir| !dir.exists()) {
                return Err(crate::Error::SegmentCorrupted {
                    detail: format!(
                        "timeseries collection '{collection}': registered partition {} is missing",
                        missing.display()
                    ),
                });
            }
            let remaining = limit.saturating_sub(results.len());
            if remaining > 0 && !partition_dirs.is_empty() {
                let partition_rows = scan_partitions_parallel(
                    &partition_dirs,
                    &PartitionScan {
                        time_range,
                        limit: remaining,
                        filter_predicates,
                        has_filters,
                        rls_predicates,
                        kek: self.segment_keks.ts_segment_kek.as_ref(),
                    },
                    &crs,
                )?;
                results.extend(partition_rows);
                results.truncate(limit);
            }
        }

        Ok(results)
    }
}
