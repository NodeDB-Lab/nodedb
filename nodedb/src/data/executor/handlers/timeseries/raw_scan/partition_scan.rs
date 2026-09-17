// SPDX-License-Identifier: BUSL-1.1

//! Parallel and sequential disk-partition scanning for raw mode.

use std::collections::HashMap;

use crate::bridge::scan_filter::ScanFilter;
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;

use super::row_emit::{emit_partition_row, extract_timestamp, row_admitted};

/// Scan disk partitions in parallel, returning rmpv rows sorted by timestamp.
///
/// `rls_predicates` is the caller's read policy: every row is checked
/// against it before it counts toward `limit`. `Err` when a stored time
/// cell cannot be read as its column's instant or a predicate expression
/// divides by zero.
pub(super) fn scan_partitions_parallel(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[ScanFilter],
    has_filters: bool,
    rls_predicates: &[ScanFilter],
) -> crate::Result<Vec<rmpv::Value>> {
    if partition_dirs.len() <= 1 {
        return match partition_dirs.first() {
            Some(dir) => scan_one_partition(
                dir,
                time_range,
                limit,
                filter_predicates,
                has_filters,
                rls_predicates,
            ),
            None => Ok(Vec::new()),
        };
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let available = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let thread_count = available.min(partition_dirs.len()).min(8);

        if thread_count <= 1 {
            return scan_partitions_sequential(
                partition_dirs,
                time_range,
                limit,
                filter_predicates,
                has_filters,
                rls_predicates,
            );
        }

        let chunk_size = partition_dirs.len().div_ceil(thread_count);
        let filters_ref = filter_predicates;
        let rls_ref = rls_predicates;

        let mut thread_results: Vec<Vec<rmpv::Value>> = std::thread::scope(|s| {
            let handles: Vec<_> = partition_dirs
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        scan_partitions_sequential(
                            chunk,
                            time_range,
                            limit,
                            filters_ref,
                            has_filters,
                            rls_ref,
                        )
                    })
                })
                .collect();

            handles
                .into_iter()
                .filter_map(|h| h.join().ok())
                .collect::<crate::Result<Vec<_>>>()
        })?;

        // Merge: each thread's results are already time-sorted (partitions are
        // time-ordered). Flatten, sort globally, truncate to limit.
        let total: usize = thread_results.iter().map(|v| v.len()).sum();
        let mut merged = Vec::with_capacity(total.min(limit));
        for batch in &mut thread_results {
            merged.append(batch);
        }
        // Sort by timestamp (first field in each row map).
        merged.sort_by_key(extract_timestamp);
        merged.truncate(limit);
        Ok(merged)
    }

    #[cfg(target_arch = "wasm32")]
    {
        scan_partitions_sequential(
            partition_dirs,
            time_range,
            limit,
            filter_predicates,
            has_filters,
            rls_predicates,
        )
    }
}

pub(super) fn scan_partitions_sequential(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[ScanFilter],
    has_filters: bool,
    rls_predicates: &[ScanFilter],
) -> crate::Result<Vec<rmpv::Value>> {
    let mut results = Vec::new();
    for dir in partition_dirs {
        if results.len() >= limit {
            break;
        }
        let remaining = limit - results.len();
        let rows = scan_one_partition(
            dir,
            time_range,
            remaining,
            filter_predicates,
            has_filters,
            rls_predicates,
        )?;
        results.extend(rows);
    }
    results.truncate(limit);
    Ok(results)
}

/// Scan a single disk partition, returning rmpv rows.
///
/// The WHERE predicates run on the typed columns when the evaluator can
/// lower them and per row otherwise. The read policy runs per row after
/// them, before the row counts toward `limit`. `Err` when a stored time
/// cell cannot be read as its column's instant or a predicate expression
/// divides by zero.
pub(super) fn scan_one_partition(
    part_dir: &std::path::Path,
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[ScanFilter],
    has_filters: bool,
    rls_predicates: &[ScanFilter],
) -> crate::Result<Vec<rmpv::Value>> {
    let schema = match ColumnarSegmentReader::read_schema(part_dir, None) {
        Ok(s) => s,
        Err(_) => return Ok(Vec::new()),
    };

    // Prefetch all column files into page cache before reading.
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
    crate::data::io::fadvise::prefetch_partition_columns(part_dir, &all_col_names);

    let col_data: Vec<Option<ColumnData>> = schema
        .columns
        .iter()
        .map(|(name, ty)| ColumnarSegmentReader::read_column(part_dir, name, *ty, None).ok())
        .collect();

    let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
        .filter_map(|(i, (name, _))| {
            ColumnarSegmentReader::read_symbol_dict(part_dir, name, None)
                .ok()
                .map(|dict| (i, dict))
        })
        .collect();

    let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
    let Some(ts_col) = ts_col else {
        return Ok(Vec::new());
    };
    let timestamps = ts_col.as_timestamps();
    let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

    let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
    let part_src = crate::data::executor::handlers::columnar_filter::PartitionColumns {
        schema: &schema_vec,
        columns: &col_data,
        sym_dicts: &sym_dicts,
    };

    // `row_filters` carries the WHERE predicates only when the typed-column
    // evaluator could not lower them, so they still apply per row instead
    // of being dropped.
    let row_count = timestamps.len();
    let (filtered_indices, row_filters): (Vec<u32>, &[ScanFilter]) = if has_filters {
        if let Some(bitmask) =
            crate::data::executor::handlers::columnar_filter::eval_filters_bitmask(
                &part_src,
                filter_predicates,
                row_count,
            )
        {
            (nodedb_query::simd_filter::bitmask_to_indices(&bitmask), &[])
        } else {
            match crate::data::executor::handlers::columnar_filter::eval_filters_sparse(
                &part_src,
                filter_predicates,
                &indices,
            ) {
                Some(mask) => (
                    crate::data::executor::handlers::columnar_filter::apply_mask(&indices, &mask),
                    &[],
                ),
                None => (indices, filter_predicates),
            }
        }
    } else {
        (indices, &[])
    };

    let mut rows = Vec::with_capacity(filtered_indices.len().min(limit));
    for &idx in &filtered_indices {
        if rows.len() >= limit {
            break;
        }
        let row = emit_partition_row(&schema_vec, &col_data, &sym_dicts, idx as usize)?;
        if !row_admitted(&row, row_filters, rls_predicates)? {
            continue;
        }
        rows.push(row);
    }

    // Release page cache for this partition.
    crate::data::io::fadvise::release_partition_columns(part_dir, &all_col_names);

    Ok(rows)
}
