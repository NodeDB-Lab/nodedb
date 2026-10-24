// SPDX-License-Identifier: BUSL-1.1

//! Parallel and sequential disk-partition scanning for raw mode.

use nodedb_crs::CrsContext;
use nodedb_wal::crypto::WalEncryptionKey;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::handlers::timeseries::partition_read::{
    TsPartitionColumns, read_ts_partition,
};
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::engine::timeseries::columnar_memtable::ColumnType;

use super::row_emit::{emit_partition_row, extract_timestamp, row_admitted};

/// The read-only inputs every partition scan shares.
///
/// The CRS context is not part of it: a context is not `Sync`, so each scan
/// thread takes its own fork and passes it alongside.
#[derive(Clone, Copy)]
pub(super) struct PartitionScan<'a> {
    /// Inclusive `(min_ms, max_ms)` bounds on the time key.
    pub time_range: (i64, i64),
    /// Row ceiling.
    pub limit: usize,
    /// WHERE predicates; applied only when `has_filters` is set.
    pub filter_predicates: &'a [ScanFilter],
    pub has_filters: bool,
    /// The caller's read policy: every row is checked against it before it
    /// counts toward `limit`.
    pub rls_predicates: &'a [ScanFilter],
    /// The core's timeseries segment key.
    pub kek: Option<&'a WalEncryptionKey>,
}

/// Scan disk partitions in parallel, returning rmpv rows sorted by timestamp.
///
/// `Err` when a stored time cell cannot be read as its column's instant or a
/// predicate expression divides by zero.
pub(super) fn scan_partitions_parallel(
    partition_dirs: &[std::path::PathBuf],
    scan: &PartitionScan<'_>,
    crs: &CrsContext<'_>,
) -> crate::Result<Vec<rmpv::Value>> {
    if partition_dirs.len() <= 1 {
        return match partition_dirs.first() {
            Some(dir) => scan_one_partition(dir, scan, crs),
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
            return scan_partitions_sequential(partition_dirs, scan, crs);
        }

        let chunk_size = partition_dirs.len().div_ceil(thread_count);

        let mut thread_results: Vec<Vec<rmpv::Value>> = std::thread::scope(|s| {
            let handles: Vec<_> = partition_dirs
                .chunks(chunk_size)
                .map(|chunk| {
                    let thread_crs = crs.fork();
                    s.spawn(move || scan_partitions_sequential(chunk, scan, &thread_crs))
                })
                .collect();

            // A scan thread that panicked refuses the scan: dropping its
            // result would answer without its partitions' rows.
            handles
                .into_iter()
                .map(|h| {
                    h.join().map_err(|_| crate::Error::Internal {
                        detail: "timeseries partition scan thread panicked".into(),
                    })?
                })
                .collect::<crate::Result<Vec<_>>>()
        })?;

        // Merge: each thread's results are already time-sorted (partitions are
        // time-ordered). Flatten, sort globally, truncate to limit.
        let total: usize = thread_results.iter().map(|v| v.len()).sum();
        let limit = scan.limit;
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
        scan_partitions_sequential(partition_dirs, scan, crs)
    }
}

pub(super) fn scan_partitions_sequential(
    partition_dirs: &[std::path::PathBuf],
    scan: &PartitionScan<'_>,
    crs: &CrsContext<'_>,
) -> crate::Result<Vec<rmpv::Value>> {
    let limit = scan.limit;
    let mut results = Vec::new();
    for dir in partition_dirs {
        if results.len() >= limit {
            break;
        }
        let remaining = PartitionScan {
            limit: limit - results.len(),
            ..*scan
        };
        let rows = scan_one_partition(dir, &remaining, crs)?;
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
    scan: &PartitionScan<'_>,
    crs: &CrsContext<'_>,
) -> crate::Result<Vec<rmpv::Value>> {
    let PartitionScan {
        time_range,
        limit,
        filter_predicates,
        has_filters,
        rls_predicates,
        kek,
    } = *scan;
    // A partition that does not read refuses the scan. Skipping it would
    // answer without the partition's rows.
    let TsPartitionColumns {
        schema,
        columns: col_data,
        sym_dicts,
    } = read_ts_partition(part_dir, "timeseries_raw_scan", kek)?;
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();

    let Some(ts_col) = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref()) else {
        return Err(crate::Error::SegmentCorrupted {
            detail: format!(
                "timeseries partition {}: time column index {} is outside its schema",
                part_dir.display(),
                schema.timestamp_idx
            ),
        });
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
        let row = emit_partition_row(part_dir, &schema_vec, &col_data, &sym_dicts, idx as usize)?;
        if !row_admitted(&row, row_filters, rls_predicates, crs)? {
            continue;
        }
        rows.push(row);
    }

    // Release page cache for this partition.
    crate::data::io::fadvise::release_partition_columns(part_dir, &all_col_names);

    Ok(rows)
}
