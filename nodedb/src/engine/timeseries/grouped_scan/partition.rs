// SPDX-License-Identifier: BUSL-1.1

//! Entry points: aggregate_memtable and aggregate_partition.
//!
//! Builds filter bitmask, applies sparse index skip, then dispatches
//! to the tiered grouping strategies.

use std::collections::HashMap;
use std::path::Path;

use nodedb_query::simd_filter;

use super::super::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use super::super::columnar_segment::ColumnarSegmentReader;
use super::super::grouped_filter::{self, UnsupportedPredicate};
use super::strategies::dispatch_grouping;
use super::types::{GroupedAggResult, resolve_schema};
use crate::bridge::envelope::Priority;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::io::IoMetrics;

/// Aggregate from a columnar memtable with GROUP BY + optional time_bucket.
///
/// `Ok(None)` when a GROUP BY or aggregate column is not in the memtable's
/// schema. `Err` when a predicate in `filters` cannot be lowered onto the
/// typed columns: the caller fails the statement rather than aggregating
/// rows the predicate never excluded.
pub fn aggregate_memtable(
    mt: &ColumnarMemtable,
    group_by: &[String],
    aggregates: &[(String, String)],
    filters: &[ScanFilter],
    time_range: (i64, i64),
    bucket_interval_ms: i64,
) -> Result<Option<GroupedAggResult>, UnsupportedPredicate> {
    let schema = mt.schema();
    let num_aggs = aggregates.len();
    let row_count = mt.row_count() as usize;
    if row_count == 0 {
        return Ok(Some(GroupedAggResult::new(num_aggs)));
    }

    let Some(resolved) =
        resolve_schema(&schema.columns, schema.timestamp_idx, group_by, aggregates)
    else {
        return Ok(None);
    };

    let col_refs: Vec<Option<&ColumnData>> = (0..schema.columns.len())
        .map(|i| Some(mt.column(i)))
        .collect();
    let sym_lookup = |col_idx: usize| mt.symbol_dict(col_idx);

    let mut mask = if !filters.is_empty() {
        grouped_filter::eval_filters_to_bitmask(
            filters,
            &schema.columns,
            &col_refs,
            &sym_lookup,
            row_count,
        )?
    } else {
        simd_filter::bitmask_all(row_count)
    };

    let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;
    if has_time_range {
        let timestamps = mt.column(resolved.ts_idx).as_timestamps();
        let rt = simd_filter::filter_runtime();
        let ts_mask = (rt.range_i64)(timestamps, time_range.0, time_range.1);
        mask = simd_filter::bitmask_and(&mask, &ts_mask);
    }

    if simd_filter::popcount(&mask) == 0 {
        return Ok(Some(GroupedAggResult::new(num_aggs)));
    }

    let timestamps = if bucket_interval_ms > 0 {
        Some(mt.column(resolved.ts_idx).as_timestamps())
    } else {
        None
    };

    Ok(Some(dispatch_grouping(
        super::strategies::GroupedScanInputs {
            resolved: &resolved,
            columns: &col_refs,
            mask: &mask,
            row_count,
            num_aggs,
        },
        group_by,
        &sym_lookup,
        timestamps,
        bucket_interval_ms,
    )))
}

/// Parameters for partition-level grouped aggregation.
pub struct PartitionAggParams<'a> {
    pub partition_dir: &'a Path,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub filters: &'a [ScanFilter],
    pub time_range: (i64, i64),
    pub needed_columns: &'a [String],
    pub bucket_interval_ms: i64,
    pub uring_reader: Option<&'a mut crate::data::io::uring_reader::UringReader>,
    /// Priority of the originating request, forwarded to `UringReader` for
    /// per-tier IO wait metrics.  `None` when the caller does not have
    /// `UringReader` access (e.g. parallel fallback threads).
    pub io_priority: Option<Priority>,
    /// IO metrics sink.  `None` when `uring_reader` is `None`.
    pub io_metrics: Option<&'a IoMetrics>,
}

/// Aggregate from a sealed disk partition with GROUP BY + optional time_bucket.
///
/// When `uring_reader` is `Some`, column files are batch-read via io_uring
/// (parallel kernel I/O). When `None`, falls back to fadvise + std::fs::read.
///
/// `Ok(None)` when the partition's schema or metadata cannot be read or does
/// not carry a GROUP BY / aggregate column. `Err` when a predicate in
/// `filters` cannot be lowered onto the partition's typed columns: the
/// caller fails the statement rather than aggregating rows the predicate
/// never excluded.
pub fn aggregate_partition(
    p: PartitionAggParams<'_>,
) -> Result<Option<GroupedAggResult>, UnsupportedPredicate> {
    let num_aggs = p.aggregates.len();

    let Ok(schema) = ColumnarSegmentReader::read_schema(p.partition_dir, None) else {
        return Ok(None);
    };
    let Ok(meta) = ColumnarSegmentReader::read_meta(p.partition_dir, None) else {
        return Ok(None);
    };
    let row_count = meta.row_count as usize;
    if row_count == 0 {
        return Ok(Some(GroupedAggResult::new(num_aggs)));
    }

    let Some(resolved) = resolve_schema(
        &schema.columns,
        schema.timestamp_idx,
        p.group_by,
        p.aggregates,
    ) else {
        return Ok(None);
    };

    // Load sparse index for block-level skip.
    let sparse_idx = ColumnarSegmentReader::read_sparse_index(p.partition_dir, None)
        .ok()
        .flatten();

    // Determine surviving blocks (if sparse index available).
    let surviving_blocks: Option<Vec<usize>> = sparse_idx
        .as_ref()
        .map(|idx| idx.filter_blocks(p.time_range.0, p.time_range.1, &[]));

    // If sparse index skips blocks and we have surviving blocks, use
    // block-level read. Otherwise read full columns.
    let total_blocks = sparse_idx
        .as_ref()
        .map(|idx| idx.block_count())
        .unwrap_or(0);
    let use_block_read = surviving_blocks
        .as_ref()
        .is_some_and(|sb| !sb.is_empty() && sb.len() < total_blocks);

    let col_data: Vec<Option<ColumnData>> = read_partition_columns(ReadPartitionColumnsParams {
        partition_dir: p.partition_dir,
        schema_columns: &schema.columns,
        needed_columns: p.needed_columns,
        meta: &meta,
        use_block_read,
        surviving_blocks: surviving_blocks.as_deref(),
        uring_reader: p.uring_reader,
        io_priority: p.io_priority,
        io_metrics: p.io_metrics,
    });

    // When block-level read was used, row_count is the number of
    // decoded rows (only surviving blocks), not the partition total.
    let effective_row_count = if use_block_read {
        col_data
            .iter()
            .find_map(|c| {
                c.as_ref().map(|d| match d {
                    ColumnData::Timestamp(v) => v.len(),
                    ColumnData::Float64(v) => v.len(),
                    ColumnData::Int64(v) => v.len(),
                    ColumnData::Symbol(v) => v.len(),
                    ColumnData::DictEncoded { ids, .. } => ids.len(),
                })
            })
            .unwrap_or(0)
    } else {
        row_count
    };

    let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
        .filter_map(|(i, (name, _))| {
            if p.needed_columns.is_empty() || p.needed_columns.iter().any(|n| n == name) {
                ColumnarSegmentReader::read_symbol_dict(p.partition_dir, name, None)
                    .ok()
                    .map(|dict| (i, dict))
            } else {
                None
            }
        })
        .collect();

    // Build bitmask over the decoded data.
    // When block-level read was used, data is already filtered to surviving
    // blocks — just need predicate + time range filters on the decoded rows.
    // When full read was used, need time range + sparse skip + predicate.
    let has_time_range = p.time_range.0 > 0 || p.time_range.1 < i64::MAX;

    let mut mask = if use_block_read {
        // Block-level read already filtered by sparse index.
        // Only need time range filter within surviving blocks.
        if has_time_range {
            let Some(ts_col) = col_data.get(resolved.ts_idx).and_then(|d| d.as_ref()) else {
                return Ok(None);
            };
            let timestamps = ts_col.as_timestamps();
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, p.time_range.0, p.time_range.1)
        } else {
            simd_filter::bitmask_all(effective_row_count)
        }
    } else {
        let partition_fully_in_range =
            !has_time_range || (meta.min_ts >= p.time_range.0 && meta.max_ts <= p.time_range.1);
        let m = if partition_fully_in_range {
            simd_filter::bitmask_all(effective_row_count)
        } else {
            let Some(ts_col) = col_data.get(resolved.ts_idx).and_then(|d| d.as_ref()) else {
                return Ok(None);
            };
            let timestamps = ts_col.as_timestamps();
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, p.time_range.0, p.time_range.1)
        };
        // Apply sparse index block-level skip on full data.
        if let Some(ref idx) = sparse_idx {
            let mut m = m;
            grouped_filter::apply_sparse_skip(&mut m, idx, p.time_range, effective_row_count);
            m
        } else {
            m
        }
    };

    // Apply predicate filters. A predicate the bitmask evaluator cannot
    // lower fails the partition, and with it the statement.
    if !p.filters.is_empty() {
        let col_refs_tmp: Vec<Option<&ColumnData>> = col_data.iter().map(|c| c.as_ref()).collect();
        let sym_lookup_tmp =
            |col_idx: usize| -> Option<&nodedb_types::timeseries::SymbolDictionary> {
                sym_dicts.get(&col_idx)
            };
        let filter_mask = grouped_filter::eval_filters_to_bitmask(
            p.filters,
            &schema.columns,
            &col_refs_tmp,
            &sym_lookup_tmp,
            effective_row_count,
        )?;
        mask = simd_filter::bitmask_and(&mask, &filter_mask);
    }

    if simd_filter::popcount(&mask) == 0 {
        return Ok(Some(GroupedAggResult::new(num_aggs)));
    }

    let col_refs: Vec<Option<&ColumnData>> = col_data.iter().map(|c| c.as_ref()).collect();
    let sym_lookup = |col_idx: usize| -> Option<&nodedb_types::timeseries::SymbolDictionary> {
        sym_dicts.get(&col_idx)
    };

    let timestamps = if p.bucket_interval_ms > 0 {
        col_data
            .get(resolved.ts_idx)
            .and_then(|d| d.as_ref())
            .map(|d| d.as_timestamps())
    } else {
        None
    };

    let result = dispatch_grouping(
        super::strategies::GroupedScanInputs {
            resolved: &resolved,
            columns: &col_refs,
            mask: &mask,
            row_count: effective_row_count,
            num_aggs,
        },
        p.group_by,
        &sym_lookup,
        timestamps,
        p.bucket_interval_ms,
    );

    // Release page cache for this partition's columns — frees cache
    // for other engines (vector, graph) sharing the same process.
    crate::data::io::fadvise::release_partition_columns(p.partition_dir, p.needed_columns);

    Ok(Some(result))
}

struct ReadPartitionColumnsParams<'a> {
    partition_dir: &'a Path,
    schema_columns: &'a [(String, super::super::columnar_memtable::ColumnType)],
    needed_columns: &'a [String],
    meta: &'a nodedb_types::timeseries::PartitionMeta,
    use_block_read: bool,
    surviving_blocks: Option<&'a [usize]>,
    uring_reader: Option<&'a mut crate::data::io::uring_reader::UringReader>,
    io_priority: Option<Priority>,
    io_metrics: Option<&'a IoMetrics>,
}

/// Read column data for a partition, using io_uring when available.
///
/// With `uring_reader`: batch-reads all needed `.col` files in parallel
/// via io_uring, then decodes each. Without: fadvise + sequential std::fs::read.
fn read_partition_columns(p: ReadPartitionColumnsParams<'_>) -> Vec<Option<ColumnData>> {
    let ReadPartitionColumnsParams {
        partition_dir,
        schema_columns,
        needed_columns,
        meta,
        use_block_read,
        surviving_blocks,
        uring_reader,
        io_priority,
        io_metrics,
    } = p;
    // Determine which schema columns are actually needed.
    let needed_indices: Vec<usize> = schema_columns
        .iter()
        .enumerate()
        .filter(|(_, (name, _))| {
            needed_columns.is_empty() || needed_columns.iter().any(|n| n == name)
        })
        .map(|(i, _)| i)
        .collect();

    // Block-level reads can't use io_uring batching (need per-block decode).
    // io_uring batching only benefits full-column reads.
    if use_block_read || uring_reader.is_none() {
        // Fallback: fadvise + sequential read.
        crate::data::io::fadvise::prefetch_partition_columns(partition_dir, needed_columns);

        return schema_columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| {
                if !needed_indices.contains(&i) {
                    return None;
                }
                let codec = meta.column_stats.get(name).map(|s| s.codec);
                if use_block_read {
                    ColumnarSegmentReader::read_column_blocks(
                        partition_dir,
                        name,
                        *ty,
                        codec,
                        surviving_blocks.unwrap_or(&[]),
                        None,
                    )
                    .ok()
                    .map(|(data, _)| data)
                } else {
                    ColumnarSegmentReader::read_column_with_codec(
                        partition_dir,
                        name,
                        *ty,
                        codec,
                        None,
                    )
                    .ok()
                }
            })
            .collect();
    }

    // io_uring path: batch-read all needed .col files in parallel.
    let Some(reader) = uring_reader else {
        unreachable!("guarded by is_none() check above");
    };

    let col_paths: Vec<std::path::PathBuf> = needed_indices
        .iter()
        .map(|&i| partition_dir.join(format!("{}.col", schema_columns[i].0)))
        .collect();
    let path_refs: Vec<&Path> = col_paths.iter().map(|p| p.as_path()).collect();

    let raw_buffers = match (io_priority, io_metrics) {
        (Some(priority), Some(metrics)) => {
            reader.read_files_with_priority(&path_refs, priority, metrics)
        }
        _ => reader.read_files(&path_refs),
    };

    // Decode each raw buffer into ColumnData.
    let mut decoded: HashMap<usize, ColumnData> = HashMap::new();
    for (buf_idx, &schema_idx) in needed_indices.iter().enumerate() {
        let raw = &raw_buffers[buf_idx];
        if raw.is_empty() {
            continue;
        }
        let (name, ty) = &schema_columns[schema_idx];
        let codec = meta.column_stats.get(name).map(|s| s.codec);
        if let Ok(data) = ColumnarSegmentReader::decode_column_from_bytes(
            partition_dir,
            name,
            *ty,
            codec,
            raw,
            None,
        ) {
            decoded.insert(schema_idx, data);
        }
    }

    schema_columns
        .iter()
        .enumerate()
        .map(|(i, _)| decoded.remove(&i))
        .collect()
}
