// SPDX-License-Identifier: BUSL-1.1

//! Entry points: aggregate_memtable and aggregate_partition.
//!
//! Builds filter bitmask, applies sparse index skip, then dispatches
//! to the tiered grouping strategies.

use std::collections::HashMap;
use std::path::Path;

use nodedb_query::simd_filter;

use super::super::columnar_memtable::{ColumnData, ColumnarMemtable};
use super::super::columnar_segment::ColumnarSegmentReader;
use super::super::grouped_filter::{self, UnsupportedPredicate};
use super::strategies::dispatch_grouping;
use super::types::{GroupedAggResult, resolve_schema};
use crate::bridge::envelope::Priority;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::handlers::timeseries::partition_read::{
    partition_corrupt, require_partition_dir,
};
use crate::data::io::IoMetrics;

/// The read path the corruption report names.
const SITE: &str = "timeseries_grouped_scan";

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
    /// per-tier IO wait metrics. `None` when the caller has no `UringReader`.
    pub io_priority: Option<Priority>,
    /// IO metrics sink.  `None` when `uring_reader` is `None`.
    pub io_metrics: Option<&'a IoMetrics>,
    /// The core's timeseries segment key, the key every partition write
    /// uses. `None` reads plaintext partitions.
    pub kek: Option<&'a nodedb_wal::crypto::WalEncryptionKey>,
}

/// Aggregate from a sealed disk partition with GROUP BY + optional time_bucket.
///
/// When `uring_reader` is `Some`, column files are batch-read via io_uring
/// (parallel kernel I/O). When `None`, falls back to fadvise + std::fs::read.
///
/// `Ok(None)` when the partition's schema does not carry a GROUP BY or
/// aggregate column. `Err` when a predicate in `filters` cannot be lowered
/// onto the partition's typed columns: the caller fails the statement
/// rather than aggregating rows the predicate never excluded. `Err` when the
/// partition does not read: its directory, schema, metadata, sparse index,
/// a needed column, or a symbol dictionary. The partition is never skipped.
pub fn aggregate_partition(p: PartitionAggParams<'_>) -> crate::Result<Option<GroupedAggResult>> {
    let num_aggs = p.aggregates.len();
    let dir = p.partition_dir;

    require_partition_dir(dir, SITE)?;
    let kek = p.kek;
    let schema = ColumnarSegmentReader::read_schema(dir, kek)
        .map_err(|e| partition_corrupt(dir, "schema", SITE, e))?;
    let meta = ColumnarSegmentReader::read_meta(dir, kek)
        .map_err(|e| partition_corrupt(dir, "meta", SITE, e))?;
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

    // Load sparse index for block-level skip. Every partition carries one.
    let sparse_idx = ColumnarSegmentReader::read_sparse_index(dir, kek)
        .map_err(|e| partition_corrupt(dir, "sparse_index", SITE, e))?;

    let surviving_blocks = sparse_idx.filter_blocks(p.time_range.0, p.time_range.1, &[]);

    // If sparse index skips blocks and we have surviving blocks, use
    // block-level read. Otherwise read full columns.
    let use_block_read =
        !surviving_blocks.is_empty() && surviving_blocks.len() < sparse_idx.block_count();

    let has_time_range = p.time_range.0 > 0 || p.time_range.1 < i64::MAX;
    // The time column is read whenever a time range or a bucket needs it,
    // named in `needed_columns` or not.
    let time_column = (has_time_range || p.bucket_interval_ms > 0).then_some(resolved.ts_idx);

    let col_data: Vec<Option<ColumnData>> = read_partition_columns(ReadPartitionColumnsParams {
        partition_dir: dir,
        schema_columns: &schema.columns,
        needed_columns: p.needed_columns,
        time_column,
        meta: &meta,
        use_block_read,
        surviving_blocks: &surviving_blocks,
        uring_reader: p.uring_reader,
        io_priority: p.io_priority,
        io_metrics: p.io_metrics,
        kek,
    })?;

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
    // The strategies index every decoded column by row. A column of another
    // length is corruption.
    if let Some((i, column)) = col_data.iter().enumerate().find_map(|(i, c)| {
        c.as_ref()
            .filter(|c| c.len() != effective_row_count)
            .map(|c| (i, c))
    }) {
        return Err(partition_corrupt(
            dir,
            "column",
            SITE,
            format!(
                "column '{}' holds {} rows, the partition holds {effective_row_count}",
                schema.columns[i].0,
                column.len()
            ),
        ));
    }

    // A `.sym` file is written only for a symbol or text column that has a
    // dictionary, so an absent one is no dictionary. A present one that does
    // not read is corruption.
    let mut sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        let needed = p.needed_columns.is_empty() || p.needed_columns.iter().any(|n| n == name);
        if !ty.is_dictionary() || !needed || !dir.join(format!("{name}.sym")).exists() {
            continue;
        }
        let dict = ColumnarSegmentReader::read_symbol_dict(dir, name, kek)
            .map_err(|e| partition_corrupt(dir, "symbol_dict", SITE, e))?;
        sym_dicts.insert(i, dict);
    }

    // Build bitmask over the decoded data.
    // When block-level read was used, data is already filtered to surviving
    // blocks — just need predicate + time range filters on the decoded rows.
    // When full read was used, need time range + sparse skip + predicate.
    let mut mask = if use_block_read {
        // Block-level read already filtered by sparse index.
        // Only need time range filter within surviving blocks.
        if has_time_range {
            let timestamps = time_cells(dir, &col_data, resolved.ts_idx)?;
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, p.time_range.0, p.time_range.1)
        } else {
            simd_filter::bitmask_all(effective_row_count)
        }
    } else {
        let partition_fully_in_range =
            !has_time_range || (meta.min_ts >= p.time_range.0 && meta.max_ts <= p.time_range.1);
        let mut m = if partition_fully_in_range {
            simd_filter::bitmask_all(effective_row_count)
        } else {
            let timestamps = time_cells(dir, &col_data, resolved.ts_idx)?;
            let rt = simd_filter::filter_runtime();
            (rt.range_i64)(timestamps, p.time_range.0, p.time_range.1)
        };
        // Apply sparse index block-level skip on full data.
        grouped_filter::apply_sparse_skip(&mut m, &sparse_idx, p.time_range, effective_row_count);
        m
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
        Some(time_cells(dir, &col_data, resolved.ts_idx)?)
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

/// The time column of a partition read, as millisecond cells.
///
/// `Err` when the column was not decoded or does not hold time cells: the
/// partition is unreadable, and skipping it would drop its rows.
fn time_cells<'a>(
    dir: &Path,
    col_data: &'a [Option<ColumnData>],
    ts_idx: usize,
) -> crate::Result<&'a [i64]> {
    match col_data.get(ts_idx).and_then(Option::as_ref) {
        Some(ColumnData::Timestamp(v)) => Ok(v),
        Some(_) => Err(partition_corrupt(
            dir,
            "column",
            SITE,
            format!("time column {ts_idx} does not hold time cells"),
        )),
        None => Err(partition_corrupt(
            dir,
            "column",
            SITE,
            format!("time column {ts_idx} was not decoded"),
        )),
    }
}

struct ReadPartitionColumnsParams<'a> {
    partition_dir: &'a Path,
    schema_columns: &'a [(String, super::super::columnar_memtable::ColumnType)],
    needed_columns: &'a [String],
    /// A column read whether `needed_columns` names it or not.
    time_column: Option<usize>,
    meta: &'a nodedb_types::timeseries::PartitionMeta,
    use_block_read: bool,
    surviving_blocks: &'a [usize],
    uring_reader: Option<&'a mut crate::data::io::uring_reader::UringReader>,
    io_priority: Option<Priority>,
    io_metrics: Option<&'a IoMetrics>,
    kek: Option<&'a nodedb_wal::crypto::WalEncryptionKey>,
}

/// Read column data for a partition, using io_uring when available.
///
/// With `uring_reader`: batch-reads all needed `.col` files in parallel
/// via io_uring, then decodes each. Without: fadvise + sequential std::fs::read.
///
/// A column that is not needed is `None`. A needed column that does not read
/// is an error: the partition is corrupt.
fn read_partition_columns(
    p: ReadPartitionColumnsParams<'_>,
) -> crate::Result<Vec<Option<ColumnData>>> {
    let ReadPartitionColumnsParams {
        partition_dir,
        schema_columns,
        needed_columns,
        time_column,
        meta,
        use_block_read,
        surviving_blocks,
        uring_reader,
        io_priority,
        io_metrics,
        kek,
    } = p;
    // Determine which schema columns are actually needed.
    let needed_indices: Vec<usize> = schema_columns
        .iter()
        .enumerate()
        .filter(|(i, (name, _))| {
            needed_columns.is_empty()
                || needed_columns.iter().any(|n| n == name)
                || time_column == Some(*i)
        })
        .map(|(i, _)| i)
        .collect();

    // One column read on its own: whole, or its surviving blocks.
    let read_one = |i: usize, blocks: bool| -> crate::Result<ColumnData> {
        let (name, ty) = &schema_columns[i];
        let codec = meta.column_stats.get(name).map(|s| s.codec);
        let read = if blocks {
            ColumnarSegmentReader::read_column_blocks(
                partition_dir,
                name,
                *ty,
                codec,
                surviving_blocks,
                kek,
            )
            .map(|(data, _)| data)
        } else {
            ColumnarSegmentReader::read_column_with_codec(partition_dir, name, *ty, codec, kek)
        };
        read.map_err(|e| partition_corrupt(partition_dir, "column", SITE, e))
    };
    let mut columns: Vec<Option<ColumnData>> = (0..schema_columns.len()).map(|_| None).collect();

    // Block-level reads can't use io_uring batching (need per-block decode).
    // io_uring batching only benefits full-column reads.
    let reader = match uring_reader {
        Some(reader) if !use_block_read => reader,
        _ => {
            // Fallback: fadvise + sequential read.
            crate::data::io::fadvise::prefetch_partition_columns(partition_dir, needed_columns);
            for &i in &needed_indices {
                columns[i] = Some(read_one(i, use_block_read)?);
            }
            return Ok(columns);
        }
    };

    // io_uring path: batch-read all needed .col files in parallel.
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

    // Decode each raw buffer into ColumnData. The batch returns an empty
    // buffer for a file it could not read. That file reads again on its own,
    // so the error names why it does not read.
    for (buf_idx, &schema_idx) in needed_indices.iter().enumerate() {
        let raw = raw_buffers
            .get(buf_idx)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let data = if raw.is_empty() {
            read_one(schema_idx, false)?
        } else {
            let (name, ty) = &schema_columns[schema_idx];
            let codec = meta.column_stats.get(name).map(|s| s.codec);
            ColumnarSegmentReader::decode_column_from_bytes(
                partition_dir,
                name,
                *ty,
                codec,
                raw,
                kek,
            )
            .map_err(|e| partition_corrupt(partition_dir, "column", SITE, e))?
        };
        columns[schema_idx] = Some(data);
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate(dir: &Path) -> crate::Result<Option<GroupedAggResult>> {
        aggregate_partition(PartitionAggParams {
            partition_dir: dir,
            group_by: &[],
            aggregates: &[("count".to_string(), "*".to_string())],
            filters: &[],
            time_range: (0, i64::MAX),
            needed_columns: &[],
            bucket_interval_ms: 0,
            uring_reader: None,
            io_priority: None,
            io_metrics: None,
            kek: None,
        })
    }

    #[test]
    fn a_listed_partition_without_a_directory_refuses_the_aggregate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = aggregate(&dir.path().join("ts-0001"));
        assert!(matches!(
            result,
            Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("directory is missing")
        ));
    }

    #[test]
    fn a_partition_whose_schema_does_not_read_refuses_the_aggregate() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            aggregate(dir.path()),
            Err(crate::Error::SegmentCorrupted { .. })
        ));
    }

    /// A column shorter than the partition's row count refuses the aggregate.
    /// The strategies index it by row, so reading it panics the core.
    #[test]
    fn a_short_column_refuses_the_aggregate() {
        use crate::engine::timeseries::columnar_segment::test_partition::write_ramp_partition;

        let dir = tempfile::tempdir().expect("tempdir");
        write_ramp_partition(dir.path(), "ts-full", 10);
        write_ramp_partition(dir.path(), "ts-short", 3);
        std::fs::copy(
            dir.path().join("ts-short/value.col"),
            dir.path().join("ts-full/value.col"),
        )
        .expect("swap the column");

        let result = aggregate(&dir.path().join("ts-full"));

        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { .. })),
            "{:?}",
            result.map(|_| "aggregated")
        );
    }
}
