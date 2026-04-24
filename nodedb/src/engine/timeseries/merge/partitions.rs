//! Merge multiple sealed partitions into a single output partition.

use std::path::{Path, PathBuf};

use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::{
    ColumnarSegmentReader, ColumnarSegmentWriter, SegmentError,
};

/// Merge multiple source partitions into one output partition.
///
/// Reads all columns from each source, concatenates them (sorted by timestamp),
/// unifies symbol dictionaries, and writes the result.
pub fn merge_partitions(
    base_dir: &Path,
    source_dirs: &[PathBuf],
    output_name: &str,
) -> Result<MergeResult, SegmentError> {
    if source_dirs.is_empty() {
        return Err(SegmentError::Io("no source partitions to merge".into()));
    }

    // Read schemas — use the first partition's schema as the reference.
    let ref_schema = ColumnarSegmentReader::read_schema(&source_dirs[0])?;

    // Read and concatenate columns from all sources.
    let mut merged_columns: Vec<ColumnData> = ref_schema
        .columns
        .iter()
        .map(|(_, ty)| ColumnData::new_empty(*ty))
        .collect();

    let mut total_rows = 0u64;
    let mut global_min_ts = i64::MAX;
    let mut global_max_ts = i64::MIN;
    let mut total_size = 0u64;

    // Per-column symbol dictionaries: merge as we go.
    let mut merged_dicts: std::collections::HashMap<usize, SymbolDictionary> =
        std::collections::HashMap::new();
    for (i, (_, ty)) in ref_schema.columns.iter().enumerate() {
        if *ty == ColumnType::Symbol {
            merged_dicts.insert(i, SymbolDictionary::new());
        }
    }

    for source_dir in source_dirs {
        let meta = ColumnarSegmentReader::read_meta(source_dir)?;
        total_rows += meta.row_count;
        if meta.min_ts < global_min_ts {
            global_min_ts = meta.min_ts;
        }
        if meta.max_ts > global_max_ts {
            global_max_ts = meta.max_ts;
        }
        total_size += meta.size_bytes;

        // Read partition schema for column mapping (handles schema evolution).
        let part_schema = ColumnarSegmentReader::read_schema(source_dir)?;

        for (i, (col_name, col_type)) in ref_schema.columns.iter().enumerate() {
            // Check if this column exists in the source partition.
            let source_idx = part_schema.columns.iter().position(|(n, _)| n == col_name);

            match source_idx {
                Some(_src_i) => {
                    let col = ColumnarSegmentReader::read_column(source_dir, col_name, *col_type)?;

                    if *col_type == ColumnType::Symbol {
                        // Remap symbol IDs through merged dictionary.
                        let source_dict =
                            ColumnarSegmentReader::read_symbol_dict(source_dir, col_name)?;
                        let merged_dict =
                            merged_dicts
                                .get_mut(&i)
                                .ok_or(SegmentError::Corrupt(format!(
                                    "missing merged dict for column index {i}"
                                )))?;
                        let remap = merged_dict.merge(&source_dict, 100_000);

                        let remapped = remap_symbols(col.as_symbols(), &remap);
                        merged_columns[i].extend_symbols(&remapped);
                    } else {
                        merged_columns[i].extend_from(&col);
                    }
                }
                None => {
                    // Column missing in this source — fill with defaults.
                    let fill_count = meta.row_count as usize;
                    merged_columns[i].extend_nulls(fill_count);
                }
            }
        }
    }

    // Sort by timestamp column.
    let ts_idx = ref_schema.timestamp_idx;
    sort_by_timestamp(&mut merged_columns, ts_idx);

    // Write merged partition.
    let writer = ColumnarSegmentWriter::new(base_dir);

    // Preserve the merged partition's system-time max so retention's
    // bitemporal path sees the true latest write time, not just event-time.
    let max_system_ts = ref_schema
        .ts_system_idx()
        .and_then(|idx| merged_columns.get(idx))
        .map(|col| match col {
            ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.iter().copied().max().unwrap_or(0),
            _ => 0,
        })
        .unwrap_or(0);

    // Build a drain-like result for the writer.
    let drain = crate::engine::timeseries::columnar_memtable::ColumnarDrainResult {
        columns: merged_columns,
        schema: ref_schema.clone(),
        symbol_dicts: merged_dicts,
        row_count: total_rows,
        min_ts: global_min_ts,
        max_ts: global_max_ts,
        max_system_ts,
        series_row_counts: std::collections::HashMap::new(),
    };

    let interval_ms = if global_max_ts > global_min_ts {
        (global_max_ts - global_min_ts) as u64
    } else {
        0
    };

    tracing::debug!(
        output = output_name,
        total_rows,
        total_size,
        sources = source_dirs.len(),
        "merge partition written"
    );

    let meta = writer.write_partition(output_name, &drain, interval_ms, 0)?;

    Ok(MergeResult {
        meta,
        dir_name: output_name.to_string(),
        source_count: source_dirs.len(),
    })
}

/// Remap symbol IDs using a remap table.
fn remap_symbols(ids: &[u32], remap: &[u32]) -> Vec<u32> {
    ids.iter()
        .map(|&id| {
            if (id as usize) < remap.len() {
                remap[id as usize]
            } else {
                u32::MAX // sentinel for unknown
            }
        })
        .collect()
}

/// Sort all columns by the timestamp column (in-place).
fn sort_by_timestamp(columns: &mut [ColumnData], ts_idx: usize) {
    let len = columns[ts_idx].len();
    if len <= 1 {
        return;
    }

    // Build sort indices from timestamp column.
    let timestamps = columns[ts_idx].as_timestamps();
    let mut indices: Vec<usize> = (0..len).collect();
    indices.sort_by_key(|&i| timestamps[i]);

    // Apply permutation to all columns.
    for col in columns.iter_mut() {
        col.apply_permutation(&indices);
    }
}

/// Result of a merge operation.
#[derive(Debug)]
pub struct MergeResult {
    pub meta: PartitionMeta,
    pub dir_name: String,
    pub source_count: usize,
}

// Extension methods for ColumnData needed by merge.
impl ColumnData {
    pub(super) fn new_empty(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Timestamp => Self::Timestamp(Vec::new()),
            ColumnType::Float64 => Self::Float64(Vec::new()),
            ColumnType::Int64 => Self::Int64(Vec::new()),
            ColumnType::Symbol => Self::Symbol(Vec::new()),
        }
    }

    pub(super) fn extend_from(&mut self, other: &ColumnData) {
        match (self, other) {
            (Self::Timestamp(a), Self::Timestamp(b)) => a.extend_from_slice(b),
            (Self::Float64(a), Self::Float64(b)) => a.extend_from_slice(b),
            (Self::Int64(a), Self::Int64(b)) => a.extend_from_slice(b),
            (Self::Symbol(a), Self::Symbol(b)) => a.extend_from_slice(b),
            _ => {} // type mismatch — skip
        }
    }

    pub(super) fn extend_symbols(&mut self, ids: &[u32]) {
        if let Self::Symbol(v) = self {
            v.extend_from_slice(ids);
        }
    }

    pub(super) fn extend_nulls(&mut self, count: usize) {
        match self {
            Self::Timestamp(v) => v.extend(std::iter::repeat_n(0i64, count)),
            Self::Float64(v) => v.extend(std::iter::repeat_n(f64::NAN, count)),
            Self::Int64(v) => v.extend(std::iter::repeat_n(0i64, count)),
            Self::Symbol(v) => v.extend(std::iter::repeat_n(u32::MAX, count)),
            Self::DictEncoded { ids, valid, .. } => {
                ids.extend(std::iter::repeat_n(u32::MAX, count));
                valid.extend(std::iter::repeat_n(false, count));
            }
        }
    }

    pub(super) fn apply_permutation(&mut self, indices: &[usize]) {
        match self {
            Self::Timestamp(v) => permute_vec(v, indices),
            Self::Float64(v) => permute_vec(v, indices),
            Self::Int64(v) => permute_vec(v, indices),
            Self::Symbol(v) => permute_vec(v, indices),
            Self::DictEncoded { ids, valid, .. } => {
                permute_vec(ids, indices);
                permute_vec(valid, indices);
            }
        }
    }
}

/// Apply a permutation to a Vec in-place.
fn permute_vec<T: Clone>(v: &mut Vec<T>, indices: &[usize]) {
    let sorted: Vec<T> = indices.iter().map(|&i| v[i].clone()).collect();
    *v = sorted;
}
