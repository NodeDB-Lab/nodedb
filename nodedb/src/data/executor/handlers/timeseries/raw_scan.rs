//! Raw scan mode: emit rows from memtable + disk partitions.
//!
//! No aggregation — returns individual rows as MessagePack.

use std::collections::HashMap;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;

use super::super::columnar_filter;

impl CoreLoop {
    /// Raw scan mode: emit rows from memtable + partitions.
    pub(in crate::data::executor) fn execute_ts_raw_scan(
        &self,
        task: &ExecutionTask,
        collection: &str,
        time_range: (i64, i64),
        limit: usize,
        filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
        has_filters: bool,
    ) -> Response {
        let mut results: Vec<rmpv::Value> = Vec::new();

        // 1. Read from memtable.
        if let Some(mt) = self.columnar_memtables.get(collection)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let timestamps = mt.column(schema.timestamp_idx).as_timestamps();
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            let (filtered_indices, need_json_filter) = if has_filters {
                let row_count = mt.row_count() as usize;
                if let Some(bitmask) =
                    columnar_filter::eval_filters_bitmask(mt, filter_predicates, row_count)
                {
                    let bm_indices = nodedb_query::simd_filter::bitmask_to_indices(&bitmask);
                    (bm_indices, false)
                } else {
                    match columnar_filter::eval_filters_sparse(mt, filter_predicates, &indices) {
                        Some(mask) => (columnar_filter::apply_mask(&indices, &mask), false),
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
                let row = emit_memtable_row(mt, &columns, idx as usize);
                if need_json_filter {
                    // Fallback: build serde_json::Value for complex filter eval.
                    let json_row = rmpv_to_json_value(&row);
                    if !filter_predicates.iter().all(|f| f.matches(&json_row)) {
                        continue;
                    }
                }
                results.push(row);
            }
        }

        // 2. Read from disk partitions.
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

                let remaining = limit.saturating_sub(results.len());
                if remaining > 0 && !partition_dirs.is_empty() {
                    let partition_rows = scan_partitions_parallel(
                        &partition_dirs,
                        time_range,
                        remaining,
                        filter_predicates,
                        has_filters,
                    );
                    results.extend(partition_rows);
                    results.truncate(limit);
                }
            }
        }

        let array = rmpv::Value::Array(results);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
        self.response_with_payload(task, buf)
    }
}

// ---------------------------------------------------------------------------
// Parallel partition scan for raw mode
// ---------------------------------------------------------------------------

/// Scan disk partitions in parallel, returning rmpv rows sorted by timestamp.
fn scan_partitions_parallel(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    if partition_dirs.len() <= 1 {
        return partition_dirs
            .first()
            .map(|dir| scan_one_partition(dir, time_range, limit, filter_predicates, has_filters))
            .unwrap_or_default();
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
            );
        }

        let chunk_size = partition_dirs.len().div_ceil(thread_count);
        let filters_ref = filter_predicates;

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
                        )
                    })
                })
                .collect();

            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        });

        // Merge: each thread's results are already time-sorted (partitions are
        // time-ordered). Flatten, sort globally, truncate to limit.
        let total: usize = thread_results.iter().map(|v| v.len()).sum();
        let mut merged = Vec::with_capacity(total.min(limit));
        for batch in &mut thread_results {
            merged.append(batch);
        }
        // Sort by timestamp (first field in each row map).
        merged.sort_by_key(|row| extract_timestamp(row));
        merged.truncate(limit);
        merged
    }

    #[cfg(target_arch = "wasm32")]
    {
        scan_partitions_sequential(
            partition_dirs,
            time_range,
            limit,
            filter_predicates,
            has_filters,
        )
    }
}

fn scan_partitions_sequential(
    partition_dirs: &[std::path::PathBuf],
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    let mut results = Vec::new();
    for dir in partition_dirs {
        if results.len() >= limit {
            break;
        }
        let remaining = limit - results.len();
        let rows = scan_one_partition(dir, time_range, remaining, filter_predicates, has_filters);
        results.extend(rows);
    }
    results.truncate(limit);
    results
}

/// Scan a single disk partition, returning rmpv rows.
fn scan_one_partition(
    part_dir: &std::path::Path,
    time_range: (i64, i64),
    limit: usize,
    filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
    has_filters: bool,
) -> Vec<rmpv::Value> {
    let schema = match ColumnarSegmentReader::read_schema(part_dir) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let col_data: Vec<Option<ColumnData>> = schema
        .columns
        .iter()
        .map(|(name, ty)| ColumnarSegmentReader::read_column(part_dir, name, *ty).ok())
        .collect();

    let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
        .filter_map(|(i, (name, _))| {
            ColumnarSegmentReader::read_symbol_dict(part_dir, name)
                .ok()
                .map(|dict| (i, dict))
        })
        .collect();

    let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
    let Some(ts_col) = ts_col else {
        return Vec::new();
    };
    let timestamps = ts_col.as_timestamps();
    let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

    let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
    let part_src = columnar_filter::PartitionColumns {
        schema: &schema_vec,
        columns: &col_data,
        sym_dicts: &sym_dicts,
    };

    let row_count = timestamps.len();
    let filtered_indices = if has_filters {
        if let Some(bitmask) =
            columnar_filter::eval_filters_bitmask(&part_src, filter_predicates, row_count)
        {
            nodedb_query::simd_filter::bitmask_to_indices(&bitmask)
        } else {
            match columnar_filter::eval_filters_sparse(&part_src, filter_predicates, &indices) {
                Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                None => indices,
            }
        }
    } else {
        indices
    };

    let mut rows = Vec::with_capacity(filtered_indices.len().min(limit));
    for &idx in &filtered_indices {
        if rows.len() >= limit {
            break;
        }
        let row = emit_partition_row(&schema_vec, &col_data, &sym_dicts, idx as usize);
        rows.push(row);
    }
    rows
}

// ---------------------------------------------------------------------------
// Row emission helpers — build rmpv::Value directly
// ---------------------------------------------------------------------------

/// Emit a single row from the memtable as rmpv::Value::Map.
fn emit_memtable_row(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    columns: &[(usize, &String, &ColumnType, &ColumnData)],
    idx: usize,
) -> rmpv::Value {
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(columns.len());
    for (col_idx, col_name, col_type, col_data) in columns {
        let val =
            super::super::columnar_read::emit_column_value(mt, *col_idx, col_type, col_data, idx);
        fields.push((
            rmpv::Value::String(col_name.as_str().into()),
            json_to_rmpv(&val),
        ));
    }
    rmpv::Value::Map(fields)
}

/// Emit a single row from a disk partition as rmpv::Value::Map.
fn emit_partition_row(
    schema: &[(String, ColumnType)],
    col_data: &[Option<ColumnData>],
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> rmpv::Value {
    let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(schema.len());
    for (col_i, (col_name, col_type)) in schema.iter().enumerate() {
        let Some(data) = &col_data[col_i] else {
            continue;
        };
        let val = match col_type {
            ColumnType::Timestamp => rmpv::Value::Integer(data.as_timestamps()[idx].into()),
            ColumnType::Float64 => {
                let v = data.as_f64()[idx];
                if v.is_nan() {
                    rmpv::Value::Nil
                } else {
                    rmpv::Value::F64(v)
                }
            }
            ColumnType::Int64 => {
                if let ColumnData::Int64(vals) = data {
                    rmpv::Value::Integer(vals[idx].into())
                } else {
                    rmpv::Value::Nil
                }
            }
            ColumnType::Symbol => {
                if let ColumnData::Symbol(ids) = data {
                    sym_dicts
                        .get(&col_i)
                        .and_then(|dict| dict.get(ids[idx]))
                        .map(|s| rmpv::Value::String(s.into()))
                        .unwrap_or(rmpv::Value::Nil)
                } else {
                    rmpv::Value::Nil
                }
            }
        };
        fields.push((rmpv::Value::String(col_name.as_str().into()), val));
    }
    rmpv::Value::Map(fields)
}

/// Extract timestamp from a row (first integer field) for sort-merge.
fn extract_timestamp(row: &rmpv::Value) -> i64 {
    if let rmpv::Value::Map(fields) = row {
        for (_, v) in fields {
            if let rmpv::Value::Integer(n) = v {
                return n.as_i64().unwrap_or(0);
            }
        }
    }
    0
}

/// Convert rmpv::Value row to serde_json::Value (fallback for complex filter eval).
fn rmpv_to_json_value(row: &rmpv::Value) -> serde_json::Value {
    match row {
        rmpv::Value::Map(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => continue,
                };
                let val = match v {
                    rmpv::Value::Integer(n) => {
                        if let Some(i) = n.as_i64() {
                            serde_json::Value::Number(i.into())
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    rmpv::Value::String(s) => {
                        serde_json::Value::String(s.as_str().unwrap_or("").to_string())
                    }
                    rmpv::Value::Nil => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                map.insert(key, val);
            }
            serde_json::Value::Object(map)
        }
        _ => serde_json::Value::Null,
    }
}

/// Convert serde_json::Value to rmpv::Value.
fn json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(i.into())
            } else if let Some(f) = n.as_f64() {
                rmpv::Value::F64(f)
            } else {
                rmpv::Value::Nil
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(s.as_str().into()),
        serde_json::Value::Array(arr) => rmpv::Value::Array(arr.iter().map(json_to_rmpv).collect()),
        serde_json::Value::Object(map) => {
            let fields = map
                .iter()
                .map(|(k, v)| (rmpv::Value::String(k.as_str().into()), json_to_rmpv(v)))
                .collect();
            rmpv::Value::Map(fields)
        }
    }
}
