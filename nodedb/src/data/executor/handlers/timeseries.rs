//! Data Plane handlers for timeseries scan and ingest.
//!
//! `execute_timeseries_scan` is the universal timeseries query path.
//! It reads from both the active columnar memtable and sealed disk
//! partitions, supporting three execution modes:
//!
//! 1. **Raw scan**: no aggregation — emit rows directly.
//! 2. **Time-bucket aggregation**: `bucket_interval_ms > 0`.
//! 3. **Generic GROUP BY**: `!aggregates.is_empty() && bucket_interval_ms == 0`.
//!
//! Aggregation uses typed `AggAccum` accumulators merged across all data
//! sources. JSON is constructed once for the final response.

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_agg::{AggAccum, timestamp_range_filter};
use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;
use crate::engine::timeseries::partition_registry::PartitionRegistry;

use std::collections::HashMap;

use super::columnar_filter;

/// Parameters for a timeseries scan operation.
pub(in crate::data::executor) struct TimeseriesScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filters: &'a [u8],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
}

// ---------------------------------------------------------------------------
// Aggregate merge types (typed accumulators, no intermediate JSON)
// ---------------------------------------------------------------------------

/// Merged aggregate result across memtable + partitions.
/// Key is a stringified group key (for cross-partition symbol reconciliation).
type GroupMap = HashMap<String, Vec<AggAccum>>;

/// Build a string group key from column values at a given row index.
/// Within a single source, symbol IDs are resolved to strings here
/// so that cross-partition merging is correct.
fn build_group_key_str<'a>(
    group_by: &[String],
    schema: &[(String, ColumnType)],
    columns: &[Option<&'a ColumnData>],
    sym_dicts: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    row_idx: usize,
) -> String {
    if group_by.is_empty() {
        return String::new(); // single group for whole-table agg
    }
    let mut key = String::with_capacity(group_by.len() * 16);
    for (i, field) in group_by.iter().enumerate() {
        if i > 0 {
            key.push('\0');
        }
        if let Some(col_idx) = schema.iter().position(|(n, _)| n == field)
            && let Some(data) = columns[col_idx]
        {
            match &schema[col_idx].1 {
                ColumnType::Symbol => {
                    if let ColumnData::Symbol(ids) = data {
                        let sym_id = ids[row_idx];
                        if let Some(dict) = sym_dicts(col_idx)
                            && let Some(s) = dict.get(sym_id)
                        {
                            key.push_str(s);
                        }
                    }
                }
                ColumnType::Int64 => {
                    if let ColumnData::Int64(vals) = data {
                        key.push_str(&vals[row_idx].to_string());
                    }
                }
                ColumnType::Float64 => {
                    if let ColumnData::Float64(vals) = data {
                        key.push_str(&vals[row_idx].to_string());
                    }
                }
                ColumnType::Timestamp => {
                    if let ColumnData::Timestamp(vals) = data {
                        key.push_str(&vals[row_idx].to_string());
                    }
                }
            }
        }
    }
    key
}

/// Accumulate one row into the group map.
fn accumulate_row(
    groups: &mut GroupMap,
    key: String,
    aggregates: &[(String, String)],
    schema: &[(String, ColumnType)],
    columns: &[Option<&ColumnData>],
    row_idx: usize,
    num_aggs: usize,
) {
    let accums = groups
        .entry(key)
        .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());

    for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
        if field == "*" {
            accums[agg_idx].feed_count_only();
            continue;
        }
        if let Some(col_idx) = schema.iter().position(|(n, _)| n == field) {
            if let Some(data) = columns[col_idx] {
                let val = match data {
                    ColumnData::Float64(vals) => vals[row_idx],
                    ColumnData::Int64(vals) => vals[row_idx] as f64,
                    ColumnData::Timestamp(vals) => vals[row_idx] as f64,
                    _ => {
                        // Symbol column with count op — just count.
                        if op == "count" {
                            accums[agg_idx].feed_count_only();
                        }
                        continue;
                    }
                };
                if op == "count" {
                    accums[agg_idx].feed_count_only();
                } else {
                    accums[agg_idx].feed(val);
                }
            } else {
                // Column not present in this partition → count as NULL.
                // Don't feed value, but do count if op is count.
                if op == "count" || field == "*" {
                    accums[agg_idx].feed_count_only();
                }
            }
        } else if op == "count" {
            // Field not in schema at all → count NULL.
            accums[agg_idx].feed_count_only();
        }
    }
}

/// Emit final JSON from the merged group map.
fn emit_aggregate_results(
    groups: &GroupMap,
    group_by: &[String],
    aggregates: &[(String, String)],
    limit: usize,
) -> Vec<serde_json::Value> {
    let mut results = Vec::with_capacity(groups.len().min(limit));
    for (key, accums) in groups {
        let mut row = serde_json::Map::new();

        // Parse group key back into field values.
        if !group_by.is_empty() {
            let parts: Vec<&str> = key.split('\0').collect();
            for (i, field) in group_by.iter().enumerate() {
                let val = parts
                    .get(i)
                    .filter(|s| !s.is_empty())
                    .map(|s| serde_json::Value::String(s.to_string()))
                    .unwrap_or(serde_json::Value::Null);
                row.insert(field.clone(), val);
            }
        }

        // Emit aggregate values.
        for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
            let agg_key = format!("{op}_{field}").replace('*', "all");
            let accum = &accums[agg_idx];
            let val = match op.as_str() {
                "count" => serde_json::json!(accum.count),
                "sum" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.sum())
                    }
                }
                "avg" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.sum() / accum.count as f64)
                    }
                }
                "min" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.min)
                    }
                }
                "max" => {
                    if accum.count == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(accum.max)
                    }
                }
                _ => serde_json::Value::Null,
            };
            row.insert(agg_key, val);
        }

        results.push(serde_json::Value::Object(row));
        if results.len() >= limit {
            break;
        }
    }
    results
}

// ---------------------------------------------------------------------------
// Scan handler
// ---------------------------------------------------------------------------

impl CoreLoop {
    /// Execute a timeseries scan — the universal timeseries query path.
    ///
    /// Three modes:
    /// 1. Raw scan: `aggregates.is_empty()` — emit rows.
    /// 2. Time-bucket agg: `bucket_interval_ms > 0` — bucket + aggregate.
    /// 3. Generic GROUP BY: `!aggregates.is_empty()` — group + aggregate.
    ///
    /// All modes read from both memtable and disk partitions.
    pub(in crate::data::executor) fn execute_timeseries_scan(
        &mut self,
        params: TimeseriesScanParams<'_>,
    ) -> Response {
        let TimeseriesScanParams {
            task,
            collection,
            time_range,
            limit,
            filters,
            bucket_interval_ms,
            group_by,
            aggregates,
        } = params;

        // Lazy-load partition registry from disk if not yet loaded.
        self.ensure_ts_registry(collection);

        let filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            rmp_serde::from_slice(filters).unwrap_or_default()
        };
        let has_filters = !filter_predicates.is_empty();
        let is_aggregate = !aggregates.is_empty();
        let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;

        // ── Fast path: COUNT(*) with no GROUP BY, no filters ──
        if !is_aggregate && bucket_interval_ms == 0 && group_by.is_empty() {
            // Raw scan mode — fall through to row emission below.
        } else if is_aggregate
            && bucket_interval_ms == 0
            && group_by.is_empty()
            && !has_filters
            && !has_time_range
            && aggregates.len() == 1
            && aggregates[0].0 == "count"
            && aggregates[0].1 == "*"
        {
            // COUNT(*) metadata fast path — zero I/O.
            let mut total: u64 = 0;
            if let Some(mt) = self.columnar_memtables.get(collection) {
                total += mt.row_count();
            }
            if let Some(registry) = self.ts_registries.get(collection) {
                let query_range =
                    nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
                for entry in registry.query_partitions(&query_range) {
                    total += entry.meta.row_count;
                }
            }
            let results = vec![serde_json::json!({"count_all": total})];
            let json = serde_json::to_vec(&results).unwrap_or_default();
            return Response {
                request_id: task.request.request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: Payload::from_vec(json),
                watermark_lsn: self.watermark,
                error_code: None,
            };
        }

        // ── Determine needed columns (projection pushdown) ──
        let needed_columns: Vec<String> = if is_aggregate || bucket_interval_ms > 0 {
            let mut needed: Vec<String> = vec!["timestamp".to_string()];
            for g in group_by {
                if !needed.contains(g) {
                    needed.push(g.clone());
                }
            }
            for (_, field) in aggregates {
                if field != "*" && !needed.contains(field) {
                    needed.push(field.clone());
                }
            }
            // Add filter fields.
            for fp in &filter_predicates {
                if !needed.contains(&fp.field) {
                    needed.push(fp.field.clone());
                }
            }
            needed
        } else {
            Vec::new() // empty = read all columns
        };

        // ── Mode dispatch ──
        if is_aggregate || bucket_interval_ms > 0 {
            self.execute_ts_aggregate(
                task,
                collection,
                time_range,
                limit,
                &filter_predicates,
                has_filters,
                bucket_interval_ms,
                group_by,
                aggregates,
                &needed_columns,
            )
        } else {
            self.execute_ts_raw_scan(
                task,
                collection,
                time_range,
                limit,
                &filter_predicates,
                has_filters,
            )
        }
    }

    /// Raw scan mode: emit rows from memtable + partitions.
    fn execute_ts_raw_scan(
        &self,
        task: &ExecutionTask,
        collection: &str,
        time_range: (i64, i64),
        limit: usize,
        filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
        has_filters: bool,
    ) -> Response {
        let mut results = Vec::new();

        // 1. Read from memtable.
        if let Some(mt) = self.columnar_memtables.get(collection)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let timestamps = mt.column(schema.timestamp_idx).as_timestamps();
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            let (filtered_indices, need_json_filter) = if has_filters {
                match columnar_filter::eval_filters_sparse(mt, filter_predicates, &indices) {
                    Some(mask) => (columnar_filter::apply_mask(&indices, &mask), false),
                    None => (indices, true),
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
                let mut row = serde_json::Map::new();
                for (col_idx, col_name, col_type, col_data) in &columns {
                    let val = super::columnar_read::emit_column_value(
                        mt,
                        *col_idx,
                        col_type,
                        col_data,
                        idx as usize,
                    );
                    row.insert(col_name.to_string(), val);
                }
                let row_val = serde_json::Value::Object(row);
                if need_json_filter && !filter_predicates.iter().all(|f| f.matches(&row_val)) {
                    continue;
                }
                results.push(row_val);
            }
        }

        // 2. Read from disk partitions.
        if let Some(registry) = self.ts_registries.get(collection) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            for entry in registry.query_partitions(&query_range) {
                if results.len() >= limit {
                    break;
                }
                let part_dir = self
                    .data_dir
                    .join("ts")
                    .join(collection)
                    .join(&entry.dir_name);
                if !part_dir.exists() {
                    continue;
                }

                let schema = match ColumnarSegmentReader::read_schema(&part_dir) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let col_data: Vec<Option<ColumnData>> = schema
                    .columns
                    .iter()
                    .map(|(name, ty)| ColumnarSegmentReader::read_column(&part_dir, name, *ty).ok())
                    .collect();

                let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
                    .filter_map(|(i, (name, _))| {
                        ColumnarSegmentReader::read_symbol_dict(&part_dir, name)
                            .ok()
                            .map(|dict| (i, dict))
                    })
                    .collect();

                let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
                let Some(ts_col) = ts_col else { continue };
                let timestamps = ts_col.as_timestamps();
                let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

                let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
                let part_src = columnar_filter::PartitionColumns {
                    schema: &schema_vec,
                    columns: &col_data,
                    sym_dicts: &sym_dicts,
                };

                let filtered_indices = if has_filters {
                    match columnar_filter::eval_filters_sparse(
                        &part_src,
                        filter_predicates,
                        &indices,
                    ) {
                        Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                        None => indices,
                    }
                } else {
                    indices
                };

                for &idx in &filtered_indices {
                    if results.len() >= limit {
                        break;
                    }
                    let mut row = serde_json::Map::new();
                    for (col_i, (col_name, col_type)) in schema_vec.iter().enumerate() {
                        let Some(data) = &col_data[col_i] else {
                            continue;
                        };
                        let val = match col_type {
                            ColumnType::Timestamp => serde_json::Value::Number(
                                serde_json::Number::from(data.as_timestamps()[idx as usize]),
                            ),
                            ColumnType::Float64 => {
                                let v = data.as_f64()[idx as usize];
                                serde_json::Number::from_f64(v)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            }
                            ColumnType::Int64 => {
                                if let ColumnData::Int64(vals) = data {
                                    serde_json::Value::Number(serde_json::Number::from(
                                        vals[idx as usize],
                                    ))
                                } else {
                                    serde_json::Value::Null
                                }
                            }
                            ColumnType::Symbol => {
                                if let ColumnData::Symbol(ids) = data {
                                    sym_dicts
                                        .get(&col_i)
                                        .and_then(|dict| dict.get(ids[idx as usize]))
                                        .map(|s| serde_json::Value::String(s.to_string()))
                                        .unwrap_or(serde_json::Value::Null)
                                } else {
                                    serde_json::Value::Null
                                }
                            }
                        };
                        row.insert(col_name.clone(), val);
                    }
                    results.push(serde_json::Value::Object(row));
                }
            }
        }

        let json = serde_json::to_vec(&results).unwrap_or_default();
        self.response_with_payload(task, json)
    }

    /// Aggregate mode: time-bucket or generic GROUP BY across memtable + partitions.
    ///
    /// Uses typed `AggAccum` accumulators merged across all sources.
    /// JSON constructed once at the end.
    #[allow(clippy::too_many_arguments)]
    fn execute_ts_aggregate(
        &self,
        task: &ExecutionTask,
        collection: &str,
        time_range: (i64, i64),
        limit: usize,
        filter_predicates: &[crate::bridge::scan_filter::ScanFilter],
        has_filters: bool,
        bucket_interval_ms: i64,
        group_by: &[String],
        aggregates: &[(String, String)],
        needed_columns: &[String],
    ) -> Response {
        let num_aggs = aggregates.len();
        let is_time_bucket = bucket_interval_ms > 0;

        // For time-bucket mode, use BTreeMap<bucket_ts, AggAccum> for the
        // value aggregate. For generic GROUP BY, use GroupMap.
        let mut bucket_map: std::collections::BTreeMap<i64, Vec<AggAccum>> =
            std::collections::BTreeMap::new();
        let mut group_map: GroupMap = HashMap::new();

        // ── Aggregate from memtable ──
        if let Some(mt) = self.columnar_memtables.get(collection)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let timestamps = mt.column(schema.timestamp_idx).as_timestamps();
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            let passing_indices = if has_filters {
                match columnar_filter::eval_filters_sparse(mt, filter_predicates, &indices) {
                    Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                    None => indices,
                }
            } else {
                indices
            };

            let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
            let col_refs: Vec<Option<&ColumnData>> = (0..schema.columns.len())
                .map(|i| Some(mt.column(i)))
                .collect();
            let sym_lookup = |col_idx: usize| mt.symbol_dict(col_idx);

            for &idx in &passing_indices {
                let row_idx = idx as usize;

                if is_time_bucket {
                    let bucket_key = crate::engine::timeseries::time_bucket::time_bucket(
                        bucket_interval_ms,
                        timestamps[row_idx],
                    );
                    let key = if group_by.is_empty() {
                        String::new()
                    } else {
                        build_group_key_str(group_by, &schema_vec, &col_refs, &sym_lookup, row_idx)
                    };
                    let composite_key = format!("{bucket_key}\x01{key}");
                    let accums = bucket_map
                        .entry(bucket_key)
                        .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
                    // Accumulate aggregates.
                    for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
                        if field == "*" {
                            accums[agg_idx].feed_count_only();
                        } else if let Some(ci) = schema_vec.iter().position(|(n, _)| n == field)
                            && let Some(data) = col_refs[ci]
                        {
                            let val = match data {
                                ColumnData::Float64(vals) => vals[row_idx],
                                ColumnData::Int64(vals) => vals[row_idx] as f64,
                                ColumnData::Timestamp(vals) => vals[row_idx] as f64,
                                _ => {
                                    if op == "count" {
                                        accums[agg_idx].feed_count_only();
                                    }
                                    continue;
                                }
                            };
                            if op == "count" {
                                accums[agg_idx].feed_count_only();
                            } else {
                                accums[agg_idx].feed(val);
                            }
                        }
                    }
                    drop(composite_key);
                } else {
                    // Generic GROUP BY.
                    let key =
                        build_group_key_str(group_by, &schema_vec, &col_refs, &sym_lookup, row_idx);
                    accumulate_row(
                        &mut group_map,
                        key,
                        aggregates,
                        &schema_vec,
                        &col_refs,
                        row_idx,
                        num_aggs,
                    );
                }
            }
        }

        // ── Aggregate from disk partitions ──
        if let Some(registry) = self.ts_registries.get(collection) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);

            for entry in registry.query_partitions(&query_range) {
                let part_dir = self
                    .data_dir
                    .join("ts")
                    .join(collection)
                    .join(&entry.dir_name);
                if !part_dir.exists() {
                    continue;
                }

                let schema = match ColumnarSegmentReader::read_schema(&part_dir) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                // Projection pushdown: only read needed columns.
                let col_data: Vec<Option<ColumnData>> = schema
                    .columns
                    .iter()
                    .map(|(name, ty)| {
                        if needed_columns.is_empty() || needed_columns.iter().any(|n| n == name) {
                            ColumnarSegmentReader::read_column(&part_dir, name, *ty).ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
                    .filter_map(|(i, (name, _))| {
                        if needed_columns.is_empty() || needed_columns.iter().any(|n| n == name) {
                            ColumnarSegmentReader::read_symbol_dict(&part_dir, name)
                                .ok()
                                .map(|dict| (i, dict))
                        } else {
                            None
                        }
                    })
                    .collect();

                let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
                let Some(ts_col) = ts_col else { continue };
                let timestamps = ts_col.as_timestamps();
                let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

                let schema_vec: Vec<(String, ColumnType)> = schema.columns.clone();
                let part_src = columnar_filter::PartitionColumns {
                    schema: &schema_vec,
                    columns: &col_data,
                    sym_dicts: &sym_dicts,
                };

                let passing_indices = if has_filters {
                    match columnar_filter::eval_filters_sparse(
                        &part_src,
                        filter_predicates,
                        &indices,
                    ) {
                        Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                        None => indices,
                    }
                } else {
                    indices
                };

                let col_refs: Vec<Option<&ColumnData>> =
                    col_data.iter().map(|c| c.as_ref()).collect();
                let sym_lookup =
                    |col_idx: usize| -> Option<&nodedb_types::timeseries::SymbolDictionary> {
                        sym_dicts.get(&col_idx)
                    };

                for &idx in &passing_indices {
                    let row_idx = idx as usize;

                    if is_time_bucket {
                        let bucket_key = crate::engine::timeseries::time_bucket::time_bucket(
                            bucket_interval_ms,
                            timestamps[row_idx],
                        );
                        let accums = bucket_map.entry(bucket_key).or_insert_with(|| {
                            (0..num_aggs).map(|_| AggAccum::default()).collect()
                        });
                        for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
                            if field == "*" {
                                accums[agg_idx].feed_count_only();
                            } else if let Some(ci) = schema_vec.iter().position(|(n, _)| n == field)
                                && let Some(data) = col_refs[ci]
                            {
                                let val = match data {
                                    ColumnData::Float64(vals) => vals[row_idx],
                                    ColumnData::Int64(vals) => vals[row_idx] as f64,
                                    ColumnData::Timestamp(vals) => vals[row_idx] as f64,
                                    _ => {
                                        if op == "count" {
                                            accums[agg_idx].feed_count_only();
                                        }
                                        continue;
                                    }
                                };
                                if op == "count" {
                                    accums[agg_idx].feed_count_only();
                                } else {
                                    accums[agg_idx].feed(val);
                                }
                            }
                        }
                    } else {
                        let key = build_group_key_str(
                            group_by,
                            &schema_vec,
                            &col_refs,
                            &sym_lookup,
                            row_idx,
                        );
                        accumulate_row(
                            &mut group_map,
                            key,
                            aggregates,
                            &schema_vec,
                            &col_refs,
                            row_idx,
                            num_aggs,
                        );
                    }
                }
            }
        }

        // ── Emit results ──
        let results = if is_time_bucket {
            let mut out = Vec::with_capacity(bucket_map.len().min(limit));
            for (bucket_ts, accums) in &bucket_map {
                let mut row = serde_json::Map::new();
                row.insert("bucket".into(), serde_json::json!(bucket_ts));
                for (agg_idx, (op, field)) in aggregates.iter().enumerate() {
                    let agg_key = format!("{op}_{field}").replace('*', "all");
                    let accum = &accums[agg_idx];
                    let val = match op.as_str() {
                        "count" => serde_json::json!(accum.count),
                        "sum" => serde_json::json!(accum.sum()),
                        "avg" => {
                            if accum.count == 0 {
                                serde_json::Value::Null
                            } else {
                                serde_json::json!(accum.sum() / accum.count as f64)
                            }
                        }
                        "min" => serde_json::json!(accum.min),
                        "max" => serde_json::json!(accum.max),
                        _ => serde_json::Value::Null,
                    };
                    row.insert(agg_key, val);
                }
                out.push(serde_json::Value::Object(row));
                if out.len() >= limit {
                    break;
                }
            }
            out
        } else {
            emit_aggregate_results(&group_map, group_by, aggregates, limit)
        };

        let json = serde_json::to_vec(&results).unwrap_or_default();
        self.response_with_payload(task, json)
    }

    /// Ensure the partition registry is loaded for a timeseries collection.
    ///
    /// On first access, scans the `ts/{collection}/` directory for existing
    /// partition directories and populates the registry from partition metadata.
    fn ensure_ts_registry(&mut self, collection: &str) {
        if self.ts_registries.contains_key(collection) {
            return;
        }
        let ts_dir = self.data_dir.join("ts").join(collection);
        if !ts_dir.exists() {
            return;
        }

        let mut registry = PartitionRegistry::new(
            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
        );

        // Scan for existing partition directories.
        if let Ok(entries) = std::fs::read_dir(&ts_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name_str) = name.to_str() else {
                    continue;
                };
                if !name_str.starts_with("ts-") || !entry.path().is_dir() {
                    continue;
                }
                // Read partition metadata and import directly.
                // We use import() instead of get_or_create_partition()
                // because the latter aligns to boundary intervals, creating
                // a key mismatch with the raw min_ts.
                let meta_path = entry.path().join("partition.meta");
                if let Ok(meta_bytes) = std::fs::read(&meta_path)
                    && let Ok(meta) =
                        sonic_rs::from_slice::<nodedb_types::timeseries::PartitionMeta>(&meta_bytes)
                {
                    let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                        meta,
                        dir_name: name_str.to_string(),
                    };
                    registry.import(vec![(pe.meta.min_ts, pe)]);
                }
            }
        }

        if registry.partition_count() > 0 {
            tracing::info!(
                collection,
                partitions = registry.partition_count(),
                "loaded partition registry from disk"
            );
        }
        self.ts_registries.insert(collection.to_string(), registry);
    }

    /// Execute a timeseries ingest.
    ///
    /// `wal_lsn` is set by the WAL catch-up task to enable deduplication:
    /// if the record has already been ingested (LSN <= max ingested) or
    /// flushed to disk (LSN <= max flushed), the ingest is skipped.
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        format: &str,
        wal_lsn: Option<u64>,
    ) -> Response {
        // LSN-based deduplication: only skip records that are provably
        // already flushed to sealed disk partitions. We do NOT track a
        // max-ingested-LSN watermark because SPSC drops create gaps —
        // LSN 5 ingested does not mean LSNs 1-4 were also ingested.
        if let Some(lsn) = wal_lsn
            && let Some(registry) = self.ts_registries.get(collection)
        {
            let max_flushed = registry
                .iter()
                .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                .max()
                .unwrap_or(0);
            if max_flushed > 0 && lsn <= max_flushed {
                let result = serde_json::json!({
                    "accepted": 0,
                    "rejected": 0,
                    "collection": collection,
                    "dedup_skipped": true,
                });
                let json = serde_json::to_vec(&result).unwrap_or_default();
                return Response {
                    request_id: task.request.request_id,
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(json),
                    watermark_lsn: self.watermark,
                    error_code: None,
                };
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match format {
            "ilp" => {
                let input = match std::str::from_utf8(payload) {
                    Ok(s) => s,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("invalid UTF-8 in ILP: {e}"),
                            },
                        );
                    }
                };

                let lines: Vec<_> = ilp::parse_batch(input)
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .collect();

                if lines.is_empty() {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "no valid ILP lines in payload".into(),
                        },
                    );
                }

                // Ensure memtable exists (auto-create on first write).
                let is_new_memtable = !self.columnar_memtables.contains_key(collection);
                if is_new_memtable {
                    let schema = ilp_ingest::infer_schema(&lines);
                    let config = ColumnarMemtableConfig {
                        max_memory_bytes: 64 * 1024 * 1024,
                        hard_memory_limit: 80 * 1024 * 1024,
                        max_tag_cardinality: 100_000,
                    };
                    let mt = ColumnarMemtable::new(schema, config);
                    self.columnar_memtables.insert(collection.to_string(), mt);
                }

                // Schema evolution: detect new fields and expand memtable schema.
                // Track column count before evolution to detect changes.
                let cols_before = if !is_new_memtable {
                    self.columnar_memtables
                        .get(collection)
                        .map(|mt| mt.schema().columns.len())
                        .unwrap_or(0)
                } else {
                    0
                };
                if !is_new_memtable && let Some(mt) = self.columnar_memtables.get_mut(collection) {
                    ilp_ingest::evolve_schema(mt, &lines);
                }
                let schema_changed = !is_new_memtable
                    && self
                        .columnar_memtables
                        .get(collection)
                        .is_some_and(|mt| mt.schema().columns.len() != cols_before);

                // Pre-flush: flush BEFORE ingesting if memtable is at the soft
                // limit. Without this, the batch pushes the memtable past
                // the hard_memory_limit (80MB), rows are rejected mid-batch,
                // and end up in WAL but never in the memtable — causing
                // silent undercounting on all queries until restart.
                if let Some(mt) = self.columnar_memtables.get(collection)
                    && mt.memory_bytes() >= 64 * 1024 * 1024
                {
                    self.flush_ts_collection(collection, now_ms);
                }

                let Some(mt) = self.columnar_memtables.get_mut(collection) else {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("memtable missing after init: {collection}"),
                        },
                    );
                };
                let mut series_keys = HashMap::new();
                let (mut accepted, rejected) =
                    ilp_ingest::ingest_batch(mt, &lines, &mut series_keys, now_ms);

                // If rows were rejected (memtable hit hard limit), flush and
                // re-ingest the rejected portion. This prevents silent data
                // loss when batches arrive faster than the flush pipeline.
                if rejected > 0 {
                    tracing::warn!(
                        collection,
                        accepted,
                        rejected,
                        "ILP batch rows rejected by hard limit, flushing and retrying"
                    );
                    self.flush_ts_collection(collection, now_ms);
                    if let Some(mt) = self.columnar_memtables.get_mut(collection) {
                        let mut retry_keys = HashMap::new();
                        let retry_lines = &lines[accepted..];
                        let (retry_accepted, _) =
                            ilp_ingest::ingest_batch(mt, retry_lines, &mut retry_keys, now_ms);
                        accepted += retry_accepted;
                    }
                }

                // Post-flush: standard 64MB threshold check.
                let Some(mt) = self.columnar_memtables.get(collection) else {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("memtable missing after ingest: {collection}"),
                        },
                    );
                };
                if mt.memory_bytes() >= 64 * 1024 * 1024 {
                    self.flush_ts_collection(collection, now_ms);
                }

                // Track WAL LSN and last ingest time for dedup + idle flush.
                if accepted > 0 {
                    if let Some(lsn) = wal_lsn {
                        let entry = self
                            .ts_max_ingested_lsn
                            .entry(collection.to_string())
                            .or_insert(0);
                        *entry = (*entry).max(lsn);
                    }
                    self.last_ts_ingest = Some(std::time::Instant::now());
                }

                self.checkpoint_coordinator
                    .mark_dirty("timeseries", accepted);
                // Include schema_columns when schema is new OR evolved,
                // so the ILP listener can propagate changes to the catalog.
                let include_schema = is_new_memtable || schema_changed;
                let result =
                    if include_schema && let Some(mt) = self.columnar_memtables.get(collection) {
                        let schema_columns: Vec<serde_json::Value> = mt
                            .schema()
                            .columns
                            .iter()
                            .map(|(name, col_type)| {
                                let type_str = match col_type {
                                    ColumnType::Timestamp => "TIMESTAMP",
                                    ColumnType::Float64 => "FLOAT",
                                    ColumnType::Int64 => "BIGINT",
                                    ColumnType::Symbol => "VARCHAR",
                                };
                                serde_json::json!([name, type_str])
                            })
                            .collect();
                        serde_json::json!({
                            "accepted": accepted,
                            "rejected": rejected,
                            "collection": collection,
                            "schema_columns": schema_columns,
                        })
                    } else {
                        serde_json::json!({
                            "accepted": accepted,
                            "rejected": rejected,
                            "collection": collection,
                        })
                    };
                let json = serde_json::to_vec(&result).unwrap_or_default();
                Response {
                    request_id: task.request.request_id,
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(json),
                    watermark_lsn: self.watermark,
                    error_code: None,
                }
            }
            _ => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("unknown ingest format: {format}"),
                },
            ),
        }
    }

    /// Flush a timeseries collection's memtable to L1 segments.
    ///
    /// Drains the columnar memtable, writes segments via `ColumnarSegmentWriter`,
    /// registers the new partition in `ts_registries`, and fires the continuous
    /// aggregate hook.
    pub(in crate::data::executor) fn flush_ts_collection(&mut self, collection: &str, now_ms: i64) {
        let Some(mt) = self.columnar_memtables.get_mut(collection) else {
            return;
        };
        if mt.is_empty() {
            return;
        }

        let drain = mt.drain();

        // Write to L1 segments.
        let segment_dir = self.data_dir.join(format!("ts/{collection}"));
        let writer =
            crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter::new(&segment_dir);
        let partition_name = format!("ts-{}_{}", drain.min_ts, drain.max_ts);

        // Use the max ingested WAL LSN for this collection so the partition
        // records which WAL records have been flushed. This enables both
        // WAL GC and LSN-based dedup during startup replay and catch-up.
        let flush_wal_lsn = self
            .ts_max_ingested_lsn
            .get(collection)
            .copied()
            .unwrap_or(0);
        match writer.write_partition(&partition_name, &drain, 0, flush_wal_lsn) {
            Ok(meta) => {
                tracing::info!(
                    collection,
                    rows = meta.row_count,
                    "timeseries columnar flush complete"
                );

                // Register partition in ts_registries via direct import.
                // Avoids get_or_create_partition which aligns to boundary
                // intervals and creates a key mismatch with update_meta.
                let registry = self
                    .ts_registries
                    .entry(collection.to_string())
                    .or_insert_with(|| {
                        PartitionRegistry::new(
                            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                        )
                    });
                let mut reg_meta = meta;
                reg_meta.min_ts = drain.min_ts;
                reg_meta.max_ts = drain.max_ts;
                reg_meta.state = nodedb_types::timeseries::PartitionState::Sealed;
                let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                    meta: reg_meta,
                    dir_name: partition_name,
                };
                registry.import(vec![(drain.min_ts, pe)]);
            }
            Err(e) => {
                tracing::error!(
                    collection,
                    error = %e,
                    "timeseries columnar flush failed"
                );
                return;
            }
        }

        // Fire continuous aggregate hook.
        let refreshed = self.continuous_agg_mgr.on_flush(collection, &drain, now_ms);
        if !refreshed.is_empty() {
            tracing::debug!(
                collection,
                aggregates = ?refreshed,
                "continuous aggregates refreshed on flush"
            );
        }
    }
}
