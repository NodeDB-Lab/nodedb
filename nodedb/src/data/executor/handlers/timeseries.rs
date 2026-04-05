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
use crate::engine::timeseries::columnar_agg::timestamp_range_filter;
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
    /// Gap-fill strategy. Empty = no gap-fill.
    pub gap_fill: &'a str,
}

// ---------------------------------------------------------------------------
// Aggregate result emission — MessagePack (no serde_json::Value intermediate)
// ---------------------------------------------------------------------------

/// Serialize GroupedAggResult directly to MessagePack bytes.
///
/// Avoids building `Vec<serde_json::Value>` (2M allocations for 2M groups).
/// Writes an array of maps directly to the MessagePack buffer.
///
/// Key format in GroupedAggResult:
/// - GROUP BY only: "group1\0group2"
/// - time_bucket only: "bucket_ts"
/// - time_bucket + GROUP BY: "bucket_ts\0group1\0group2"
fn encode_grouped_results(
    result: &crate::engine::timeseries::grouped_scan::GroupedAggResult,
    group_by: &[String],
    aggregates: &[(String, String)],
    limit: usize,
    bucket_interval_ms: i64,
) -> Vec<u8> {
    let has_bucket = bucket_interval_ms > 0;
    let num_groups = result.groups.len().min(limit);

    // Pre-compute aggregate key names once (not per group).
    let agg_keys: Vec<String> = aggregates
        .iter()
        .map(|(op, field)| format!("{op}_{field}").replace('*', "all"))
        .collect();

    // Fields per row: group_by columns + aggregates + optional bucket.
    let fields_per_row = group_by.len() + aggregates.len() + if has_bucket { 1 } else { 0 };

    // Build result as Vec of lightweight structs for MessagePack encoding.
    // Using rmpv::Value is faster than serde_json::Value (no string interning).
    let mut rows: Vec<rmpv::Value> = Vec::with_capacity(num_groups);

    for (count, (key, accums)) in result.groups.iter().enumerate() {
        if count >= limit {
            break;
        }

        let mut fields: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(fields_per_row);
        let parts: Vec<&str> = key.split('\0').collect();

        if has_bucket {
            let bucket_ts = parts
                .first()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            fields.push((
                rmpv::Value::String("bucket".into()),
                rmpv::Value::Integer(bucket_ts.into()),
            ));

            for (i, field) in group_by.iter().enumerate() {
                let val = parts
                    .get(i + 1)
                    .filter(|s| !s.is_empty())
                    .map(|s| rmpv::Value::String((*s).into()))
                    .unwrap_or(rmpv::Value::Nil);
                fields.push((rmpv::Value::String(field.as_str().into()), val));
            }
        } else {
            for (i, field) in group_by.iter().enumerate() {
                let val = parts
                    .get(i)
                    .filter(|s| !s.is_empty())
                    .map(|s| rmpv::Value::String((*s).into()))
                    .unwrap_or(rmpv::Value::Nil);
                fields.push((rmpv::Value::String(field.as_str().into()), val));
            }
        }

        for (agg_idx, agg_key) in agg_keys.iter().enumerate() {
            let accum = &accums[agg_idx];
            let op = &aggregates[agg_idx].0;
            let val = match op.as_str() {
                "count" => rmpv::Value::Integer((accum.count as i64).into()),
                "sum" if accum.count > 0 => rmpv::Value::F64(accum.sum()),
                "avg" if accum.count > 0 => rmpv::Value::F64(accum.sum() / accum.count as f64),
                "min" if accum.count > 0 => rmpv::Value::F64(accum.min),
                "max" if accum.count > 0 => rmpv::Value::F64(accum.max),
                "first" if accum.count > 0 => rmpv::Value::F64(accum.first()),
                "last" if accum.count > 0 => rmpv::Value::F64(accum.last()),
                "stddev" | "ts_stddev" if accum.count >= 2 => {
                    rmpv::Value::F64(accum.stddev_population())
                }
                _ => rmpv::Value::Nil,
            };
            fields.push((rmpv::Value::String(agg_key.as_str().into()), val));
        }

        rows.push(rmpv::Value::Map(fields));
    }

    let array = rmpv::Value::Array(rows);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
    buf
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
            gap_fill,
        } = params;

        // Lazy-load partition registry from disk if not yet loaded.
        self.ensure_ts_registry(collection);

        let filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(filters).unwrap_or_default()
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
            let row = rmpv::Value::Map(vec![(
                rmpv::Value::String("count_all".into()),
                rmpv::Value::Integer((total as i64).into()),
            )]);
            let array = rmpv::Value::Array(vec![row]);
            let mut json = Vec::new();
            rmpv::encode::write_value(&mut json, &array).unwrap_or(());
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
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
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
                // Try SIMD bitmask path first (faster), fall back to sparse bool.
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

                let row_count = timestamps.len();
                let filtered_indices = if has_filters {
                    // Try SIMD bitmask path first, fall back to sparse bool.
                    if let Some(bitmask) = columnar_filter::eval_filters_bitmask(
                        &part_src,
                        filter_predicates,
                        row_count,
                    ) {
                        nodedb_query::simd_filter::bitmask_to_indices(&bitmask)
                    } else {
                        match columnar_filter::eval_filters_sparse(
                            &part_src,
                            filter_predicates,
                            &indices,
                        ) {
                            Some(mask) => columnar_filter::apply_mask(&indices, &mask),
                            None => indices,
                        }
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

    /// Aggregate mode: GROUP BY / time-bucket across memtable + partitions.
    ///
    /// Uses the grouped_scan engine with:
    /// - SIMD bitmask filters (not Vec<bool>)
    /// - Direct-index Vec for low-cardinality symbol GROUP BY
    /// - FxHashMap<u32> for high-cardinality symbol GROUP BY
    /// - Parallel partition processing via std::thread::scope
    /// - Sparse index block-level skip
    /// - Single metadata read per partition
    /// - String key resolution once per group, not per row
    #[allow(clippy::too_many_arguments)]
    fn execute_ts_aggregate(
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
        use crate::engine::timeseries::grouped_scan::{
            GroupedAggResult, aggregate_memtable, aggregate_partition,
        };

        let num_aggs = aggregates.len();

        // ── Phase 1: Aggregate memtable (on TPC core) ──
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

        // ── Phase 2: Aggregate disk partitions (parallel) ──
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

        // ── Phase 3: Apply gap-fill if requested (bucketed results only) ──
        let merged = if !gap_fill.is_empty() && bucket_interval_ms > 0 {
            super::timeseries_gap_fill::apply_gap_fill_to_grouped(
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

        // ── Phase 4: Encode response (MessagePack, no serde_json intermediate) ──
        let payload =
            encode_grouped_results(&merged, group_by, aggregates, limit, bucket_interval_ms);
        self.response_with_payload(task, payload)
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
                let lvc = self.ts_last_value_caches.get_mut(collection);
                let mut series_keys = HashMap::new();
                let (mut accepted, rejected) =
                    ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc);

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
                        let retry_lvc = self.ts_last_value_caches.get_mut(collection);
                        let (retry_accepted, _) = ilp_ingest::ingest_batch_with_lvc(
                            mt,
                            retry_lines,
                            &mut retry_keys,
                            now_ms,
                            retry_lvc,
                        );
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
