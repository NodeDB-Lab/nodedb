//! Data Plane handlers for timeseries scan and ingest.

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_agg::{aggregate_by_time_bucket, timestamp_range_filter};
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

use std::collections::HashMap;

/// Parameters for a timeseries scan operation.
pub(in crate::data::executor) struct TimeseriesScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filters: &'a [u8],
    pub bucket_interval_ms: i64,
}

impl CoreLoop {
    /// Execute a timeseries scan.
    pub(in crate::data::executor) fn execute_timeseries_scan(
        &mut self,
        params: TimeseriesScanParams<'_>,
    ) -> Response {
        let TimeseriesScanParams {
            task,
            collection,
            time_range,
            limit,
            filters: _filters,
            bucket_interval_ms,
        } = params;
        let mut results = Vec::new();

        // 1. Read from in-memory memtable (hot data).
        if let Some(mt) = self.ts_memtables.get(collection)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let ts_col = mt.column(schema.timestamp_idx);
            let timestamps = ts_col.as_timestamps();

            // Apply time range filter.
            let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

            if bucket_interval_ms > 0 && schema.columns.len() > 1 {
                // time_bucket aggregation.
                let val_col = mt.column(1);
                let values = val_col.as_f64();
                let buckets = aggregate_by_time_bucket(timestamps, values, bucket_interval_ms);
                for (bucket_ts, agg) in &buckets {
                    let row = serde_json::json!({
                        "bucket": bucket_ts,
                        "count": agg.count,
                        "sum": agg.sum,
                        "min": agg.min,
                        "max": agg.max,
                        "avg": agg.avg(),
                        "first": agg.first,
                        "last": agg.last,
                    });
                    results.push(row);
                }
            } else if !indices.is_empty() && schema.columns.len() > 1 {
                // Raw row output.
                let val_col = mt.column(1);
                let values = val_col.as_f64();
                for &idx in indices.iter().take(limit) {
                    let row = serde_json::json!({
                        "timestamp": timestamps[idx as usize],
                        "value": values[idx as usize],
                    });
                    results.push(row);
                }
            }
        }

        // 2. Read from sealed partitions on disk.
        if let Some(registry) = self.ts_registries.get(collection) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            let partitions = registry.query_partitions(&query_range);

            for entry in partitions {
                if results.len() >= limit {
                    break;
                }
                let part_dir = self.data_dir.join("timeseries").join(&entry.dir_name);
                if !part_dir.exists() {
                    continue;
                }

                // Read timestamp column.
                let ts_data = match ColumnarSegmentReader::read_column(
                    &part_dir,
                    "timestamp",
                    ColumnType::Timestamp,
                ) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let timestamps = ts_data.as_timestamps();

                // Read value column if it exists.
                let val_data =
                    ColumnarSegmentReader::read_column(&part_dir, "value", ColumnType::Float64);

                let indices = timestamp_range_filter(timestamps, time_range.0, time_range.1);

                if bucket_interval_ms > 0 {
                    if let Ok(ref vd) = val_data {
                        let values = vd.as_f64();
                        let filtered_ts: Vec<i64> =
                            indices.iter().map(|&i| timestamps[i as usize]).collect();
                        let filtered_vals: Vec<f64> =
                            indices.iter().map(|&i| values[i as usize]).collect();
                        let buckets = aggregate_by_time_bucket(
                            &filtered_ts,
                            &filtered_vals,
                            bucket_interval_ms,
                        );
                        for (bucket_ts, agg) in &buckets {
                            results.push(serde_json::json!({
                                "bucket": bucket_ts,
                                "count": agg.count,
                                "sum": agg.sum,
                                "min": agg.min,
                                "max": agg.max,
                                "avg": agg.avg(),
                            }));
                        }
                    }
                } else if let Ok(ref vd) = val_data {
                    let values = vd.as_f64();
                    for &idx in indices.iter().take(limit.saturating_sub(results.len())) {
                        results.push(serde_json::json!({
                            "timestamp": timestamps[idx as usize],
                            "value": values[idx as usize],
                        }));
                    }
                }
            }
        }

        let json = serde_json::to_vec(&results).unwrap_or_default();
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

    /// Execute a timeseries ingest.
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        format: &str,
    ) -> Response {
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
                if !self.ts_memtables.contains_key(collection) {
                    let schema = ilp_ingest::infer_schema(&lines);
                    let config = ColumnarMemtableConfig {
                        max_memory_bytes: 64 * 1024 * 1024,
                        hard_memory_limit: 80 * 1024 * 1024,
                        max_tag_cardinality: 100_000,
                    };
                    let mt = ColumnarMemtable::new(schema, config);
                    self.ts_memtables.insert(collection.to_string(), mt);
                }

                let mt = self.ts_memtables.get_mut(collection).unwrap();
                let mut series_keys = HashMap::new();
                let (accepted, rejected) =
                    ilp_ingest::ingest_batch(mt, &lines, &mut series_keys, now_ms);

                // Check if memtable needs flushing.
                let mt = self.ts_memtables.get(collection).unwrap();
                if mt.memory_bytes() >= 64 * 1024 * 1024 {
                    self.flush_ts_collection(collection, now_ms);
                }

                self.checkpoint_coordinator
                    .mark_dirty("timeseries", accepted);
                let result = serde_json::json!({
                    "accepted": accepted,
                    "rejected": rejected,
                    "collection": collection,
                });
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
    /// and fires the continuous aggregate hook with the flushed data.
    fn flush_ts_collection(&mut self, collection: &str, now_ms: i64) {
        let Some(mt) = self.ts_memtables.get_mut(collection) else {
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

        match writer.write_partition(&partition_name, &drain, 0, 0) {
            Ok(meta) => {
                tracing::info!(
                    collection,
                    rows = meta.row_count,
                    "timeseries columnar flush complete"
                );
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
