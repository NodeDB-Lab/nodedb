//! Data Plane handlers for timeseries scan and ingest.

pub mod aggregate;
pub mod encode;
pub mod flush;
pub mod ingest;
pub mod raw_scan;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

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

impl CoreLoop {
    /// Execute a timeseries scan — the universal timeseries query path.
    ///
    /// Three modes:
    /// 1. Raw scan: `aggregates.is_empty()` — emit rows.
    /// 2. Time-bucket agg: `bucket_interval_ms > 0` — bucket + aggregate.
    /// 3. Generic GROUP BY: `!aggregates.is_empty()` — group + aggregate.
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

        // Fast path: COUNT(*) with no GROUP BY, no filters.
        if is_aggregate
            && bucket_interval_ms == 0
            && group_by.is_empty()
            && !has_filters
            && !has_time_range
            && aggregates.len() == 1
            && aggregates[0].0 == "count"
            && aggregates[0].1 == "*"
        {
            return self.execute_ts_count_star(task, collection, time_range);
        }

        // Determine needed columns (projection pushdown).
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
            for fp in &filter_predicates {
                if !needed.contains(&fp.field) {
                    needed.push(fp.field.clone());
                }
            }
            needed
        } else {
            Vec::new() // empty = read all columns
        };

        // Mode dispatch.
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

    /// COUNT(*) metadata fast path — zero I/O.
    fn execute_ts_count_star(
        &self,
        task: &ExecutionTask,
        collection: &str,
        time_range: (i64, i64),
    ) -> Response {
        let mut total: u64 = 0;
        if let Some(mt) = self.columnar_memtables.get(collection) {
            total += mt.row_count();
        }
        if let Some(registry) = self.ts_registries.get(collection) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            for entry in registry.query_partitions(&query_range) {
                total += entry.meta.row_count;
            }
        }
        let row = rmpv::Value::Map(vec![(
            rmpv::Value::String("count_all".into()),
            rmpv::Value::Integer((total as i64).into()),
        )]);
        let array = rmpv::Value::Array(vec![row]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap_or(());
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(buf),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}
