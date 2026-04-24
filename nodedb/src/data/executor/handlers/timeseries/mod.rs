//! Data Plane handlers for timeseries scan and ingest.

pub mod aggregate;
pub mod encode;
pub mod flush;
pub mod ingest;
pub mod ingest_formats;
mod msgpack_decode;
pub mod raw_scan;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::agg_key::canonical_agg_key;

/// Parameters for a timeseries scan operation.
pub(in crate::data::executor) struct TimeseriesScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filters: &'a [u8],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    /// Gap-fill strategy. Empty = no gap-fill.
    pub gap_fill: &'a str,
    /// Serialized computed columns for scalar projection expressions.
    pub computed_columns: &'a [u8],
    /// Bitemporal system-time cutoff: rows whose `_ts_system` exceeds
    /// this cutoff are hidden. `None` on non-bitemporal collections.
    pub system_as_of_ms: Option<i64>,
    /// Bitemporal valid-time point. `None` skips valid-time filtering.
    pub valid_at_ms: Option<i64>,
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
            tid,
            collection,
            time_range,
            limit,
            filters,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            system_as_of_ms,
            valid_at_ms,
        } = params;

        // Lazy-load partition registry from disk if not yet loaded.
        self.ensure_ts_registry(tid, collection);

        let mut filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> =
            if filters.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(filters).unwrap_or_default()
            };
        // Bitemporal cutoffs: translate to column-level predicates on
        // `_ts_system` / `_ts_valid_from` / `_ts_valid_until`. The
        // segment reader's block-skip infrastructure applies these
        // against per-block min/max automatically.
        if let Some(cutoff) = system_as_of_ms {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: "_ts_system".into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(cutoff),
                clauses: Vec::new(),
                expr: None,
            });
        }
        if let Some(point) = valid_at_ms {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: "_ts_valid_from".into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: "_ts_valid_until".into(),
                op: crate::bridge::scan_filter::FilterOp::Gt,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
        }
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
            return self.execute_ts_count_star(task, tid, collection, time_range);
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
                tid,
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
            self.execute_ts_raw_scan(raw_scan::RawScanParams {
                task,
                tid,
                collection,
                time_range,
                limit,
                filter_predicates: &filter_predicates,
                has_filters,
                computed_columns,
            })
        }
    }

    /// COUNT(*) metadata fast path — zero I/O.
    fn execute_ts_count_star(
        &self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        time_range: (i64, i64),
    ) -> Response {
        let key = (tid, collection.to_string());
        let mut total: u64 = 0;
        if let Some(mt) = self.columnar_memtables.get(&key) {
            total += mt.row_count();
        }
        if let Some(registry) = self.ts_registries.get(&key) {
            let query_range = nodedb_types::timeseries::TimeRange::new(time_range.0, time_range.1);
            for entry in registry.query_partitions(&query_range) {
                total += entry.meta.row_count;
            }
        }
        let count_key = canonical_agg_key("count", "*");
        let row = rmpv::Value::Map(vec![(
            rmpv::Value::String(count_key.into()),
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
