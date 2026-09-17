// SPDX-License-Identifier: BUSL-1.1

//! Data Plane timeseries scan parameters and execution.

use crate::bridge::envelope::{Payload, Response, Status};
use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::filter::decode_rls_filters;
use crate::data::executor::task::ExecutionTask;
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use super::{aggregate, raw_scan};

/// Parameters for a timeseries scan operation.
pub(in crate::data::executor) struct TimeseriesScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filters: &'a [u8],
    /// The caller's read policy as a MessagePack `Vec<ScanFilter>`, injected
    /// by the planner. Empty when no policy governs the caller. The raw scan
    /// evaluates it per row after `filters`, before computed columns, sort,
    /// and limit, on memtable, partition, and in-transaction overlay rows
    /// alike. The aggregate branch pushes it into the grouped scan so it
    /// excludes rows before aggregation, and fails when the grouped scan
    /// cannot lower it.
    pub rls_filters: &'a [u8],
    /// `ORDER BY` keys as `(column, ascending)`. Applied to the materialized
    /// result before `limit`, on both the raw and the aggregate branch.
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub bucket_interval_ms: i64,
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    /// Gap-fill strategy. Empty = no gap-fill.
    pub gap_fill: &'a str,
    /// Serialized computed columns for scalar projection expressions.
    pub computed_columns: &'a [u8],
    /// Bitemporal system-time selection. `AsOf(ms)` hides rows whose
    /// `_ts_system` exceeds the cutoff; `AllVersions` emits every `_ts_system`
    /// row ordered ascending (audit log); `Current` is unconstrained.
    pub system_time: nodedb_types::SystemTimeScope,
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
            rls_filters,
            sort_keys,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            system_time,
            valid_at_ms,
        } = params;

        let all_versions = system_time.is_all_versions();
        let system_as_of_ms = system_time.as_of_ms();

        // The collection's declared TIME_KEY drives partition pruning and
        // projection pushdown. Resolved once here and threaded through both
        // branches; nothing downstream guesses it from a column name.
        let time_key = self.ts_time_column(task.request.database_id, tid, collection);
        let time_key_kind = self.ts_time_key_kind(task.request.database_id, tid, collection);

        // Lazy-load partition registry from disk if not yet loaded.
        if let Err(e) = self.ensure_ts_registry(tid, task.request.database_id, collection) {
            return self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        // Both predicate sets are decoded before any row is read. A payload
        // the Data Plane cannot decode fails the statement: for the policy
        // that is the difference between hiding governed rows and returning
        // them, and for the WHERE clause it is the difference between the
        // asked-for rows and every row.
        let mut filter_predicates: Vec<ScanFilter> =
            match decode_scan_filters(filters, "timeseries filter") {
                Ok(p) => p,
                Err(e) => return self.response_error(task, e),
            };
        let rls_predicates: Vec<ScanFilter> = match decode_rls_filters(rls_filters) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        // Bitemporal cutoffs: translate to column-level predicates on
        // `_ts_system` / `_ts_valid_from` / `_ts_valid_until`. The
        // segment reader's block-skip infrastructure applies these
        // against per-block min/max automatically.
        if let Some(cutoff) = system_as_of_ms {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_SYSTEM.into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(cutoff),
                clauses: Vec::new(),
                expr: None,
            });
        }
        if let Some(point) = valid_at_ms {
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_VALID_FROM.into(),
                op: crate::bridge::scan_filter::FilterOp::Lte,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
            filter_predicates.push(crate::bridge::scan_filter::ScanFilter {
                field: TS_VALID_UNTIL.into(),
                op: crate::bridge::scan_filter::FilterOp::Gt,
                value: nodedb_types::Value::Integer(point),
                clauses: Vec::new(),
                expr: None,
            });
        }
        // Narrow the plan's envelope with the query's own bounds on the
        // declared time column. The Control Plane sends an unbounded range
        // precisely because only this core knows which column that is.
        let time_range = super::time_range::narrow_time_range(
            time_range,
            &filter_predicates,
            Some((time_key.as_str(), time_key_kind)),
        );

        let has_filters = !filter_predicates.is_empty();
        let is_aggregate = !aggregates.is_empty();
        let has_time_range = time_range.0 > 0 || time_range.1 < i64::MAX;

        // Fast path: COUNT(*) with no GROUP BY, no filters, no read policy.
        // The metadata count covers every stored row, so a governed read
        // takes the grouped scan, where the policy excludes rows first.
        if is_aggregate
            && bucket_interval_ms == 0
            && group_by.is_empty()
            && !has_filters
            && rls_predicates.is_empty()
            && !has_time_range
            && aggregates.len() == 1
            && aggregates[0].0 == "count"
            && aggregates[0].1 == "*"
        {
            return self.execute_ts_count_star(task, tid, collection, time_range);
        }

        // Determine needed columns (projection pushdown).
        let needed_columns: Vec<String> = if is_aggregate || bucket_interval_ms > 0 {
            // The aggregate pipeline always needs the time column: it is the
            // bucketing key and the ordering key.
            let mut needed: Vec<String> = vec![time_key.clone()];
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
            for fp in filter_predicates.iter().chain(&rls_predicates) {
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
            // The policy joins the WHERE predicates so the grouped scan
            // excludes governed rows before any accumulator sees them. The
            // grouped scan fails on a predicate it cannot lower, so a policy
            // shape it does not support fails the statement rather than
            // aggregating ungoverned rows.
            let governed_predicates: Vec<ScanFilter> = filter_predicates
                .iter()
                .chain(&rls_predicates)
                .cloned()
                .collect();
            self.execute_ts_aggregate(aggregate::TsAggregateParams {
                task,
                tid,
                collection,
                time_range,
                limit,
                filter_predicates: &governed_predicates,
                bucket_interval_ms,
                group_by,
                aggregates,
                gap_fill,
                needed_columns: &needed_columns,
                sort_keys,
            })
        } else {
            // In-transaction read-your-own-writes is confined to the RAW scan
            // branch and to current-version reads: audit-log (`all_versions`)
            // and `AS OF SYSTEM TIME` reads are committed-only, so the overlay
            // merge is pre-gated out here rather than in the raw handler.
            let overlay_txn = if all_versions || system_as_of_ms.is_some() {
                None
            } else {
                task.request.txn_id
            };
            self.execute_ts_raw_scan(raw_scan::RawScanParams {
                task,
                tid,
                collection,
                time_range,
                limit,
                filter_predicates: &filter_predicates,
                has_filters,
                rls_predicates: &rls_predicates,
                computed_columns,
                all_versions,
                txn_id: overlay_txn,
                sort_keys,
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
        let key = (task.request.database_id, tid, collection.to_string());
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
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}
