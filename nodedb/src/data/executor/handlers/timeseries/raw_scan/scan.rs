// SPDX-License-Identifier: BUSL-1.1

//! Raw scan entry point: `RawScanParams` and `execute_ts_raw_scan`.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::row_emit::{apply_computed_columns_rmpv, rmpv_system_time};

/// Parameters for a timeseries raw scan (no aggregation).
pub(in crate::data::executor) struct RawScanParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub time_range: (i64, i64),
    pub limit: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub has_filters: bool,
    /// The caller's decoded read policy; empty admits every row. Evaluated
    /// per row after `filter_predicates` and before a row is gathered, so a
    /// row the policy excludes never counts toward `limit`, on memtable,
    /// partition, and in-transaction overlay rows alike.
    pub rls_predicates: &'a [ScanFilter],
    pub computed_columns: &'a [u8],
    /// `AS OF SYSTEM TIME NULL`: emit every `_ts_system` version ordered
    /// ascending by system time (audit-log semantics).
    pub all_versions: bool,
    /// In-transaction read-your-own-writes: when `Some`, fold this
    /// transaction's staged `TimeseriesOp::Ingest` rows into the result.
    /// Pre-gated by the caller — `None` for autocommit reads and for
    /// audit-log / `AS OF SYSTEM TIME` reads (committed-only). The
    /// aggregate branch never reaches this handler, so continuous
    /// aggregates stay committed-only.
    pub txn_id: Option<crate::types::TxnId>,
    /// `ORDER BY` keys as `(column, ascending)`. Empty = the engine's natural
    /// order (memtable rows, then partition rows).
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
}

impl CoreLoop {
    /// Raw scan mode: emit rows from memtable + partitions.
    pub(in crate::data::executor) fn execute_ts_raw_scan(
        &self,
        params: RawScanParams<'_>,
    ) -> Response {
        let crs = self.crs_context();
        let RawScanParams {
            task,
            tid,
            collection,
            time_range,
            limit,
            filter_predicates,
            has_filters,
            rls_predicates,
            computed_columns: computed_columns_bytes,
            all_versions,
            txn_id,
            sort_keys,
        } = params;

        // A no-LIMIT SQL `SELECT * FROM <timeseries>` arrives as
        // `limit == usize::MAX`. Bound the row fetch to a ceiling derived from
        // the per-query memory budget (+1 row to detect "more exist") so the
        // materialized result cannot grow to the whole collection. Aggregate /
        // COUNT paths never reach this handler.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = limit == usize::MAX;
        let limit = if unbounded {
            // Timeseries raw scans have no row offset.
            crate::data::executor::handlers::scan_budget::fetch_limit_for(
                limit,
                0,
                scan_budget_bytes,
            )
        } else {
            limit
        };

        // An ordered query cannot stop at `limit` while gathering: the first
        // `limit` rows the engine happens to find are not the first `limit` of
        // the requested order. Gather up to the memory budget instead, sort,
        // then cut to `limit`.
        let gather_limit = if sort_keys.is_empty() {
            limit
        } else {
            crate::data::executor::handlers::scan_budget::fetch_limit_for(
                usize::MAX,
                0,
                scan_budget_bytes,
            )
        };

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid.as_u64(), collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // 1-2. Committed rows: memtable, then flushed partitions.
        let mut results = match self.gather_ts_rows(super::gather::TsGatherParams {
            database_id: task.request.database_id,
            tid,
            collection,
            time_range,
            filter_predicates,
            rls_predicates,
            limit: gather_limit,
        }) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        // 3. In-transaction read-your-own-writes: fold this transaction's
        // staged `TimeseriesOp::Ingest` rows into the base result. Gated on
        // `txn_id` (autocommit reads never carry one) and already excluded by
        // the caller for audit-log / temporal reads. The aggregate / bucket
        // branch runs in `execute_ts_aggregate`, never here, so continuous
        // aggregates and bucketed queries remain committed-only.
        if let Some(txn_id) = txn_id {
            let coll_key = (task.request.database_id, tid, collection.to_string());
            if let Err(e) = self.merge_overlay_into_timeseries_scan(
                crate::data::executor::handlers::transaction::overlay::TimeseriesOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    time_range,
                    filter_predicates,
                    has_filters,
                    rls_predicates,
                    limit: gather_limit,
                },
                &mut results,
            ) {
                return self.response_error(task, e);
            }
        }

        // Apply computed columns (e.g. time_bucket) if present.
        let results = if !computed_columns_bytes.is_empty() {
            let computed_cols_result: Result<Vec<crate::bridge::expr_eval::ComputedColumn>, _> =
                zerompk::from_msgpack(computed_columns_bytes);
            let computed_cols = match computed_cols_result {
                Ok(cols) => cols,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("computed_columns deserialize failed: {e}"),
                        },
                    );
                }
            };
            if computed_cols.is_empty() {
                results
            } else {
                match results
                    .into_iter()
                    .map(|row| apply_computed_columns_rmpv(row, &computed_cols, &crs))
                    .collect::<crate::Result<Vec<_>>>()
                {
                    Ok(rows) => rows,
                    Err(e) => return self.response_error(task, e),
                }
            }
        } else {
            results
        };

        // Audit-log order: ascending by system time across all versions.
        let mut results = if all_versions {
            let mut sorted = results;
            sorted.sort_by_key(rmpv_system_time);
            sorted
        } else {
            results
        };

        // ORDER BY, then the row limit — so an ordered query returns the first
        // `limit` rows of the requested order rather than an arbitrary `limit`
        // rows sorted among themselves. Audit-log reads keep their system-time
        // order, which is the semantics of `AS OF SYSTEM TIME NULL`.
        if !all_versions
            && let Err(e) = super::super::sort::sort_rows(&mut results, sort_keys, &crs)
        {
            return self.response_error(task, e);
        }
        results.truncate(limit);

        let array = rmpv::Value::Array(results);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap_or(());

        // Bound an unbounded (no-LIMIT) scan by the memory budget. The encoded
        // msgpack payload is the authoritative size of the materialized result;
        // surface a deterministic error if it exceeds the budget rather than
        // silently truncating.
        if unbounded
            && crate::data::executor::handlers::scan_budget::budget_exceeded(
                buf.len(),
                scan_budget_bytes,
            )
        {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        self.response_with_payload(task, buf)
    }
}
