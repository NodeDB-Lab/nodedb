// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base timeseries RAW scan
//! result, so an in-transaction raw timeseries `SELECT` observes the
//! transaction's own uncommitted `TimeseriesOp::Ingest` rows
//! (read-your-own-writes).
//!
//! Additive-only merge — this is the deliberate divergence from
//! [`super::columnar_merge`]. A columnar base row carries a cross-engine
//! surrogate, so the columnar merge can supersede / tombstone an existing base
//! row by surrogate. A timeseries base row has NO surrogate identity in the
//! scan (it is keyed internally by `series_id`, a measurement + tag hash), so
//! there is no base row to supersede or tombstone: RYOW for timeseries is
//! INSERT-only, and the merge simply APPENDS each staged `Put` row that
//! satisfies the scan's time-range and WHERE predicate. A staged `Tombstone`
//! (only reachable via a future in-transaction timeseries DELETE, not staged
//! today) has no base row to remove and is skipped.
//!
//! Only invoked on the RAW-scan branch (`execute_ts_raw_scan`); the
//! aggregate / time-bucket branch (`execute_ts_aggregate`) is committed-only
//! and never merges the overlay (continuous-aggregate correctness).

use nodedb_types::value::Value;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::filter::value_matches_filters;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::types::{DatabaseId, TenantId, TxnId};
use crate::util::rmpv_value::value_to_rmpv;

/// Inputs for [`CoreLoop::merge_overlay_into_timeseries_scan`].
pub(in crate::data::executor) struct TimeseriesOverlayMergeParams<'a> {
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    /// `(min_ts_ms, max_ts_ms)` — same inclusive range the base memtable /
    /// partition scan applied. `(0, i64::MAX)` = no time filter.
    pub time_range: (i64, i64),
    pub filter_predicates: &'a [ScanFilter],
    pub has_filters: bool,
    /// The caller's decoded read policy. A staged row the policy excludes is
    /// dropped exactly like a base row; empty admits every row.
    pub rls_predicates: &'a [ScanFilter],
    /// Row ceiling for the whole scan (base + staged). Staged rows are only
    /// appended while the result is below this bound, so the merge never
    /// exceeds the SQL `LIMIT`.
    pub limit: usize,
}

/// Decode a staged timeseries row body (a `Value::Object` field map encoded by
/// `stage_timeseries_insert` via `nodedb_types::value_to_msgpack`). Returns
/// `None` for a body that fails to decode or is not an object — defensively
/// treated as "does not match" rather than surfacing a panic.
fn decode_staged_row(body: &[u8]) -> Option<Value> {
    match nodedb_types::value_from_msgpack(body) {
        Ok(v @ Value::Object(_)) => Some(v),
        _ => None,
    }
}

/// Extract the time value (ms) from a staged row map.
///
/// A staged row keeps the INSERT's own column names, so the lookup is by the
/// collection's declared time column — the same name the base scan prunes on.
/// A row that carries no readable instant under that name falls outside every
/// bounded range and is treated as non-matching by the caller.
fn row_timestamp_ms(row: &Value, time_column: &str) -> Option<i64> {
    let Value::Object(map) = row else {
        return None;
    };
    map.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(time_column))
        .and_then(|(_, value)| nodedb_query::scan_filter::value_as_timestamp_ms(value))
}

/// Convert a decoded staged row (`Value::Object`) into the `rmpv::Value::Map`
/// row shape the base raw scan emits, so a merged staged row is
/// indistinguishable from a base row downstream (computed columns, encoding).
fn staged_row_to_rmpv(row: &Value) -> rmpv::Value {
    match row {
        Value::Object(_) => value_to_rmpv(row),
        _ => rmpv::Value::Nil,
    }
}

impl CoreLoop {
    /// Append this transaction's staged `TimeseriesOp::Ingest` rows to
    /// `results` (base raw-scan rows), each subject to the scan's time-range,
    /// WHERE predicate, read policy, and row limit. No-op when the
    /// transaction has no overlay entries for this collection.
    pub(in crate::data::executor) fn merge_overlay_into_timeseries_scan(
        &self,
        params: TimeseriesOverlayMergeParams<'_>,
        results: &mut Vec<rmpv::Value>,
    ) -> crate::Result<()> {
        let TimeseriesOverlayMergeParams {
            txn_id,
            coll_key,
            time_range,
            filter_predicates,
            has_filters,
            rls_predicates,
            limit,
        } = params;

        let time_column = self.ts_time_column(coll_key.0, coll_key.1, &coll_key.2);

        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };

        for (_surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if results.len() >= limit {
                break;
            }
            // A base timeseries row carries no surrogate, so a staged
            // `Tombstone` has nothing to remove here — skip it. Only staged
            // inserts (`Put`) contribute new rows.
            let Staged::Put(body) = staged else {
                continue;
            };
            let Some(row) = decode_staged_row(body) else {
                continue;
            };

            // Time-range prune, mirroring the base memtable scan's
            // `timestamp_range_filter` (inclusive bounds).
            match row_timestamp_ms(&row, &time_column) {
                Some(ts) if ts >= time_range.0 && ts <= time_range.1 => {}
                _ => continue,
            }

            // Re-apply the scan's WHERE predicate on the staged body (already
            // msgpack), exactly like the raw scan's `need_json_filter` path
            // does per base row.
            if has_filters && !ScanFilter::all_match_binary(filter_predicates, body)? {
                continue;
            }
            // The caller's read policy, on the decoded row, after the WHERE
            // predicate and before the row takes a limit slot.
            if !value_matches_filters(&row, rls_predicates)? {
                continue;
            }

            results.push(staged_row_to_rmpv(&row));
        }
        Ok(())
    }
}
