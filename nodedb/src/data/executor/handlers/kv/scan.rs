// SPDX-License-Identifier: BUSL-1.1

//! KV Scan handler and filter extraction.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvScanParams;
use crate::engine::kv::current_ms;

/// Parameters for the KV SCAN handler.
pub(in crate::data::executor) struct KvScanHandlerParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    pub match_pattern: Option<&'a str>,
    pub filters: &'a [u8],
    pub projection: &'a [String],
    pub computed_columns_bytes: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub surrogate_ceiling: Option<u32>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        params: KvScanHandlerParams<'_>,
    ) -> Response {
        let KvScanHandlerParams {
            did,
            tid,
            collection,
            cursor,
            count,
            match_pattern,
            filters,
            projection,
            computed_columns_bytes,
            sort_keys,
            surrogate_ceiling,
        } = params;

        debug!(core = self.core_id, %collection, count, "kv scan");

        // Scan-quiesce gate: refuse new scans against a draining
        // collection so the purge handler can unlink on-disk files
        // without racing an in-flight reader.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let now_ms = current_ms();

        // A no-LIMIT SQL `SELECT * FROM <kv>` arrives as `count == usize::MAX`.
        // Bound the engine fetch to a row ceiling derived from the per-query
        // memory budget (+1 row to detect "more exist") so the materialized
        // `Vec` cannot grow to the whole collection. The RESP cursor
        // pagination path always carries a finite `count`, so it is unaffected.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = count == usize::MAX;
        let fetch_count = if unbounded {
            // KV scans have no row offset (pagination is cursor-based).
            scan_budget::fetch_limit_for(count, 0, scan_budget_bytes)
        } else {
            count
        };

        // Try to extract a single equality filter for index pushdown.
        let (filter_field, filter_value) = extract_eq_filter(filters);
        let (mut entries, _next_cursor) = self.kv_engine.scan(KvScanParams {
            database_id: did,
            tenant_id: tid,
            collection,
            cursor,
            count: fetch_count,
            now_ms,
            match_pattern,
            filter_field: filter_field.as_deref(),
            filter_value: filter_value.as_deref(),
            surrogate_ceiling,
        });

        // Read-your-own-writes: fold this transaction's staged KV writes
        // into the base scan result before the shared filter/sort/encode
        // pipeline below, which then treats merged and base rows alike.
        // `matches` always accepts here -- the per-entry filter loop further
        // down re-applies `filter_predicates` uniformly to every row
        // (base or merged), so there is no need to duplicate that check
        // in the merge itself.
        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                crate::types::DatabaseId::new(did),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_kv_overlay_into_scan(
                txn_id,
                &coll_key,
                &mut entries,
                &|_key: &[u8], _value: &[u8]| true,
            );
        }

        // Bound an unbounded (no-LIMIT) scan by the memory budget. Sum the raw
        // key+value bytes and surface a deterministic error if the result would
        // exceed the budget rather than silently truncating it.
        if unbounded {
            let total = entries.iter().fold(0usize, |acc, (k, v)| {
                acc.saturating_add(k.len()).saturating_add(v.len())
            });
            if scan_budget::budget_exceeded(total, scan_budget_bytes) {
                return self.response_error(task, ErrorCode::ResourcesExhausted);
            }
        }

        // Parse filter predicates for post-scan evaluation.
        // Index pushdown handles eq filters on indexed fields, but general
        // predicates (gt, lt, in, etc.) need post-scan evaluation.
        let filter_predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if !filters.is_empty()
        {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Build results as raw msgpack — no serde_json::Value intermediary.
        let mut result_entries: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        for (k, v) in &entries {
            // The two storage shapes (msgpack map vs raw bytes) are resolved by
            // the one shared shaper, so this scan, the streaming/materializing
            // scans, and a write's `RETURNING` projection cannot disagree about
            // what a KV row looks like. A private copy of that logic in this
            // loop risks diverging from the others on the raw-bytes case.
            let (_key_str, entry_mp) = crate::data::executor::scan_normalize::kv_row_to_doc(k, v);

            // Apply filter predicates post-scan (already works on raw msgpack).
            if !filter_predicates.is_empty() {
                match crate::bridge::scan_filter::ScanFilter::all_match_binary(
                    &filter_predicates,
                    &entry_mp,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(_e) => {
                        return self.response_error(task, ErrorCode::DivisionByZero);
                    }
                }
            }

            result_entries.push(entry_mp);
        }

        result_entries = match self.apply_kv_projection_and_computed(
            task,
            result_entries,
            projection,
            computed_columns_bytes,
        ) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        if !sort_keys.is_empty()
            && let Err(e) =
                super::super::sort_utils::sort_msgpack_rows(&mut result_entries, sort_keys)
        {
            return self.response_error(task, crate::Error::from(e));
        }

        // Build response as flat msgpack array — same format as document/columnar scan.
        // RESP SCAN handles cursor pagination at its own handler layer.
        let mut payload =
            Vec::with_capacity(result_entries.iter().map(|e| e.len()).sum::<usize>() + 64);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, result_entries.len());
        for entry in &result_entries {
            payload.extend_from_slice(entry);
        }
        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }

    /// Apply projection and computed columns to a kv scan's raw msgpack rows.
    ///
    /// Runs before sort so `ORDER BY` can name a computed alias. An empty
    /// projection with non-empty computed columns keeps the full row (kv has
    /// no fixed schema to project against) and appends the computed aliases;
    /// a non-empty projection keeps only the named columns plus the computed
    /// aliases. Returns `Err(Response)` on malformed computed-column bytes
    /// or a per-row evaluation error, ready to return directly to the caller.
    fn apply_kv_projection_and_computed(
        &self,
        task: &ExecutionTask,
        result_entries: Vec<Vec<u8>>,
        projection: &[String],
        computed_columns_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, Response> {
        if projection.is_empty() && computed_columns_bytes.is_empty() {
            return Ok(result_entries);
        }

        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(computed_columns_bytes) {
                    Ok(cols) => cols,
                    Err(e) => {
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("kv scan: malformed computed bytes: {e}"),
                            },
                        ));
                    }
                }
            };

        if projection.is_empty() {
            crate::data::executor::handlers::provider_scan_compute::apply_windows_and_computed(
                result_entries,
                &[],
                computed_columns_bytes,
            )
            .map_err(|e| self.response_error(task, e))
        } else {
            let mut projected_entries = Vec::with_capacity(result_entries.len());
            for entry in &result_entries {
                let row = crate::data::executor::handlers::document::read::projection::apply_projection_msgpack(
                    entry,
                    &computed_cols,
                    projection,
                )
                .map_err(|e| self.response_error(task, e))?;
                projected_entries.push(row);
            }
            Ok(projected_entries)
        }
    }
}

/// Extract a single equality filter from serialized ScanFilter bytes.
///
/// Looks for the first `{"field": "x", "op": "eq", "value": "y"}` filter.
/// Returns `(Some(field), Some(value_bytes))` if found, `(None, None)` otherwise.
pub(in crate::data::executor) fn extract_eq_filter(
    filters: &[u8],
) -> (Option<String>, Option<Vec<u8>>) {
    if filters.is_empty() {
        return (None, None);
    }

    // Filters are MessagePack-encoded Vec<ScanFilter>.
    let Ok(parsed) = zerompk::from_msgpack::<Vec<nodedb_types::json_msgpack::JsonValue>>(filters)
        .map(|v| {
            v.into_iter()
                .map(|jv| jv.0)
                .collect::<Vec<serde_json::Value>>()
        })
    else {
        tracing::trace!(
            len = filters.len(),
            "filter deserialization failed, falling back to full scan"
        );
        return (None, None);
    };

    for filter in &parsed {
        let Some(field) = filter.get("field").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(op) = filter.get("op").and_then(|v| v.as_str()) else {
            continue;
        };
        if op != "eq" {
            continue;
        }
        let Some(value) = filter.get("value") else {
            continue;
        };

        let value_bytes = match value {
            serde_json::Value::String(s) => s.as_bytes().to_vec(),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    let sortable = (i as u64) ^ (1u64 << 63);
                    sortable.to_be_bytes().to_vec()
                } else {
                    n.to_string().into_bytes()
                }
            }
            other => other.to_string().into_bytes(),
        };

        return (Some(field.to_string()), Some(value_bytes));
    }

    (None, None)
}
