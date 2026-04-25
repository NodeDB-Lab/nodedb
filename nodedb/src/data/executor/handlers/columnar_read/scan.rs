//! Scan-params struct and the base scan entry point.

use nodedb_types::surrogate_bitmap::SurrogateBitmap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

use super::bitemporal::bitemporal_row_visible;
use super::convert::value_to_json;
use super::filter::row_matches_filters;
use super::sort::sort_rows_by_keys;

/// Parameters for a columnar base scan. Bundled as a struct because the
/// raw parameter list exceeds the project's too-many-arguments bound.
pub(in crate::data::executor) struct ColumnarScanParams<'a> {
    pub collection: &'a str,
    pub projection: &'a [String],
    pub limit: usize,
    pub filters: &'a [u8],
    /// RLS filter bytes — wiring is the responsibility of a separate
    /// enforcement pass; the base scan handler itself does not consume
    /// them (hence the `_` destructure).
    #[allow(dead_code)]
    pub rls_filters: &'a [u8],
    pub sort_keys: &'a [(String, bool)],
    /// Bitemporal system-time cutoff: drop rows with `_ts_system > cutoff`.
    /// `None` is a current-state read (no filter applied).
    pub system_as_of_ms: Option<i64>,
    /// Bitemporal valid-time point: drop rows whose
    /// `[_ts_valid_from, _ts_valid_until)` interval does not contain this
    /// point. `None` skips valid-time filtering entirely.
    pub valid_at_ms: Option<i64>,
    /// Optional cross-engine surrogate prefilter. When `Some`, the scan
    /// skips whole memtable blocks whose surrogate range does not intersect
    /// the bitmap (block boundary) and skips individual rows whose surrogate
    /// is absent from the bitmap (row boundary). `None` = no prefilter.
    pub prefilter: Option<&'a SurrogateBitmap>,
}

impl CoreLoop {
    /// Execute a base columnar scan: read from MutationEngine memtable.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        params: ColumnarScanParams<'_>,
    ) -> Response {
        let ColumnarScanParams {
            collection,
            projection,
            limit,
            filters,
            rls_filters: _,
            sort_keys,
            system_as_of_ms,
            valid_at_ms,
            prefilter,
        } = params;
        let limit = if limit == 0 { 1000 } else { limit };

        // Scan-quiesce gate.
        let _scan_guard =
            match self.acquire_scan_guard(task, task.request.tenant_id.as_u32(), collection) {
                Ok(g) => g,
                Err(resp) => return resp,
            };

        let engine_key = (task.request.tenant_id, collection.to_string());

        let engine = match self.columnar_engines.get(&engine_key) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Collect matched rows as (row_values, json_object) pairs. We keep
        // the raw `Vec<Value>` for sort-key comparison — the JSON form is
        // emitted only after ORDER BY + limit are applied. When no sort
        // is requested we short-circuit the limit enforcement inside the
        // loop to avoid materialising the entire memtable.
        let mut matched: Vec<(Vec<nodedb_types::value::Value>, serde_json::Value)> = Vec::new();
        let scan_budget = if sort_keys.is_empty() {
            limit.saturating_mul(10).max(limit)
        } else {
            usize::MAX
        };
        // Resolve hidden bitemporal column positions once; `None` means
        // the collection is not bitemporal, so the per-row filter is a
        // no-op regardless of `system_as_of_ms` / `valid_at_ms` values.
        let ts_system_idx = schema.columns.iter().position(|c| c.name == "_ts_system");
        let ts_valid_from_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_from");
        let ts_valid_until_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "_ts_valid_until");

        // Block-boundary prefilter: if a prefilter is present and none of
        // the memtable's surrogates fall within the bitmap's [min, max]
        // range, the entire memtable block can be skipped before any row
        // decoding takes place.
        let block_skipped = if let Some(bitmap) = prefilter {
            if bitmap.is_empty() {
                true
            } else {
                let surrogates = engine.memtable_surrogates();
                // Compute the surrogate range of non-None entries in the memtable.
                let (mt_min, mt_max) = surrogates
                    .iter()
                    .flatten()
                    .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
                        (lo.min(s.0), hi.max(s.0))
                    });
                // If no surrogate was found (mt_min > mt_max) or the bitmap's
                // range lies entirely outside the memtable range, skip.
                if mt_min > mt_max {
                    // No surrogates in memtable — cannot apply block skip.
                    false
                } else {
                    let bm_min = bitmap.0.min().unwrap_or(0);
                    let bm_max = bitmap.0.max().unwrap_or(0);
                    // Disjoint ranges: bitmap entirely before or after memtable.
                    bm_max < mt_min || bm_min > mt_max
                }
            }
        } else {
            false
        };

        if !block_skipped {
            for (row_surrogate, row) in engine
                .scan_memtable_rows_with_surrogates()
                .take(scan_budget)
            {
                // Row-boundary prefilter: skip this row when its surrogate is
                // absent from the bitmap. Rows without a recorded surrogate
                // (legacy / test paths) are always included when no prefilter
                // is active; when a prefilter is active they are excluded
                // because the surrogate identity is unknown.
                if let Some(bitmap) = prefilter {
                    match row_surrogate {
                        Some(s) if bitmap.contains(s) => {}
                        _ => continue,
                    }
                }

                if !bitemporal_row_visible(
                    &row,
                    ts_system_idx,
                    ts_valid_from_idx,
                    ts_valid_until_idx,
                    system_as_of_ms,
                    valid_at_ms,
                ) {
                    continue;
                }
                if !filter_predicates.is_empty()
                    && !row_matches_filters(&row, schema, &filter_predicates)
                {
                    continue;
                }
                let mut obj = serde_json::Map::new();
                for (i, col_def) in schema.columns.iter().enumerate() {
                    if !projection.is_empty() && !projection.iter().any(|p| p == &col_def.name) {
                        continue;
                    }
                    if i < row.len() {
                        obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                    }
                }
                matched.push((row, serde_json::Value::Object(obj)));
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
            }
        }

        if !sort_keys.is_empty() {
            matched.sort_by(|(a, _), (b, _)| sort_rows_by_keys(a, b, schema, sort_keys));
        }

        let results: Vec<serde_json::Value> =
            matched.into_iter().take(limit).map(|(_, j)| j).collect();

        match response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
