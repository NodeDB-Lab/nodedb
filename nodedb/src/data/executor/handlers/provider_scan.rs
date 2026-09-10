// SPDX-License-Identifier: BUSL-1.1

//! Executor handler for `QueryOp::ProviderScan`.
//!
//! Decodes the pre-materialized msgpack row array, applies predicate filtering,
//! offset, sort, distinct deduplication, column projection, and limit — in that
//! order — then emits the resulting rows via `response_with_payload`.

use nodedb_query::msgpack_scan;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::sort_utils::sort_msgpack_rows;
use crate::data::executor::msgpack_utils::write_str;
use crate::data::executor::response_codec::encode_binary_rows;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_provider_scan`].
pub(in crate::data::executor) struct ProviderScanParams<'a> {
    pub rows_bytes: &'a [u8],
    pub filters_bytes: &'a [u8],
    pub projection: &'a [String],
    pub computed_columns: &'a [u8],
    pub window_functions: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
}

impl CoreLoop {
    /// Execute a `ProviderScan` plan node.
    ///
    /// Processing order: decode rows → filter → offset → sort → distinct →
    /// project → limit → emit.
    pub(in crate::data::executor) fn execute_provider_scan(
        &mut self,
        task: &ExecutionTask,
        params: ProviderScanParams<'_>,
    ) -> Response {
        let ProviderScanParams {
            rows_bytes,
            filters_bytes,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            limit,
            offset,
            distinct,
        } = params;
        // ── 1. Decode the flat msgpack row array. ────────────────────────────
        // ProviderScan rows are flat column maps *by contract*: catalog and
        // constant results are produced flat, and a gathered storage side is
        // flattened at the Exchange-resolution boundary (`exchange::resolve`)
        // before it is embedded here. The relational-operator layer therefore
        // sees exactly one row shape — this handler never sniffs or unwraps a
        // `{id, data}` storage wrapper.
        let mut rows = decode_flat_row_array(rows_bytes);

        // ── 2. Filter. ────────────────────────────────────────────────────────
        if !filters_bytes.is_empty() {
            let predicates: Vec<ScanFilter> = match zerompk::from_msgpack(filters_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("ProviderScan: malformed filter bytes: {e}"),
                        },
                    );
                }
            };
            if !predicates.is_empty() {
                // `Vec::retain`'s closure must return `bool`, so a division/
                // modulo-by-zero is captured via this `Cell` side-channel
                // and checked once the retain finishes.
                let predicate_err: std::cell::Cell<Option<nodedb_query::EvalError>> =
                    std::cell::Cell::new(None);
                rows.retain(|row| {
                    if predicate_err.get().is_some() {
                        return true;
                    }
                    match ScanFilter::all_match_binary(&predicates, row) {
                        Ok(keep) => keep,
                        Err(e) => {
                            predicate_err.set(Some(e));
                            true
                        }
                    }
                });
                if predicate_err.take().is_some() {
                    return self.response_error(task, ErrorCode::DivisionByZero);
                }
            }
        }

        // ── 3. Offset. ────────────────────────────────────────────────────────
        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows.drain(..offset);
            }
        }

        // ── 4. Sort. ──────────────────────────────────────────────────────────
        if !sort_keys.is_empty()
            && let Err(e) = sort_msgpack_rows(&mut rows, sort_keys)
        {
            return self.response_error(task, crate::Error::from(e));
        }

        // ── 5. Computed columns. ──────────────────────────────────────────────
        // Expression projections over materialized rows (derived tables,
        // constant subqueries) ride as computed columns: evaluate each per
        // row BEFORE distinct/project so the aliased value exists in the row
        // map and division/accessor errors fail the query instead of
        // silently NULLing (issue #295).
        if !computed_columns.is_empty() {
            let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
                match zerompk::from_msgpack(computed_columns) {
                    Ok(c) => c,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("ProviderScan: malformed computed columns: {e}"),
                            },
                        );
                    }
                };
            for row in rows.iter_mut() {
                let Ok(doc_val) = nodedb_types::value_from_msgpack(row) else {
                    continue;
                };
                let mut map = match doc_val {
                    nodedb_types::Value::Object(m) => m,
                    _ => continue,
                };
                for cc in &computed_cols {
                    if matches!(map.get(&cc.alias), Some(v) if !v.is_null()) {
                        continue;
                    }
                    match cc.expr.eval(&nodedb_types::Value::Object(map.clone())) {
                        Ok(v) => {
                            map.insert(cc.alias.clone(), v);
                        }
                        Err(e) => {
                            return self
                                .response_error(task, ErrorCode::from(crate::Error::from(e)));
                        }
                    }
                }
                if let Ok(encoded) =
                    nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map))
                {
                    *row = encoded;
                }
            }
        }

        // ── 5b. Window functions. ───────────────────────────────────────────
        // Window-over-derived-table (issue #295 Gap 3): evaluate each spec
        // per partition AFTER computed columns (window args may reference
        // computed aliases) and BEFORE distinct/project (the window alias
        // must exist in the row map). Partition/order/argument errors —
        // including division-by-zero — fail the query instead of
        // silently NULLing. Evaluation is in place, so row order is kept.
        if !window_functions.is_empty() {
            let specs: Vec<crate::bridge::window_func::WindowFuncSpec> =
                match zerompk::from_msgpack(window_functions) {
                    Ok(s) => s,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("ProviderScan: malformed window bytes: {e}"),
                            },
                        );
                    }
                };
            if !specs.is_empty() && !rows.is_empty() {
                let mut decoded: Vec<(String, serde_json::Value)> = Vec::with_capacity(rows.len());
                for (i, row) in rows.iter().enumerate() {
                    match nodedb_types::json_from_msgpack(row) {
                        Ok(v) => decoded.push((i.to_string(), v)),
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("ProviderScan: window row decode: {e}"),
                                },
                            );
                        }
                    }
                }
                if let Err(e) =
                    crate::bridge::window_func::evaluate_window_functions(&mut decoded, &specs)
                {
                    return self.response_error(task, ErrorCode::from(crate::Error::from(e)));
                }
                for (slot, (_, v)) in rows.iter_mut().zip(decoded) {
                    match nodedb_types::json_to_msgpack(&v) {
                        Ok(encoded) => *slot = encoded,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("ProviderScan: window row encode: {e}"),
                                },
                            );
                        }
                    }
                }
            }
        }

        // ── 6. Distinct (on the would-be projected row). ──────────────────────
        // Deduplicate on the projected shape so SQL DISTINCT semantics are
        // honoured: two rows with the same projected columns but different
        // non-projected columns are considered equal.
        if distinct {
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            rows.retain(|row| {
                let key = if projection.is_empty() {
                    row.clone()
                } else {
                    project_row_by_names(row, projection)
                };
                seen.insert(key)
            });
        }

        // ── 6. Project. ───────────────────────────────────────────────────────
        let rows: Vec<Vec<u8>> = if projection.is_empty() {
            rows
        } else {
            rows.into_iter()
                .map(|row| project_row_by_names(&row, projection))
                .collect()
        };

        // ── 7. Limit. ─────────────────────────────────────────────────────────
        let rows = if let Some(n) = limit {
            rows.into_iter().take(n).collect()
        } else {
            rows
        };

        // ── 8. Emit. ──────────────────────────────────────────────────────────
        let payload = encode_binary_rows(&rows);
        self.response_with_payload(task, payload)
    }
}

/// A row key with its table qualifier removed (`a.attname` → `attname`).
fn bare_column(key: &str) -> &str {
    key.rsplit('.').next().unwrap_or(key)
}

/// Decode a flat msgpack row array (the `encode_binary_rows` format) into
/// individual row byte vectors. Each element is a flat column map; its bytes are
/// returned verbatim so subsequent steps operate per-row without re-encoding.
/// Storage `{id, data}` wrappers are NOT handled here — they are flattened at
/// the Exchange-resolution boundary before reaching a relational operator.
fn decode_flat_row_array(bytes: &[u8]) -> Vec<Vec<u8>> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let Some((count, mut pos)) = msgpack_scan::array_header(bytes, 0) else {
        return Vec::new();
    };
    // `count` comes from the msgpack payload. Grow only after `skip_value`
    // establishes that the corresponding row bytes are present.
    let mut rows = Vec::new();
    for _ in 0..count {
        let start = pos;
        let Some(end) = msgpack_scan::skip_value(bytes, pos) else {
            break;
        };
        rows.push(bytes[start..end].to_vec());
        pos = end;
    }
    rows
}

/// Project a single msgpack map row, keeping only the named columns.
///
/// Returns a new msgpack map containing only the fields whose name appears in
/// `projection`, preserving the original msgpack value bytes verbatim (no
/// decode). If a projection column is not present in the row it is silently
/// omitted. If `projection` is empty this function must not be called (the
/// caller is expected to skip projection for the empty case).
///
/// A row key matches a projection entry outright or once its table qualifier is
/// dropped — a join body emits one merged document per row whose columns keep
/// their `alias.column` prefix, so an exact-only match would project every
/// merged row down to nothing. This mirrors the qualified-then-bare lookup the
/// response shaper performs on the same rows.
fn project_row_by_names(row: &[u8], projection: &[String]) -> Vec<u8> {
    let Some((count, mut pos)) = msgpack_scan::map_header(row, 0) else {
        return row.to_vec();
    };

    let mut entries: Vec<(&str, usize, usize)> = Vec::with_capacity(projection.len());

    // We need string slices into the `row` bytes. Use `msgpack_scan::read_str`
    // which returns an `Option<&str>` backed by the input slice.
    for _ in 0..count {
        let key: Option<&str> = msgpack_scan::read_str(row, pos);
        let key_end = match msgpack_scan::skip_value(row, pos) {
            Some(p) => p,
            None => break,
        };
        let val_start = key_end;
        let val_end = match msgpack_scan::skip_value(row, val_start) {
            Some(p) => p,
            None => break,
        };
        if let Some(k) = key
            && projection
                .iter()
                .any(|p| p == k || p.as_str() == bare_column(k))
        {
            entries.push((k, val_start, val_end));
        }
        pos = val_end;
    }

    let mut buf = Vec::with_capacity(row.len());
    // Write map header.
    let n = entries.len();
    if n < 16 {
        buf.push(0x80 | n as u8);
    } else if n <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for (key, vs, ve) in &entries {
        write_str(&mut buf, key);
        buf.extend_from_slice(&row[*vs..*ve]);
    }
    buf
}
