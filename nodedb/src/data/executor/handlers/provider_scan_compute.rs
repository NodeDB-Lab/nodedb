// SPDX-License-Identifier: BUSL-1.1

//! Window-function and computed-column evaluation for `QueryOp::ProviderScan`.
//!
//! Runs after filter and before sort/distinct/offset/project/limit in the
//! `ProviderScan` pipeline. Each msgpack row decodes to a `serde_json::Value`,
//! windows evaluate over the full row set, computed columns evaluate
//! per-row, and the result re-encodes to msgpack. Skipped entirely when both
//! byte slices are empty, so the zero-decode msgpack path stays untouched for
//! a plain relational scan.

use crate::bridge::envelope::ErrorCode;
use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::window_func::{WindowFuncSpec, evaluate_window_functions};

/// Decode a `Vec<T>` from MessagePack, tagging decode failures with which
/// byte slice (`kind`, e.g. `"window"` or `"computed"`) failed.
fn decode_bytes<'a, T: zerompk::FromMessagePack<'a>>(
    bytes: &'a [u8],
    kind: &str,
) -> crate::Result<T> {
    zerompk::from_msgpack(bytes).map_err(|e| {
        crate::Error::DataPlane(ErrorCode::Internal {
            detail: format!("ProviderScan: malformed {kind} bytes: {e}"),
        })
    })
}

/// Apply window functions then computed columns to `rows`, both optional and
/// independently controlled by `window_bytes` / `computed_bytes` being
/// non-empty. Returns `rows` unchanged, still msgpack-encoded, when both are
/// empty.
pub(in crate::data::executor) fn apply_windows_and_computed(
    rows: Vec<Vec<u8>>,
    window_bytes: &[u8],
    computed_bytes: &[u8],
) -> crate::Result<Vec<Vec<u8>>> {
    if window_bytes.is_empty() && computed_bytes.is_empty() {
        return Ok(rows);
    }

    let window_specs: Vec<WindowFuncSpec> = if window_bytes.is_empty() {
        Vec::new()
    } else {
        decode_bytes(window_bytes, "window")?
    };
    let computed_cols: Vec<ComputedColumn> = if computed_bytes.is_empty() {
        Vec::new()
    } else {
        decode_bytes(computed_bytes, "computed")?
    };

    let mut json_rows: Vec<(String, serde_json::Value)> = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        let value = nodedb_types::value_from_msgpack(row).map_err(|e| {
            crate::Error::DataPlane(ErrorCode::Internal {
                detail: format!("ProviderScan: malformed row for window/computed evaluation: {e}"),
            })
        })?;
        json_rows.push((idx.to_string(), serde_json::Value::from(value)));
    }

    if !window_specs.is_empty() {
        evaluate_window_functions(&mut json_rows, &window_specs).map_err(crate::Error::from)?;
    }

    for (_, row_json) in &mut json_rows {
        if computed_cols.is_empty() {
            continue;
        }
        // Every computed column evaluates against the row as it stood before
        // this loop, matching `apply_projection`'s semantics: later computed
        // columns never observe earlier ones' results.
        let doc_val = nodedb_types::Value::from(row_json.clone());
        for cc in &computed_cols {
            let already_present = matches!(row_json.get(&cc.alias), Some(v) if !v.is_null());
            if already_present {
                continue;
            }
            let v = cc.expr.eval(&doc_val)?;
            if let serde_json::Value::Object(obj) = row_json {
                obj.insert(cc.alias.clone(), serde_json::Value::from(v));
            }
        }
    }

    let mut out = Vec::with_capacity(json_rows.len());
    for (_, row_json) in json_rows {
        let bytes = nodedb_types::json_to_msgpack(&row_json).map_err(|e| {
            crate::Error::DataPlane(ErrorCode::Internal {
                detail: format!(
                    "ProviderScan: failed to re-encode row after window/computed evaluation: {e}"
                ),
            })
        })?;
        out.push(bytes);
    }

    Ok(out)
}
