// SPDX-License-Identifier: BUSL-1.1

//! Window-function and computed-column evaluation for `QueryOp::ProviderScan`.
//!
//! Runs after filter and before sort/distinct/offset/project/limit in the
//! `ProviderScan` pipeline. Each msgpack row decodes to a `nodedb_types::Value`,
//! windows evaluate over the full row set, computed columns evaluate
//! per-row, and the result re-encodes to msgpack. `Value` keeps NaN and
//! ±Infinity results, which a JSON number cannot. Skipped entirely when both
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
    crs: &nodedb_crs::CrsContext<'_>,
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

    let mut value_rows: Vec<(String, nodedb_types::Value)> = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        let value = crate::data::executor::doc_format::decode_document_value(row).map_err(|e| {
            crate::Error::DataPlane(ErrorCode::Internal {
                detail: format!("ProviderScan: malformed row for window/computed evaluation: {e}"),
            })
        })?;
        value_rows.push((idx.to_string(), value));
    }

    if !window_specs.is_empty() {
        evaluate_window_functions(&mut value_rows, &window_specs, crs)
            .map_err(crate::Error::from)?;
    }

    for (_, row) in &mut value_rows {
        if computed_cols.is_empty() {
            continue;
        }
        // Every computed column evaluates against the row as it stood before
        // this loop, matching `apply_projection_msgpack`'s semantics: later computed
        // columns never observe earlier ones' results.
        let before = row.clone();
        for cc in &computed_cols {
            let already_present = matches!(row.get(&cc.alias), Some(v) if !v.is_null());
            if already_present {
                continue;
            }
            let v = cc.expr.eval(&before, crs)?;
            if let nodedb_types::Value::Object(obj) = row {
                obj.insert(cc.alias.clone(), v);
            }
        }
    }

    let mut out = Vec::with_capacity(value_rows.len());
    for (_, row) in value_rows {
        let bytes = zerompk::to_msgpack_vec(&row).map_err(|e| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    #[test]
    fn native_computation_preserves_typed_source_and_result() {
        let decimal = Value::Decimal("9007199254740993.0001".parse().expect("decimal"));
        let doc = Value::Object(
            [
                ("amount".into(), decimal.clone()),
                (
                    "uuid".into(),
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
                (
                    "nested".into(),
                    Value::Array(vec![Value::Bytes(vec![0, 255])]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let body = zerompk::to_msgpack_vec(&doc).expect("native body");
        let computed = zerompk::to_msgpack_vec(&vec![ComputedColumn {
            alias: "copy".into(),
            expr: nodedb_query::SqlExpr::Column("amount".into()),
        }])
        .expect("computed columns");
        let rows = apply_windows_and_computed(
            vec![body],
            &[],
            &computed,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("computed rows");
        let row =
            crate::data::executor::doc_format::decode_document_value(&rows[0]).expect("native row");
        for (name, value) in doc.as_object().expect("source") {
            assert_eq!(row.get(name), Some(value));
        }
        assert_eq!(row.get("copy"), Some(&decimal));
    }
}
