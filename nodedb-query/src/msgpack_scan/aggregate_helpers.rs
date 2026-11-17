// SPDX-License-Identifier: Apache-2.0

//! Public helpers for streaming aggregate accumulators.
//!
//! These thin wrappers expose field-extraction primitives used by the
//! streaming aggregate accumulators in the `nodedb` crate.
//! Each function operates on a single raw MessagePack document byte slice and
//! returns only the scalar value needed by the calling accumulator — no
//! document bytes are retained after the call returns. Geo functions in an
//! argument expression resolve SRIDs through the `crs` argument.

use nodedb_crs::CrsContext;
use nodedb_types::{ObjectFormat, ObjectView, Value};

use crate::expr::{EvalError, SqlExpr};
use crate::msgpack_scan::canonical_key::canonical_key_value;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_numeric, read_str, read_value};
use crate::value_ops;

// ── Expression evaluator ───────────────────────────────────────────────────

/// Evaluate `expr` against a decoded document.
///
/// The `Result<Option<Value>, EvalError>` shape distinguishes the two ways a
/// per-row aggregate argument can decline to contribute a value:
/// - `Ok(None)` — the row is *skipped* (expression legitimately yields no value). Matches SQL aggregate semantics
///   where a NULL/absent argument is excluded from the accumulation.
/// - `Err(EvalError::DivisionByZero)` — the expression divided or took a
///   modulus by zero. This is a statement failure that
///   surfaces as SQLSTATE `22012`, exactly like a WHERE/projection expression,
///   rather than silently dropping the row from the aggregate.
#[inline]
fn eval_expr(doc: &[u8], expr: &SqlExpr, crs: &CrsContext<'_>) -> Result<Option<Value>, EvalError> {
    let doc_val = nodedb_types::decode_object(doc).map_err(decode_error)?;
    Ok(Some(expr.eval(&doc_val, crs)?))
}

pub(crate) fn decode_error(error: impl std::fmt::Display) -> EvalError {
    EvalError::ArgumentValue {
        function: "document",
        position: 1,
        reason: format!("cannot decode object: {error}"),
    }
}

fn native_field(doc: &[u8], field: &str) -> Result<Option<Option<Value>>, EvalError> {
    if matches!(doc.first(), Some(0x80..=0x8f | 0xde | 0xdf)) {
        return Ok(None);
    }
    let view = ObjectView::new(doc).map_err(decode_error)?;
    if view.format() == ObjectFormat::Native {
        return view.get(field).map(Some).map_err(decode_error);
    }
    Ok(None)
}

// ── Public extraction helpers ──────────────────────────────────────────────

/// Extract a numeric (f64) value from `field`, or evaluate `expr` if provided.
/// Returns `Ok(None)` when the field is absent or cannot be converted to f64,
/// and `Err(EvalError::DivisionByZero)` when `expr` divides/mods by zero.
#[inline]
pub fn extract_f64(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<f64>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr, crs)?.and_then(|v| value_ops::value_to_f64(&v, false)));
    }
    if let Some(value) = native_field(doc, field)? {
        return Ok(value.and_then(|value| value_ops::value_to_f64(&value, false)));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(read_f64(doc, start))
}

/// Extract the SUM / AVG input of `field`, or of `expr` if provided, as the
/// number it contributes (`Integer`, `Float`, or `Decimal`). A raw field
/// and an expression result contribute by the same rule,
/// [`crate::numeric_sum::sum_input`]. Native Decimal fields retain their exact value.
/// Plain numeric strings use [`crate::numeric_sum::numeric_text`].
/// Returns `Ok(None)` when nothing contributes, and
/// `Err(EvalError::DivisionByZero)` when `expr` divides/mods by zero.
#[inline]
pub fn extract_sum_value(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<Value>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr, crs)?.and_then(|v| crate::numeric_sum::sum_input(&v)));
    }
    if let Some(value) = native_field(doc, field)? {
        return Ok(value.and_then(|value| crate::numeric_sum::sum_input(&value)));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(match read_numeric(doc, start) {
        Some(n) => Some(crate::numeric_sum::numeric_to_value(n)),
        None => read_str(doc, start).and_then(crate::numeric_sum::numeric_text),
    })
}

/// Extract a display string from `field`, or evaluate `expr` if provided.
/// Returns `Ok(None)` when the field is absent.
pub fn extract_str(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<String>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr, crs)?.map(|v| value_ops::value_to_display_string(&v)));
    }
    if let Some(value) = native_field(doc, field)? {
        return Ok(value.and_then(|value| match value {
            Value::String(value) => Some(value),
            _ => None,
        }));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(read_str(doc, start).map(|s| s.to_string()))
}

/// Extract a field as `Value`.  Uses direct msgpack→Value for scalars;
/// falls back to full document decode only for complex types.
pub fn extract_value(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<Value>, EvalError> {
    if let Some(expr) = expr {
        return eval_expr(doc, expr, crs);
    }
    if let Some(value) = native_field(doc, field)? {
        return Ok(value);
    }
    let Some((start, end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    if let Some(v) = read_value(doc, start) {
        return Ok(Some(v));
    }
    let field_bytes = &doc[start..end];
    Ok(nodedb_types::json_msgpack::value_from_msgpack(field_bytes).ok())
}

/// Extract a field or expression result as its distinct-value identity: the
/// tagged msgpack of its [`canonical_key_value`]. Used by the DISTINCT
/// accumulators, `approx_count_distinct`, and `approx_topk`, so they agree
/// with GROUP BY identity: `5`, `5.0` and `5.00` are one value, `-0.0` and
/// `0.0` are one, and every NaN is one. The identity is for equality only;
/// an accumulator that outputs the value keeps the first-seen typed value.
/// Returns `Ok(None)` when the field is absent or NULL: a NULL contributes
/// no distinct value.
pub fn extract_bytes(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<Vec<u8>>, EvalError> {
    extract_value(doc, field, expr, crs)?
        .filter(|value| !value.is_null())
        .map(|value| zerompk::to_msgpack_vec(&canonical_key_value(value)).map_err(decode_error))
        .transpose()
}

/// Returns `Ok(Some(()))` when the field is present and non-null.
/// Used by `count(field)` accumulator to count non-null values.
#[inline]
pub fn extract_non_null(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
    crs: &CrsContext<'_>,
) -> Result<Option<()>, EvalError> {
    let Some(v) = extract_value(doc, field, expr, crs)? else {
        return Ok(None);
    };
    Ok(if v.is_null() { None } else { Some(()) })
}

#[cfg(test)]
#[path = "aggregate_native_tests.rs"]
mod native_tests;
