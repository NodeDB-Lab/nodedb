// SPDX-License-Identifier: Apache-2.0

//! Public helpers for streaming aggregate accumulators.
//!
//! These thin wrappers expose field-extraction primitives used by the
//! `handlers/aggregate.rs` streaming accumulator path in the `nodedb` crate.
//! Each function operates on a single raw MessagePack document byte slice and
//! returns only the scalar value needed by the calling accumulator — no
//! document bytes are retained after the call returns.

use nodedb_types::Value;

use crate::expr::{EvalError, SqlExpr};
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_numeric, read_str, read_value};
use crate::value_ops;

// ── Expression evaluator ───────────────────────────────────────────────────

/// Evaluate `expr` against a decoded document.
///
/// The `Result<Option<Value>, EvalError>` shape distinguishes the two ways a
/// per-row aggregate argument can decline to contribute a value:
/// - `Ok(None)` — the row is *skipped* (document could not be decoded, or the
///   expression legitimately yields no value). Matches SQL aggregate semantics
///   where a NULL/absent argument is excluded from the accumulation.
/// - `Err(EvalError::DivisionByZero)` — the expression divided or took a
///   modulus by zero. This is a statement failure that
///   surfaces as SQLSTATE `22012`, exactly like a WHERE/projection expression,
///   rather than silently dropping the row from the aggregate.
#[inline]
fn eval_expr(doc: &[u8], expr: &SqlExpr) -> Result<Option<Value>, EvalError> {
    let Ok(doc_val) = nodedb_types::json_msgpack::value_from_msgpack(doc) else {
        return Ok(None);
    };
    Ok(Some(expr.eval(&doc_val)?))
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
) -> Result<Option<f64>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr)?.and_then(|v| value_ops::value_to_f64(&v, false)));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(read_f64(doc, start))
}

/// Extract the SUM / AVG input of `field`, or of `expr` if provided, as the
/// number it contributes (`Integer`, `Float`, or `Decimal`). A raw field
/// and an expression result contribute by the same rule,
/// [`crate::numeric_sum::sum_input`]: a msgpack number read exactly, or a
/// numeric string read per [`crate::numeric_sum::numeric_text`] (a
/// `DECIMAL` cell is stored as its text).
/// Returns `Ok(None)` when nothing contributes, and
/// `Err(EvalError::DivisionByZero)` when `expr` divides/mods by zero.
#[inline]
pub fn extract_sum_value(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
) -> Result<Option<Value>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr)?.and_then(|v| crate::numeric_sum::sum_input(&v)));
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
) -> Result<Option<String>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr(doc, expr)?.map(|v| value_ops::value_to_display_string(&v)));
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
) -> Result<Option<Value>, EvalError> {
    if let Some(expr) = expr {
        return eval_expr(doc, expr);
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

/// Extract a field or expression result as raw msgpack bytes.
/// Used by `count_distinct`, `approx_count_distinct`, `approx_topk`, etc.
pub fn extract_bytes(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
) -> Result<Option<Vec<u8>>, EvalError> {
    if let Some(expr) = expr {
        let Some(val) = eval_expr(doc, expr)? else {
            return Ok(None);
        };
        return Ok(nodedb_types::json_msgpack::value_to_msgpack(&val).ok());
    }
    let Some((start, end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(Some(doc[start..end].to_vec()))
}

/// Returns `Ok(Some(()))` when the field is present and non-null.
/// Used by `count(field)` accumulator to count non-null values.
#[inline]
pub fn extract_non_null(
    doc: &[u8],
    field: &str,
    expr: Option<&SqlExpr>,
) -> Result<Option<()>, EvalError> {
    let Some(v) = extract_value(doc, field, expr)? else {
        return Ok(None);
    };
    Ok(if v.is_null() { None } else { Some(()) })
}
