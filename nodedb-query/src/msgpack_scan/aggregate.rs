// SPDX-License-Identifier: Apache-2.0

//! Zero-deserialization aggregate computation on raw MessagePack documents.
//!
//! Replaces `compute_aggregate(op, field, docs: &[serde_json::Value])` with
//! direct binary field extraction. Each document is `&[u8]` (MessagePack map).
//! When an expression is provided, decodes msgpack → `nodedb_types::Value`
//! directly (no JSON intermediate) and evaluates the expression once per document.

use std::cmp::Ordering;
use std::collections::HashSet;

use nodedb_types::Value;

use crate::expr::EvalError;
use crate::msgpack_scan::aggregate_helpers::extract_sum_value;
use crate::msgpack_scan::compare::compare_field_bytes;
use crate::msgpack_scan::field::extract_field;
use crate::msgpack_scan::reader::{read_f64, read_null, read_str};
use crate::numeric_sum::ExactSum;
use crate::value_ops;
use crate::window::extremum::value_replaces;

/// Compute an aggregate function over raw MessagePack documents.
///
/// Each entry in `docs` is a complete MessagePack map (the raw bytes from storage).
/// Returns the result as `Value` — conversion to JSON happens at the
/// response boundary only.
pub fn compute_aggregate_binary(
    op: &str,
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    docs: &[&[u8]],
) -> Result<Value, EvalError> {
    Ok(match op {
        "count" => {
            if field == "*" && expr.is_none() {
                Value::Integer(docs.len() as i64)
            } else {
                let count = docs
                    .iter()
                    .map(|d| extract_as_value(d, field, expr))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .filter(|v| !v.is_null())
                    .count();
                Value::Integer(count as i64)
            }
        }

        "sum" => exact_sum(docs, field, expr)?.sum()?,

        "avg" => exact_sum(docs, field, expr)?.avg()?,

        "min" => find_minmax(docs, field, expr, false)?,
        "max" => find_minmax(docs, field, expr, true)?,

        "count_distinct" => {
            let mut seen = HashSet::new();
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, field, expr)?
                    && !value_bytes_are_null(&bytes)
                {
                    seen.insert(bytes);
                }
            }
            Value::Integer(seen.len() as i64)
        }

        "stddev" | "stddev_pop" => {
            stat_aggregate(docs, field, expr, |variance, _n| variance.sqrt(), true)?
        }

        "stddev_samp" => stat_aggregate(docs, field, expr, |variance, _n| variance.sqrt(), false)?,

        "variance" | "var_pop" => stat_aggregate(docs, field, expr, |variance, _n| variance, true)?,

        "var_samp" => stat_aggregate(docs, field, expr, |variance, _n| variance, false)?,

        "array_agg" => {
            let values: Vec<Value> = docs
                .iter()
                .map(|d| extract_as_value(d, field, expr))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .filter(|v| !v.is_null())
                .collect();
            Value::Array(values)
        }

        "array_agg_distinct" => {
            let mut seen_bytes = HashSet::new();
            let mut values = Vec::new();
            for doc in docs {
                // When expr is present, evaluate once and derive both bytes and value
                // from the result to avoid double-decoding the document.
                if let Some(expr) = expr {
                    let Some(val) = eval_expr_on_doc(doc, expr)? else {
                        continue;
                    };
                    if val.is_null() {
                        continue;
                    }
                    let bytes = zerompk::to_msgpack_vec(&val).unwrap_or_default();
                    if seen_bytes.insert(bytes) {
                        values.push(val);
                    }
                } else if let Some(bytes) = extract_value_bytes(doc, field, None)?
                    && !value_bytes_are_null(&bytes)
                    && seen_bytes.insert(bytes)
                    && let Some(v) = value_from_field(doc, field)
                {
                    values.push(v);
                }
            }
            Value::Array(values)
        }

        "string_agg" | "group_concat" => {
            let values: Vec<String> = docs
                .iter()
                .map(|d| extract_str_val(d, field, expr))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();
            Value::String(values.join(","))
        }

        "approx_count_distinct" => {
            let mut hll = nodedb_types::approx::HyperLogLog::new();
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, field, expr)?
                    && !value_bytes_are_null(&bytes)
                {
                    // Hash the raw bytes for HLL.
                    let hash = hash_bytes(&bytes);
                    hll.add(hash);
                }
            }
            Value::Integer(hll.estimate().round() as i64)
        }

        "approx_percentile" => {
            // Format: field is "quantile:actual_field" (e.g. "0.95:latency").
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<f64>() {
                    Ok(p) => (p, &field[idx + 1..]),
                    Err(_) => return Ok(Value::Null), // invalid quantile
                }
            } else {
                (0.5, field)
            };
            let mut digest = nodedb_types::approx::TDigest::new();
            for doc in docs {
                if let Some(v) = extract_f64_val(doc, actual_field, expr)? {
                    digest.add(v);
                }
            }
            let result = digest.quantile(pct);
            if result.is_nan() {
                Value::Null
            } else {
                Value::Float(result)
            }
        }

        "approx_topk" => {
            // Format: field is "k:actual_field" (e.g. "10:region").
            let (k, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<usize>() {
                    Ok(k) => (k, &field[idx + 1..]),
                    Err(_) => return Ok(Value::Null), // invalid k
                }
            } else {
                (10, field)
            };
            let mut ss = nodedb_types::approx::SpaceSaving::new(k);
            for doc in docs {
                if let Some(bytes) = extract_value_bytes(doc, actual_field, expr)?
                    && !value_bytes_are_null(&bytes)
                {
                    ss.add(hash_bytes(&bytes));
                }
            }
            // Return as array of [hash, count, error] tuples.
            let top = ss.top_k();
            let arr: Vec<Value> = top
                .into_iter()
                .map(|(item, count, error)| {
                    Value::Object(
                        [
                            ("item".to_string(), Value::Integer(item as i64)),
                            ("count".to_string(), Value::Integer(count as i64)),
                            ("error".to_string(), Value::Integer(error as i64)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect();
            Value::Array(arr)
        }

        "percentile_cont" => {
            let (pct, actual_field) = if let Some(idx) = field.find(':') {
                match field[..idx].parse::<f64>() {
                    Ok(p) => (p, &field[idx + 1..]),
                    Err(_) => return Ok(Value::Null), // invalid quantile
                }
            } else {
                (0.5, field)
            };
            let mut values: Vec<f64> = docs
                .iter()
                .map(|d| extract_f64_val(d, actual_field, expr))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let idx = (pct * (values.len() - 1) as f64).clamp(0.0, (values.len() - 1) as f64);
            let lower = idx.floor() as usize;
            let upper = idx.ceil() as usize;
            let frac = idx - lower as f64;
            let result = values[lower] * (1.0 - frac) + values[upper] * frac;
            Value::Float(result)
        }

        _ => Value::Null,
    })
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Decode a msgpack document directly to `nodedb_types::Value` and evaluate
/// the expression. No JSON intermediate — msgpack → Value → eval → Value.
///
/// `Ok(None)` means the row is skipped (document could not be decoded);
/// `Err(EvalError::DivisionByZero)` means the expression divided/modded by
/// zero and must propagate as a statement failure rather than being folded
/// to `None`.
#[inline]
fn eval_expr_on_doc(doc: &[u8], expr: &crate::expr::SqlExpr) -> Result<Option<Value>, EvalError> {
    let Ok(doc_val) = nodedb_types::json_msgpack::value_from_msgpack(doc) else {
        return Ok(None);
    };
    Ok(Some(expr.eval(&doc_val)?))
}

/// Extract a numeric value from a field or expression result.
#[inline]
fn extract_f64_val(
    doc: &[u8],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Result<Option<f64>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr_on_doc(doc, expr)?.and_then(|v| value_ops::value_to_f64(&v, false)));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(read_f64(doc, start))
}

/// Exact SUM / AVG state over `docs`, per [`ExactSum`].
fn exact_sum(
    docs: &[&[u8]],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Result<ExactSum, EvalError> {
    let mut acc = ExactSum::new();
    for doc in docs {
        if let Some(v) = extract_sum_value(doc, field, expr)? {
            acc.add_value(&v);
        }
    }
    Ok(acc)
}

/// Extract a string from a field or expression result.
fn extract_str_val(
    doc: &[u8],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Result<Option<String>, EvalError> {
    if let Some(expr) = expr {
        return Ok(eval_expr_on_doc(doc, expr)?.map(|v| value_ops::value_to_display_string(&v)));
    }
    let Some((start, _end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(read_str(doc, start).map(|s| s.to_string()))
}

/// Extract a field as `Value`. Uses direct msgpack→Value for scalars;
/// falls back to full decode only for complex types.
fn extract_as_value(
    doc: &[u8],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Result<Option<Value>, EvalError> {
    if let Some(expr) = expr {
        return eval_expr_on_doc(doc, expr);
    }
    Ok(value_from_field(doc, field))
}

#[inline]
fn value_from_field(doc: &[u8], field: &str) -> Option<Value> {
    let (start, end) = extract_field(doc, 0, field)?;
    // Fast path: scalar types (null, bool, int, float, string).
    if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
        return Some(v);
    }
    // Slow path: complex types (array, map, bin) — decode field bytes directly.
    let field_bytes = &doc[start..end];
    nodedb_types::json_msgpack::value_from_msgpack(field_bytes).ok()
}

/// The field value at `offset` when it is a msgpack integer or float.
fn read_numeric(doc: &[u8], offset: usize) -> Option<Value> {
    read_f64(doc, offset)?;
    crate::msgpack_scan::reader::read_value(doc, offset)
}

/// Find min or max across docs. The winner is returned unchanged: an
/// integer stays an integer, and a numeric pair compares exactly, so
/// integers above 2^53 never round through `f64`.
fn find_minmax(
    docs: &[&[u8]],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    want_max: bool,
) -> Result<Value, EvalError> {
    if let Some(expr) = expr {
        // Evaluate expression once per doc; compare on Value
        // since the result may be any type (not a raw field).
        let mut best: Option<Value> = None;
        for doc in docs {
            let Some(value) = eval_expr_on_doc(doc, expr)? else {
                continue;
            };
            if !value.is_null() && value_replaces(&value, best.as_ref(), want_max) {
                best = Some(value);
            }
        }
        return Ok(best.unwrap_or(Value::Null));
    }
    let wanted = if want_max {
        Ordering::Greater
    } else {
        Ordering::Less
    };

    let mut best_doc: Option<&[u8]> = None;
    let mut best_range: Option<(usize, usize)> = None;

    for doc in docs {
        if let Some(range) = extract_field(doc, 0, field) {
            if read_null(doc, range.0) {
                continue;
            }
            match best_range {
                None => {
                    best_doc = Some(doc);
                    best_range = Some(range);
                }
                Some(br) => {
                    let Some(bd) = best_doc else { continue };
                    let replace = match (read_numeric(doc, range.0), read_numeric(bd, br.0)) {
                        (Some(candidate), Some(current)) => {
                            value_replaces(&candidate, Some(&current), want_max)
                        }
                        _ => compare_field_bytes(doc, range, bd, br) == wanted,
                    };
                    if replace {
                        best_doc = Some(doc);
                        best_range = Some(range);
                    }
                }
            }
        }
    }

    Ok(match (best_doc, best_range) {
        (Some(doc), Some((start, end))) => {
            if let Some(v) = crate::msgpack_scan::reader::read_value(doc, start) {
                return Ok(v);
            }
            let bytes = &doc[start..end];
            nodedb_types::json_msgpack::value_from_msgpack(bytes).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    })
}

/// Compute stddev or variance. `population` = true for population variant.
/// `finalize` transforms the variance into the final result.
fn stat_aggregate(
    docs: &[&[u8]],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
    finalize: fn(f64, usize) -> f64,
    population: bool,
) -> Result<Value, EvalError> {
    let values: Vec<f64> = docs
        .iter()
        .map(|d| extract_f64_val(d, field, expr))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    if values.len() < 2 {
        return Ok(Value::Null);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let divisor = if population {
        values.len() as f64
    } else {
        (values.len() - 1) as f64
    };
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / divisor;
    Ok(Value::Float(finalize(variance, values.len())))
}

fn extract_value_bytes(
    doc: &[u8],
    field: &str,
    expr: Option<&crate::expr::SqlExpr>,
) -> Result<Option<Vec<u8>>, EvalError> {
    if let Some(expr) = expr {
        let Some(val) = eval_expr_on_doc(doc, expr)? else {
            return Ok(None);
        };
        return Ok(nodedb_types::json_msgpack::value_to_msgpack(&val).ok());
    }
    let Some((start, end)) = extract_field(doc, 0, field) else {
        return Ok(None);
    };
    Ok(Some(doc[start..end].to_vec()))
}

/// Check if msgpack bytes represent null. Msgpack null is the single byte 0xc0.
fn value_bytes_are_null(bytes: &[u8]) -> bool {
    bytes == [0xc0]
}

/// FNV-1a hash for raw bytes (used by approx aggregates to feed HLL/SpaceSaving).
fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn count() {
        let d1 = encode(&json!({"x": 1}));
        let d2 = encode(&json!({"x": 2}));
        let d3 = encode(&json!({"x": 3}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count", "x", None, &docs).unwrap(),
            Value::Integer(3)
        );
    }

    #[test]
    fn sum() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", None, &docs).unwrap(),
            Value::Integer(60)
        );
    }

    /// SUM and AVG of `vals` under field `v`, raw-field path then
    /// expression path.
    fn sum_avg_both_paths(vals: &[serde_json::Value]) -> [(Value, Value); 2] {
        let encoded: Vec<Vec<u8>> = vals.iter().map(|v| encode(&json!({ "v": v }))).collect();
        let docs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        let col = crate::expr::SqlExpr::Column("v".into());
        [None, Some(&col)].map(|expr| {
            (
                compute_aggregate_binary("sum", "v", expr, &docs).unwrap(),
                compute_aggregate_binary("avg", "v", expr, &docs).unwrap(),
            )
        })
    }

    #[test]
    fn sum_keeps_integers_above_2_pow_53_exact() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992_i64),
        ];
        for (sum, avg) in sum_avg_both_paths(&vals) {
            assert_eq!(sum, Value::Integer(18_014_398_509_481_985));
            assert_eq!(avg, Value::Float(9_007_199_254_740_992.0));
        }
    }

    #[test]
    fn sum_over_nanosecond_timestamps() {
        let vals = [
            json!(1_700_000_000_000_000_001_i64),
            json!(1_700_000_000_000_000_002_i64),
        ];
        for (sum, _) in sum_avg_both_paths(&vals) {
            assert_eq!(sum, Value::Integer(3_400_000_000_000_000_003));
        }
    }

    #[test]
    fn sum_past_i64_is_decimal() {
        let vals = [json!(i64::MAX), json!(i64::MAX), json!(u64::MAX)];
        let want = rust_decimal::Decimal::from_i128_with_scale(
            2 * i128::from(i64::MAX) + i128::from(u64::MAX),
            0,
        );
        for (sum, _) in sum_avg_both_paths(&vals) {
            assert_eq!(sum, Value::Decimal(want));
        }
    }

    #[test]
    fn sum_mixed_int_float_is_float() {
        let vals = [json!(2), json!(0.5)];
        for (sum, avg) in sum_avg_both_paths(&vals) {
            assert_eq!(sum, Value::Float(2.5));
            assert_eq!(avg, Value::Float(1.25));
        }
    }

    #[test]
    fn sum_empty_is_null() {
        let d1 = encode(&json!({"other": 1}));
        let docs: Vec<&[u8]> = vec![&d1];
        assert_eq!(
            compute_aggregate_binary("sum", "v", None, &docs).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn min_max_u64_above_i64_max() {
        let vals = [json!(u64::MAX), json!(i64::MAX), json!(-1)];
        for (min, max) in min_max_both_paths(&vals) {
            assert_eq!(min, Value::Integer(-1));
            assert_eq!(max, Value::Decimal(rust_decimal::Decimal::from(u64::MAX)));
        }
    }

    #[test]
    fn min_max_nan_yields_to_numbers() {
        // JSON has no NaN; write the float64 NaN by hand.
        let mut nan_doc = vec![0x81, 0xa1, b'v', 0xcb];
        nan_doc.extend_from_slice(&f64::NAN.to_bits().to_be_bytes());
        let one = encode(&json!({"v": 1}));
        let two = encode(&json!({"v": 2}));
        let docs: Vec<&[u8]> = vec![&nan_doc, &one, &two];
        assert_eq!(
            compute_aggregate_binary("min", "v", None, &docs).unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            compute_aggregate_binary("max", "v", None, &docs).unwrap(),
            Value::Integer(2)
        );
    }

    #[test]
    fn avg() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"v": 20}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("avg", "v", None, &docs).unwrap(),
            Value::Float(15.0)
        );
    }

    #[test]
    fn avg_empty() {
        let d1 = encode(&json!({"other": 1}));
        let docs: Vec<&[u8]> = vec![&d1];
        assert_eq!(
            compute_aggregate_binary("avg", "v", None, &docs).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn min_max() {
        let d1 = encode(&json!({"v": 5}));
        let d2 = encode(&json!({"v": 1}));
        let d3 = encode(&json!({"v": 9}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];

        let min = compute_aggregate_binary("min", "v", None, &docs).unwrap();
        let max = compute_aggregate_binary("max", "v", None, &docs).unwrap();
        assert_eq!(min, Value::Integer(1));
        assert_eq!(max, Value::Integer(9));
    }

    /// MIN and MAX of `vals` under field `v`, raw-field path then
    /// expression path.
    fn min_max_both_paths(vals: &[serde_json::Value]) -> [(Value, Value); 2] {
        let encoded: Vec<Vec<u8>> = vals.iter().map(|v| encode(&json!({ "v": v }))).collect();
        let docs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        let col = crate::expr::SqlExpr::Column("v".into());
        [None, Some(&col)].map(|expr| {
            (
                compute_aggregate_binary("min", "v", expr, &docs).unwrap(),
                compute_aggregate_binary("max", "v", expr, &docs).unwrap(),
            )
        })
    }

    #[test]
    fn min_max_keep_integers_above_2_pow_53_exact() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992_i64),
        ];
        for (min, max) in min_max_both_paths(&vals) {
            assert_eq!(min, Value::Integer(9_007_199_254_740_992));
            assert_eq!(max, Value::Integer(9_007_199_254_740_993));
        }
    }

    #[test]
    fn min_max_over_nanosecond_timestamps() {
        let vals = [
            json!(1_700_000_000_000_000_002_i64),
            json!(1_700_000_000_000_000_001_i64),
            json!(1_700_000_000_000_000_003_i64),
        ];
        for (min, max) in min_max_both_paths(&vals) {
            assert_eq!(min, Value::Integer(1_700_000_000_000_000_001));
            assert_eq!(max, Value::Integer(1_700_000_000_000_000_003));
        }
    }

    #[test]
    fn min_max_mixed_int_float_return_original_type() {
        let vals = [
            json!(9_007_199_254_740_993_i64),
            json!(9_007_199_254_740_992.0_f64),
            json!(1.5),
            json!(2),
        ];
        for (min, max) in min_max_both_paths(&vals) {
            assert_eq!(min, Value::Float(1.5));
            assert_eq!(max, Value::Integer(9_007_199_254_740_993));
        }
    }

    #[test]
    fn count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": "b"}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", None, &docs).unwrap(),
            Value::Integer(2)
        );
    }

    #[test]
    fn string_agg() {
        let d1 = encode(&json!({"n": "alice"}));
        let d2 = encode(&json!({"n": "bob"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        assert_eq!(
            compute_aggregate_binary("string_agg", "n", None, &docs).unwrap(),
            Value::String("alice,bob".into())
        );
    }

    #[test]
    fn array_agg() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let docs: Vec<&[u8]> = vec![&d1, &d2];
        let result = compute_aggregate_binary("array_agg", "v", None, &docs).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Integer(1), Value::Integer(2),])
        );
    }

    #[test]
    fn stddev_pop() {
        let d1 = encode(&json!({"v": 2.0}));
        let d2 = encode(&json!({"v": 4.0}));
        let d3 = encode(&json!({"v": 4.0}));
        let d4 = encode(&json!({"v": 4.0}));
        let d5 = encode(&json!({"v": 5.0}));
        let d6 = encode(&json!({"v": 5.0}));
        let d7 = encode(&json!({"v": 7.0}));
        let d8 = encode(&json!({"v": 9.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3, &d4, &d5, &d6, &d7, &d8];
        let result = compute_aggregate_binary("stddev_pop", "v", None, &docs).unwrap();
        if let Value::Float(v) = result {
            assert!((v - 2.0).abs() < 0.01);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn percentile_cont_median() {
        let d1 = encode(&json!({"v": 1.0}));
        let d2 = encode(&json!({"v": 2.0}));
        let d3 = encode(&json!({"v": 3.0}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("percentile_cont", "v", None, &docs).unwrap(),
            Value::Float(2.0)
        );
    }

    #[test]
    fn missing_field_skipped() {
        let d1 = encode(&json!({"v": 10}));
        let d2 = encode(&json!({"other": 99}));
        let d3 = encode(&json!({"v": 30}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("sum", "v", None, &docs).unwrap(),
            Value::Integer(40)
        );
    }

    #[test]
    fn null_field_skipped_in_count_distinct() {
        let d1 = encode(&json!({"v": "a"}));
        let d2 = encode(&json!({"v": null}));
        let d3 = encode(&json!({"v": "a"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        assert_eq!(
            compute_aggregate_binary("count_distinct", "v", None, &docs).unwrap(),
            Value::Integer(1)
        );
    }

    #[test]
    fn array_agg_distinct() {
        let d1 = encode(&json!({"v": 1}));
        let d2 = encode(&json!({"v": 2}));
        let d3 = encode(&json!({"v": 1}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        let result = compute_aggregate_binary("array_agg_distinct", "v", None, &docs).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Integer(1), Value::Integer(2),])
        );
    }

    #[test]
    fn sum_case_when_expression() {
        let d1 = encode(&json!({"category": "tools"}));
        let d2 = encode(&json!({"category": "books"}));
        let d3 = encode(&json!({"category": "tools"}));
        let docs: Vec<&[u8]> = vec![&d1, &d2, &d3];
        let expr = crate::expr::SqlExpr::Case {
            operand: None,
            when_thens: vec![(
                crate::expr::SqlExpr::BinaryOp {
                    left: Box::new(crate::expr::SqlExpr::Column("category".into())),
                    op: crate::expr::BinaryOp::Eq,
                    right: Box::new(crate::expr::SqlExpr::Literal(Value::String("tools".into()))),
                },
                crate::expr::SqlExpr::Literal(Value::Integer(1)),
            )],
            else_expr: Some(Box::new(crate::expr::SqlExpr::Literal(Value::Integer(0)))),
        };

        assert_eq!(
            compute_aggregate_binary("sum", "*", Some(&expr), &docs).unwrap(),
            Value::Integer(2)
        );
    }

    #[test]
    fn approx_count_distinct_basic() {
        let docs: Vec<Vec<u8>> = vec![
            encode(&json!({"region": "us"})),
            encode(&json!({"region": "eu"})),
            encode(&json!({"region": "us"})),
            encode(&json!({"region": "ap"})),
        ];
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result =
            compute_aggregate_binary("approx_count_distinct", "region", None, &refs).unwrap();
        // HLL may not be exactly 3 but should be close.
        if let Value::Integer(n) = result {
            assert!((2..=4).contains(&n), "expected ~3 distinct, got {n}");
        } else {
            panic!("expected Integer, got {result:?}");
        }
    }

    #[test]
    fn approx_percentile_basic() {
        let docs: Vec<Vec<u8>> = (1..=100).map(|i| encode(&json!({"val": i}))).collect();
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result = compute_aggregate_binary("approx_percentile", "0.5:val", None, &refs).unwrap();
        if let Value::Float(f) = result {
            assert!(
                (f - 50.0).abs() < 10.0,
                "p50 of 1..100 should be ~50, got {f}"
            );
        } else {
            panic!("expected Float, got {result:?}");
        }
    }

    #[test]
    fn approx_topk_basic() {
        let mut docs: Vec<Vec<u8>> = Vec::new();
        for _ in 0..10 {
            docs.push(encode(&json!({"cat": "a"})));
        }
        for _ in 0..5 {
            docs.push(encode(&json!({"cat": "b"})));
        }
        for _ in 0..1 {
            docs.push(encode(&json!({"cat": "c"})));
        }
        let refs: Vec<&[u8]> = docs.iter().map(|d| d.as_slice()).collect();
        let result = compute_aggregate_binary("approx_topk", "3:cat", None, &refs).unwrap();
        if let Value::Array(arr) = result {
            assert!(!arr.is_empty(), "should have top-k results");
        } else {
            panic!("expected Array, got {result:?}");
        }
    }

    #[test]
    fn division_by_zero_in_expr_propagates() {
        // `SUM(a / b)` over a document with `b = 0` must surface
        // `EvalError::DivisionByZero` rather than folding the offending row
        // to NULL and silently continuing the aggregate.
        let d1 = encode(&json!({"a": 10, "b": 0}));
        let docs: Vec<&[u8]> = vec![&d1];
        let expr = crate::expr::SqlExpr::BinaryOp {
            left: Box::new(crate::expr::SqlExpr::Column("a".into())),
            op: crate::expr::BinaryOp::Div,
            right: Box::new(crate::expr::SqlExpr::Column("b".into())),
        };
        let err = compute_aggregate_binary("sum", "*", Some(&expr), &docs).unwrap_err();
        assert_eq!(err, EvalError::DivisionByZero);
    }
}
