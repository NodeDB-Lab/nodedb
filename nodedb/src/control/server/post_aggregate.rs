// SPDX-License-Identifier: BUSL-1.1

//! Post-join aggregation in the Control Plane.
//!
//! When a query has `GROUP BY` over a `JOIN` result, the Data Plane cores
//! return raw join rows. This module aggregates them in the Control Plane.
//! All processing stays in msgpack — no JSON intermediary.
//!
//! SUM / AVG total exactly per `nodedb_query::ExactSum`. MIN / MAX keep the
//! original number and compare exactly, so integers above 2^53 never round
//! through `f64`.

use std::collections::HashMap;

use nodedb_query::agg_key::canonical_agg_key;
use nodedb_query::msgpack_scan::reader;
use nodedb_query::numeric_sum::{ExactSum, sum_input};
use nodedb_query::window::extremum::value_replaces;
use nodedb_types::Value;

use crate::bridge::envelope::{Payload, Response};

/// Apply GROUP BY + aggregate functions on a join response payload.
///
/// The input response payload is a msgpack array of maps (merged from all cores).
/// Returns a new response with aggregated results (also msgpack).
pub fn apply_post_aggregation(
    resp: Response,
    group_by: &[String],
    aggregates: &[(String, String)],
) -> crate::Result<Response> {
    let payload_bytes = resp.payload.as_bytes();

    // Parse the msgpack array of row-maps.
    let rows = parse_msgpack_rows(payload_bytes)?;

    // Group rows by the GROUP BY columns.
    let mut groups: HashMap<Vec<String>, Vec<&[u8]>> = HashMap::new();
    for row in &rows {
        let key: Vec<String> = group_by
            .iter()
            .map(|col| extract_field_str(row, col).unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(row);
    }

    // Build result as msgpack array.
    use nodedb_query::msgpack_scan::writer;
    let mut buf = Vec::with_capacity(payload_bytes.len());
    writer::write_array_header(&mut buf, groups.len());

    for (key, group_rows) in &groups {
        let field_count = group_by.len() + aggregates.len();
        writer::write_map_header(&mut buf, field_count);

        // Write GROUP BY columns.
        for (i, col) in group_by.iter().enumerate() {
            writer::write_kv_str(&mut buf, col, &key[i]);
        }

        // Compute and write each aggregate.
        for (op, field) in aggregates {
            let agg_key = canonical_agg_key(op, field);
            let value = compute_aggregate(op, field, group_rows)?;
            write_kv_number(&mut buf, &agg_key, &value);
        }
    }

    Ok(Response {
        payload: Payload::from_vec(buf),
        ..resp
    })
}

/// Write an aggregate result: an integer, a float, NULL, or a `Decimal`
/// as its exact text (the msgpack form of a `Decimal`).
fn write_kv_number(buf: &mut Vec<u8>, key: &str, value: &Value) {
    use nodedb_query::msgpack_scan::writer;
    match value {
        Value::Integer(n) => writer::write_kv_i64(buf, key, *n),
        Value::Float(f) => writer::write_kv_f64(buf, key, *f),
        Value::Decimal(d) => writer::write_kv_str(buf, key, &d.to_string()),
        _ => writer::write_kv_null(buf, key),
    }
}

/// Parse a msgpack array payload into individual row slices.
fn parse_msgpack_rows(bytes: &[u8]) -> crate::Result<Vec<&[u8]>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let (count, mut pos) =
        reader::array_header(bytes, 0).ok_or_else(|| crate::Error::PlanError {
            detail: "post-aggregation: invalid msgpack array header".into(),
        })?;

    // The msgpack array header is untrusted; reserve only after each row has
    // been proven present by `skip_value`.
    let mut rows = Vec::new();
    for _ in 0..count {
        let start = pos;
        pos = reader::skip_value(bytes, pos).ok_or_else(|| crate::Error::PlanError {
            detail: "post-aggregation: truncated msgpack row".into(),
        })?;
        rows.push(&bytes[start..pos]);
    }
    Ok(rows)
}

/// The value range of `field` in a msgpack map row: an exact key match
/// first, then a `"collection.field"` suffix match.
fn field_range(row: &[u8], field: &str) -> Option<(usize, usize)> {
    if let Some(range) = nodedb_query::msgpack_scan::extract_field(row, 0, field) {
        return Some(range);
    }
    let suffix = format!(".{field}");
    let (count, mut pos) = reader::map_header(row, 0)?;
    for _ in 0..count {
        let key = reader::read_str(row, pos)?;
        let key_end = reader::skip_value(row, pos)?;
        let val_end = reader::skip_value(row, key_end)?;
        if key.ends_with(&suffix) {
            return Some((key_end, val_end));
        }
        pos = val_end;
    }
    None
}

/// Extract a field value as string from a msgpack map row.
fn extract_field_str(row: &[u8], field: &str) -> Option<String> {
    let (start, end) = field_range(row, field)?;
    Some(read_value_as_string(row, start, end))
}

/// The number a row contributes to SUM / AVG / MIN / MAX of `field`: a
/// number as itself, a numeric string as the number it spells. Integers
/// stay exact. `*` contributes `1`.
fn extract_number(row: &[u8], field: &str) -> Option<Value> {
    if field == "*" {
        return Some(Value::Integer(1));
    }
    let (start, _end) = field_range(row, field)?;
    sum_input(&reader::read_value(row, start)?)
}

/// Read a msgpack value at [start..end) as a display string.
fn read_value_as_string(bytes: &[u8], start: usize, end: usize) -> String {
    if let Some(s) = reader::read_str(bytes, start) {
        return s.to_string();
    }
    if let Some(i) = reader::read_integer(bytes, start) {
        return i.to_string();
    }
    if let Some(f) = reader::read_f64(bytes, start) {
        return f.to_string();
    }
    if let Some(b) = reader::read_bool(bytes, start) {
        return b.to_string();
    }
    if reader::read_null(bytes, start) {
        return String::new();
    }
    // Complex value — transcode slice to JSON string.
    nodedb_types::msgpack_to_json_string(&bytes[start..end]).unwrap_or_default()
}

/// Compute a single aggregate over a group of msgpack rows.
///
/// Fails with `EvalError::NumericOverflow` when an exact integer SUM / AVG
/// total lies outside the `Decimal` range.
fn compute_aggregate(
    op: &str,
    field: &str,
    rows: &[&[u8]],
) -> Result<Value, nodedb_query::EvalError> {
    let numbers = || rows.iter().filter_map(|r| extract_number(r, field));
    let exact_sum = || {
        let mut acc = ExactSum::new();
        for v in numbers() {
            acc.add_value(&v);
        }
        acc
    };
    let extremum = |want_max: bool| {
        let mut best: Option<Value> = None;
        for v in numbers() {
            if value_replaces(&v, best.as_ref(), want_max) {
                best = Some(v);
            }
        }
        best.unwrap_or(Value::Null)
    };
    Ok(match op {
        "count" => Value::Integer(rows.len() as i64),
        "sum" => exact_sum().sum()?,
        "avg" => exact_sum().avg()?,
        "min" => extremum(false),
        "max" => extremum(true),
        _ => Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    fn agg(op: &str, vals: &[serde_json::Value]) -> Value {
        let docs: Vec<Vec<u8>> = vals.iter().map(|v| encode(&json!({"t.v": v}))).collect();
        let rows: Vec<&[u8]> = docs.iter().map(Vec::as_slice).collect();
        compute_aggregate(op, "v", &rows).unwrap()
    }

    #[test]
    fn integers_above_2_pow_53_stay_exact() {
        let vals = [json!(ABOVE), json!(AT)];
        assert_eq!(agg("sum", &vals), Value::Integer(ABOVE + AT));
        assert_eq!(agg("min", &vals), Value::Integer(AT));
        assert_eq!(agg("max", &vals), Value::Integer(ABOVE));
        assert_eq!(agg("avg", &vals), Value::Float(AT as f64));
    }

    #[test]
    fn nanosecond_timestamps_stay_exact() {
        let vals = [
            json!(1_700_000_000_000_000_002_i64),
            json!(1_700_000_000_000_000_001_i64),
        ];
        assert_eq!(agg("min", &vals), Value::Integer(1_700_000_000_000_000_001));
        assert_eq!(agg("sum", &vals), Value::Integer(3_400_000_000_000_000_003));
    }

    #[test]
    fn u64_above_i64_max_and_sum_past_i64() {
        let vals = [json!(u64::MAX), json!(i64::MAX)];
        assert_eq!(
            agg("max", &vals),
            Value::Decimal(rust_decimal::Decimal::from(u64::MAX))
        );
        assert_eq!(agg("min", &vals), Value::Integer(i64::MAX));
        assert_eq!(
            agg("sum", &vals),
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                i128::from(u64::MAX) + i128::from(i64::MAX),
                0
            ))
        );
    }

    #[test]
    fn mixed_int_float_and_numeric_strings() {
        assert_eq!(
            agg("sum", &[json!(2), json!(0.5), json!("7")]),
            Value::Float(9.5)
        );
        let vals = [json!(AT), json!("9007199254740993"), json!(0.5)];
        assert_eq!(agg("max", &vals), Value::Integer(ABOVE));
        assert_eq!(agg("min", &vals), Value::Float(0.5));
        assert_eq!(agg("sum", &[json!("x")]), Value::Null);
    }

    #[test]
    fn group_key_text_keeps_u64_digits() {
        let doc = encode(&json!({"k": u64::MAX}));
        assert_eq!(
            extract_field_str(&doc, "k"),
            Some("18446744073709551615".to_string())
        );
    }

    #[test]
    fn decimal_results_write_exact_text() {
        // A one-entry map: fixmap header, then the written key/value pair.
        let mut doc = vec![0x81];
        let total = Value::Decimal(rust_decimal::Decimal::from(u64::MAX));
        write_kv_number(&mut doc, "sum(v)", &total);
        let (start, _) = nodedb_query::msgpack_scan::extract_field(&doc, 0, "sum(v)").unwrap();
        assert_eq!(reader::read_str(&doc, start), Some("18446744073709551615"));
    }
}
