// SPDX-License-Identifier: Apache-2.0

//! Pure value computation for `INCR`/`INCR_FLOAT`/`CAS`/`GETSET`.
//!
//! Every executor computes a stored value with these functions. On Origin
//! that is the autocommit `KvEngine` methods, transaction staging, the
//! resolve handlers, and WAL replay. On Lite it is the KV write path. All of
//! them store the same bytes for the same op.
//!
//! A body has one of two shapes ([`kv_body_shape`]). A typed row (a msgpack
//! map) keeps its typed column semantics. A raw body (the single-`value` SQL
//! form, RESP `SET`) is a byte string. `INCR` and `INCR_FLOAT` read it as
//! decimal text by the Redis rules and store the result as decimal text.

use std::collections::HashMap;

use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_shape, row_to_kv_body};
use nodedb_types::Value;

use super::counter_fault::CounterFault;
use super::error::AtomicComputeError;
use super::float_text;
use crate::physical_plan::KvCounterShape;

/// The field of a typed row an atomic never targets.
const KEY_FIELD: &str = "key";

/// Decode a map-shaped body into its typed columns. Returns `Ok(None)` for a
/// raw body, and `TypeMismatch` for a map-shaped body that does not decode.
fn typed_row(bytes: &[u8]) -> Result<Option<HashMap<String, Value>>, AtomicComputeError> {
    if kv_body_shape(bytes) != KvBodyShape::Map {
        return Ok(None);
    }
    match nodedb_types::value_from_msgpack(bytes) {
        Ok(Value::Object(map)) => Ok(Some(map)),
        Ok(other) => Err(AtomicComputeError::TypeMismatch {
            detail: format!("stored row is {}, not an object", other.type_name()),
        }),
        Err(e) => Err(AtomicComputeError::TypeMismatch {
            detail: format!("stored row does not decode: {e}"),
        }),
    }
}

/// Encode typed columns back into a map-shaped body. The fields are written
/// in key order, so every replica and every WAL replay stores the same
/// bytes.
fn encode_map(map: HashMap<String, Value>) -> Result<Vec<u8>, AtomicComputeError> {
    row_to_kv_body(&Value::Object(map), KvBodyShape::Map).map_err(|e| AtomicComputeError::Encode {
        detail: format!("typed row re-encode: {e}"),
    })
}

/// The column an atomic reads and writes in a typed row: the first column
/// in key order that `pick` accepts, never the `key` column.
///
/// Key order is the order the row is stored in. A `HashMap` iterates in a
/// per-process random order, so choosing by iteration order lets two
/// replicas move two different columns.
fn target_field<T>(
    map: &HashMap<String, Value>,
    pick: impl Fn(&Value) -> Option<T>,
) -> Option<(String, T)> {
    let mut chosen: Option<(&String, T)> = None;
    for (name, value) in map {
        if name == KEY_FIELD {
            continue;
        }
        if chosen.as_ref().is_some_and(|(best, _)| *best <= name) {
            continue;
        }
        if let Some(picked) = pick(value) {
            chosen = Some((name, picked));
        }
    }
    chosen.map(|(name, picked)| (name.clone(), picked))
}

/// The i64 an `INCR` reads from a typed column.
fn column_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) => integral_f64_to_i64(*f),
        _ => None,
    }
}

/// The f64 an `INCR_FLOAT` reads from a typed column.
fn column_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    }
}

/// The string a `CAS` or `GETSET` addresses in a typed column.
fn column_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// A whole `f64` inside the i64 range, as an i64.
fn integral_f64_to_i64(v: f64) -> Option<i64> {
    (v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64).then_some(v as i64)
}

fn not_an_integer_column() -> AtomicComputeError {
    AtomicComputeError::TypeMismatch {
        detail: "row has no integer column".into(),
    }
}

fn not_a_numeric_column() -> AtomicComputeError {
    AtomicComputeError::TypeMismatch {
        detail: "row has no numeric column".into(),
    }
}

/// Read a raw body as a decimal i64 by the Redis rule.
fn parse_raw_i64(bytes: &[u8]) -> Result<i64, AtomicComputeError> {
    std::str::from_utf8(bytes)
        .ok()
        .filter(|text| is_canonical_integer(text))
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(AtomicComputeError::Counter(CounterFault::NotAnInteger))
}

/// The Redis integer grammar: `0`, or an optional `-` then digits with no
/// leading zero. A `+` sign, whitespace, and an empty body are refused.
fn is_canonical_integer(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    text == "0"
        || (digits
            .bytes()
            .next()
            .is_some_and(|b| (b'1'..=b'9').contains(&b))
            && digits.bytes().all(|b| b.is_ascii_digit()))
}

/// The raw body for an integer: its decimal text, the text
/// `scalar_to_raw_bytes` writes for the same value.
fn raw_decimal(v: i64) -> Vec<u8> {
    v.to_string().into_bytes()
}

/// The row an absent key becomes under a typed [`KvCounterShape`]: the
/// template with `column` set to `value`.
fn fresh_typed_row(
    column: &Option<String>,
    template: &[u8],
    value: Value,
    missing_column: AtomicComputeError,
) -> Result<Vec<u8>, AtomicComputeError> {
    let column = column.as_ref().ok_or(missing_column)?;
    let mut map = typed_row(template)?.ok_or(AtomicComputeError::TypeMismatch {
        detail: "fresh row template is not a typed row".into(),
    })?;
    map.insert(column.clone(), value);
    encode_map(map)
}

/// Compute the new value for `INCR`, given the current body (if any).
/// Returns `(new_i64, new_bytes)`.
///
/// A typed row keeps its shape: the integer column [`target_field`] picks
/// moves, and every other column stays. A raw body is decimal text in and
/// decimal text out. An absent key starts at 0 and takes `shape`.
pub fn incr(
    current: Option<&[u8]>,
    delta: i64,
    shape: &KvCounterShape,
) -> Result<(i64, Vec<u8>), AtomicComputeError> {
    let overflow = AtomicComputeError::Counter(CounterFault::IntegerOverflow);
    let Some(bytes) = current else {
        let written = match shape {
            KvCounterShape::Raw => raw_decimal(delta),
            KvCounterShape::Typed { column, template } => fresh_typed_row(
                column,
                template,
                Value::Integer(delta),
                not_an_integer_column(),
            )?,
        };
        return Ok((delta, written));
    };
    if let Some(mut map) = typed_row(bytes)? {
        let (field, old_i64) = target_field(&map, column_i64).ok_or(not_an_integer_column())?;
        let new_i64 = old_i64.checked_add(delta).ok_or(overflow)?;
        map.insert(field, Value::Integer(new_i64));
        return Ok((new_i64, encode_map(map)?));
    }
    let new_i64 = parse_raw_i64(bytes)?.checked_add(delta).ok_or(overflow)?;
    Ok((new_i64, raw_decimal(new_i64)))
}

/// Compute the new value for `INCR_FLOAT`. `delta` is the client's decimal
/// text. Returns `(new_f64, new_bytes)`.
///
/// A typed row keeps its shape, as in [`incr`], and its column adds in
/// `f64`. A raw body is decimal text in and decimal text out, added exactly
/// by the Redis rules (see `float_text`). An absent key starts at 0 and takes
/// `shape`.
pub fn incr_float(
    current: Option<&[u8]>,
    delta: &str,
    shape: &KvCounterShape,
) -> Result<(f64, Vec<u8>), AtomicComputeError> {
    let Some(bytes) = current else {
        return match shape {
            KvCounterShape::Raw => float_text::fresh(delta),
            KvCounterShape::Typed { column, template } => {
                let value = float_text::delta_to_f64(delta)?;
                let written = fresh_typed_row(
                    column,
                    template,
                    Value::Float(value),
                    not_a_numeric_column(),
                )?;
                Ok((value, written))
            }
        };
    };
    let Some(mut map) = typed_row(bytes)? else {
        return float_text::add(bytes, delta);
    };
    let delta = float_text::delta_to_f64(delta)?;
    let (field, old_f64) = target_field(&map, column_f64).ok_or(not_a_numeric_column())?;
    let new_f64 = old_f64 + delta;
    if !new_f64.is_finite() {
        return Err(AtomicComputeError::Counter(CounterFault::NonFinite));
    }
    map.insert(field, Value::Float(new_f64));
    Ok((new_f64, encode_map(map)?))
}

/// Write `new_value` into the string column of the typed row `row` and
/// encode it. `column` is the column [`target_field`] picked.
fn swap_string_column(
    mut row: HashMap<String, Value>,
    column: String,
    new_value: &[u8],
) -> Result<Vec<u8>, AtomicComputeError> {
    row.insert(
        column,
        Value::String(String::from_utf8_lossy(new_value).into_owned()),
    );
    encode_map(row)
}

/// A typed row and its string column, when `current` is a typed row with
/// one. [`cas`] and [`getset`] address the same column.
fn string_column(current: Option<&[u8]>) -> Option<(HashMap<String, Value>, String, String)> {
    let row = typed_row(current?).ok().flatten()?;
    let (column, text) = target_field(&row, column_string)?;
    Some((row, column, text))
}

/// Compute the CAS outcome: whether `expected` matches the current value,
/// and the bytes to write when it does.
///
/// The current value matches when its bytes equal `expected`, or when it is
/// a typed row whose string column holds `expected`. A typed row with a
/// string column keeps its shape: only that column is swapped.
pub fn cas(
    current: Option<&[u8]>,
    expected: &[u8],
    new_value: &[u8],
) -> Result<(bool, Vec<u8>), AtomicComputeError> {
    let Some(cur) = current else {
        return Ok(if expected.is_empty() {
            (true, new_value.to_vec())
        } else {
            (false, Vec::new())
        });
    };
    let typed = string_column(current);
    let column_matches = typed
        .as_ref()
        .is_some_and(|(_, _, text)| *text == String::from_utf8_lossy(expected));
    if cur != expected && !column_matches {
        return Ok((false, Vec::new()));
    }
    let write_bytes = match typed {
        Some((row, column, _)) => swap_string_column(row, column, new_value)?,
        None => new_value.to_vec(),
    };
    Ok((true, write_bytes))
}

/// Compute the bytes to write for `GETSET`: the string column of a typed
/// row swapped in place, or a plain overwrite.
pub fn getset(current: Option<&[u8]>, new_value: &[u8]) -> Result<Vec<u8>, AtomicComputeError> {
    match string_column(current) {
        Some((row, column, _)) => swap_string_column(row, column, new_value),
        None => Ok(new_value.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static RAW: KvCounterShape = KvCounterShape::Raw;

    /// A typed shape moving `column`, with `rest` as the other stored columns.
    fn typed_shape(column: Option<&str>, rest: &[(&str, Value)]) -> KvCounterShape {
        KvCounterShape::Typed {
            column: column.map(str::to_string),
            template: row(rest),
        }
    }

    fn row(fields: &[(&str, Value)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode row")
    }

    fn columns(bytes: &[u8]) -> HashMap<String, Value> {
        typed_row(bytes)
            .expect("a typed row decodes")
            .expect("a typed row stays a typed row")
    }

    #[test]
    fn incr_on_a_one_column_typed_row_keeps_the_row() {
        let current = row(&[("n", Value::Integer(5))]);
        let (new_i64, bytes) = incr(Some(&current), 3, &RAW).expect("incr");
        assert_eq!(new_i64, 8);
        assert_eq!(columns(&bytes).get("n"), Some(&Value::Integer(8)));
    }

    #[test]
    fn incr_moves_the_first_numeric_column_in_key_order() {
        let current = row(&[
            ("b", Value::Integer(100)),
            ("a", Value::Integer(1)),
            ("label", Value::String("x".into())),
        ]);
        let (new_i64, bytes) = incr(Some(&current), 1, &RAW).expect("incr");
        assert_eq!(new_i64, 2);
        let cols = columns(&bytes);
        assert_eq!(cols.get("a"), Some(&Value::Integer(2)));
        assert_eq!(cols.get("b"), Some(&Value::Integer(100)));
        assert_eq!(cols.get("label"), Some(&Value::String("x".into())));
    }

    #[test]
    fn incr_on_a_typed_row_encodes_the_same_bytes_every_time() {
        let current = row(&[
            ("a", Value::Integer(1)),
            ("b", Value::Integer(2)),
            ("c", Value::Integer(3)),
        ]);
        let (_, first) = incr(Some(&current), 1, &RAW).expect("incr");
        for _ in 0..16 {
            let (_, again) = incr(Some(&current), 1, &RAW).expect("incr");
            assert_eq!(again, first);
        }
    }

    #[test]
    fn incr_on_a_typed_row_without_a_numeric_column_is_a_type_mismatch() {
        let current = row(&[("label", Value::String("x".into()))]);
        assert!(matches!(
            incr(Some(&current), 1, &RAW),
            Err(AtomicComputeError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn incr_on_a_raw_body_reads_and_writes_decimal_text() {
        let (new_i64, bytes) = incr(Some(b"5"), 1, &RAW).expect("incr");
        assert_eq!(new_i64, 6);
        assert_eq!(bytes, b"6".to_vec());

        let (new_i64, bytes) = incr(Some(b"-10"), 3, &RAW).expect("incr");
        assert_eq!(new_i64, -7);
        assert_eq!(bytes, b"-7".to_vec());

        let (fresh, bytes) = incr(None, 4, &RAW).expect("incr");
        assert_eq!(fresh, 4);
        assert_eq!(bytes, b"4".to_vec());
    }

    #[test]
    fn incr_on_non_integer_raw_text_is_not_an_integer() {
        for body in [
            b"abc".as_slice(),
            b"",
            b"1.5",
            b"+5",
            b"05",
            b"-0",
            b" 5",
            b"5 ",
            b"99999999999999999999",
        ] {
            assert!(
                matches!(
                    incr(Some(body), 1, &RAW),
                    Err(AtomicComputeError::Counter(CounterFault::NotAnInteger))
                ),
                "{:?}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn incr_past_the_i64_range_is_an_overflow() {
        let max = i64::MAX.to_string();
        assert!(matches!(
            incr(Some(max.as_bytes()), 1, &RAW),
            Err(AtomicComputeError::Counter(CounterFault::IntegerOverflow))
        ));
        let min = i64::MIN.to_string();
        assert!(matches!(
            incr(Some(min.as_bytes()), -1, &RAW),
            Err(AtomicComputeError::Counter(CounterFault::IntegerOverflow))
        ));
        let (value, bytes) = incr(Some(min.as_bytes()), 0, &RAW).expect("i64::MIN parses");
        assert_eq!(value, i64::MIN);
        assert_eq!(bytes, min.into_bytes());
    }

    #[test]
    fn incr_float_on_a_raw_body_reads_and_writes_decimal_text() {
        let (new_f64, bytes) = incr_float(Some(b"1.5"), "1", &RAW).expect("incr_float");
        assert_eq!(new_f64, 2.5);
        assert_eq!(bytes, b"2.5".to_vec());

        let (new_f64, bytes) = incr_float(Some(b"10.5"), "0.5", &RAW).expect("incr_float");
        assert_eq!(new_f64, 11.0);
        assert_eq!(bytes, b"11".to_vec());

        let (_, bytes) = incr_float(Some(b"5"), "0.25", &RAW).expect("incr_float");
        assert_eq!(bytes, b"5.25".to_vec());

        for (stored, delta, expected) in [
            ("0.1", "0.2", "0.3"),
            ("10.5", "0.1", "10.6"),
            ("5.0e3", "200", "5200"),
            ("3.0", "0", "3"),
            ("-1.5", "1.5", "0"),
            ("1", "0.12345678901234567891", "1.12345678901234567891"),
        ] {
            let (_, bytes) = incr_float(Some(stored.as_bytes()), delta, &RAW).expect("incr_float");
            assert_eq!(bytes, expected.as_bytes().to_vec(), "{stored} + {delta}");
        }
    }

    #[test]
    fn incr_float_on_non_numeric_raw_text_is_not_a_float() {
        for body in [b"abc".as_slice(), b"", b"NaN", b" 1.5"] {
            assert!(
                matches!(
                    incr_float(Some(body), "1", &RAW),
                    Err(AtomicComputeError::Counter(CounterFault::NotAFloat))
                ),
                "{:?}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn incr_float_to_infinity_is_non_finite() {
        let max = f64::MAX.to_string();
        assert!(matches!(
            incr_float(Some(max.as_bytes()), &max, &RAW),
            Err(AtomicComputeError::Counter(CounterFault::NonFinite))
        ));
    }

    #[test]
    fn incr_float_on_a_one_column_typed_row_keeps_the_row() {
        let current = row(&[("score", Value::Float(1.5))]);
        let (new_f64, bytes) = incr_float(Some(&current), "1", &RAW).expect("incr_float");
        assert_eq!(new_f64, 2.5);
        assert_eq!(columns(&bytes).get("score"), Some(&Value::Float(2.5)));
    }

    #[test]
    fn incr_on_an_absent_key_under_a_typed_shape_creates_the_typed_row() {
        let shape = typed_shape(Some("n"), &[("status", Value::String("new".into()))]);
        let (value, bytes) = incr(None, 7, &shape).expect("incr");
        assert_eq!(value, 7);
        let cols = columns(&bytes);
        assert_eq!(cols.get("n"), Some(&Value::Integer(7)));
        assert_eq!(cols.get("status"), Some(&Value::String("new".into())));
    }

    #[test]
    fn incr_float_on_an_absent_key_under_a_typed_shape_creates_the_typed_row() {
        let shape = typed_shape(Some("score"), &[]);
        let (value, bytes) = incr_float(None, "2.5", &shape).expect("incr_float");
        assert_eq!(value, 2.5);
        assert_eq!(columns(&bytes).get("score"), Some(&Value::Float(2.5)));
    }

    #[test]
    fn an_absent_key_under_a_typed_shape_without_a_column_is_a_type_mismatch() {
        let shape = typed_shape(None, &[]);
        assert!(matches!(
            incr(None, 1, &shape),
            Err(AtomicComputeError::TypeMismatch { .. })
        ));
        assert!(matches!(
            incr_float(None, "1", &shape),
            Err(AtomicComputeError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn cas_on_a_one_column_typed_row_swaps_the_column() {
        let current = row(&[("state", Value::String("idle".into()))]);
        let (matched, bytes) = cas(Some(&current), b"idle", b"busy").expect("cas");
        assert!(matched);
        assert_eq!(
            columns(&bytes).get("state"),
            Some(&Value::String("busy".into()))
        );
        let (matched, _) = cas(Some(&current), b"busy", b"idle").expect("cas");
        assert!(!matched);
    }

    #[test]
    fn getset_on_a_one_column_typed_row_swaps_the_column() {
        let current = row(&[("token", Value::String("old".into()))]);
        let bytes = getset(Some(&current), b"new").expect("getset");
        assert_eq!(
            columns(&bytes).get("token"),
            Some(&Value::String("new".into()))
        );
        assert_eq!(getset(None, b"raw").expect("getset"), b"raw".to_vec());
    }
}
