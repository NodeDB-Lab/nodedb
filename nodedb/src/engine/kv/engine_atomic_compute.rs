// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for `INCR`/`INCR_FLOAT`/`CAS`/`GETSET`, shared by
//! the autocommit `KvEngine` methods (`engine_atomic.rs`) and the
//! in-transaction staging handlers (`stage_kv_atomic.rs`), so a staged value
//! and its COMMIT-time durable replay are always computed by the exact same
//! code. Split out of `engine_atomic.rs` to keep that file under the
//! file-size limit.

use std::collections::HashMap;

use nodedb_query::msgpack_scan::{KvBodyShape, row_to_kv_body};
use nodedb_types::Value;

use super::engine_atomic::AtomicError;

/// The field of a typed row an atomic never targets.
const KEY_FIELD: &str = "key";

/// Decode a map-shaped body into its typed columns. Returns `None` for a
/// body of any other shape.
fn decode_map(bytes: &[u8]) -> Option<HashMap<String, Value>> {
    match nodedb_types::value_from_msgpack(bytes) {
        Ok(Value::Object(map)) => Some(map),
        _ => None,
    }
}

/// Encode typed columns back into a map-shaped body. The fields are written
/// in key order, so every replica and every WAL replay stores the same
/// bytes.
fn encode_map(map: HashMap<String, Value>) -> Result<Vec<u8>, AtomicError> {
    row_to_kv_body(&Value::Object(map), KvBodyShape::Map).map_err(|e| AtomicError::Encode {
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

fn not_an_integer() -> AtomicError {
    AtomicError::TypeMismatch {
        detail: "value is not an integer".into(),
    }
}

fn not_numeric() -> AtomicError {
    AtomicError::TypeMismatch {
        detail: "value is not numeric".into(),
    }
}

/// Decode a bare MessagePack scalar as i64.
fn decode_scalar_i64(bytes: &[u8]) -> Result<i64, AtomicError> {
    // Try i64 first, then u64 (MessagePack encodes small positive as u64).
    if let Ok(v) = zerompk::from_msgpack::<i64>(bytes) {
        return Ok(v);
    }
    if let Ok(v) = zerompk::from_msgpack::<u64>(bytes) {
        return i64::try_from(v).map_err(|_| AtomicError::Overflow);
    }
    // A float with no fractional part truncates to i64.
    zerompk::from_msgpack::<f64>(bytes)
        .ok()
        .and_then(integral_f64_to_i64)
        .ok_or(not_an_integer())
}

/// Decode a bare MessagePack scalar as f64.
fn decode_scalar_f64(bytes: &[u8]) -> Result<f64, AtomicError> {
    if let Ok(v) = zerompk::from_msgpack::<f64>(bytes) {
        return Ok(v);
    }
    // Accept integer values promoted to float.
    if let Ok(v) = zerompk::from_msgpack::<i64>(bytes) {
        return Ok(v as f64);
    }
    if let Ok(v) = zerompk::from_msgpack::<u64>(bytes) {
        return Ok(v as f64);
    }
    Err(not_numeric())
}

/// Encode an `i64` as MessagePack, wrapping the (practically unreachable, but
/// not type-system-excluded) encode failure in [`AtomicError::Encode`] rather
/// than panicking.
fn encode_i64(v: i64) -> Result<Vec<u8>, AtomicError> {
    zerompk::to_msgpack_vec(&v).map_err(|e| AtomicError::Encode {
        detail: format!("i64 re-encode: {e}"),
    })
}

/// Encode an `f64` as MessagePack, same rationale as [`encode_i64`].
fn encode_f64(v: f64) -> Result<Vec<u8>, AtomicError> {
    zerompk::to_msgpack_vec(&v).map_err(|e| AtomicError::Encode {
        detail: format!("f64 re-encode: {e}"),
    })
}

/// Compute the new value for `INCR`, given the current raw bytes (if
/// any). Returns `(new_i64, new_bytes)`.
///
/// A typed row keeps its shape: the numeric column [`target_field`] picks
/// moves, and every other column stays. A bare scalar stays a bare scalar.
pub fn incr(current: Option<&[u8]>, delta: i64) -> Result<(i64, Vec<u8>), AtomicError> {
    if let Some(mut map) = current.and_then(decode_map) {
        let (field, old_i64) = target_field(&map, column_i64).ok_or(not_an_integer())?;
        let new_i64 = old_i64.checked_add(delta).ok_or(AtomicError::Overflow)?;
        map.insert(field, Value::Integer(new_i64));
        return Ok((new_i64, encode_map(map)?));
    }
    let old_i64 = match current {
        None => 0i64,
        Some(bytes) => decode_scalar_i64(bytes)?,
    };
    let new_i64 = old_i64.checked_add(delta).ok_or(AtomicError::Overflow)?;
    Ok((new_i64, encode_i64(new_i64)?))
}

/// Compute the new value for `INCR_FLOAT`. Returns `(new_f64, new_bytes)`.
///
/// A typed row keeps its shape, as in [`incr`]. A bare scalar is stored as
/// a bare f64.
pub fn incr_float(current: Option<&[u8]>, delta: f64) -> Result<(f64, Vec<u8>), AtomicError> {
    let mut row = current.and_then(decode_map);
    let (field, old_f64) = match (&row, current) {
        (Some(map), _) => {
            let (field, old) = target_field(map, column_f64).ok_or(not_numeric())?;
            (Some(field), old)
        }
        (None, None) => (None, 0.0f64),
        (None, Some(bytes)) => (None, decode_scalar_f64(bytes)?),
    };
    let new_f64 = old_f64 + delta;
    if new_f64.is_nan() || new_f64.is_infinite() {
        return Err(AtomicError::Overflow);
    }
    let new_bytes = match (row.take(), field) {
        (Some(mut map), Some(field)) => {
            map.insert(field, Value::Float(new_f64));
            encode_map(map)?
        }
        _ => encode_f64(new_f64)?,
    };
    Ok((new_f64, new_bytes))
}

/// Write `new_value` into the string column of the typed row `row` and
/// encode it. `column` is the column [`target_field`] picked.
fn swap_string_column(
    mut row: HashMap<String, Value>,
    column: String,
    new_value: &[u8],
) -> Result<Vec<u8>, AtomicError> {
    row.insert(
        column,
        Value::String(String::from_utf8_lossy(new_value).into_owned()),
    );
    encode_map(row)
}

/// A typed row and its string column, when `current` is a typed row with
/// one. [`cas`] and [`getset`] address the same column.
fn string_column(current: Option<&[u8]>) -> Option<(HashMap<String, Value>, String, String)> {
    let row = current.and_then(decode_map)?;
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
) -> Result<(bool, Vec<u8>), AtomicError> {
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
pub fn getset(current: Option<&[u8]>, new_value: &[u8]) -> Result<Vec<u8>, AtomicError> {
    match string_column(current) {
        Some((row, column, _)) => swap_string_column(row, column, new_value),
        None => Ok(new_value.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(fields: &[(&str, Value)]) -> Vec<u8> {
        let map: HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode row")
    }

    fn columns(bytes: &[u8]) -> HashMap<String, Value> {
        decode_map(bytes).expect("a typed row stays a typed row")
    }

    #[test]
    fn incr_on_a_one_column_typed_row_keeps_the_row() {
        let current = row(&[("n", Value::Integer(5))]);
        let (new_i64, bytes) = incr(Some(&current), 3).expect("incr");
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
        let (new_i64, bytes) = incr(Some(&current), 1).expect("incr");
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
        let (_, first) = incr(Some(&current), 1).expect("incr");
        for _ in 0..16 {
            let (_, again) = incr(Some(&current), 1).expect("incr");
            assert_eq!(again, first);
        }
    }

    #[test]
    fn incr_on_a_typed_row_without_a_numeric_column_is_a_type_mismatch() {
        let current = row(&[("label", Value::String("x".into()))]);
        assert!(matches!(
            incr(Some(&current), 1),
            Err(AtomicError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn incr_on_a_bare_scalar_stays_a_bare_scalar() {
        let current = zerompk::to_msgpack_vec(&5i64).expect("encode");
        let (new_i64, bytes) = incr(Some(&current), 3).expect("incr");
        assert_eq!(new_i64, 8);
        assert_eq!(zerompk::from_msgpack::<i64>(&bytes).expect("decode"), 8);
        let (fresh, _) = incr(None, 4).expect("incr");
        assert_eq!(fresh, 4);
    }

    #[test]
    fn incr_float_on_a_one_column_typed_row_keeps_the_row() {
        let current = row(&[("score", Value::Float(1.5))]);
        let (new_f64, bytes) = incr_float(Some(&current), 1.0).expect("incr_float");
        assert_eq!(new_f64, 2.5);
        assert_eq!(columns(&bytes).get("score"), Some(&Value::Float(2.5)));
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
