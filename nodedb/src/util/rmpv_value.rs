// SPDX-License-Identifier: BUSL-1.1

//! The one converter pair between `rmpv::Value` and `nodedb_types::Value`.
//!
//! Timeseries rows, trigger batches and `RETURNING` projections all move
//! between the two dynamic forms. Both directions live here so an instant,
//! a binary blob or a nested map renders the same way on every path.
//!
//! Instants cross as the msgpack ext `nodedb_types::write_instant` produces:
//! `rmpv::Value::Ext(1 | 2, <i64 BE micros>)`. Every other ext type reads as
//! `Value::Null`, the rule `nodedb_types::value_from_msgpack` applies.

use std::collections::HashMap;

use nodedb_types::json_msgpack::instant_from_ext;
use nodedb_types::{InstantKind, Value};

/// Convert an `rmpv::Value` to a `nodedb_types::Value`.
///
/// - `Integer` outside `i64` reads as its `u64` bits cast to `i64`.
/// - A `String` that is not UTF-8 reads as `Value::Bytes` of its raw bytes.
/// - A map key that is not a string renders through rmpv's `Display`.
/// - `Ext` reads as an instant for ext types 1 and 2, else `Value::Null`.
pub fn rmpv_to_value(v: &rmpv::Value) -> Value {
    match v {
        rmpv::Value::Nil => Value::Null,
        rmpv::Value::Boolean(b) => Value::Bool(*b),
        rmpv::Value::Integer(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => Value::Integer(i),
            (None, Some(u)) => Value::Integer(u as i64),
            (None, None) => Value::Null,
        },
        rmpv::Value::F32(f) => Value::Float(f64::from(*f)),
        rmpv::Value::F64(f) => Value::Float(*f),
        rmpv::Value::String(s) => match s.as_str() {
            Some(text) => Value::String(text.to_string()),
            None => Value::Bytes(s.as_bytes().to_vec()),
        },
        rmpv::Value::Binary(b) => Value::Bytes(b.clone()),
        rmpv::Value::Array(items) => Value::Array(items.iter().map(rmpv_to_value).collect()),
        rmpv::Value::Map(entries) => {
            let mut map = HashMap::with_capacity(entries.len());
            for (key, value) in entries {
                let name = match key {
                    rmpv::Value::String(s) => match s.as_str() {
                        Some(text) => text.to_string(),
                        None => key.to_string(),
                    },
                    other => other.to_string(),
                };
                map.insert(name, rmpv_to_value(value));
            }
            Value::Object(map)
        }
        rmpv::Value::Ext(ext_type, payload) => match instant_from_ext(*ext_type, payload) {
            Some((kind, micros)) => kind.from_micros(micros),
            None => Value::Null,
        },
    }
}

/// Convert a `nodedb_types::Value` to an `rmpv::Value`.
///
/// Mirrors `nodedb_types::value_to_msgpack` variant for variant, so an rmpv
/// row encodes to the same bytes a `Value` row does:
///
/// - `Uuid`, `Ulid`, `Regex`, `Duration`, `Decimal` and `Geometry` are strings.
/// - `Set` is an array. `Vector` is an array of `F64`.
/// - `ArrayCell` is a `{coords, attrs}` map, plus `_ts_system` when versioned.
/// - `Range` and `Record` have no rmpv form and become `Nil`.
pub fn value_to_rmpv(v: &Value) -> rmpv::Value {
    match v {
        Value::Null => rmpv::Value::Nil,
        Value::Bool(b) => rmpv::Value::Boolean(*b),
        Value::Integer(n) => rmpv::Value::Integer((*n).into()),
        Value::Float(f) => rmpv::Value::F64(*f),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
            rmpv::Value::String(s.as_str().into())
        }
        Value::Bytes(b) => rmpv::Value::Binary(b.clone()),
        Value::Array(items) | Value::Set(items) => {
            rmpv::Value::Array(items.iter().map(value_to_rmpv).collect())
        }
        Value::Object(map) => rmpv::Value::Map(
            map.iter()
                .map(|(k, v)| (rmpv::Value::String(k.as_str().into()), value_to_rmpv(v)))
                .collect(),
        ),
        Value::DateTime(dt) => instant_ext(InstantKind::Utc, dt.micros),
        Value::NaiveDateTime(dt) => instant_ext(InstantKind::Naive, dt.micros),
        Value::Duration(d) => rmpv::Value::String(d.to_string().into()),
        Value::Decimal(d) => rmpv::Value::String(d.to_string().into()),
        Value::Geometry(g) => match sonic_rs::to_string(g) {
            Ok(text) => rmpv::Value::String(text.into()),
            Err(_) => rmpv::Value::Nil,
        },
        Value::Range { .. } | Value::Record { .. } => rmpv::Value::Nil,
        Value::Vector(floats) => rmpv::Value::Array(
            floats
                .iter()
                .map(|f| rmpv::Value::F64(f64::from(*f)))
                .collect(),
        ),
        Value::ArrayCell(cell) => {
            let mut fields = vec![
                (
                    rmpv::Value::String("coords".into()),
                    rmpv::Value::Array(cell.coords.iter().map(value_to_rmpv).collect()),
                ),
                (
                    rmpv::Value::String("attrs".into()),
                    rmpv::Value::Array(cell.attrs.iter().map(value_to_rmpv).collect()),
                ),
            ];
            if let Some(ts) = cell.system_time {
                fields.push((
                    rmpv::Value::String("_ts_system".into()),
                    rmpv::Value::Integer(ts.into()),
                ));
            }
            rmpv::Value::Map(fields)
        }
        // `Value` is `#[non_exhaustive]`: a variant this crate cannot name
        // has no rmpv form.
        _ => rmpv::Value::Nil,
    }
}

fn instant_ext(kind: InstantKind, micros: i64) -> rmpv::Value {
    rmpv::Value::Ext(kind.ext_type(), micros.to_be_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::NdbDateTime;

    #[test]
    fn instants_round_trip_both_kinds() {
        for v in [
            Value::DateTime(NdbDateTime::from_micros(1_583_402_400_000_000)),
            Value::NaiveDateTime(NdbDateTime::from_micros(1_583_402_400_000_000)),
        ] {
            let ext = value_to_rmpv(&v);
            assert!(matches!(ext, rmpv::Value::Ext(1 | 2, ref b) if b.len() == 8));
            assert_eq!(rmpv_to_value(&ext), v);
        }
    }

    #[test]
    fn negative_micros_round_trip() {
        let v = Value::NaiveDateTime(NdbDateTime::from_micros(-86_400_000_000));
        assert_eq!(rmpv_to_value(&value_to_rmpv(&v)), v);
    }

    #[test]
    fn instant_ext_matches_the_native_encoding() {
        let v = Value::DateTime(NdbDateTime::from_micros(42));
        let mut via_rmpv = Vec::new();
        rmpv::encode::write_value(&mut via_rmpv, &value_to_rmpv(&v)).expect("encode");
        let native = nodedb_types::value_to_msgpack(&v).expect("native");
        assert_eq!(via_rmpv, native);
    }

    #[test]
    fn nested_object_and_array_round_trip() {
        let mut inner = HashMap::new();
        inner.insert("n".to_string(), Value::Integer(-7));
        inner.insert("b".to_string(), Value::Bytes(vec![9, 8]));
        let mut outer = HashMap::new();
        outer.insert(
            "arr".to_string(),
            Value::Array(vec![
                Value::Null,
                Value::Bool(true),
                Value::Float(1.5),
                Value::String("x".into()),
                Value::Object(inner),
            ]),
        );
        let v = Value::Object(outer);
        assert_eq!(rmpv_to_value(&value_to_rmpv(&v)), v);
    }

    #[test]
    fn unknown_ext_reads_as_null() {
        assert_eq!(rmpv_to_value(&rmpv::Value::Ext(7, vec![0; 8])), Value::Null);
    }

    #[test]
    fn u64_beyond_i64_reads_as_its_bits() {
        let v = rmpv::Value::Integer(rmpv::Integer::from(u64::MAX));
        assert_eq!(rmpv_to_value(&v), Value::Integer(-1));
    }
}
