// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;

use nodedb_types::columnar::ColumnType;
use nodedb_types::datetime::NdbDateTime;
use nodedb_types::decode_bounds::checked_decode_capacity;
use nodedb_types::value::Value;

use crate::error::StrictError;

/// Decode a fixed-size raw byte slice into a value.
pub(super) fn decode_fixed_value(col_type: &ColumnType, raw: &[u8]) -> Value {
    match col_type {
        ColumnType::Int64 => Value::Integer(i64::from_le_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ])),
        ColumnType::Float64 => Value::Float(f64::from_le_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ])),
        ColumnType::Bool => Value::Bool(raw[0] != 0),
        ColumnType::Timestamp => {
            let micros = i64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Value::NaiveDateTime(NdbDateTime::from_micros(micros))
        }
        ColumnType::Timestamptz | ColumnType::SystemTimestamp => {
            let micros = i64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Value::DateTime(NdbDateTime::from_micros(micros))
        }
        ColumnType::Ulid => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Ulid(ulid::Ulid::from_bytes(bytes).to_string())
        }
        ColumnType::Duration => {
            let micros = i64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Value::Duration(nodedb_types::NdbDuration::from_micros(micros))
        }
        ColumnType::Decimal { .. } => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Decimal(rust_decimal::Decimal::deserialize(bytes))
        }
        ColumnType::Uuid => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Uuid(uuid::Uuid::from_bytes(bytes).to_string())
        }
        ColumnType::Vector(dim) => {
            let Ok(d) = usize::try_from(*dim) else {
                return Value::Null;
            };
            // The schema dimension is configuration, but corrupt tuple bytes
            // must still bound the allocation used to materialize it.
            if d > raw.len() / 4 {
                return Value::Null;
            }
            let Some(floats_capacity) = checked_decode_capacity(
                d,
                size_of::<Value>(),
                raw.len(),
                4,
                raw.len() / 4,
                usize::MAX,
            ) else {
                return Value::Null;
            };
            let mut floats = Vec::with_capacity(floats_capacity);
            for chunk in raw.as_chunks::<4>().0.iter().take(d) {
                let bytes = [chunk[0], chunk[1], chunk[2], chunk[3]];
                floats.push(Value::Float(f32::from_le_bytes(bytes) as f64));
            }
            Value::Array(floats)
        }
        _ => Value::Null,
    }
}

/// Decode a variable-length raw byte slice into a value. A geometry cell
/// that is not EWKB is [`StrictError::CorruptCell`].
pub(super) fn decode_variable_value(
    col_type: &ColumnType,
    raw: &[u8],
) -> Result<Value, StrictError> {
    Ok(match col_type {
        ColumnType::String | ColumnType::SparseVector => {
            Value::String(std::str::from_utf8(raw).unwrap_or_default().to_string())
        }
        ColumnType::Bytes => Value::Bytes(raw.to_vec()),
        // EWKB: SRID, Z and M. A stored SRID is checked for syntax only.
        ColumnType::Geometry => {
            nodedb_spatial::value_of_ewkb(raw).map_err(|e| StrictError::CorruptCell {
                column_type: *col_type,
                detail: e.to_string(),
            })?
        }
        ColumnType::Json => match nodedb_types::value_from_msgpack(raw) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(len = raw.len(), error = %error, "corrupted JSON msgpack in tuple");
                Value::Null
            }
        },
        ColumnType::Array => decode_typed_msgpack(raw, |value| matches!(value, Value::Array(_))),
        ColumnType::Set => decode_typed_msgpack(raw, |value| matches!(value, Value::Set(_))),
        ColumnType::Regex => decode_typed_msgpack(raw, |value| matches!(value, Value::Regex(_))),
        ColumnType::Range => {
            decode_typed_msgpack(raw, |value| matches!(value, Value::Range { .. }))
        }
        ColumnType::Record => {
            decode_typed_msgpack(raw, |value| matches!(value, Value::Record { .. }))
        }
        _ => Value::Null,
    })
}

/// Decode MessagePack only when it carries the exact Value variant required
/// by the typed column. Corrupt or mismatched payloads never leak as another
/// dynamic value kind.
fn decode_typed_msgpack(raw: &[u8], accepts: impl FnOnce(&Value) -> bool) -> Value {
    match zerompk::from_msgpack::<Value>(raw) {
        Ok(value) if accepts(&value) => value,
        Ok(_) | Err(_) => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_cell_decodes_srid_z_and_m() {
        use nodedb_types::geometry::{Geometry, Ordinates, Srid, SridGeometry};
        let g = SridGeometry::with_ordinates(
            Srid::new(32648).unwrap(),
            Geometry::point(134_445.5, 349_624.7),
            Ordinates::new(Some(vec![12.0]), Some(vec![3.0])),
        )
        .unwrap();
        let raw = nodedb_spatial::geometry_to_ewkb(&g).unwrap();
        assert_eq!(
            decode_variable_value(&ColumnType::Geometry, &raw),
            Ok(Value::Geometry(g))
        );
        assert!(matches!(
            decode_variable_value(&ColumnType::Geometry, b"POINT(1 2)"),
            Err(StrictError::CorruptCell { .. })
        ));
    }

    /// One value per column type, at the extremes that grow most.
    fn growing_cells() -> Vec<(ColumnType, Value)> {
        use nodedb_types::geometry::{Geometry, Srid, SridGeometry};
        let geometry = |geometry| Value::Geometry(SridGeometry::new(Srid::WGS84, geometry));
        vec![
            (ColumnType::Int64, Value::Integer(i64::MIN)),
            (ColumnType::Float64, Value::Float(f64::MIN_POSITIVE)),
            (ColumnType::Bool, Value::Bool(true)),
            (
                ColumnType::Timestamp,
                Value::NaiveDateTime(NdbDateTime::from_micros(i64::MIN)),
            ),
            (
                ColumnType::Timestamptz,
                Value::DateTime(NdbDateTime::from_micros(0)),
            ),
            (
                ColumnType::Decimal(None),
                Value::Decimal(rust_decimal::Decimal::new(-1, 28)),
            ),
            (
                ColumnType::Decimal(None),
                Value::Decimal(rust_decimal::Decimal::MAX),
            ),
            (
                ColumnType::Uuid,
                Value::Uuid("01234567-89ab-cdef-0123-456789abcdef".into()),
            ),
            (
                ColumnType::Ulid,
                Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            ),
            (
                ColumnType::Duration,
                Value::Duration(nodedb_types::NdbDuration::from_micros(i64::MIN + 1)),
            ),
            (ColumnType::Vector(1), Value::Array(vec![Value::Float(1.0)])),
            (
                ColumnType::Vector(64),
                Value::Array(vec![Value::Float(0.5); 64]),
            ),
            (ColumnType::String, Value::String(String::new())),
            (ColumnType::String, Value::String("s".repeat(70_000))),
            (ColumnType::SparseVector, Value::String("{1: 0.5}".into())),
            (ColumnType::Bytes, Value::Bytes(Vec::new())),
            (ColumnType::Bytes, Value::Bytes(vec![7; 300])),
            (
                ColumnType::Json,
                Value::Array(vec![Value::Float(0.25); 100]),
            ),
            (
                ColumnType::Array,
                Value::Array(vec![Value::DateTime(NdbDateTime::from_micros(0)); 50]),
            ),
            (ColumnType::Set, Value::Set(vec![Value::Integer(1); 20])),
            (ColumnType::Regex, Value::Regex("a+".into())),
            (
                ColumnType::Range,
                Value::Range {
                    start: Some(Box::new(Value::Integer(1))),
                    end: None,
                    inclusive: false,
                },
            ),
            (
                ColumnType::Record,
                Value::Record {
                    table: "t".into(),
                    id: "i".into(),
                },
            ),
            (
                ColumnType::Geometry,
                geometry(Geometry::point(-180.0, 90.0)),
            ),
            (
                ColumnType::Geometry,
                geometry(Geometry::GeometryCollection {
                    geometries: Vec::new(),
                }),
            ),
        ]
    }

    /// A strict cell renders within `G·stored + C`, where `stored` is the
    /// column's fixed width or variable bytes. A null renders within `C`.
    #[test]
    fn every_strict_cell_renders_within_the_growth_bound() {
        use nodedb_types::columnar::{ColumnDef, StrictSchema};
        use nodedb_types::{native_cell_bound, value_to_msgpack};

        use crate::{TupleDecoder, TupleEncoder};

        for (column_type, value) in growing_cells() {
            let schema = StrictSchema::new(vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::nullable("cell", column_type),
            ])
            .expect("schema");
            let encoder = TupleEncoder::new(&schema);
            let tuple = encoder.encode(&[Value::Integer(1), value]).expect("encode");
            let null = encoder
                .encode(&[Value::Integer(1), Value::Null])
                .expect("encode null");
            let stored = (tuple.len() - null.len()) + column_type.fixed_size().unwrap_or(0);
            let decoder = TupleDecoder::new(&schema);
            for (stored, tuple) in [(stored, &tuple), (0, &null)] {
                let cell = decoder.extract_value(tuple, 1).expect("decode");
                let native = value_to_msgpack(&cell).expect("render").len();
                assert!(
                    native <= native_cell_bound(stored),
                    "{column_type:?}: {stored} stored bytes render {native}"
                );
            }
        }
    }

    #[test]
    fn vector_dimension_larger_than_raw_bytes_returns_null_without_reserving() {
        assert!(matches!(
            decode_fixed_value(&ColumnType::Vector(u32::MAX), &[]),
            Value::Null
        ));
    }
}
