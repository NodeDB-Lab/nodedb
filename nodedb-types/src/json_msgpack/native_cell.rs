// SPDX-License-Identifier: Apache-2.0

//! `NativeCell`: a `Value` carried in plain msgpack inside a zerompk struct.
//!
//! `Value`'s own zerompk impl writes the tagged `[tag, payload]` form. A
//! struct that derives `ToMessagePack` with a `Value` field therefore ships
//! `"alice"` as `[4, "alice"]`, which the JSON transcoder renders verbatim.
//! Wrapping the field in `NativeCell` writes the same bytes
//! `value_to_msgpack` produces, so the transcoder renders a plain cell and
//! `value_from_msgpack` reads it back typed.

use zerompk::{FromMessagePack, Read, ToMessagePack, Write};

use super::reader::native::read_native_value;
use super::writer::write_native_value;
use crate::Value;

/// A `Value` written and read as plain msgpack.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NativeCell(pub Value);

impl From<Value> for NativeCell {
    #[inline]
    fn from(v: Value) -> Self {
        Self(v)
    }
}

impl From<NativeCell> for Value {
    #[inline]
    fn from(c: NativeCell) -> Self {
        c.0
    }
}

impl ToMessagePack for NativeCell {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        write_native_value(writer, &self.0)
    }
}

impl<'a> FromMessagePack<'a> for NativeCell {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        read_native_value(reader).map(NativeCell)
    }
}

/// Numerator of G, the most times its stored bytes a cell renders to.
///
/// A cell is a field value decoded from its stored row and written as a
/// [`NativeCell`]. G is `14/3`, set by the largest growth per stored byte:
///
/// - A tagged `ArrayCell` with a system time, `[18, [[], [], 0]]`, is 6
///   bytes. Its cell map with the `coords`, `attrs` and `_ts_system` keys is
///   28. Nested in an array, each repeats at `28/6`.
/// - A text cell under a `FLOAT8[]` column renders `[1,1,...]` as floats:
///   9 bytes per 2 text bytes, `9/2`.
/// - Every other kind grows less: a strict vector element 4 to 9 bytes, a
///   tagged instant 4 to 10, a strict UUID 16 to 38, a plain UUID ext 18 to
///   38, a strict decimal 16 to 32, a plain `float 32` 5 to 9, a strict
///   duration 8 to 18. Tagged headers, ranges and records shrink.
///
/// The stored bytes of a cell are its payload alone: a strict column's
/// fixed width or variable bytes, a plain or tagged field's encoded value,
/// a KV key's bytes.
///
/// Every map key is a string: a write path refuses a plain body with any
/// other key ([`super::find_body_non_string_key`]).
pub const NATIVE_CELL_GROWTH_NUM: usize = 14;

/// Denominator of G. See [`NATIVE_CELL_GROWTH_NUM`].
pub const NATIVE_CELL_GROWTH_DEN: usize = 3;

/// C, the bytes a cell renders to beyond `G` times its stored bytes.
///
/// C covers a near-empty stored form: a float from one text digit renders
/// 9 bytes, an empty strict `BYTES` 2, a null strict column 1. C rounds the
/// largest of these up to 16.
pub const NATIVE_CELL_CONSTANT: usize = 16;

/// The most bytes a cell renders to whose value occupies `stored` bytes of
/// its stored row: `ceil(G·stored) + C`. Saturates at `usize::MAX`.
pub fn native_cell_bound(stored: usize) -> usize {
    stored
        .checked_mul(NATIVE_CELL_GROWTH_NUM)
        .map_or(usize::MAX, |scaled| {
            scaled
                .div_ceil(NATIVE_CELL_GROWTH_DEN)
                .saturating_add(NATIVE_CELL_CONSTANT)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NdbDateTime;
    use crate::json_msgpack::{value_from_msgpack, value_to_msgpack};

    #[derive(ToMessagePack, FromMessagePack, PartialEq, Debug)]
    #[msgpack(map)]
    struct Row {
        cells: Vec<NativeCell>,
    }

    fn sample() -> Vec<Value> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("n".to_string(), Value::Integer(-200));
        obj.insert(
            "arr".to_string(),
            Value::Array(vec![Value::Float(1.5), Value::Null]),
        );
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Integer(300),
            Value::Integer(-5),
            Value::Float(2.25),
            Value::String("alice".into()),
            Value::Bytes(vec![1, 2, 3]),
            Value::DateTime(NdbDateTime::from_micros(1_583_402_400_000_000)),
            Value::NaiveDateTime(NdbDateTime::from_micros(-86_400_000_000)),
            Value::Object(obj),
        ]
    }

    #[test]
    fn cells_round_trip_through_a_derived_struct() {
        let row = Row {
            cells: sample().into_iter().map(NativeCell).collect(),
        };
        let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
        let back: Row = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, row);
    }

    #[test]
    fn cell_bytes_match_value_to_msgpack() {
        for v in sample() {
            let plain = value_to_msgpack(&v).expect("plain");
            let cell = zerompk::to_msgpack_vec(&NativeCell(v.clone())).expect("cell");
            assert_eq!(cell, plain, "{v:?}");
            assert_eq!(value_from_msgpack(&cell).expect("read"), v);
        }
    }

    /// One value of every kind, at the extremes that grow most.
    fn every_kind() -> Vec<Value> {
        use crate::geometry::{Geometry, Srid, SridGeometry};
        use crate::{ArrayCell, NdbDuration};
        let geometry = |geometry| Value::Geometry(SridGeometry::new(Srid::WGS84, geometry));
        let cell = |system_time| {
            Value::ArrayCell(ArrayCell {
                coords: Vec::new(),
                attrs: Vec::new(),
                system_time,
            })
        };
        vec![
            Value::Null,
            Value::Bool(false),
            Value::Integer(0),
            Value::Integer(200),
            Value::Integer(i64::MIN),
            Value::Float(f64::MIN_POSITIVE),
            Value::String(String::new()),
            Value::String("s".repeat(70_000)),
            Value::Bytes(Vec::new()),
            Value::Bytes(vec![7; 300]),
            Value::Uuid("01234567-89ab-cdef-0123-456789abcdef".into()),
            Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            Value::Regex("a+".into()),
            Value::DateTime(NdbDateTime::from_micros(0)),
            Value::NaiveDateTime(NdbDateTime::from_micros(i64::MIN)),
            Value::Duration(NdbDuration::from_micros(0)),
            Value::Duration(NdbDuration::from_micros(999)),
            Value::Duration(NdbDuration::from_micros(i64::MIN + 1)),
            Value::Decimal(rust_decimal::Decimal::MAX),
            Value::Decimal(rust_decimal::Decimal::new(-1, 28)),
            Value::from_u64(u64::MAX),
            geometry(Geometry::point(-180.0, 90.0)),
            geometry(Geometry::GeometryCollection {
                geometries: Vec::new(),
            }),
            Value::Vector(vec![1.5f32; 9].into()),
            Value::Range {
                start: Some(Box::new(Value::Integer(1))),
                end: None,
                inclusive: true,
            },
            Value::Record {
                table: "t".into(),
                id: "i".into(),
            },
            cell(None),
            cell(Some(0)),
            cell(Some(i64::MIN)),
        ]
    }

    /// Every kind, alone, repeated in an array, a set and an object, and
    /// nested two deep.
    fn corpus() -> Vec<Value> {
        let mut values = Vec::new();
        for value in every_kind() {
            values.push(Value::Array(vec![value.clone(); 50]));
            values.push(Value::Set(vec![value.clone(); 3]));
            values.push(Value::Object(
                (0..20)
                    .map(|index| (format!("f{index}"), value.clone()))
                    .collect(),
            ));
            values.push(Value::Array(vec![Value::Array(vec![value.clone(); 3]); 3]));
            values.push(value);
        }
        values.push(Value::Array(every_kind()));
        values
    }

    /// Plain MessagePack forms no `Value` writes: narrow numbers, an
    /// unknown ext, and arrays of them.
    fn raw_plain() -> Vec<Vec<u8>> {
        let float32 = [0xCA, 0x3F, 0x80, 0x00, 0x00];
        let uint8 = [0xCC, 200];
        let array_of = |item: &[u8]| {
            let mut bytes = vec![0xDC, 0x03, 0xE8];
            (0..1000).for_each(|_| bytes.extend_from_slice(item));
            bytes
        };
        vec![
            float32.to_vec(),
            uint8.to_vec(),
            vec![0xCD, 0x9C, 0x40],
            vec![0xCE, 0xB2, 0xD0, 0x5E, 0x00],
            vec![0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            vec![0xD4, 0x07, 0x00],
            array_of(&float32),
            array_of(&uint8),
            array_of(&[0xCE, 0xB2, 0xD0, 0x5E, 0x00]),
        ]
    }

    /// Check that `stored`, read by `read`, renders within its bound.
    fn assert_within(format: &str, stored: &[u8], read: impl Fn(&[u8]) -> Value) {
        let value = read(stored);
        let native = value_to_msgpack(&value).expect("render").len();
        assert!(
            native <= native_cell_bound(stored.len()),
            "{format}: {} stored bytes render {native}, over {}",
            stored.len(),
            native_cell_bound(stored.len())
        );
    }

    fn read_plain(bytes: &[u8]) -> Value {
        value_from_msgpack(bytes).expect("plain")
    }

    fn read_tagged(bytes: &[u8]) -> Value {
        zerompk::from_msgpack::<Value>(bytes).expect("tagged")
    }

    /// `native ≤ G·stored + C` for every kind in the plain map, typed plain,
    /// native tagged and KV value forms.
    #[test]
    fn every_stored_form_renders_within_the_growth_bound() {
        for value in corpus() {
            let tagged = zerompk::to_msgpack_vec(&value).expect("tagged");
            assert_within("tagged", &tagged, read_tagged);
            let plain = value_to_msgpack(&value).expect("plain");
            assert_within("plain", &plain, read_plain);
            let typed = crate::json_msgpack::value_to_typed_msgpack(&value).expect("typed");
            assert_within("typed plain", &typed, read_plain);
        }
        for raw in raw_plain() {
            assert_within("raw plain", &raw, read_plain);
        }
        // A KV key and a KV value that is not a map are bytes or text.
        for len in [0usize, 1, 31, 32, 255, 256, 65_535, 65_536] {
            let payload = vec![b'k'; len];
            assert_within("kv bytes", &payload, |bytes| Value::Bytes(bytes.to_vec()));
            assert_within("kv text", &payload, |bytes| {
                Value::String(String::from_utf8_lossy(bytes).into_owned())
            });
        }
    }

    /// The tagged array cell sets G: its growth is `28/6`, within `14/3`.
    #[test]
    fn a_tagged_array_cell_grows_at_g() {
        let cell = Value::ArrayCell(crate::ArrayCell {
            coords: Vec::new(),
            attrs: Vec::new(),
            system_time: Some(0),
        });
        let stored = zerompk::to_msgpack_vec(&cell).expect("tagged");
        let native = value_to_msgpack(&read_tagged(&stored)).expect("render");
        assert_eq!((stored.len(), native.len()), (6, 28));
        assert_eq!(
            native.len() * NATIVE_CELL_GROWTH_DEN,
            stored.len() * NATIVE_CELL_GROWTH_NUM
        );
    }

    #[test]
    fn the_bound_rounds_up_and_saturates() {
        assert_eq!(native_cell_bound(0), NATIVE_CELL_CONSTANT);
        assert_eq!(native_cell_bound(1), 5 + NATIVE_CELL_CONSTANT);
        assert_eq!(native_cell_bound(3), 14 + NATIVE_CELL_CONSTANT);
        assert_eq!(native_cell_bound(usize::MAX), usize::MAX);
    }
}
