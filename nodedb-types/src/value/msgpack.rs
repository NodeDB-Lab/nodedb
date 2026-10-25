// SPDX-License-Identifier: Apache-2.0

//! Hand-rolled zerompk implementations for `Value`.
//!
//! Cannot use derive because `rust_decimal::Decimal` is an external type.
//! Format: [variant_tag: u8, ...payload fields] as a msgpack array.

use std::collections::HashMap;
use std::sync::Arc;

use super::core::Value;
use crate::array_cell::ArrayCell;
use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::tagged::msgpack::{SRID_GEOMETRY_FIELDS, read_fields, write_fields};

impl zerompk::ToMessagePack for Value {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            Value::Null => {
                writer.write_array_len(1)?;
                writer.write_u8(0)
            }
            Value::Bool(b) => {
                writer.write_array_len(2)?;
                writer.write_u8(1)?;
                writer.write_boolean(*b)
            }
            Value::Integer(i) => {
                writer.write_array_len(2)?;
                writer.write_u8(2)?;
                writer.write_i64(*i)
            }
            Value::Float(f) => {
                writer.write_array_len(2)?;
                writer.write_u8(3)?;
                writer.write_f64(*f)
            }
            Value::String(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(4)?;
                writer.write_string(s)
            }
            Value::Bytes(b) => {
                writer.write_array_len(2)?;
                writer.write_u8(5)?;
                writer.write_binary(b)
            }
            Value::Array(arr) => {
                writer.write_array_len(2)?;
                writer.write_u8(6)?;
                arr.write(writer)
            }
            Value::Object(map) => {
                writer.write_array_len(2)?;
                writer.write_u8(7)?;
                map.write(writer)
            }
            Value::Uuid(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(8)?;
                writer.write_string(s)
            }
            Value::Ulid(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(9)?;
                writer.write_string(s)
            }
            Value::DateTime(dt) => {
                writer.write_array_len(2)?;
                writer.write_u8(10)?;
                dt.write(writer)
            }
            Value::Duration(d) => {
                writer.write_array_len(2)?;
                writer.write_u8(11)?;
                d.write(writer)
            }
            Value::Decimal(d) => {
                writer.write_array_len(2)?;
                writer.write_u8(12)?;
                writer.write_binary(&d.serialize())
            }
            // `[13, srid_code_u32, geometry, z | nil, m | nil]`.
            Value::Geometry(g) => {
                writer.write_array_len(1 + SRID_GEOMETRY_FIELDS)?;
                writer.write_u8(13)?;
                write_fields(g, writer)
            }
            Value::Set(s) => {
                writer.write_array_len(2)?;
                writer.write_u8(14)?;
                s.write(writer)
            }
            Value::Regex(r) => {
                writer.write_array_len(2)?;
                writer.write_u8(15)?;
                writer.write_string(r)
            }
            Value::Range {
                start,
                end,
                inclusive,
            } => {
                writer.write_array_len(4)?;
                writer.write_u8(16)?;
                start.write(writer)?;
                end.write(writer)?;
                writer.write_boolean(*inclusive)
            }
            Value::Record { table, id } => {
                writer.write_array_len(3)?;
                writer.write_u8(17)?;
                writer.write_string(table)?;
                writer.write_string(id)
            }
            Value::ArrayCell(cell) => {
                writer.write_array_len(2)?;
                writer.write_u8(18)?;
                cell.write(writer)
            }
            Value::NaiveDateTime(dt) => {
                writer.write_array_len(2)?;
                writer.write_u8(19)?;
                dt.write(writer)
            }
            Value::Vector(v) => {
                writer.write_array_len(2)?;
                writer.write_u8(20)?;
                writer.write_binary(bytemuck::cast_slice(v.as_ref()))
            }
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for Value {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len == 0 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 1,
                actual: 0,
            });
        }
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(Value::Null),
            1 => Ok(Value::Bool(reader.read_boolean()?)),
            2 => Ok(Value::Integer(reader.read_i64()?)),
            3 => Ok(Value::Float(reader.read_f64()?)),
            4 => Ok(Value::String(reader.read_string()?.into_owned())),
            5 => Ok(Value::Bytes(reader.read_binary()?.into_owned())),
            6 => Ok(Value::Array(Vec::<Value>::read(reader)?)),
            7 => Ok(Value::Object(HashMap::<String, Value>::read(reader)?)),
            8 => Ok(Value::Uuid(reader.read_string()?.into_owned())),
            9 => Ok(Value::Ulid(reader.read_string()?.into_owned())),
            10 => Ok(Value::DateTime(NdbDateTime::read(reader)?)),
            11 => Ok(Value::Duration(NdbDuration::read(reader)?)),
            12 => {
                let cow = reader.read_binary()?;
                if cow.len() != 16 {
                    return Err(zerompk::Error::BufferTooSmall);
                }
                let mut buf = [0u8; 16];
                buf.copy_from_slice(&cow);
                Ok(Value::Decimal(rust_decimal::Decimal::deserialize(buf)))
            }
            13 => {
                if len != 1 + SRID_GEOMETRY_FIELDS {
                    return Err(zerompk::Error::ArrayLengthMismatch {
                        expected: 1 + SRID_GEOMETRY_FIELDS,
                        actual: len,
                    });
                }
                // A stored SRID is checked for syntax only.
                Ok(Value::Geometry(read_fields(reader)?))
            }
            14 => Ok(Value::Set(Vec::<Value>::read(reader)?)),
            15 => Ok(Value::Regex(reader.read_string()?.into_owned())),
            16 => {
                let start = Option::<Box<Value>>::read(reader)?;
                let end = Option::<Box<Value>>::read(reader)?;
                let inclusive = reader.read_boolean()?;
                Ok(Value::Range {
                    start,
                    end,
                    inclusive,
                })
            }
            17 => {
                let table = reader.read_string()?.into_owned();
                let id = reader.read_string()?.into_owned();
                Ok(Value::Record { table, id })
            }
            18 => Ok(Value::ArrayCell(ArrayCell::read(reader)?)),
            19 => Ok(Value::NaiveDateTime(NdbDateTime::read(reader)?)),
            20 => {
                let cow = reader.read_binary()?;
                if cow.len() % 4 != 0 {
                    return Err(zerompk::Error::BufferTooSmall);
                }
                // Deserialize each f32 from its 4-byte little-endian
                // representation. This is safe for any alignment and matches
                // the byte order produced by bytemuck::cast_slice on the
                // encode side (native endian = little-endian on x86/ARM).
                let floats: Arc<[f32]> = cow
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|chunk| {
                        let arr = [chunk[0], chunk[1], chunk[2], chunk[3]];
                        f32::from_ne_bytes(arr)
                    })
                    .collect();
                Ok(Value::Vector(floats))
            }
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{Geometry, Ordinates, Srid, SridGeometry};

    #[test]
    fn geometry_round_trip_keeps_z_and_m() {
        let g = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
        )
        .unwrap();
        let value = Value::Geometry(g);
        let bytes = zerompk::to_msgpack_vec(&value).unwrap();
        let back: Value = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn geometry_round_trip_keeps_srid() {
        for code in [4326, 32650, 4742, 990_001, 999] {
            let srid = Srid::new(code).unwrap();
            let value = Value::Geometry(SridGeometry::new(srid, Geometry::point(1.0, 2.0)));
            let bytes = zerompk::to_msgpack_vec(&value).unwrap();
            let back: Value = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(back, value);
        }
    }
}
