//! Hand-rolled zerompk implementations for `Value`.
//!
//! Cannot use derive because `rust_decimal::Decimal` is an external type.
//! Format: [variant_tag: u8, ...payload fields] as a msgpack array.

use std::collections::HashMap;

use super::core::Value;
use crate::array_cell::ArrayCell;
use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::Geometry;

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
            Value::Geometry(g) => {
                writer.write_array_len(2)?;
                writer.write_u8(13)?;
                g.write(writer)
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
            Value::NdArrayCell(cell) => {
                writer.write_array_len(2)?;
                writer.write_u8(18)?;
                cell.write(writer)
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
            13 => Ok(Value::Geometry(Geometry::read(reader)?)),
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
            18 => Ok(Value::NdArrayCell(ArrayCell::read(reader)?)),
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}
