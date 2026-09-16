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
}
