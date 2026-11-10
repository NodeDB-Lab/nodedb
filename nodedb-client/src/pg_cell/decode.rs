// SPDX-License-Identifier: Apache-2.0

//! Pick the decoder for one result cell from its PG column type.

use nodedb_types::Value;
use nodedb_types::error::NodeDbResult;
use tokio_postgres::types::{Kind, Type};

use super::error::{DecodeReason, cell_error};
use super::{array, binary, text};

/// Decode one result cell of column `column` with PG type `ty`. `raw` is
/// `None` for SQL NULL.
///
/// `tokio_postgres` requests the binary result format for every column. The
/// server sends `bool`, `int2`, `int4`, `int8`, `float4`, `float8`,
/// `timestamp` and `timestamptz` in binary. It sends every other type as its
/// PostgreSQL text form. A cell that does not decode is an error naming the
/// column, the PG type and the cell text, never a NULL.
pub(crate) fn decode_cell(column: &str, ty: &Type, raw: Option<&[u8]>) -> NodeDbResult<Value> {
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    decode_bytes(ty, raw).map_err(|reason| cell_error(column, ty, raw, &reason))
}

/// Decode `text`, the PostgreSQL text form of a scalar of PG type `ty`.
///
/// For a value whose type the client knows from elsewhere than a binary
/// result cell, such as a declared column read back as JSON text. Text that
/// does not decode is an error naming the column, the PG type and the text.
pub(crate) fn decode_text_cell(column: &str, ty: &Type, text: &str) -> NodeDbResult<Value> {
    text::scalar(ty, text).map_err(|reason| cell_error(column, ty, text.as_bytes(), &reason))
}

fn decode_bytes(ty: &Type, raw: &[u8]) -> Result<Value, DecodeReason> {
    match *ty {
        Type::BOOL => return binary::boolean(raw),
        Type::INT2 => return binary::int2(raw),
        Type::INT4 => return binary::int4(raw),
        Type::INT8 => return binary::int8(raw),
        Type::FLOAT4 => return binary::float4(raw),
        Type::FLOAT8 => return binary::float8(raw),
        Type::TIMESTAMP => return binary::timestamp(raw),
        Type::TIMESTAMPTZ => return binary::timestamptz(raw),
        _ => {}
    }
    let cell_text = std::str::from_utf8(raw).map_err(|_| DecodeReason::NotUtf8)?;
    match ty.kind() {
        Kind::Array(element) => array::array(cell_text, element),
        _ => text::scalar(ty, cell_text),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::NdbDateTime;
    use rust_decimal::Decimal;

    use super::*;

    fn decoded(ty: &Type, raw: &[u8]) -> Value {
        decode_cell("c", ty, Some(raw)).unwrap_or_else(|e| panic!("{ty}: {e}"))
    }

    fn error_message(ty: &Type, raw: &[u8]) -> String {
        match decode_cell("price", ty, Some(raw)) {
            Ok(v) => panic!("{ty} must refuse {raw:?}, got {v:?}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn null_is_null_for_every_type() {
        for ty in [Type::INT8, Type::NUMERIC, Type::BYTEA, Type::FLOAT4_ARRAY] {
            assert_eq!(decode_cell("c", &ty, None).ok(), Some(Value::Null));
        }
    }

    #[test]
    fn binary_scalars_decode_from_binary() {
        assert_eq!(decoded(&Type::BOOL, &[1]), Value::Bool(true));
        assert_eq!(decoded(&Type::INT2, &5i16.to_be_bytes()), Value::Integer(5));
        assert_eq!(decoded(&Type::INT4, &5i32.to_be_bytes()), Value::Integer(5));
        assert_eq!(decoded(&Type::INT8, &5i64.to_be_bytes()), Value::Integer(5));
        assert_eq!(
            decoded(&Type::FLOAT4, &0.5f32.to_be_bytes()),
            Value::Float(0.5)
        );
        assert!(
            matches!(decoded(&Type::FLOAT8, &f64::NAN.to_be_bytes()), Value::Float(f) if f.is_nan())
        );
        assert_eq!(
            decoded(&Type::TIMESTAMPTZ, &0i64.to_be_bytes()),
            Value::DateTime(NdbDateTime::from_micros(946_684_800_000_000))
        );
        assert_eq!(
            decoded(&Type::TIMESTAMP, &0i64.to_be_bytes()),
            Value::NaiveDateTime(NdbDateTime::from_micros(946_684_800_000_000))
        );
    }

    #[test]
    fn other_types_decode_from_text() {
        assert_eq!(decoded(&Type::TEXT, b"abc"), Value::String("abc".into()));
        assert_eq!(decoded(&Type::VARCHAR, b"abc"), Value::String("abc".into()));
        assert_eq!(
            decoded(&Type::NUMERIC, b"1.25"),
            Value::Decimal(Decimal::new(125, 2))
        );
        assert_eq!(decoded(&Type::BYTEA, b"\\x0102"), Value::Bytes(vec![1, 2]));
        assert_eq!(
            decoded(&Type::UUID, b"550e8400-e29b-41d4-a716-446655440000"),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(
            decoded(&Type::JSONB, b"[true]"),
            Value::Array(vec![Value::Bool(true)])
        );
        assert_eq!(
            decoded(&Type::FLOAT8_ARRAY, b"{1,-Infinity}"),
            Value::Array(vec![Value::Float(1.0), Value::Float(f64::NEG_INFINITY)])
        );
    }

    #[test]
    fn errors_name_column_type_and_text() {
        let message = error_message(&Type::NUMERIC, b"12,5");
        assert!(message.contains("\"price\""), "{message}");
        assert!(message.contains("numeric"), "{message}");
        assert!(message.contains("\"12,5\""), "{message}");

        let message = error_message(&Type::INT8, b"42");
        assert!(message.contains("int8"), "{message}");
        assert!(message.contains("needs 8 bytes, found 2"), "{message}");

        let message = error_message(&Type::BYTEA, b"AQI");
        assert!(message.contains("bytea"), "{message}");
        assert!(message.contains("\"AQI\""), "{message}");

        let message = error_message(&Type::TEXT, &[0xff, 0xfe]);
        assert!(message.contains("\\xfffe"), "{message}");
        assert!(message.contains("not UTF-8"), "{message}");

        let message = error_message(&Type::FLOAT4_ARRAY, b"[0.5,1]");
        assert!(message.contains("_float4"), "{message}");
        assert!(message.contains("array literal syntax"), "{message}");

        let message = error_message(&Type::FLOAT8_ARRAY, b"{1,x}");
        assert!(message.contains("array element 1"), "{message}");

        let message = error_message(&Type::POINT, b"(1,2)");
        assert!(message.contains("no decoder"), "{message}");
    }
}
