// SPDX-License-Identifier: BUSL-1.1

//! Encode one typed cell into a pgwire `DataRow` per its column type.
//!
//! Every pgwire row encoder renders through [`encode_cell`], so a column
//! type has one rendering. Each arm reads the typed
//! [`nodedb_types::Value`] directly: a timestamp column renders only from an
//! instant — a typed `DateTime`/`NaiveDateTime` or ISO-8601 text — and an
//! integer under it is an error, never a guessed epoch unit. A binary
//! numeric or bool column likewise takes only its own scalar shape, because
//! the RowDescription already told the client how many bytes to read.

use std::error::Error;

use bytes::{BufMut, BytesMut};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, accepts, to_sql_checked};

use nodedb_types::columnar::{FloatWidth, IntWidth};
use nodedb_types::error::NodeDbError;
use nodedb_types::value::non_finite_float_text;
use nodedb_types::{NdbDateTime, Value};

use crate::control::server::pgwire::numeric_narrow::{checked_narrow, checked_narrow_f32};
use crate::control::server::pgwire::types::error_map::shape_error_to_pg;
use crate::control::server::pgwire::types::wire_type::effective_format;
use crate::control::server::response_shape::cell::{
    bytea_hex, cell_text, instant_of, shape_mismatch, value_to_wire_json,
};
use crate::control::server::response_shape::types::DdlColType;

use super::array_text::{PgArrayLiteral, float_array_text};

/// Microseconds from the Unix epoch to the PostgreSQL epoch
/// (2000-01-01 00:00:00 UTC), which binary `timestamp`/`timestamptz` count
/// from.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Encode one cell of column `column` into `encoder` per its column type
/// `ct` and wire `format`.
///
/// `Value::Null` is SQL NULL for every type and format. Under the text
/// format, `Float8`/`Float4` numbers go through pgwire's native float
/// encoder (ryu + `extra_float_digits`) so their bytes match PostgreSQL. A
/// non-finite float renders `NaN`, `Infinity` or `-Infinity`, the
/// PostgreSQL text, where pgwire's encoder would emit `inf`. Binary floats
/// carry the IEEE bits, non-finite ones included.
/// `Timestamp`/`Timestamptz` cells render the instant [`instant_of`] reads
/// as ISO-8601. A `bytea` cell renders `\x` hex of its bytes, a `json` or
/// `jsonb` cell its JSON text, and a float array cell its `{...}` literal.
/// Every other type renders [`cell_text`]. Under the binary format each
/// scalar type takes only its own shape; a mismatch is an error, because
/// the client reads the advertised type's bytes and a text value under it
/// would be misread.
///
/// `format` is the requested format. A type with no binary form renders
/// text under either, as [`effective_format`] decides, and the encoder's
/// schema field carries the same format by [`result_field`].
///
/// [`result_field`]: crate::control::server::pgwire::types::wire_type::result_field
pub(in crate::control::server::pgwire) fn encode_cell(
    encoder: &mut DataRowEncoder,
    column: &str,
    ct: DdlColType,
    format: FieldFormat,
    v: &Value,
) -> PgWireResult<()> {
    if matches!(v, Value::Null) {
        return encoder.encode_field(&None::<&str>);
    }
    let format = effective_format(ct, format);

    // A timestamp column renders the one instant its cell denotes; the
    // encoder picks text (ISO-8601) or binary (PostgreSQL-epoch micros) from
    // the schema's format.
    if matches!(ct, DdlColType::Timestamp | DdlColType::Timestamptz) {
        let instant = instant_of(v, column).map_err(to_pg_error)?;
        return encoder.encode_field(&PgTimestamp(instant));
    }

    // Binary result format: the column's `FieldInfo` is Binary, so
    // `encode_field` emits the value's binary wire form. `effective_format`
    // leaves Binary only for the binary-capable types; the others take the
    // text arms below.
    if format == FieldFormat::Binary {
        match ct {
            DdlColType::Int8 => return encoder.encode_field(&integer_of(column, v)?),
            // Narrowing casts are fallible, so they are `try_from`, not `as`.
            // A stored value wider than the column's declared width cannot be
            // transmitted under a narrowed OID: the client reads exactly 2 or 4
            // bytes and would silently decode a wrapped number. Writes are
            // range-checked (`nodedb_sql::planner::dml`), so this is
            // unreachable for data written through SQL — but rows predating the
            // declared width, or arriving via a non-SQL ingest path, can still
            // be out of range, and those must surface as an error rather than
            // corrupt a value in flight.
            DdlColType::Int4 => {
                // `as i32` is lossless here: `checked_narrow` has already
                // proved the value is inside `IntWidth::I32`.
                let n = checked_narrow(integer_of(column, v)?, IntWidth::I32)?;
                return encoder.encode_field(&(n as i32));
            }
            DdlColType::Int2 => {
                let n = checked_narrow(integer_of(column, v)?, IntWidth::I16)?;
                return encoder.encode_field(&(n as i16));
            }
            DdlColType::Float8 => return encoder.encode_field(&float_of(column, v)?),
            // Unlike the integer arms above this is not a range *constraint*
            // check: narrowing an f64 rounds rather than wraps, so `1.1`
            // arriving as `1.10000002` is correct PostgreSQL `real` behaviour
            // and never an error. Only overflow-to-infinity is refused.
            DdlColType::Float4 => {
                let f = checked_narrow_f32(float_of(column, v)?)?;
                return encoder.encode_field(&f);
            }
            DdlColType::Bool => match v {
                Value::Bool(b) => return encoder.encode_field(b),
                other => return Err(to_pg_error(shape_mismatch(column, "a bool", other))),
            },
            // TEXT/VARCHAR binary wire bytes are identical to text bytes, so
            // the cell renders its text form and is emitted as binary.
            DdlColType::Text | DdlColType::Varchar => return encoder.encode_field(&cell_text(v)),
            DdlColType::Bytea
            | DdlColType::Json
            | DdlColType::Jsonb
            | DdlColType::Float4Array
            | DdlColType::Float8Array
            | DdlColType::Numeric
            | DdlColType::Uuid
            | DdlColType::Timestamp
            | DdlColType::Timestamptz => {}
        }
    }

    match ct {
        DdlColType::Float8 => match v {
            Value::Float(f) => match non_finite_float_text(*f) {
                Some(text) => encoder.encode_field(&text),
                None => encoder.encode_field(f),
            },
            Value::Integer(i) => encoder.encode_field(&(*i as f64)),
            other => encoder.encode_field(&cell_text(other)),
        },
        // Same overflow guard as the binary arm: the text rendering of a
        // `real` column must not silently read `Infinity` for a finite stored
        // value either.
        DdlColType::Float4 => match v {
            Value::Float(f) => {
                let narrowed = checked_narrow_f32(*f)?;
                match non_finite_float_text(f64::from(narrowed)) {
                    Some(text) => encoder.encode_field(&text),
                    None => encoder.encode_field(&narrowed),
                }
            }
            Value::Integer(i) => encoder.encode_field(&checked_narrow_f32(*i as f64)?),
            other => encoder.encode_field(&cell_text(other)),
        },
        DdlColType::Bytea => encoder.encode_field(&bytea_text(v)),
        DdlColType::Json | DdlColType::Jsonb => encoder.encode_field(&json_text(v)),
        DdlColType::Float4Array => encoder.encode_field(&PgArrayLiteral(float_array_text(
            column,
            v,
            FloatWidth::F32,
        )?)),
        DdlColType::Float8Array => encoder.encode_field(&PgArrayLiteral(float_array_text(
            column,
            v,
            FloatWidth::F64,
        )?)),
        // `numeric` text is the decimal digits and `uuid` text is the
        // hyphenated hex, which is the cell's own text.
        DdlColType::Text
        | DdlColType::Varchar
        | DdlColType::Int8
        | DdlColType::Int4
        | DdlColType::Int2
        | DdlColType::Bool
        | DdlColType::Numeric
        | DdlColType::Uuid
        | DdlColType::Timestamp
        | DdlColType::Timestamptz => encoder.encode_field(&cell_text(v)),
    }
}

/// The text of a `bytea` cell: `\x` hex of its bytes. A cell that is not
/// bytes stands for the UTF-8 bytes of its text form.
fn bytea_text(v: &Value) -> Option<String> {
    match v {
        Value::Bytes(bytes) => Some(bytea_hex(bytes)),
        other => cell_text(other).map(|text| bytea_hex(text.as_bytes())),
    }
}

/// The text of a `json` or `jsonb` cell: the cell's wire JSON. A string
/// cell is a JSON string, quoted.
fn json_text(v: &Value) -> Option<String> {
    match value_to_wire_json(v) {
        serde_json::Value::Null => None,
        json => Some(json.to_string()),
    }
}

/// The integer a binary integer column transmits, or the shape error.
fn integer_of(column: &str, v: &Value) -> PgWireResult<i64> {
    match v {
        Value::Integer(i) => Ok(*i),
        other => Err(to_pg_error(shape_mismatch(column, "an integer", other))),
    }
}

/// The float a binary float column transmits, or the shape error. An
/// integer cell widens losslessly up to 2^53, as it does under the text
/// format.
fn float_of(column: &str, v: &Value) -> PgWireResult<f64> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Integer(i) => Ok(*i as f64),
        other => Err(to_pg_error(shape_mismatch(column, "a float", other))),
    }
}

/// Map a cell error to the pgwire error the client reads, with the SQLSTATE
/// its numeric code maps to.
fn to_pg_error(e: NodeDbError) -> PgWireError {
    shape_error_to_pg(&e)
}

/// An instant as pgwire encodes it under a `timestamp`/`timestamptz`
/// column: ISO-8601 text under the text format, an `i64` of microseconds
/// since the PostgreSQL epoch under the binary format.
#[derive(Debug)]
struct PgTimestamp(NdbDateTime);

impl ToSql for PgTimestamp {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let micros = self
            .0
            .micros
            .checked_sub(PG_EPOCH_OFFSET_MICROS)
            .ok_or_else(|| {
                Box::<dyn Error + Sync + Send>::from(format!(
                    "timestamp {} is out of range for the PostgreSQL binary encoding",
                    self.0.to_iso8601()
                ))
            })?;
        out.put_i64(micros);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP, TIMESTAMPTZ);

    to_sql_checked!();
}

impl ToSqlText for PgTimestamp {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.to_iso8601().as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pgwire::api::results::FieldInfo;

    use super::*;
    use crate::control::server::pgwire::types::wire_type::result_field;

    /// The one instant these tests use: 2020-03-05T10:00:00Z.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    /// Encode one cell under a one-column schema and return its raw field
    /// bytes, or `None` for SQL NULL.
    fn encode_one(ct: DdlColType, format: FieldFormat, v: &Value) -> PgWireResult<Option<Vec<u8>>> {
        let schema: Arc<Vec<FieldInfo>> = Arc::new(vec![result_field("c", ct, format)]);
        let mut encoder = DataRowEncoder::new(schema);
        encode_cell(&mut encoder, "c", ct, format, v)?;
        let row = encoder.take_row();
        let data = &row.data;
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(data[4..4 + len as usize].to_vec()))
    }

    fn encode_text(ct: DdlColType, v: &Value) -> PgWireResult<Option<String>> {
        Ok(encode_one(ct, FieldFormat::Text, v)?
            .map(|b| String::from_utf8(b).expect("text cell is UTF-8")))
    }

    /// The SQLSTATE and message an encode error carries.
    fn error_of(err: PgWireError) -> (String, String) {
        let PgWireError::UserError(info) = err else {
            panic!("expected a UserError, got {err:?}");
        };
        (info.code.clone(), info.message.clone())
    }

    #[test]
    fn null_is_sql_null_under_every_type_and_format() {
        for ct in [
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Float8,
            DdlColType::Bool,
            DdlColType::Timestamp,
        ] {
            for format in [FieldFormat::Text, FieldFormat::Binary] {
                assert_eq!(
                    encode_one(ct, format, &Value::Null).expect("null encodes"),
                    None,
                    "{ct:?}/{format:?} must encode NULL"
                );
            }
        }
    }

    /// An integer under a timestamp column is an error naming the column,
    /// under both formats.
    #[test]
    fn integer_under_timestamp_is_an_error_naming_the_column() {
        for ct in [DdlColType::Timestamp, DdlColType::Timestamptz] {
            for format in [FieldFormat::Text, FieldFormat::Binary] {
                let err = encode_one(ct, format, &Value::Integer(EARLY_MICROS))
                    .expect_err("an integer carries no unit");
                let (_, message) = error_of(err);
                assert!(
                    message.contains("column \"c\" holds an integer where a timestamp is required"),
                    "{ct:?}/{format:?}: {message}"
                );
            }
        }
    }

    #[test]
    fn typed_instant_under_timestamp_renders_iso8601() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        assert_eq!(
            encode_text(DdlColType::Timestamp, &Value::NaiveDateTime(at))
                .expect("encodes")
                .as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
        assert_eq!(
            encode_text(DdlColType::Timestamptz, &Value::DateTime(at))
                .expect("encodes")
                .as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
    }

    /// An ISO string re-parses, so its rendering is canonical, not verbatim.
    #[test]
    fn iso_text_under_timestamp_renders_canonical_iso8601() {
        assert_eq!(
            encode_text(
                DdlColType::Timestamp,
                &Value::String("2020-03-05 10:00:00".into())
            )
            .expect("encodes")
            .as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
    }

    /// Binary `timestamp` is a big-endian `i64` of microseconds since
    /// 2000-01-01, under both timestamp types.
    #[test]
    fn binary_timestamp_is_pg_epoch_micros_big_endian() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        let expected = (EARLY_MICROS - PG_EPOCH_OFFSET_MICROS)
            .to_be_bytes()
            .to_vec();
        assert_eq!(
            encode_one(
                DdlColType::Timestamp,
                FieldFormat::Binary,
                &Value::NaiveDateTime(at)
            )
            .expect("encodes"),
            Some(expected.clone())
        );
        assert_eq!(
            encode_one(
                DdlColType::Timestamptz,
                FieldFormat::Binary,
                &Value::DateTime(at)
            )
            .expect("encodes"),
            Some(expected)
        );
    }

    /// Binary `timestamp` reads back as the same instant through
    /// postgres-types' own `SystemTime` decoder.
    #[test]
    fn binary_timestamp_round_trips_through_postgres_types() {
        use postgres_types::FromSql;

        let at = NdbDateTime::from_micros(EARLY_MICROS);
        let bytes = encode_one(
            DdlColType::Timestamp,
            FieldFormat::Binary,
            &Value::NaiveDateTime(at),
        )
        .expect("encodes")
        .expect("not null");
        let decoded = std::time::SystemTime::from_sql(&Type::TIMESTAMP, &bytes).expect("decodes");
        assert_eq!(
            decoded,
            std::time::UNIX_EPOCH + std::time::Duration::from_micros(EARLY_MICROS as u64)
        );
    }

    /// The text arms render the pinned PostgreSQL text forms.
    #[test]
    fn text_arms_render_postgres_text() {
        assert_eq!(
            encode_text(DdlColType::Bool, &Value::Bool(true))
                .expect("encodes")
                .as_deref(),
            Some("t")
        );
        assert_eq!(
            encode_text(DdlColType::Int8, &Value::Integer(42))
                .expect("encodes")
                .as_deref(),
            Some("42")
        );
        // Float columns go through pgwire's float encoder: shortest form.
        assert_eq!(
            encode_text(DdlColType::Float8, &Value::Float(0.0))
                .expect("encodes")
                .as_deref(),
            Some("0")
        );
        // A text column keeps the JSON text of a float.
        assert_eq!(
            encode_text(DdlColType::Text, &Value::Float(0.0))
                .expect("encodes")
                .as_deref(),
            Some("0.0")
        );
        assert_eq!(
            encode_text(DdlColType::Text, &Value::Bytes(vec![0, 255, 7]))
                .expect("encodes")
                .as_deref(),
            Some("\\x00ff07")
        );
    }

    /// A `bytea` cell is `\x` lowercase hex under either requested format,
    /// and a cell that is not bytes is the hex of its text bytes.
    #[test]
    fn bytea_renders_hex_text_under_either_format() {
        for format in [FieldFormat::Text, FieldFormat::Binary] {
            let bytes = encode_one(DdlColType::Bytea, format, &Value::Bytes(vec![0, 0xAB, 7]))
                .expect("encodes")
                .expect("not null");
            assert_eq!(bytes, b"\\x00ab07".to_vec(), "{format:?}");
        }
        assert_eq!(
            encode_text(DdlColType::Bytea, &Value::Bytes(Vec::new()))
                .expect("encodes")
                .as_deref(),
            Some("\\x")
        );
        assert_eq!(
            encode_text(DdlColType::Bytea, &Value::String("hi".into()))
                .expect("encodes")
                .as_deref(),
            Some("\\x6869")
        );
    }

    /// `numeric` and `uuid` cells render their PostgreSQL text under either
    /// requested format.
    #[test]
    fn numeric_and_uuid_render_text_under_either_format() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        for format in [FieldFormat::Text, FieldFormat::Binary] {
            assert_eq!(
                encode_one(
                    DdlColType::Numeric,
                    format,
                    &Value::Decimal(rust_decimal::Decimal::new(1250, 2))
                )
                .expect("encodes"),
                Some(b"12.50".to_vec()),
                "{format:?}"
            );
            assert_eq!(
                encode_one(DdlColType::Uuid, format, &Value::Uuid(uuid.into())).expect("encodes"),
                Some(uuid.as_bytes().to_vec()),
                "{format:?}"
            );
        }
    }

    /// A float array cell renders its `{...}` literal under either requested
    /// format, NULL and non-finite elements included.
    #[test]
    fn float_arrays_render_array_literals() {
        let v = Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.5),
            Value::Float(f64::NAN),
            Value::Null,
        ]);
        for format in [FieldFormat::Text, FieldFormat::Binary] {
            for ct in [DdlColType::Float4Array, DdlColType::Float8Array] {
                assert_eq!(
                    encode_one(ct, format, &v).expect("encodes"),
                    Some(b"{1,2.5,NaN,NULL}".to_vec()),
                    "{ct:?}/{format:?}"
                );
            }
        }
    }

    /// A `json` cell is its JSON text: a string cell is a quoted JSON string.
    #[test]
    fn json_renders_json_text() {
        assert_eq!(
            encode_text(DdlColType::Jsonb, &Value::String("x".into()))
                .expect("encodes")
                .as_deref(),
            Some("\"x\"")
        );
        assert_eq!(
            encode_text(
                DdlColType::Json,
                &Value::Array(vec![Value::Integer(1), Value::Bool(true)])
            )
            .expect("encodes")
            .as_deref(),
            Some("[1,true]")
        );
    }

    /// A finite `f64` beyond `f32` range is refused under `real`, in text
    /// and binary alike.
    #[test]
    fn float4_overflow_is_refused_in_both_formats() {
        for format in [FieldFormat::Text, FieldFormat::Binary] {
            let err = encode_one(DdlColType::Float4, format, &Value::Float(1e39))
                .expect_err("overflow must be refused");
            assert_eq!(error_of(err).0, "22003");
        }
    }

    /// A non-finite float renders the PostgreSQL text under the text
    /// format, and its IEEE bits under the binary format.
    #[test]
    fn non_finite_floats_render_postgres_text_and_ieee_bits() {
        for ct in [DdlColType::Float8, DdlColType::Float4, DdlColType::Text] {
            for (f, text) in [
                (f64::NAN, "NaN"),
                (f64::INFINITY, "Infinity"),
                (f64::NEG_INFINITY, "-Infinity"),
            ] {
                assert_eq!(
                    encode_text(ct, &Value::Float(f))
                        .expect("encodes")
                        .as_deref(),
                    Some(text),
                    "{ct:?}"
                );
            }
        }
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                encode_one(DdlColType::Float8, FieldFormat::Binary, &Value::Float(f))
                    .expect("encodes"),
                Some(f.to_bits().to_be_bytes().to_vec())
            );
        }
        assert_eq!(
            encode_one(
                DdlColType::Float4,
                FieldFormat::Binary,
                &Value::Float(f64::INFINITY)
            )
            .expect("encodes"),
            Some(f32::INFINITY.to_bits().to_be_bytes().to_vec())
        );
    }

    /// A binary scalar column takes only its own shape.
    #[test]
    fn binary_scalar_shape_mismatch_is_an_error() {
        let text = Value::String("42".into());
        for (ct, expected) in [
            (DdlColType::Int8, "an integer"),
            (DdlColType::Int4, "an integer"),
            (DdlColType::Float8, "a float"),
            (DdlColType::Bool, "a bool"),
        ] {
            let err = encode_one(ct, FieldFormat::Binary, &text)
                .expect_err("text under a binary scalar column must be refused");
            let (code, message) = error_of(err);
            assert_eq!(
                code,
                nodedb_types::error::sqlstate::DATA_EXCEPTION,
                "{ct:?}"
            );
            assert!(
                message.contains(&format!("holds text where {expected} is required")),
                "{ct:?}: {message}"
            );
        }
    }

    /// Text under a text-format integer column renders verbatim: a
    /// schemaless row can hold `"42"` under a declared `INT`.
    #[test]
    fn text_under_text_format_integer_renders_verbatim() {
        assert_eq!(
            encode_text(DdlColType::Int8, &Value::String("42".into()))
                .expect("encodes")
                .as_deref(),
            Some("42")
        );
    }

    /// Binary integer narrowing is range-checked.
    #[test]
    fn binary_narrowing_rejects_out_of_range() {
        let err = encode_one(
            DdlColType::Int2,
            FieldFormat::Binary,
            &Value::Integer(i16::MAX as i64 + 1),
        )
        .expect_err("out of range must be refused");
        assert_eq!(error_of(err).0, "22003");
        assert_eq!(
            encode_one(DdlColType::Int2, FieldFormat::Binary, &Value::Integer(7)).expect("encodes"),
            Some(7i16.to_be_bytes().to_vec())
        );
    }
}
