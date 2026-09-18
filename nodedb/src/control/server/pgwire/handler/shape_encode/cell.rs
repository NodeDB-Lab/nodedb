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

use nodedb_types::columnar::IntWidth;
use nodedb_types::error::NodeDbError;
use nodedb_types::{NdbDateTime, Value};

use crate::control::server::pgwire::numeric_narrow::{checked_narrow, checked_narrow_f32};
use crate::control::server::pgwire::types::error_map::shape_error_to_pg;
use crate::control::server::response_shape::cell::{cell_text, instant_of, shape_mismatch};
use crate::control::server::response_shape::types::DdlColType;

/// Microseconds from the Unix epoch to the PostgreSQL epoch
/// (2000-01-01 00:00:00 UTC), which binary `timestamp`/`timestamptz` count
/// from.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Encode one cell of column `column` into `encoder` per its column type
/// `ct` and wire `format`.
///
/// `Value::Null` is SQL NULL for every type and format. Under the text
/// format, `Float8`/`Float4` numbers go through pgwire's native float
/// encoder (ryu + `extra_float_digits`) so their bytes match PostgreSQL,
/// `Timestamp`/`Timestamptz` cells render the instant [`instant_of`] reads
/// as ISO-8601, and every other type renders [`cell_text`]. Under the binary
/// format each scalar type takes only its own shape; a mismatch is an error,
/// because the client reads the advertised type's bytes and a text value
/// under it would be misread.
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

    // A timestamp column renders the one instant its cell denotes; the
    // encoder picks text (ISO-8601) or binary (PostgreSQL-epoch micros) from
    // the schema's format.
    if matches!(ct, DdlColType::Timestamp | DdlColType::Timestamptz) {
        let instant = instant_of(v, column).map_err(to_pg_error)?;
        return encoder.encode_field(&PgTimestamp(instant));
    }

    // Binary result format: the column's `FieldInfo` is Binary, so
    // `encode_field` emits the value's binary wire form. Only the
    // binary-supported types reach a Binary format (the resolver downgrades
    // the rest to Text upstream); any other `ct` under Binary falls through
    // to the text arms below.
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
            | DdlColType::Timestamp
            | DdlColType::Timestamptz => {}
        }
    }

    match ct {
        DdlColType::Float8 => match v {
            Value::Float(f) => encoder.encode_field(f),
            Value::Integer(i) => encoder.encode_field(&(*i as f64)),
            other => encoder.encode_field(&cell_text(other)),
        },
        // Same overflow guard as the binary arm: the text rendering of a
        // `real` column must not silently read `Infinity` for a finite stored
        // value either.
        DdlColType::Float4 => match v {
            Value::Float(f) => encoder.encode_field(&checked_narrow_f32(*f)?),
            Value::Integer(i) => encoder.encode_field(&checked_narrow_f32(*i as f64)?),
            other => encoder.encode_field(&cell_text(other)),
        },
        DdlColType::Text
        | DdlColType::Varchar
        | DdlColType::Int8
        | DdlColType::Int4
        | DdlColType::Int2
        | DdlColType::Bool
        | DdlColType::Bytea
        | DdlColType::Json
        | DdlColType::Jsonb
        | DdlColType::Float4Array
        | DdlColType::Float8Array
        | DdlColType::Timestamp
        | DdlColType::Timestamptz => encoder.encode_field(&cell_text(v)),
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
    use crate::control::server::pgwire::ddl_encode::col_type_to_field_with_format;

    /// The one instant these tests use: 2020-03-05T10:00:00Z.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    /// Encode one cell under a one-column schema and return its raw field
    /// bytes, or `None` for SQL NULL.
    fn encode_one(ct: DdlColType, format: FieldFormat, v: &Value) -> PgWireResult<Option<Vec<u8>>> {
        let schema: Arc<Vec<FieldInfo>> =
            Arc::new(vec![col_type_to_field_with_format("c", ct, format)]);
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
            Some("AP8H")
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
            let (_, message) = error_of(err);
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
