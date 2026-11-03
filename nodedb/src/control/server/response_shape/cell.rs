// SPDX-License-Identifier: BUSL-1.1

//! Cell-level conversions a protocol entrypoint renders a shaped row through.
//!
//! The scalar JSON conversion, [`value_to_wire_json`], lives in
//! [`crate::util::wire_json`] — a neutral home so `control::security`
//! (which `response_shape` depends on for redaction) never has to depend
//! back on `control::server`. This module re-exports it and adds the
//! row-level helper, which depends on [`ShapedRow`], plus the two typed
//! readings pgwire renders a cell from: its PostgreSQL text form
//! ([`cell_text`]) and, for a timestamp column, the instant it denotes
//! ([`instant_of`]).

use nodedb_types::error::NodeDbError;
use nodedb_types::{NdbDateTime, Value};

use super::types::ShapedRow;

pub use crate::util::wire_json::value_to_wire_json;

/// Render one shaped row as a JSON object, cell by cell.
pub fn row_to_wire_json(row: &ShapedRow) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .map(|(k, v)| (k.clone(), value_to_wire_json(v)))
        .collect()
}

/// The PostgreSQL text form of one cell, or `None` for SQL NULL.
///
/// `None` covers `Value::Null` and every shape with no JSON form: a range,
/// a record. Scalars render directly — a string verbatim, a bool as
/// `t`/`f`, an integer as its decimal digits, a finite float as its
/// shortest round-trip JSON text (`0.0` stays `0.0`), a non-finite float as
/// `NaN`, `Infinity` or `-Infinity` as PostgreSQL renders it, an instant as
/// ISO-8601, bytes as [`bytea_hex`], a decimal as its digits with its
/// scale (`1.10` stays `1.10`). Every other shape
/// renders through [`value_to_wire_json`], so its text is the JSON a text
/// protocol emits for it.
pub fn cell_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_owned()),
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => wire_json_text(&nodedb_types::value::float_to_json(*f)),
        Value::String(s) => Some(s.clone()),
        Value::Bytes(bytes) => Some(bytea_hex(bytes)),
        // Every digit and the scale, as PostgreSQL renders `numeric`. The
        // wire JSON number is an `f64` and drops both.
        Value::Decimal(d) => Some(d.to_string()),
        Value::DateTime(at) | Value::NaiveDateTime(at) => Some(at.to_iso8601()),
        Value::Array(_)
        | Value::Object(_)
        | Value::Uuid(_)
        | Value::Ulid(_)
        | Value::Duration(_)
        | Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_)
        | Value::Vector(_) => wire_json_text(&value_to_wire_json(v)),
        // `Value` is `#[non_exhaustive]`: a variant this crate cannot name
        // renders through its wire JSON, like the composite arm above.
        _ => wire_json_text(&value_to_wire_json(v)),
    }
}

/// The PostgreSQL text form of a wire-JSON cell, or `None` for JSON null.
///
/// A string is verbatim, a bool is `t`/`f`, and every other JSON value is
/// its `Display` text.
fn wire_json_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_owned()),
        other => Some(other.to_string()),
    }
}

/// The PostgreSQL text output of a `bytea`: `\x` followed by two lowercase
/// hex digits per byte.
pub fn bytea_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    text.push_str(&hex::encode(bytes));
    text
}

/// The instant a cell under a timestamp column denotes.
///
/// A typed instant is itself. A string is the instant it parses to as
/// ISO-8601. Any other shape, including an integer, is an error: an integer
/// carries no unit, so reading it as any epoch scale would be a guess. The
/// error names `column` and the shape found.
pub fn instant_of(v: &Value, column: &str) -> Result<NdbDateTime, NodeDbError> {
    match v {
        Value::DateTime(at) | Value::NaiveDateTime(at) => Ok(*at),
        Value::String(s) => NdbDateTime::parse(s).ok_or_else(|| {
            NodeDbError::data_exception(format!(
                "column \"{column}\" holds text that is not a timestamp: {s:?}"
            ))
        }),
        other => Err(shape_mismatch(column, "a timestamp", other)),
    }
}

/// The error for a cell whose shape is not the one its column requires:
/// `column "ts" holds an integer where a timestamp is required`.
///
/// The stored value is the fault, so the error is a data exception
/// (SQLSTATE `22000`).
pub fn shape_mismatch(column: &str, expected: &str, found: &Value) -> NodeDbError {
    NodeDbError::data_exception(format!(
        "column \"{column}\" holds {} where {expected} is required",
        shape_name(found)
    ))
}

/// The article-prefixed shape name an error names a cell by.
fn shape_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Integer(_) => "an integer",
        Value::Float(_) => "a float",
        Value::String(_) => "text",
        Value::Bytes(_) => "bytes",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
        Value::Uuid(_) => "a uuid",
        Value::Ulid(_) => "a ulid",
        Value::DateTime(_) | Value::NaiveDateTime(_) => "a timestamp",
        Value::Duration(_) => "a duration",
        Value::Decimal(_) => "a decimal",
        Value::Geometry(_) => "a geometry",
        Value::Set(_) => "a set",
        Value::Regex(_) => "a regex",
        Value::Range { .. } => "a range",
        Value::Record { .. } => "a record",
        Value::ArrayCell(_) => "an array cell",
        Value::Vector(_) => "a vector",
        // `Value` is `#[non_exhaustive]`: a variant this crate cannot name.
        _ => "an unsupported value",
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::ErrorCode;

    use super::*;

    /// The one instant these tests use: 2020-03-05T10:00:00Z.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    /// Every scalar except bytes and decimals renders the same text its wire
    /// JSON renders to, so the direct arms and the JSON edge cannot drift
    /// apart. Bytes are base64 in JSON and `\x` hex in PostgreSQL text. A
    /// decimal is an `f64` JSON number and exact digits in PostgreSQL text.
    #[test]
    fn scalar_text_matches_the_wire_json_text() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        let scalars = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Integer(42),
            Value::Integer(-7),
            Value::Float(0.0),
            Value::Float(1.5),
            Value::Float(1e20),
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::String("hello".into()),
            Value::DateTime(at),
            Value::NaiveDateTime(at),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::Duration(nodedb_types::NdbDuration::from_micros(1_500_000)),
            Value::Array(vec![Value::Integer(1), Value::Bool(true)]),
            Value::Range {
                start: None,
                end: None,
                inclusive: false,
            },
        ];
        for (index, v) in scalars.iter().enumerate() {
            assert_eq!(
                cell_text(v),
                wire_json_text(&value_to_wire_json(v)),
                "scalar {index} must render the same text either way"
            );
        }
    }

    /// The pinned text of the scalars pgwire renders most.
    #[test]
    fn scalar_text_is_the_postgres_text_form() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        assert_eq!(cell_text(&Value::Null), None);
        assert_eq!(cell_text(&Value::Bool(true)).as_deref(), Some("t"));
        assert_eq!(cell_text(&Value::Bool(false)).as_deref(), Some("f"));
        assert_eq!(cell_text(&Value::Integer(42)).as_deref(), Some("42"));
        assert_eq!(cell_text(&Value::Float(0.0)).as_deref(), Some("0.0"));
        assert_eq!(cell_text(&Value::Float(f64::NAN)).as_deref(), Some("NaN"));
        assert_eq!(
            cell_text(&Value::Float(f64::INFINITY)).as_deref(),
            Some("Infinity")
        );
        assert_eq!(
            cell_text(&Value::Float(f64::NEG_INFINITY)).as_deref(),
            Some("-Infinity")
        );
        assert_eq!(cell_text(&Value::String("x".into())).as_deref(), Some("x"));
        assert_eq!(
            cell_text(&Value::Bytes(vec![0, 255, 7])).as_deref(),
            Some("\\x00ff07")
        );
        assert_eq!(cell_text(&Value::Bytes(Vec::new())).as_deref(), Some("\\x"));
        assert_eq!(
            cell_text(&Value::Decimal(rust_decimal::Decimal::new(110, 2))).as_deref(),
            Some("1.10")
        );
        assert_eq!(
            cell_text(&Value::Decimal(
                "0.1234567890123456789012345678".parse().expect("decimal")
            ))
            .as_deref(),
            Some("0.1234567890123456789012345678")
        );
        assert_eq!(
            cell_text(&Value::NaiveDateTime(at)).as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
    }

    /// A typed instant is itself, whichever kind it is.
    #[test]
    fn instant_of_reads_a_typed_instant() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        assert_eq!(instant_of(&Value::DateTime(at), "ts").expect("instant"), at);
        assert_eq!(
            instant_of(&Value::NaiveDateTime(at), "ts").expect("instant"),
            at
        );
    }

    /// An ISO-8601 string, with or without the `T` separator, is the instant
    /// it denotes.
    #[test]
    fn instant_of_parses_iso8601_text() {
        let at = NdbDateTime::from_micros(EARLY_MICROS);
        for text in [
            "2020-03-05 10:00:00",
            "2020-03-05T10:00:00Z",
            "2020-03-05T10:00:00.000000Z",
        ] {
            assert_eq!(
                instant_of(&Value::String(text.into()), "ts").expect("parses"),
                at,
                "{text} must parse to the instant"
            );
        }
    }

    /// An integer under a timestamp column is refused, and the error names
    /// the column and the shape.
    #[test]
    fn instant_of_refuses_an_integer() {
        let err = instant_of(&Value::Integer(EARLY_MICROS), "created_at")
            .expect_err("an integer carries no unit");
        assert_eq!(err.code(), ErrorCode::DATA_EXCEPTION);
        assert!(
            err.message()
                .contains("column \"created_at\" holds an integer where a timestamp is required"),
            "message must name the column and the shape, got: {}",
            err.message()
        );
    }

    /// Text that is not a timestamp is refused, and the error names it.
    #[test]
    fn instant_of_refuses_text_that_does_not_parse() {
        let err = instant_of(&Value::String("yesterday".into()), "ts")
            .expect_err("free text is not an instant");
        assert_eq!(err.code(), ErrorCode::DATA_EXCEPTION);
        assert!(
            err.message()
                .contains("column \"ts\" holds text that is not a timestamp"),
            "message must name the column, got: {}",
            err.message()
        );
        assert!(err.message().contains("yesterday"));
    }
}
