// SPDX-License-Identifier: BUSL-1.1

//! Value coercion and JSON conversion for strict document columns.

use nodedb_types::columnar::{ColumnDef, ColumnType, FloatWidth, IntWidth};
use nodedb_types::value::Value;

/// Coerce a `nodedb_types::Value` to the value `column` stores.
///
/// The value is re-typed to the column type, then checked against the
/// declared integer or float width. A value out of the declared range is
/// refused with [`crate::Error::NumericValueOutOfRange`] (SQLSTATE 22003).
pub fn coerce_value(val: &Value, column: &ColumnDef) -> crate::Result<Value> {
    coerce_declared_value(
        val,
        &column.column_type,
        column.int_width,
        column.float_width,
        &column.name,
    )
}

/// The declared numeric rule every write path applies: re-type `val` to
/// `col_type`, then check the declared width. Strict, columnar, schemaless
/// and KV columns all run this one rule.
pub(crate) fn coerce_declared_value(
    val: &Value,
    col_type: &ColumnType,
    int_width: Option<IntWidth>,
    float_width: Option<FloatWidth>,
    col_name: &str,
) -> crate::Result<Value> {
    let typed = coerce_to_type(val, col_type, col_name)?;
    check_declared_width(&typed, int_width, float_width, col_name)?;
    Ok(typed)
}

/// Refuse an integer outside the declared `SMALLINT` / `INTEGER` range, and
/// a finite float that overflows `REAL`. Rounding into `REAL` is accepted.
fn check_declared_width(
    value: &Value,
    int_width: Option<IntWidth>,
    float_width: Option<FloatWidth>,
    col_name: &str,
) -> crate::Result<()> {
    match value {
        Value::Integer(n) => match int_width {
            Some(width) if !width.contains(*n) => {
                Err(out_of_range(col_name, &n.to_string(), width.pg_type_name()))
            }
            _ => Ok(()),
        },
        Value::Float(f) => match float_width {
            Some(width @ FloatWidth::F32) if f.is_finite() && !(*f as f32).is_finite() => {
                Err(out_of_range(col_name, &f.to_string(), width.pg_type_name()))
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// A value outside the range of `declared_type`: SQLSTATE `22003`.
fn out_of_range(column: &str, value: &str, declared_type: &str) -> crate::Error {
    crate::Error::NumericValueOutOfRange {
        detail: format!(
            "value {value} is out of range for column '{column}' of type {declared_type}"
        ),
    }
}

/// Text that does not parse as `declared_type`: SQLSTATE `22P02`.
fn invalid_text(column: &str, text: &str, declared_type: &str) -> crate::Error {
    crate::Error::InvalidTextRepresentation {
        detail: format!("column '{column}': cannot parse '{text}' as {declared_type}"),
    }
}

/// A value of a kind `declared_type` does not hold: SQLSTATE `42804`.
fn wrong_kind(column: &str, value: &Value, declared_type: &str) -> crate::Error {
    crate::Error::DatatypeMismatch {
        detail: format!("column '{column}': expected {declared_type}, got {value:?}"),
    }
}

/// The error for a float with no `i64` image. A finite float with a
/// fraction, such as `2.5`, is the wrong kind for an integer column. NaN, an
/// infinity, and a magnitude past `i64` are out of range.
fn float_to_int_error(column: &str, f: f64) -> crate::Error {
    if f.is_finite() && f.fract() != 0.0 {
        crate::Error::DatatypeMismatch {
            detail: format!("column '{column}': {f} is not a whole number, expected INT"),
        }
    } else {
        out_of_range(column, &f.to_string(), "INT")
    }
}

/// The error for text that does not parse as an integer of `declared_type`.
/// Digits past the `i64` range are out of range. Anything else is text the
/// type cannot read.
fn int_text_error(
    column: &str,
    text: &str,
    declared_type: &str,
    error: &std::num::ParseIntError,
) -> crate::Error {
    match error.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
            out_of_range(column, text, declared_type)
        }
        _ => invalid_text(column, text, declared_type),
    }
}

/// Re-type `val` to `col_type`. No declared width is checked.
///
/// A refusal carries the SQLSTATE PostgreSQL gives the same assignment:
/// text that does not parse is `22P02`, a value past the type's range is
/// `22003`, and a value of the wrong kind is `42804`. A timestamp column
/// refuses text that is not a date-time with `22007`, and an instant past
/// its range with `22008`.
fn coerce_to_type(val: &Value, col_type: &ColumnType, col_name: &str) -> crate::Result<Value> {
    match col_type {
        ColumnType::Bool => match val {
            Value::Bool(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Bool(*n != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(invalid_text(col_name, s, "BOOL")),
            },
            _ => Err(wrong_kind(col_name, val, "BOOL")),
        },
        ColumnType::Int64 => match val {
            Value::Integer(_) => Ok(val.clone()),
            // A whole float in `i64` range; any other float has no INT image.
            Value::Float(f) => float_to_i64(*f)
                .map(Value::Integer)
                .ok_or_else(|| float_to_int_error(col_name, *f)),
            // A whole decimal in `i64` range. A fractional decimal is the
            // wrong kind. A whole one past `i64`, such as a `u64` above
            // `i64::MAX`, is out of range.
            Value::Decimal(d) if !d.is_integer() => Err(crate::Error::DatatypeMismatch {
                detail: format!("column '{col_name}': {d} is not a whole number, expected INT"),
            }),
            Value::Decimal(d) => rust_decimal::prelude::ToPrimitive::to_i64(d)
                .map(Value::Integer)
                .ok_or_else(|| out_of_range(col_name, &d.to_string(), "INT")),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|e| int_text_error(col_name, s, "INT", &e)),
            _ => Err(wrong_kind(col_name, val, "INT")),
        },
        ColumnType::Float64 => match val {
            Value::Float(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Float(*n as f64)),
            Value::Decimal(d) => rust_decimal::prelude::ToPrimitive::to_f64(d)
                .map(Value::Float)
                .ok_or_else(|| out_of_range(col_name, &d.to_string(), "FLOAT")),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| invalid_text(col_name, s, "FLOAT")),
            _ => Err(wrong_kind(col_name, val, "FLOAT")),
        },
        ColumnType::String | ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex => match val {
            Value::String(_) | Value::Uuid(_) | Value::Ulid(_) | Value::Regex(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::String(n.to_string())),
            Value::Float(f) => Ok(Value::String(f.to_string())),
            Value::Decimal(d) => Ok(Value::String(d.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Ok(Value::String(format!("{other:?}"))),
        },
        ColumnType::Bytes => match val {
            Value::Bytes(_) => Ok(val.clone()),
            Value::String(s) => {
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec());
                Ok(Value::Bytes(bytes))
            }
            _ => Err(wrong_kind(col_name, val, "BYTES")),
        },
        ColumnType::Timestamp => {
            coerce_instant(val, col_name, "TIMESTAMP").map(Value::NaiveDateTime)
        }
        ColumnType::Timestamptz => {
            coerce_instant(val, col_name, "TIMESTAMPTZ").map(Value::DateTime)
        }
        ColumnType::SystemTimestamp => {
            // Engine-assigned; user-supplied values must not reach coercion.
            let _ = val;
            Err(crate::Error::BadRequest {
                detail: format!(
                    "column '{col_name}': SYSTEM_TIMESTAMP is engine-assigned, not user-supplied"
                ),
            })
        }
        ColumnType::Decimal(typmod) => {
            let d = match val {
                Value::Decimal(d) => *d,
                Value::Float(f) => rust_decimal::Decimal::try_from(*f)
                    .map_err(|_| out_of_range(col_name, &f.to_string(), "DECIMAL"))?,
                Value::Integer(n) => rust_decimal::Decimal::from(*n),
                Value::String(s) => {
                    s.parse::<rust_decimal::Decimal>()
                        .map_err(|error| match error {
                            rust_decimal::Error::ExceedsMaximumPossibleValue
                            | rust_decimal::Error::LessThanMinimumPossibleValue => {
                                out_of_range(col_name, s, "DECIMAL")
                            }
                            _ => invalid_text(col_name, s, "DECIMAL"),
                        })?
                }
                _ => return Err(wrong_kind(col_name, val, "DECIMAL")),
            };
            match typmod {
                Some(typmod) => typmod.fit(d).map(Value::Decimal).map_err(|error| {
                    crate::Error::NumericValueOutOfRange {
                        detail: format!("column '{col_name}': {error}"),
                    }
                }),
                None => Ok(Value::Decimal(d)),
            }
        }
        ColumnType::Vector(dim) => match val {
            Value::Bytes(b) if b.len() == *dim as usize * 4 => Ok(val.clone()),
            Value::Array(arr) => {
                let floats = extract_vector_floats(arr, col_name, *dim)?;
                validate_and_encode_vector(col_name, *dim, &floats)
            }
            Value::String(s) => {
                // UPDATE path may serialize ARRAY literal as string — parse it.
                match crate::data::executor::vector_string::parse_vector_string(s) {
                    Some(floats) => validate_and_encode_vector(col_name, *dim, &floats),
                    None => Err(invalid_text(col_name, s, &format!("VECTOR({dim})"))),
                }
            }
            _ => Err(wrong_kind(col_name, val, &format!("VECTOR({dim})"))),
        },
        ColumnType::SparseVector => {
            // Variable-length string-backed: the `'{id: weight}'` literal (or a
            // raw byte form) passes through unmodified and is parsed at
            // index-build time. Schema validation catches genuine mismatches.
            Ok(val.clone())
        }
        // The tuple encoder stores EWKB from a native geometry, its text
        // (GeoJSON, EWKT, WKT, hex EWKB) or WKB bytes.
        ColumnType::Geometry => match val {
            Value::Geometry(_) | Value::String(_) | Value::Bytes(_) => Ok(val.clone()),
            _ => Err(wrong_kind(col_name, val, "GEOMETRY")),
        },
        ColumnType::Duration => match val {
            Value::Duration(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Integer(*n)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|e| int_text_error(col_name, s, "DURATION", &e)),
            _ => Err(wrong_kind(col_name, val, "DURATION")),
        },
        ColumnType::Json
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => {
            // Variable-length inline MessagePack column: val is raw bytes — deserialize to Value.
            if let Value::Bytes(b) = val {
                nodedb_types::value_from_msgpack(b).map_err(|e| {
                    crate::Error::InvalidTextRepresentation {
                        detail: format!(
                            "column '{col_name}': {} bytes are not a MessagePack value of \
                             type {col_type}: {e}",
                            b.len()
                        ),
                    }
                })
            } else {
                Ok(val.clone())
            }
        }
        // ColumnType is #[non_exhaustive]; unknown future types pass the value
        // through unmodified — the schema validator will catch type mismatches.
        _ => Ok(val.clone()),
    }
}

/// `f` as an `i64` when it is a whole number in `i64` range. NaN and the
/// infinities have no `i64` image.
pub(crate) fn float_to_i64(f: f64) -> Option<i64> {
    // `i64::MAX as f64` rounds up to 2^63, which is out of range.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    (f.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&f)).then_some(f as i64)
}

/// The instant a `TIMESTAMP` or `TIMESTAMPTZ` column of type name `kind`
/// stores for `val`. An integer or a float is epoch milliseconds.
fn coerce_instant(
    val: &Value,
    col_name: &str,
    kind: &str,
) -> crate::Result<nodedb_types::NdbDateTime> {
    match val {
        Value::NaiveDateTime(dt) | Value::DateTime(dt) => Ok(*dt),
        Value::Integer(ms) => nodedb_types::NdbDateTime::from_millis(*ms)
            .map_err(|_| instant_overflow(col_name, &ms.to_string(), kind)),
        Value::Float(f) => float_millis_instant(*f, col_name, kind),
        Value::String(s) => parse_instant(s, col_name, kind),
        _ => Err(wrong_kind(col_name, val, kind)),
    }
}

/// Microseconds per millisecond.
const MICROS_PER_MILLI: f64 = 1_000.0;

/// A float count of epoch milliseconds as an instant. An instant holds epoch
/// microseconds, so a fractional millisecond is kept to the microsecond. A
/// part below one microsecond rounds half to even, as PostgreSQL's
/// `to_timestamp(double precision)` does. NaN, an infinity, and a value past
/// the microsecond range overflow the instant.
fn float_millis_instant(
    ms: f64,
    col_name: &str,
    kind: &str,
) -> crate::Result<nodedb_types::NdbDateTime> {
    float_to_i64((ms * MICROS_PER_MILLI).round_ties_even())
        .map(nodedb_types::NdbDateTime::from_micros)
        .ok_or_else(|| instant_overflow(col_name, &ms.to_string(), kind))
}

/// A time text as an instant: an integer count of epoch milliseconds, or a
/// date-time spelling. A count past the instant range overflows the
/// instant. Any other text is not a date-time.
fn parse_instant(s: &str, col_name: &str, kind: &str) -> crate::Result<nodedb_types::NdbDateTime> {
    if let Ok(ms) = s.parse::<i64>() {
        return nodedb_types::NdbDateTime::from_millis(ms)
            .map_err(|_| instant_overflow(col_name, s, kind));
    }
    nodedb_types::datetime::NdbDateTime::parse(s).ok_or_else(|| {
        crate::Error::InvalidDatetimeFormat {
            detail: format!("column '{col_name}': cannot parse '{s}' as {kind}"),
        }
    })
}

/// An instant outside the range `kind` holds: SQLSTATE `22008`.
fn instant_overflow(column: &str, value: &str, kind: &str) -> crate::Error {
    crate::Error::DatetimeFieldOverflow {
        detail: format!("value {value} is out of range for column '{column}' of type {kind}"),
    }
}

/// The `f32` elements of a `Value::Array` bound for a `VECTOR(dim)` column.
/// An element that is not a number is the wrong kind.
fn extract_vector_floats(arr: &[Value], col_name: &str, dim: u32) -> crate::Result<Vec<f32>> {
    arr.iter()
        .map(|v| match v {
            Value::Float(f) => Ok(*f as f32),
            Value::Integer(n) => Ok(*n as f32),
            other => Err(wrong_kind(
                col_name,
                other,
                &format!("a number for VECTOR({dim})"),
            )),
        })
        .collect()
}

/// Check the dimension count and encode as little-endian bytes. A wrong
/// count is SQLSTATE `22000` (data_exception), as pgvector gives it.
fn validate_and_encode_vector(col_name: &str, dim: u32, floats: &[f32]) -> crate::Result<Value> {
    if floats.len() != dim as usize {
        return Err(crate::Error::DataException {
            detail: format!(
                "column '{col_name}': expected VECTOR({dim}), got {} elements",
                floats.len()
            ),
        });
    }
    let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
    Ok(Value::Bytes(bytes))
}

/// Convert a typed `Value` to JSON (for pgwire output only). A set renders
/// as the JSON array of its members, as the wire rendering gives it.
pub fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => {
            serde_json::Value::String(dt.to_iso8601())
        }
        Value::Duration(d) => serde_json::Value::String(d.to_string()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Array(arr) | Value::Set(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        // GeoJSON, `crs` member included for a non-WGS 84 SRID.
        Value::Geometry(g) => nodedb_types::geometry::geojson_tree(g),
        Value::Regex(_) | Value::Range { .. } | Value::Record { .. } | Value::ArrayCell(_) => {
            serde_json::Value::Null
        }
        // Value is #[non_exhaustive]; future variants collapse to JSON null
        // at the pgwire output boundary.
        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::{ColumnDef, StrictSchema};

    fn test_schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        }
    }

    #[test]
    fn roundtrip_via_value() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Alice".into()));
        map.insert("age".into(), Value::Integer(30));

        let tuple_bytes =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
                .unwrap();
        let decoded = super::super::decode::binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u1");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["age"], 30);
    }

    #[test]
    fn nullable_field_omitted() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u2".into()));
        map.insert("name".into(), Value::String("Bob".into()));

        let tuple_bytes =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
                .unwrap();
        let decoded = super::super::decode::binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u2");
        assert!(decoded["age"].is_null());
    }

    #[test]
    fn non_nullable_missing_errors() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u3".into()));

        let result =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }

    #[test]
    fn bitemporal_value_roundtrip() {
        let schema = StrictSchema::new_bitemporal(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .unwrap();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Alice".into()));

        let tuple = super::super::encode::value_to_binary_tuple_bitemporal(
            &Value::Object(map),
            &schema,
            1_700_000_000_000,
            0,
            i64::MAX,
            "docs",
        )
        .unwrap();

        let decoder = nodedb_strict::TupleDecoder::new(&schema);
        let (sys, vf, vu) = decoder.extract_bitemporal_timestamps(&tuple).unwrap();
        assert_eq!(sys, 1_700_000_000_000);
        assert_eq!(vf, 0);
        assert_eq!(vu, i64::MAX);
        assert_eq!(
            decoder.extract_by_name(&tuple, "id").unwrap(),
            Value::String("u1".into())
        );
    }

    #[test]
    fn bitemporal_encode_rejects_non_bitemporal_schema() {
        let schema = test_schema();
        let map = std::collections::HashMap::new();
        let result = super::super::encode::value_to_binary_tuple_bitemporal(
            &Value::Object(map),
            &schema,
            0,
            0,
            0,
            "docs",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not bitemporal"));
    }

    fn dec(text: &str) -> rust_decimal::Decimal {
        text.parse().expect("test decimal parses")
    }

    fn decimal(precision: i64, scale: i64) -> ColumnType {
        ColumnType::Decimal(Some(
            nodedb_types::columnar::DecimalTypmod::new(precision, scale)
                .expect("test typmod is valid"),
        ))
    }

    /// Coerce `val` into a nullable column of `col_type` with no width.
    fn coerce_typed(val: &Value, col_type: &ColumnType, name: &str) -> crate::Result<Value> {
        coerce_value(val, &ColumnDef::nullable(name, *col_type))
    }

    /// Coerce `val` into a nullable column declared as `declared`.
    fn coerce_declared_as(val: &Value, declared: &str) -> crate::Result<Value> {
        let col_type: ColumnType = declared.parse().expect("declared type parses");
        coerce_value(
            val,
            &ColumnDef::nullable("v", col_type).with_declared_width(declared),
        )
    }

    /// A value rounds to the declared scale, half away from zero, and carries
    /// exactly that many fractional digits.
    #[test]
    fn decimal_rounds_to_scale_half_away_from_zero() {
        for (input, expected) in [
            ("1.005", "1.01"),
            ("-1.005", "-1.01"),
            ("1.004", "1.00"),
            ("12.5", "12.50"),
            ("999.994", "999.99"),
        ] {
            let got = coerce_typed(&Value::Decimal(dec(input)), &decimal(5, 2), "d").unwrap();
            let Value::Decimal(d) = got else {
                panic!("{input}: expected a decimal, got {got:?}");
            };
            assert_eq!(d.to_string(), expected, "{input}");
        }
        let from_text = coerce_typed(&Value::String("1.005".into()), &decimal(5, 2), "d").unwrap();
        assert_eq!(from_text, Value::Decimal(dec("1.01")));
        let from_int = coerce_typed(&Value::Integer(7), &decimal(5, 2), "d").unwrap();
        assert_eq!(from_int, Value::Decimal(dec("7.00")));
    }

    /// A value whose rounded integer part has more than `precision - scale`
    /// digits is refused as numeric value out of range.
    #[test]
    fn decimal_past_precision_is_numeric_out_of_range() {
        for input in ["123456.789", "1000", "999.995", "-1000.00"] {
            let err =
                coerce_typed(&Value::Decimal(dec(input)), &decimal(5, 2), "d").expect_err(input);
            assert!(
                matches!(err, crate::Error::NumericValueOutOfRange { .. }),
                "{input}: {err:?}"
            );
        }
        let err = coerce_typed(&Value::Decimal(dec("0.995")), &decimal(2, 2), "d").unwrap_err();
        assert!(matches!(err, crate::Error::NumericValueOutOfRange { .. }));
        assert_eq!(
            coerce_typed(&Value::Decimal(dec("0.994")), &decimal(2, 2), "d").unwrap(),
            Value::Decimal(dec("0.99"))
        );
    }

    /// A plain `DECIMAL` keeps every digit it is given.
    #[test]
    fn unconstrained_decimal_is_not_limited() {
        let wide = dec("12345678901234567890.123456789");
        assert_eq!(
            coerce_typed(&Value::Decimal(wide), &ColumnType::Decimal(None), "d").unwrap(),
            Value::Decimal(wide)
        );
    }

    /// A strict or columnar `SMALLINT` column refuses a value past `i16`, in
    /// every form the value arrives in, as numeric value out of range.
    #[test]
    fn smallint_column_refuses_a_value_past_its_width() {
        assert_eq!(
            coerce_declared_as(&Value::Integer(32767), "SMALLINT").expect("fits"),
            Value::Integer(32767)
        );
        for value in [
            Value::Integer(40000),
            Value::Integer(-40000),
            Value::Float(40000.0),
            Value::String("40000".into()),
            Value::Decimal(dec("40000")),
        ] {
            let err = coerce_declared_as(&value, "SMALLINT").expect_err("past smallint");
            assert!(
                matches!(err, crate::Error::NumericValueOutOfRange { .. }),
                "{value:?}: {err:?}"
            );
        }
        let err = coerce_declared_as(&Value::Integer(i64::from(i32::MAX) + 1), "INTEGER")
            .expect_err("past integer");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
        assert_eq!(
            coerce_declared_as(&Value::Integer(40000), "BIGINT").expect("fits bigint"),
            Value::Integer(40000)
        );
    }

    /// A `REAL` column refuses a finite value past `f32` and accepts one it
    /// rounds. A `DOUBLE PRECISION` column holds the same value.
    #[test]
    fn real_column_refuses_only_an_overflow() {
        let err = coerce_declared_as(&Value::Float(1e39), "REAL").expect_err("past f32");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
        assert_eq!(
            coerce_declared_as(&Value::Float(1.1), "REAL").expect("rounds"),
            Value::Float(1.1)
        );
        assert_eq!(
            coerce_declared_as(&Value::Float(1e39), "DOUBLE PRECISION").expect("fits f64"),
            Value::Float(1e39)
        );
    }

    /// The strict encoder applies the width: a row with an out-of-range
    /// `SMALLINT` cell does not encode.
    #[test]
    fn strict_encoder_refuses_a_row_past_a_declared_width() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Int64).with_declared_width("SMALLINT"),
        ])
        .expect("valid schema");
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("a".into()));
        map.insert("v".into(), Value::Integer(40000));
        let err = super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "c")
            .expect_err("past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }

    /// A float epoch-millisecond count keeps its fraction to the microsecond.
    /// NaN, an infinity, and a value past the range overflow the instant.
    #[test]
    fn float_timestamp_is_exact_or_refused() {
        for col_type in [ColumnType::Timestamp, ColumnType::Timestamptz] {
            let got = coerce_typed(&Value::Float(1.5), &col_type, "t").unwrap();
            let (Value::NaiveDateTime(dt) | Value::DateTime(dt)) = got else {
                panic!("expected an instant, got {got:?}");
            };
            assert_eq!(dt.micros, 1_500);
            for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1e300] {
                let err = coerce_typed(&Value::Float(bad), &col_type, "t").expect_err("refused");
                assert!(
                    matches!(err, crate::Error::DatetimeFieldOverflow { .. }),
                    "{err:?}"
                );
            }
        }
    }

    /// The SQLSTATE `crate::Error` variant a refusal carries.
    fn refusal_kind(err: &crate::Error) -> &'static str {
        match err {
            crate::Error::InvalidTextRepresentation { .. } => "22P02",
            crate::Error::NumericValueOutOfRange { .. } => "22003",
            crate::Error::DatatypeMismatch { .. } => "42804",
            crate::Error::DataException { .. } => "22000",
            crate::Error::InvalidDatetimeFormat { .. } => "22007",
            crate::Error::DatetimeFieldOverflow { .. } => "22008",
            other => panic!("not a value refusal: {other:?}"),
        }
    }

    /// Each refusal carries the SQLSTATE PostgreSQL gives the same
    /// assignment, and its message names the column, the value and the type.
    #[test]
    fn value_refusals_carry_their_postgres_sqlstate() {
        let text = |s: &str| Value::String(s.into());
        let cases: Vec<(Value, ColumnType, &str, &str)> = vec![
            (text("x"), ColumnType::Int64, "22P02", "'x'"),
            (text("2.5"), ColumnType::Int64, "22P02", "'2.5'"),
            (
                text("99999999999999999999"),
                ColumnType::Int64,
                "22003",
                "99999999999999999999",
            ),
            (Value::Float(2.5), ColumnType::Int64, "42804", "2.5"),
            (Value::Float(f64::NAN), ColumnType::Int64, "22003", "NaN"),
            (
                Value::Float(1e19),
                ColumnType::Int64,
                "22003",
                "10000000000000000000",
            ),
            (
                Value::Decimal(dec("2.5")),
                ColumnType::Int64,
                "42804",
                "2.5",
            ),
            (
                Value::Decimal(dec("18446744073709551615")),
                ColumnType::Int64,
                "22003",
                "18446744073709551615",
            ),
            (Value::Bool(true), ColumnType::Int64, "42804", "Bool(true)"),
            (text("abc"), ColumnType::Float64, "22P02", "'abc'"),
            (
                Value::Bool(true),
                ColumnType::Float64,
                "42804",
                "Bool(true)",
            ),
            (text("maybe"), ColumnType::Bool, "22P02", "'maybe'"),
            (
                Value::Array(vec![Value::Integer(1)]),
                ColumnType::Bool,
                "42804",
                "Array",
            ),
            (Value::Integer(1), ColumnType::Bytes, "42804", "Integer(1)"),
            (text("1.2.3"), ColumnType::Decimal(None), "22P02", "'1.2.3'"),
            (
                Value::Float(f64::INFINITY),
                ColumnType::Decimal(None),
                "22003",
                "inf",
            ),
            (
                text("not a date"),
                ColumnType::Timestamp,
                "22007",
                "'not a date'",
            ),
            (
                text("not a date"),
                ColumnType::Timestamptz,
                "22007",
                "'not a date'",
            ),
            (
                Value::Bool(true),
                ColumnType::Timestamptz,
                "42804",
                "Bool(true)",
            ),
            (
                Value::Integer(i64::MAX),
                ColumnType::Timestamp,
                "22008",
                "9223372036854775807",
            ),
            (
                text("9223372036854775807"),
                ColumnType::Timestamptz,
                "22008",
                "9223372036854775807",
            ),
            (
                Value::Float(f64::NAN),
                ColumnType::Timestamp,
                "22008",
                "NaN",
            ),
            (text("soon"), ColumnType::Duration, "22P02", "'soon'"),
            (
                Value::Integer(1),
                ColumnType::Geometry,
                "42804",
                "Integer(1)",
            ),
            (
                Value::Array(vec![Value::Float(0.5)]),
                ColumnType::Vector(2),
                "22000",
                "1 elements",
            ),
            (
                Value::Array(vec![Value::Float(0.5), text("x")]),
                ColumnType::Vector(2),
                "42804",
                "String(\"x\")",
            ),
        ];
        for (value, col_type, sqlstate, shown) in cases {
            let err = coerce_typed(&value, &col_type, "c").expect_err("refused");
            assert_eq!(
                refusal_kind(&err),
                sqlstate,
                "{value:?} into {col_type}: {err:?}"
            );
            let message = err.to_string();
            assert!(message.contains("'c'"), "names the column: {message}");
            assert!(message.contains(shown), "names the value: {message}");
        }
    }

    #[test]
    fn unknown_field_errors() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u4".into()));
        map.insert("name".into(), Value::String("Eve".into()));
        map.insert("extra".into(), Value::String("boom".into()));

        let result =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("\"extra\"") && msg.contains("does not exist"));
    }
}
