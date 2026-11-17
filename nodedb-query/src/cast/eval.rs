// SPDX-License-Identifier: Apache-2.0

//! `eval_cast`: a value cast to a target type.

use nodedb_types::Value;
use nodedb_types::columnar::DecimalTypmod;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::expr::EvalError;
use crate::value_ops::{is_truthy, value_to_display_string};

use super::int_width::IntWidth;
use super::number::{float_to_int, float_to_numeric, numeric_to_int};
use super::text::{parse_bool, parse_float8, parse_int, parse_numeric};

/// `val` cast to `to_type`, as PostgreSQL casts it. `NULL` casts to `NULL`
/// for every type.
///
/// - Text input follows PostgreSQL's input function for the type
///   ([`super::text`]). Text that spells no value of the type is
///   [`EvalError::InvalidTextRepresentation`] (`22P02`), or
///   [`EvalError::InvalidDatetimeFormat`] (`22007`) for a timestamp.
/// - A float casts to `smallint`, `integer` or `bigint` rounded half to
///   even, a decimal rounded half away from zero. A result past the type's
///   range is [`EvalError::OutOfRange`] (`22003`).
/// - A float casts to `DECIMAL` at 15 significant digits. NaN and the
///   infinities are [`EvalError::NumericSpecialValue`] (`22003`).
/// - A decimal whose integer part `DECIMAL(p,s)` cannot hold is
///   [`EvalError::NumericFieldOverflow`] (`22003`).
pub fn eval_cast(val: &Value, to_type: &crate::expr::CastType) -> Result<Value, EvalError> {
    use crate::expr::CastType;
    if matches!(val, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(match to_type {
        CastType::Int => to_int(val, IntWidth::Int8)?,
        CastType::Int4 => to_int(val, IntWidth::Int4)?,
        CastType::Int2 => to_int(val, IntWidth::Int2)?,
        CastType::Float => match val {
            Value::Float(_) => val.clone(),
            Value::Integer(i) => Value::Float(*i as f64),
            Value::String(s) => Value::Float(parse_float8(s)?),
            Value::Decimal(d) => d.to_f64().map(Value::Float).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        CastType::String => Value::String(value_to_display_string(val)),
        CastType::Bool => match val {
            Value::Bool(_) => val.clone(),
            Value::String(s) => Value::Bool(parse_bool(s)?),
            other => Value::Bool(is_truthy(other)),
        },
        CastType::Decimal(typmod) => Value::Decimal(fit(to_decimal(val)?, *typmod)?),
        CastType::Uuid => match val {
            Value::Uuid(u) => nodedb_types::id_gen::canonical_uuid(u)
                .map(Value::Uuid)
                .ok_or_else(|| invalid_text("uuid", u))?,
            Value::String(s) => nodedb_types::id_gen::canonical_uuid(s.trim())
                .map(Value::Uuid)
                .ok_or_else(|| invalid_text("uuid", s))?,
            other => return Err(invalid_text("uuid", &value_to_display_string(other))),
        },
        CastType::Timestamp => Value::NaiveDateTime(instant(val, "timestamp without time zone")?),
        CastType::Timestamptz => Value::DateTime(instant(val, "timestamp with time zone")?),
        CastType::Duration => match val {
            Value::Duration(_) => val.clone(),
            Value::String(s) => nodedb_types::NdbDuration::parse(s)
                .map(Value::Duration)
                .ok_or_else(|| invalid_text("duration", s))?,
            other => return Err(invalid_text("duration", &value_to_display_string(other))),
        },
    })
}

/// `val` as an integer of `width`. An integer past the width, and a float
/// or decimal that rounds past it, are out of range. A value of any other
/// type is `NULL`.
fn to_int(val: &Value, width: IntWidth) -> Result<Value, EvalError> {
    Ok(Value::Integer(match val {
        Value::Integer(i) => width.fit(*i)?,
        Value::Float(f) => float_to_int(*f, width)?,
        Value::String(s) => parse_int(s, width)?,
        Value::Bool(b) => i64::from(*b),
        Value::Decimal(d) => numeric_to_int(*d, width)?,
        _ => return Ok(Value::Null),
    }))
}

/// The exact decimal `val` holds or spells.
fn to_decimal(val: &Value) -> Result<Decimal, EvalError> {
    match val {
        Value::Decimal(d) => Ok(*d),
        Value::Integer(i) => Ok(Decimal::from(*i)),
        Value::Bool(b) => Ok(Decimal::from(u8::from(*b))),
        Value::Float(f) => float_to_numeric(*f),
        Value::String(s) => parse_numeric(s),
        other => Err(invalid_text("numeric", &value_to_display_string(other))),
    }
}

/// `value` fitted to `typmod` when one is declared, rounded to its scale.
fn fit(value: Decimal, typmod: Option<DecimalTypmod>) -> Result<Decimal, EvalError> {
    match typmod {
        None => Ok(value),
        Some(typmod) => typmod
            .fit(value)
            .map_err(|e| EvalError::NumericFieldOverflow {
                detail: e.to_string(),
            }),
    }
}

/// The instant `val` holds or spells. `type_name` names the target type in
/// the error.
fn instant(val: &Value, type_name: &'static str) -> Result<nodedb_types::NdbDateTime, EvalError> {
    match val {
        Value::DateTime(at) | Value::NaiveDateTime(at) => Ok(*at),
        Value::String(s) => nodedb_types::NdbDateTime::parse(s.trim()).ok_or_else(|| {
            EvalError::InvalidDatetimeFormat {
                type_name,
                text: s.clone(),
            }
        }),
        other => Err(EvalError::InvalidDatetimeFormat {
            type_name,
            text: value_to_display_string(other),
        }),
    }
}

fn invalid_text(type_name: &'static str, text: &str) -> EvalError {
    EvalError::InvalidTextRepresentation {
        type_name,
        text: text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::CastType;

    fn cast_ok(val: &Value, to_type: &CastType) -> Value {
        eval_cast(val, to_type).expect("cast")
    }

    #[test]
    fn cast_string_to_int() {
        assert_eq!(
            cast_ok(&Value::String("42".into()), &CastType::Int),
            Value::Integer(42)
        );
    }

    /// A float casts to `bigint` rounded half to even, as `float8_int8`.
    #[test]
    fn cast_float_to_int() {
        assert_eq!(
            cast_ok(&Value::Float(3.7), &CastType::Int),
            Value::Integer(4)
        );
        assert_eq!(
            cast_ok(&Value::Float(2.5), &CastType::Int),
            Value::Integer(2)
        );
        let err = eval_cast(&Value::Float(f64::NAN), &CastType::Int).unwrap_err();
        assert_eq!(err.to_string(), "bigint out of range");
    }

    /// A decimal casts to `bigint` rounded half away from zero, as
    /// `numeric_int8`.
    #[test]
    fn cast_decimal_to_int() {
        let half: Decimal = "2.5".parse().expect("decimal");
        assert_eq!(
            cast_ok(&Value::Decimal(half), &CastType::Int),
            Value::Integer(3)
        );
        assert_eq!(
            cast_ok(&Value::Decimal(-half), &CastType::Int),
            Value::Integer(-3)
        );
        let past = Decimal::from(u64::MAX);
        assert!(matches!(
            eval_cast(&Value::Decimal(past), &CastType::Int),
            Err(EvalError::OutOfRange(_))
        ));
    }

    /// Text that is no `bigint` is `22P02`, and one past the range `22003`.
    #[test]
    fn cast_bad_text_to_int_fails() {
        assert!(matches!(
            eval_cast(&Value::String("3.5".into()), &CastType::Int),
            Err(EvalError::InvalidTextRepresentation { .. })
        ));
        assert!(matches!(
            eval_cast(&Value::String("abc".into()), &CastType::Int),
            Err(EvalError::InvalidTextRepresentation { .. })
        ));
        assert!(matches!(
            eval_cast(
                &Value::String("99999999999999999999".into()),
                &CastType::Int
            ),
            Err(EvalError::OutOfRange(_))
        ));
        assert_eq!(
            cast_ok(&Value::String(" -12 ".into()), &CastType::Int),
            Value::Integer(-12)
        );
    }

    /// `smallint` and `integer` casts check their range, as PostgreSQL's.
    #[test]
    fn cast_to_narrow_integers_checks_the_range() {
        assert_eq!(
            cast_ok(&Value::Integer(32767), &CastType::Int2),
            Value::Integer(32767)
        );
        let err = eval_cast(&Value::Integer(32768), &CastType::Int2).unwrap_err();
        assert_eq!(err.to_string(), "smallint out of range");
        let err = eval_cast(&Value::Integer(1 << 31), &CastType::Int4).unwrap_err();
        assert_eq!(err.to_string(), "integer out of range");
        assert_eq!(
            cast_ok(&Value::Float(2.5), &CastType::Int4),
            Value::Integer(2)
        );
        let err = eval_cast(&Value::String("40000".into()), &CastType::Int2).unwrap_err();
        assert_eq!(
            err.to_string(),
            "value \"40000\" is out of range for type smallint"
        );
        assert_eq!(cast_ok(&Value::Null, &CastType::Int2), Value::Null);
    }

    /// `NULL` casts to `NULL` for every type.
    #[test]
    fn cast_null_is_null_for_every_type() {
        for to_type in [
            CastType::Int,
            CastType::Float,
            CastType::String,
            CastType::Bool,
            CastType::Decimal(None),
            CastType::Uuid,
            CastType::Timestamp,
            CastType::Timestamptz,
            CastType::Duration,
            CastType::Int2,
            CastType::Int4,
        ] {
            assert_eq!(cast_ok(&Value::Null, &to_type), Value::Null, "{to_type:?}");
        }
    }

    #[test]
    fn cast_int_to_string() {
        assert_eq!(
            cast_ok(&Value::Integer(42), &CastType::String),
            Value::String("42".into())
        );
    }

    #[test]
    fn cast_to_decimal_keeps_the_exact_digits() {
        let exact: rust_decimal::Decimal = "12.50".parse().expect("decimal");
        assert_eq!(
            cast_ok(&Value::String("12.50".into()), &CastType::Decimal(None)),
            Value::Decimal(exact)
        );
        assert_eq!(
            cast_ok(&Value::Decimal(exact), &CastType::Decimal(None)),
            Value::Decimal(exact)
        );
        assert_eq!(
            cast_ok(&Value::Integer(3), &CastType::Decimal(None)),
            Value::Decimal(rust_decimal::Decimal::from(3))
        );
    }

    /// Text casts to `boolean` by PostgreSQL's spellings, and any other
    /// text is `22P02`.
    #[test]
    fn cast_to_bool() {
        assert_eq!(
            cast_ok(&Value::Integer(1), &CastType::Bool),
            Value::Bool(true)
        );
        assert_eq!(
            cast_ok(&Value::Integer(0), &CastType::Bool),
            Value::Bool(false)
        );
        for (text, want) in [(" Yes ", true), ("tr", true), ("off", false), ("0", false)] {
            assert_eq!(
                cast_ok(&Value::String(text.into()), &CastType::Bool),
                Value::Bool(want),
                "{text:?}"
            );
        }
        for text in ["", "x", "o"] {
            assert!(
                matches!(
                    eval_cast(&Value::String(text.into()), &CastType::Bool),
                    Err(EvalError::InvalidTextRepresentation { .. })
                ),
                "{text:?}"
            );
        }
    }

    /// Text with an exponent casts to numeric, and a float casts at 15
    /// significant digits.
    #[test]
    fn cast_to_decimal_reads_exponents_and_floats() {
        let Value::Decimal(d) = cast_ok(&Value::String("1e5".into()), &CastType::Decimal(None))
        else {
            panic!("a numeric cast is a Decimal");
        };
        assert_eq!(d.to_string(), "100000");
        let Value::Decimal(d) = cast_ok(&Value::Float(0.1), &CastType::Decimal(None)) else {
            panic!("a numeric cast is a Decimal");
        };
        assert_eq!(d.to_string(), "0.1");
        assert_eq!(
            eval_cast(&Value::Float(f64::INFINITY), &CastType::Decimal(None)),
            Err(EvalError::NumericSpecialValue { value: "Infinity" })
        );
        assert_eq!(
            eval_cast(&Value::String("NaN".into()), &CastType::Decimal(None)),
            Err(EvalError::NumericSpecialValue { value: "NaN" })
        );
    }

    #[test]
    fn cast_to_a_decimal_typmod_fits_and_refuses_an_overflow() {
        let typmod = DecimalTypmod::new(5, 2).expect("valid typmod");
        assert_eq!(
            cast_ok(
                &Value::String("12.345".into()),
                &CastType::Decimal(Some(typmod))
            ),
            Value::Decimal("12.35".parse().expect("decimal"))
        );
        assert_eq!(
            cast_ok(&Value::Integer(7), &CastType::Decimal(Some(typmod))),
            Value::Decimal("7.00".parse().expect("decimal"))
        );
        assert!(matches!(
            eval_cast(&Value::Integer(12345), &CastType::Decimal(Some(typmod))),
            Err(EvalError::NumericFieldOverflow { .. })
        ));
        assert!(matches!(
            eval_cast(&Value::String("x".into()), &CastType::Decimal(None)),
            Err(EvalError::InvalidTextRepresentation { .. })
        ));
    }

    #[test]
    fn cast_to_uuid_is_typed_and_refuses_bad_text() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            cast_ok(&Value::String(uuid.to_ascii_uppercase()), &CastType::Uuid),
            Value::Uuid(uuid.into())
        );
        assert!(matches!(
            eval_cast(&Value::String("nope".into()), &CastType::Uuid),
            Err(EvalError::InvalidTextRepresentation { .. })
        ));
        assert_eq!(cast_ok(&Value::Null, &CastType::Uuid), Value::Null);
    }

    #[test]
    fn cast_to_a_timestamp_is_an_instant_of_its_kind() {
        let at = nodedb_types::NdbDateTime::parse("2024-01-02T03:04:05Z").expect("instant");
        assert_eq!(
            cast_ok(
                &Value::String("2024-01-02T03:04:05Z".into()),
                &CastType::Timestamptz
            ),
            Value::DateTime(at)
        );
        assert_eq!(
            cast_ok(&Value::DateTime(at), &CastType::Timestamp),
            Value::NaiveDateTime(at)
        );
        assert!(matches!(
            eval_cast(&Value::String("soon".into()), &CastType::Timestamp),
            Err(EvalError::InvalidDatetimeFormat { .. })
        ));
    }
}
