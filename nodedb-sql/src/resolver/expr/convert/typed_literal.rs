// SPDX-License-Identifier: Apache-2.0

//! Literals of the SQL types a value keeps exactly: `TIMESTAMP`,
//! `TIMESTAMPTZ`, `UUID`, `BYTEA`, `DECIMAL` / `NUMERIC`, `DURATION` and
//! `FLOAT8`.
//!
//! A typed string (`UUID '...'`), a cast of a literal (`'...'::bytea`,
//! `12.50::decimal`), and a bound parameter of one of these types (the
//! binder writes it as a typed string) all resolve here, to the typed
//! `SqlValue`. A schemaless field then stores the typed value, never its
//! text or a float.

use nodedb_types::columnar::DecimalTypmod;
use rust_decimal::Decimal;
use sqlparser::ast::{DataType, ExactNumberInfo, TimezoneInfo};

use crate::error::{Result, SqlError};
use crate::types::SqlValue;

/// A SQL type whose literal resolves to its own typed `SqlValue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TypedKind {
    Timestamp,
    Timestamptz,
    Uuid,
    Bytea,
    /// `DECIMAL` / `NUMERIC`, with the declared `(p,s)` when one is given.
    Decimal(Option<DecimalTypmod>),
    /// `DURATION`: text such as `90s`, `1h30m`, `500ms`, `250us`.
    Duration,
    /// `FLOAT8` / `DOUBLE PRECISION`: a number, or `NaN`, `Infinity`,
    /// `-Infinity`, the form a bound non-finite float takes.
    Float8,
}

impl TypedKind {
    /// The kind `data_type` names, or `None` for every other type.
    ///
    /// A `DECIMAL(p,s)` whose precision or scale is not a valid typmod is
    /// [`SqlError::DataException`].
    pub(super) fn of(data_type: &DataType) -> Result<Option<Self>> {
        Ok(match data_type {
            DataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
            | DataType::TimestampNtz(_) => Some(Self::Timestamp),
            DataType::Timestamp(_, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) => {
                Some(Self::Timestamptz)
            }
            DataType::Uuid => Some(Self::Uuid),
            DataType::Float8 | DataType::DoublePrecision | DataType::Double(_) => {
                Some(Self::Float8)
            }
            DataType::Bytea => Some(Self::Bytea),
            DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => {
                Some(Self::Decimal(decimal_typmod(info)?))
            }
            DataType::Custom(name, modifiers)
                if modifiers.is_empty()
                    && name
                        .0
                        .last()
                        .and_then(|part| part.as_ident())
                        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("duration")) =>
            {
                Some(Self::Duration)
            }
            _ => None,
        })
    }

    /// The typed value `text` spells.
    ///
    /// Text that spells no value of the kind is refused: a timestamp as
    /// [`SqlError::InvalidDatetimeFormat`], a UUID or bytes as
    /// [`SqlError::InvalidTextRepresentation`].
    pub(super) fn parse_text(self, text: &str) -> Result<SqlValue> {
        match self {
            Self::Timestamp => nodedb_types::NdbDateTime::parse(text)
                .map(SqlValue::Timestamp)
                .ok_or_else(|| SqlError::InvalidDatetimeFormat {
                    type_name: "timestamp",
                    text: text.to_owned(),
                }),
            Self::Timestamptz => nodedb_types::NdbDateTime::parse(text)
                .map(SqlValue::Timestamptz)
                .ok_or_else(|| SqlError::InvalidDatetimeFormat {
                    type_name: "timestamp with time zone",
                    text: text.to_owned(),
                }),
            Self::Uuid => nodedb_types::id_gen::canonical_uuid(text)
                .map(SqlValue::Uuid)
                .ok_or_else(|| SqlError::InvalidTextRepresentation {
                    type_name: "uuid",
                    text: text.to_owned(),
                }),
            Self::Bytea => nodedb_types::bytea_text::parse_bytea_text(text)
                .map(SqlValue::Bytes)
                .ok_or_else(|| SqlError::InvalidTextRepresentation {
                    type_name: "bytea",
                    text: text.to_owned(),
                }),
            Self::Duration => nodedb_types::NdbDuration::parse(text)
                .map(SqlValue::Duration)
                .ok_or_else(|| SqlError::InvalidTextRepresentation {
                    type_name: "duration",
                    text: text.to_owned(),
                }),
            Self::Float8 => crate::params_typed::parse_float8_text(text)
                .map(SqlValue::Float)
                .ok_or_else(|| SqlError::InvalidTextRepresentation {
                    type_name: "double precision",
                    text: text.to_owned(),
                }),
            Self::Decimal(typmod) => {
                let value = Decimal::from_str_exact(text.trim()).map_err(|_| {
                    SqlError::InvalidTextRepresentation {
                        type_name: "numeric",
                        text: text.to_owned(),
                    }
                })?;
                fit_decimal(value, typmod)
            }
        }
    }

    /// `value` cast to this kind, or `None` when the cast is not a literal
    /// conversion this kind owns and stays a runtime `CAST`.
    ///
    /// Text reads through [`TypedKind::parse_text`]. A value already of the
    /// kind passes through, and an instant takes the target's tag. `NULL`
    /// stays `NULL`.
    pub(super) fn cast(self, value: SqlValue) -> Result<Option<SqlValue>> {
        Ok(Some(match (self, value) {
            (_, SqlValue::Null) => SqlValue::Null,
            (_, SqlValue::String(text)) => self.parse_text(&text)?,
            (Self::Timestamp, SqlValue::Timestamp(at) | SqlValue::Timestamptz(at)) => {
                SqlValue::Timestamp(at)
            }
            (Self::Timestamptz, SqlValue::Timestamp(at) | SqlValue::Timestamptz(at)) => {
                SqlValue::Timestamptz(at)
            }
            (Self::Uuid, SqlValue::Uuid(u)) => SqlValue::Uuid(u),
            (Self::Bytea, SqlValue::Bytes(b)) => SqlValue::Bytes(b),
            (Self::Duration, SqlValue::Duration(d)) => SqlValue::Duration(d),
            (Self::Float8, SqlValue::Float(f)) => SqlValue::Float(f),
            (Self::Float8, SqlValue::Int(i)) => SqlValue::Float(i as f64),
            (Self::Float8, SqlValue::Decimal(d)) => {
                match rust_decimal::prelude::ToPrimitive::to_f64(&d) {
                    Some(f) => SqlValue::Float(f),
                    None => return Ok(None),
                }
            }
            (Self::Decimal(typmod), SqlValue::Decimal(d)) => fit_decimal(d, typmod)?,
            (Self::Decimal(typmod), SqlValue::Int(i)) => fit_decimal(Decimal::from(i), typmod)?,
            (Self::Decimal(typmod), SqlValue::Float(f)) => {
                let value =
                    Decimal::try_from(f).map_err(|_| SqlError::InvalidTextRepresentation {
                        type_name: "numeric",
                        text: f.to_string(),
                    })?;
                fit_decimal(value, typmod)?
            }
            _ => return Ok(None),
        }))
    }
}

/// The typmod a `DECIMAL` type's number info declares. `DECIMAL(p)` has
/// scale 0, as in PostgreSQL.
fn decimal_typmod(info: &ExactNumberInfo) -> Result<Option<DecimalTypmod>> {
    let (precision, scale) = match *info {
        ExactNumberInfo::None => return Ok(None),
        ExactNumberInfo::Precision(p) => (p, 0),
        ExactNumberInfo::PrecisionAndScale(p, s) => (p, s),
    };
    let precision = i64::try_from(precision).unwrap_or(i64::MAX);
    DecimalTypmod::new(precision, scale)
        .map(Some)
        .map_err(|e| SqlError::DataException {
            detail: e.to_string(),
        })
}

/// `value` as a `Decimal`, fitted to `typmod` when one is declared. A value
/// whose integer part the typmod cannot hold is
/// [`SqlError::ConstantOverflow`].
fn fit_decimal(value: Decimal, typmod: Option<DecimalTypmod>) -> Result<SqlValue> {
    match typmod {
        None => Ok(SqlValue::Decimal(value)),
        Some(typmod) => {
            typmod
                .fit(value)
                .map(SqlValue::Decimal)
                .map_err(|e| SqlError::ConstantOverflow {
                    detail: format!("numeric field overflow: {e}"),
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn kinds_follow_the_declared_type() {
        assert_eq!(
            TypedKind::of(&DataType::Timestamp(None, TimezoneInfo::Tz)),
            Ok(Some(TypedKind::Timestamptz))
        );
        assert_eq!(
            TypedKind::of(&DataType::Timestamp(Some(3), TimezoneInfo::None)),
            Ok(Some(TypedKind::Timestamp))
        );
        assert_eq!(TypedKind::of(&DataType::Uuid), Ok(Some(TypedKind::Uuid)));
        assert_eq!(TypedKind::of(&DataType::Bytea), Ok(Some(TypedKind::Bytea)));
        assert_eq!(
            TypedKind::of(&DataType::Numeric(ExactNumberInfo::None)),
            Ok(Some(TypedKind::Decimal(None)))
        );
        assert_eq!(TypedKind::of(&DataType::Text), Ok(None));
    }

    #[test]
    fn text_reads_as_the_typed_value() {
        assert_eq!(
            TypedKind::Uuid.parse_text(&UUID.to_ascii_uppercase()),
            Ok(SqlValue::Uuid(UUID.into()))
        );
        assert_eq!(
            TypedKind::Bytea.parse_text("\\x0102ff"),
            Ok(SqlValue::Bytes(vec![1, 2, 0xff]))
        );
    }

    #[test]
    fn invalid_text_is_an_invalid_text_representation() {
        for (kind, text) in [(TypedKind::Uuid, "not-a-uuid"), (TypedKind::Bytea, "\\x0")] {
            assert!(
                matches!(
                    kind.parse_text(text),
                    Err(SqlError::InvalidTextRepresentation { .. })
                ),
                "{kind:?} {text}"
            );
        }
    }

    #[test]
    fn invalid_timestamp_text_is_an_invalid_datetime_format() {
        for kind in [TypedKind::Timestamp, TypedKind::Timestamptz] {
            assert!(matches!(
                kind.parse_text("not a time"),
                Err(SqlError::InvalidDatetimeFormat { .. })
            ));
        }
    }

    #[test]
    fn a_cast_keeps_or_retags_a_typed_value() {
        let at = nodedb_types::NdbDateTime::from_micros(7);
        assert_eq!(
            TypedKind::Timestamptz.cast(SqlValue::Timestamp(at)),
            Ok(Some(SqlValue::Timestamptz(at)))
        );
        assert_eq!(
            TypedKind::Uuid.cast(SqlValue::Uuid(UUID.into())),
            Ok(Some(SqlValue::Uuid(UUID.into())))
        );
        assert_eq!(TypedKind::Uuid.cast(SqlValue::Int(1)), Ok(None));
    }

    #[test]
    fn a_float8_cast_reads_non_finite_text_as_the_float() {
        assert_eq!(
            TypedKind::of(&DataType::Float8),
            Ok(Some(TypedKind::Float8))
        );
        assert_eq!(
            TypedKind::of(&DataType::DoublePrecision),
            Ok(Some(TypedKind::Float8))
        );
        assert_eq!(
            TypedKind::Float8.cast(SqlValue::String("Infinity".into())),
            Ok(Some(SqlValue::Float(f64::INFINITY)))
        );
        assert_eq!(
            TypedKind::Float8.cast(SqlValue::String("-Infinity".into())),
            Ok(Some(SqlValue::Float(f64::NEG_INFINITY)))
        );
        assert!(matches!(
            TypedKind::Float8.cast(SqlValue::String("NaN".into())),
            Ok(Some(SqlValue::Float(f))) if f.is_nan()
        ));
        assert_eq!(
            TypedKind::Float8.cast(SqlValue::Int(2)),
            Ok(Some(SqlValue::Float(2.0)))
        );
        assert!(matches!(
            TypedKind::Float8.parse_text("x"),
            Err(SqlError::InvalidTextRepresentation { .. })
        ));
    }

    #[test]
    fn a_duration_cast_is_a_typed_duration() {
        assert_eq!(
            TypedKind::Duration.cast(SqlValue::String("1500us".into())),
            Ok(Some(SqlValue::Duration(
                nodedb_types::NdbDuration::from_micros(1500)
            )))
        );
        assert!(matches!(
            TypedKind::Duration.parse_text("soon"),
            Err(SqlError::InvalidTextRepresentation { .. })
        ));
    }

    #[test]
    fn a_decimal_cast_keeps_the_exact_digits() {
        let exact = |text: &str| SqlValue::Decimal(Decimal::from_str_exact(text).expect("decimal"));
        assert_eq!(
            TypedKind::Decimal(None).cast(exact("12.50")),
            Ok(Some(exact("12.50")))
        );
        assert_eq!(
            TypedKind::Decimal(None).cast(SqlValue::String("7.10".into())),
            Ok(Some(exact("7.10")))
        );
        let typmod = DecimalTypmod::new(10, 2).expect("valid typmod");
        assert_eq!(
            TypedKind::Decimal(Some(typmod)).cast(SqlValue::Int(3)),
            Ok(Some(exact("3.00")))
        );
        assert!(matches!(
            TypedKind::Decimal(None).cast(SqlValue::String("x".into())),
            Err(SqlError::InvalidTextRepresentation { .. })
        ));
    }
}
