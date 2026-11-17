// SPDX-License-Identifier: Apache-2.0

//! Typed values in an object literal: the SQL typed-string form
//! (`UUID '...'`) and the cast form (`'...'::uuid`, `12.50::decimal`).
//!
//! The types are the ones a value keeps exactly and JSON holds only as
//! text: `uuid`, `timestamptz`, `timestamp`, `bytea`, `decimal` /
//! `numeric`, and `duration`. A bound parameter of one of these types inside a DSL object
//! literal is written in the typed-string form, so it reaches the parser
//! typed.
//!
//! `float8` reads the float a text spells, including `'NaN'::FLOAT8`,
//! `'Infinity'::FLOAT8` and `'-Infinity'::FLOAT8`, the form a bound
//! non-finite float takes.

use nodedb_types::Value;

use crate::error::SqlError;

/// A type an object-literal value can be written as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiteralType {
    Uuid,
    Timestamptz,
    Timestamp,
    Bytea,
    Decimal,
    Duration,
    Float8,
}

impl LiteralType {
    /// The type `name` spells, case ignored, or `None`.
    pub(super) fn named(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "uuid" => Some(Self::Uuid),
            "timestamptz" => Some(Self::Timestamptz),
            "timestamp" => Some(Self::Timestamp),
            "bytea" => Some(Self::Bytea),
            "decimal" | "numeric" => Some(Self::Decimal),
            "duration" => Some(Self::Duration),
            "float8" | "double" => Some(Self::Float8),
            _ => None,
        }
    }

    /// The typed value `text` spells. Text that spells none is refused.
    pub(super) fn value(self, text: &str) -> Result<Value, SqlError> {
        let invalid = |type_name: &'static str| SqlError::InvalidTextRepresentation {
            type_name,
            text: text.to_owned(),
        };
        match self {
            Self::Uuid => nodedb_types::id_gen::canonical_uuid(text)
                .map(Value::Uuid)
                .ok_or_else(|| invalid("uuid")),
            Self::Timestamptz => nodedb_types::NdbDateTime::parse(text)
                .map(Value::DateTime)
                .ok_or_else(|| SqlError::InvalidDatetimeFormat {
                    type_name: "timestamp with time zone",
                    text: text.to_owned(),
                }),
            Self::Timestamp => nodedb_types::NdbDateTime::parse(text)
                .map(Value::NaiveDateTime)
                .ok_or_else(|| SqlError::InvalidDatetimeFormat {
                    type_name: "timestamp",
                    text: text.to_owned(),
                }),
            Self::Bytea => nodedb_types::bytea_text::parse_bytea_text(text)
                .map(Value::Bytes)
                .ok_or_else(|| invalid("bytea")),
            Self::Decimal => rust_decimal::Decimal::from_str_exact(text.trim())
                .map(Value::Decimal)
                .map_err(|_| invalid("numeric")),
            Self::Duration => nodedb_types::NdbDuration::parse(text)
                .map(Value::Duration)
                .ok_or_else(|| invalid("duration")),
            Self::Float8 => crate::params_typed::parse_float8_text(text)
                .map(Value::Float)
                .ok_or_else(|| invalid("double precision")),
        }
    }
}

/// The type a `::name` cast at `pos` names, consuming the cast. `None`
/// leaves `pos` unchanged: no cast follows. A cast to any other type is
/// refused.
pub(super) fn parse_cast(chars: &[char], pos: &mut usize) -> Result<Option<LiteralType>, SqlError> {
    let mut at = *pos;
    while at < chars.len() && chars[at].is_ascii_whitespace() {
        at += 1;
    }
    if chars.get(at) != Some(&':') || chars.get(at + 1) != Some(&':') {
        return Ok(None);
    }
    at += 2;
    let start = at;
    while at < chars.len() && (chars[at].is_ascii_alphanumeric() || chars[at] == '_') {
        at += 1;
    }
    let name: String = chars.get(start..at).unwrap_or_default().iter().collect();
    let ty = LiteralType::named(&name).ok_or_else(|| SqlError::Parse {
        detail: format!(
            "unsupported cast '::{name}' in object literal; \
             expected uuid, timestamptz, timestamp, bytea, decimal, numeric, duration \
             or float8"
        ),
    })?;
    *pos = at;
    Ok(Some(ty))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_values() {
        assert_eq!(LiteralType::named("UUID"), Some(LiteralType::Uuid));
        assert_eq!(LiteralType::named("text"), None);
        assert_eq!(
            LiteralType::Decimal.value("12.50"),
            Ok(Value::Decimal("12.50".parse().unwrap()))
        );
        assert!(matches!(
            LiteralType::Uuid.value("x"),
            Err(SqlError::InvalidTextRepresentation { .. })
        ));
    }

    #[test]
    fn float8_reads_finite_and_non_finite_text() {
        assert_eq!(LiteralType::named("FLOAT8"), Some(LiteralType::Float8));
        assert_eq!(LiteralType::Float8.value("1.5"), Ok(Value::Float(1.5)));
        assert_eq!(
            LiteralType::Float8.value("Infinity"),
            Ok(Value::Float(f64::INFINITY))
        );
        assert_eq!(
            LiteralType::Float8.value("-Infinity"),
            Ok(Value::Float(f64::NEG_INFINITY))
        );
        assert!(matches!(LiteralType::Float8.value("NaN"), Ok(Value::Float(f)) if f.is_nan()));
        assert!(matches!(
            LiteralType::Float8.value("x"),
            Err(SqlError::InvalidTextRepresentation { .. })
        ));
    }

    #[test]
    fn a_cast_is_consumed_only_when_present() {
        let chars: Vec<char> = " ::bytea }".chars().collect();
        let mut pos = 0;
        assert_eq!(parse_cast(&chars, &mut pos), Ok(Some(LiteralType::Bytea)));
        assert_eq!(pos, 8);
        let chars: Vec<char> = " }".chars().collect();
        let mut pos = 0;
        assert_eq!(parse_cast(&chars, &mut pos), Ok(None));
        assert_eq!(pos, 0);
        let chars: Vec<char> = "::text".chars().collect();
        assert!(parse_cast(&chars, &mut 0).is_err());
    }
}
