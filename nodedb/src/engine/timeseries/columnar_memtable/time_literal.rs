// SPDX-License-Identifier: BUSL-1.1

//! Lowering between a time column's stored milliseconds and the value its
//! kind denotes, in both directions.
//!
//! A time column stores an `i64` millisecond count whatever its kind. The
//! kind decides what that count means to a reader: an `Instant` column's
//! cell is a typed datetime, a `Millis` column's cell is the integer stored.
//! A predicate literal against the column arrives already typed for that
//! kind (the planner coerces a literal against a declared `TIMESTAMP` /
//! `TIMESTAMPTZ` column to an instant), so lowering it to the stored unit is
//! a kind dispatch with no unit guessing: an instant yields its milliseconds,
//! an integer yields itself, and a value of the wrong kind yields nothing.

use nodedb_types::datetime::{NdbDateTime, NdbDateTimeError};
use nodedb_types::value::Value;

use super::types::TimeKind;

impl TimeKind {
    /// The stored millisecond count a predicate literal (or a cell of the
    /// same kind) denotes on a column of this kind, or `None` when the value
    /// is not of the kind the column holds.
    ///
    /// `Instant`: a typed datetime, or text that parses as one (a literal
    /// that reached the engine as text). `Millis`: an integer, a finite
    /// float truncated toward zero, or text that parses as a datetime — a
    /// `BIGINT TIME_KEY` is still a point in time, so a datetime literal
    /// names its millisecond count.
    pub fn literal_ms(self, value: &Value) -> Option<i64> {
        match self {
            TimeKind::Instant(_) => match value {
                Value::DateTime(at) | Value::NaiveDateTime(at) => Some(at.unix_millis()),
                Value::String(text) => NdbDateTime::parse(text).map(|at| at.unix_millis()),
                Value::Null
                | Value::Bool(_)
                | Value::Integer(_)
                | Value::Float(_)
                | Value::Bytes(_)
                | Value::Array(_)
                | Value::Object(_)
                | Value::Uuid(_)
                | Value::Ulid(_)
                | Value::Duration(_)
                | Value::Decimal(_)
                | Value::Geometry(_)
                | Value::Set(_)
                | Value::Regex(_)
                | Value::ArrayCell(_)
                | Value::Vector(_) => None,
                // `Value` is `#[non_exhaustive]`: a kind added later carries
                // no instant either.
                _ => None,
            },
            TimeKind::Millis => match value {
                Value::Integer(ms) => Some(*ms),
                Value::Float(ms) => ms.is_finite().then(|| ms.trunc() as i64),
                Value::String(text) => NdbDateTime::parse(text).map(|at| at.unix_millis()),
                Value::Null
                | Value::Bool(_)
                | Value::Bytes(_)
                | Value::Array(_)
                | Value::Object(_)
                | Value::Uuid(_)
                | Value::Ulid(_)
                | Value::DateTime(_)
                | Value::NaiveDateTime(_)
                | Value::Duration(_)
                | Value::Decimal(_)
                | Value::Geometry(_)
                | Value::Set(_)
                | Value::Regex(_)
                | Value::ArrayCell(_)
                | Value::Vector(_) => None,
                // `Value` is `#[non_exhaustive]`: a kind added later is not a
                // millisecond count either.
                _ => None,
            },
        }
    }

    /// The value a stored millisecond cell denotes on a column of this kind.
    ///
    /// An `Instant` column yields a typed instant, a `Millis` column the
    /// integer stored. `Err` when the milliseconds overflow the microsecond
    /// range an instant carries.
    pub fn cell_value(self, millis: i64) -> Result<Value, NdbDateTimeError> {
        match self {
            TimeKind::Instant(kind) => kind.from_millis(millis),
            TimeKind::Millis => Ok(Value::Integer(millis)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::InstantKind;

    /// `2020-03-05T10:00:00Z` in epoch milliseconds.
    const EARLY_MS: i64 = 1_583_402_400_000;

    const NAIVE: TimeKind = TimeKind::Instant(InstantKind::Naive);
    const UTC: TimeKind = TimeKind::Instant(InstantKind::Utc);

    fn early() -> NdbDateTime {
        NdbDateTime::from_micros(EARLY_MS * 1_000)
    }

    #[test]
    fn an_instant_column_lowers_typed_instants_of_either_tag() {
        for kind in [NAIVE, UTC] {
            assert_eq!(
                kind.literal_ms(&Value::NaiveDateTime(early())),
                Some(EARLY_MS)
            );
            assert_eq!(kind.literal_ms(&Value::DateTime(early())), Some(EARLY_MS));
        }
    }

    #[test]
    fn an_instant_column_parses_datetime_text() {
        assert_eq!(
            NAIVE.literal_ms(&Value::String("2020-03-05 10:00:00".into())),
            Some(EARLY_MS)
        );
        assert_eq!(
            UTC.literal_ms(&Value::String("2020-03-05T10:00:00Z".into())),
            Some(EARLY_MS)
        );
        assert_eq!(NAIVE.literal_ms(&Value::String("not a date".into())), None);
    }

    /// A bare number carries no unit, so it is not an instant literal.
    #[test]
    fn an_instant_column_refuses_numbers_and_other_kinds() {
        for value in [
            Value::Integer(EARLY_MS),
            Value::Float(EARLY_MS as f64),
            Value::String(EARLY_MS.to_string()),
            Value::Null,
            Value::Bool(true),
            Value::Array(vec![Value::Integer(EARLY_MS)]),
        ] {
            assert_eq!(NAIVE.literal_ms(&value), None, "{value:?}");
        }
    }

    #[test]
    fn a_millis_column_lowers_integers_and_truncates_finite_floats() {
        assert_eq!(TimeKind::Millis.literal_ms(&Value::Integer(42)), Some(42));
        assert_eq!(TimeKind::Millis.literal_ms(&Value::Float(42.9)), Some(42));
        assert_eq!(TimeKind::Millis.literal_ms(&Value::Float(-1.5)), Some(-1));
        assert_eq!(TimeKind::Millis.literal_ms(&Value::Float(f64::NAN)), None);
        assert_eq!(
            TimeKind::Millis.literal_ms(&Value::Float(f64::INFINITY)),
            None
        );
    }

    #[test]
    fn a_millis_column_refuses_instants_and_other_kinds() {
        for value in [
            Value::NaiveDateTime(early()),
            Value::DateTime(early()),
            Value::String("42".into()),
            Value::Null,
            Value::Bool(true),
        ] {
            assert_eq!(TimeKind::Millis.literal_ms(&value), None, "{value:?}");
        }
    }

    #[test]
    fn a_millis_column_parses_datetime_text_to_its_millisecond_count() {
        let value = Value::String("2020-03-05 10:00:00".into());
        assert_eq!(
            TimeKind::Millis.literal_ms(&value),
            Some(early().unix_millis())
        );
    }

    #[test]
    fn a_cell_reads_back_as_its_kind() {
        assert_eq!(
            NAIVE.cell_value(EARLY_MS).expect("in range"),
            Value::NaiveDateTime(early())
        );
        assert_eq!(
            UTC.cell_value(EARLY_MS).expect("in range"),
            Value::DateTime(early())
        );
        assert_eq!(
            TimeKind::Millis.cell_value(EARLY_MS).expect("an integer"),
            Value::Integer(EARLY_MS)
        );
        assert!(NAIVE.cell_value(i64::MAX).is_err());
    }

    /// A cell written by `cell_value` lowers back to the milliseconds it was
    /// written from, on every kind.
    #[test]
    fn cell_value_and_literal_ms_round_trip() {
        for kind in [NAIVE, UTC, TimeKind::Millis] {
            let cell = kind.cell_value(EARLY_MS).expect("in range");
            assert_eq!(kind.literal_ms(&cell), Some(EARLY_MS), "{kind:?}");
        }
    }
}
