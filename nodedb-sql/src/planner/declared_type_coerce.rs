// SPDX-License-Identifier: Apache-2.0

//! Coerce literal DML values into the representation their declared column
//! type requires, for engines that persist the planner's values verbatim.
//!
//! # Why this exists
//!
//! A fractional SQL literal resolves to [`SqlValue::Decimal`] (see
//! `resolver::expr::value::convert_value`, which prefers exact decimal
//! arithmetic over a lossy `f64`). Decimal has no msgpack scalar form, so the
//! Origin planner serializes it as a msgpack *string*. Engines with a typed
//! write path re-type each field against the declared column type before
//! storing it — the strict document encoder parses that string back into a
//! float — so the stored cell always matches what the column declared.
//!
//! Two engines have no typed write path — key-value and document-schemaless.
//! Both store the value bytes they are handed, and the declared schema lives
//! only in the catalog, in the Control Plane. Left uncoerced, a column declared
//! `REAL` / `DOUBLE` holds the string `"1.5"` while `RowDescription` advertises
//! float4 / float8, and the pgwire encoder — which correctly refuses to encode
//! a non-number under a numeric OID — transmits SQL NULL. The client asked for
//! four bytes of `real` and gets nothing: a stored value silently lost in
//! flight.
//!
//! Coercing here, where the declared column types are already in hand, is what
//! makes the declared type authoritative on those write paths too. It is
//! applied on every engine's `VALUES` and `SET` path rather than only theirs:
//! an engine that re-types on write is unaffected by a value arriving
//! pre-typed, and branching on engine here would put a routing decision outside
//! `EngineRules` for no behavioural gain.
//!
//! # Scope
//!
//! [`SqlDataType::Int64`] and [`SqlDataType::Float64`] columns are coerced
//! symmetrically — this is not a float special case. They are two declared
//! types whose stored representation is decided by the declaration rather
//! than by the value: nodedb keeps every integer as an `i64` and every float
//! as an `f64`, and the declared width that drives the wire OID (see
//! `ColumnInfo::int_width` / `ColumnInfo::float_width`) is only honest if the
//! stored cell is a number of that family to begin with.
//!
//! [`SqlDataType::Timestamp`] and [`SqlDataType::Timestamptz`] columns are
//! coerced to a typed instant, once, here, for every engine. An integer time
//! literal carries no unit of its own, and the engines that re-type on write
//! disagree on one (the strict encoder reads an integer as microseconds, the
//! columnar family as milliseconds), while the engines that store the
//! planner's value verbatim would persist the bare integer and leave the read
//! side to guess. Resolving the literal here fixes the unit in one place:
//! SQL reads an integer time literal as epoch milliseconds, the unit every
//! engine already reads it as through its own ingest paths. A text literal is
//! parsed here for the same reason, so an unparseable spelling is refused at
//! the statement instead of being stored as text that no read can render.
//!
//! Every other declared type either has one unambiguous literal form already
//! or (like `DECIMAL`) is deliberately carried as text for exactness, and
//! passes through untouched.
//!
//! The numeric conversions mirror the strict document encoder's
//! `coerce_value`, so the two engines accept and reject exactly the same
//! literals for a given declared type.

use nodedb_types::datetime::NdbDateTime;
use rust_decimal::prelude::ToPrimitive;

use super::dml_helpers::{check_declared_float_ranges, check_declared_int_ranges};
use crate::error::{Result, SqlError};
use crate::types::{ColumnInfo, SqlDataType, SqlExpr, SqlValue};

/// Coerce one literal bound for `column` by the write-side rule, then
/// range-check it against the column's declared width.
///
/// The one entry a caller outside the planner uses for a literal that will
/// reach storage under `column`: a DDL gate checks a column `DEFAULT` through
/// it, so a default is accepted or refused exactly as the same literal in a
/// `VALUES` clause is. The primary-key column keeps its literal as written,
/// the same exemption every `VALUES` and `SET` path carries — see
/// [`coerce_rows_to_declared_types`].
///
/// Errors name the column and the literal.
pub fn coerce_write_literal(column: &ColumnInfo, value: SqlValue) -> Result<SqlValue> {
    if column.is_primary_key {
        return Ok(value);
    }
    let coerced = coerce_value(&column.name, value, &column.data_type)?;
    let row = [vec![(column.name.clone(), coerced)]];
    check_declared_int_ranges(std::slice::from_ref(column), &row)?;
    check_declared_float_ranges(std::slice::from_ref(column), &row)?;
    let [mut row] = row;
    Ok(row.swap_remove(0).1)
}

/// Coerce every value in one `(column, value)` row to its declared column
/// type, in place.
///
/// Columns absent from `declared_columns` (a KV collection's untyped
/// `key`/`value` convention, a `ttl` pseudo-column) are left untouched, as is
/// `exempt_column` — see [`coerce_rows_to_declared_types`] for why the primary
/// key is exempt.
pub(super) fn coerce_row_to_declared_types(
    declared_columns: &[ColumnInfo],
    row: &mut [(String, SqlValue)],
    exempt_column: Option<&str>,
) -> Result<()> {
    for (name, value) in row.iter_mut() {
        if is_exempt(exempt_column, name.as_str()) {
            continue;
        }
        let Some(column) = declared_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name.as_str()))
        else {
            continue;
        };
        let taken = std::mem::replace(value, SqlValue::Null);
        *value = coerce_value(name.as_str(), taken, &column.data_type)?;
    }
    Ok(())
}

/// [`coerce_row_to_declared_types`] across every row of a `VALUES` clause.
///
/// # Why the primary key is exempt
///
/// A row's key is not stored as a typed cell: the KV engine keys on
/// `sql_value_to_bytes` of the literal and the document engines on
/// `sql_value_to_string` of it, and the *read* side renders its own
/// `WHERE pk = <literal>` through those same functions with no coercion in
/// between. Re-typing only the write side would let an inserted key and the
/// key that looks it up disagree byte-for-byte — `VALUES (5.0)` stored under
/// `"5"` but searched for as `"5.0"` — turning a wire-fidelity fix into
/// unfindable rows. The key column keeps its literal exactly as written on
/// both sides.
pub(super) fn coerce_rows_to_declared_types(
    declared_columns: &[ColumnInfo],
    rows: &mut [Vec<(String, SqlValue)>],
    exempt_column: Option<&str>,
) -> Result<()> {
    for row in rows.iter_mut() {
        coerce_row_to_declared_types(declared_columns, row, exempt_column)?;
    }
    Ok(())
}

/// [`coerce_row_to_declared_types`] for `SET col = <literal>` assignments.
///
/// Only literal assignments carry a value at plan time; a computed assignment
/// (`SET n = n + 1`) is evaluated by the engine and is not this pass's to
/// re-type. `exempt_column` carries the same primary-key exemption, for the
/// same reason: `SET pk = <literal>` rewrites the row's identity, which the
/// engines derive from the literal's own rendering.
pub(super) fn coerce_assignments_to_declared_types(
    declared_columns: &[ColumnInfo],
    assignments: &mut [(String, SqlExpr)],
    exempt_column: Option<&str>,
) -> Result<()> {
    for (name, expr) in assignments.iter_mut() {
        if is_exempt(exempt_column, name.as_str()) {
            continue;
        }
        let SqlExpr::Literal(value) = expr else {
            continue;
        };
        let Some(column) = declared_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name.as_str()))
        else {
            continue;
        };
        let taken = std::mem::replace(value, SqlValue::Null);
        *value = coerce_value(name.as_str(), taken, &column.data_type)?;
    }
    Ok(())
}

/// Whether `column` is the exempted (primary-key) column, compared the same
/// case-insensitive way as every other column-name lookup here.
fn is_exempt(exempt_column: Option<&str>, column: &str) -> bool {
    exempt_column.is_some_and(|exempt| exempt.eq_ignore_ascii_case(column))
}

/// Coerce one literal to `declared`, returning it unchanged when the declared
/// type imposes no representation of its own.
///
/// The one rule for a literal bound for a declared column, on the write side
/// (`VALUES`, `SET`) and on the read side (`predicate_coerce`, for a literal
/// compared against the column), so a row is found by the same literal that
/// stored it.
pub(crate) fn coerce_value(
    column: &str,
    value: SqlValue,
    declared: &SqlDataType,
) -> Result<SqlValue> {
    match declared {
        SqlDataType::Int64 => coerce_to_int(column, value),
        SqlDataType::Float64 => coerce_to_float(column, value),
        SqlDataType::Timestamp | SqlDataType::Timestamptz => {
            coerce_to_instant(column, value, declared)
        }
        SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry
        | SqlDataType::Unknown => Ok(value),
    }
}

/// Integer column: an `i64` passes through; a float or decimal literal is
/// accepted only when it names a whole number, and a numeric string is parsed.
///
/// A fractional literal is refused rather than truncated — the column declared
/// an integer, and silently dropping the fraction is the same class of
/// invisible data change this module exists to prevent. `NULL` and non-numeric
/// values are left alone: nullability and type checking are enforced
/// elsewhere, and this pass only fixes representation.
fn coerce_to_int(column: &str, value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Float(f) => whole_f64(f)
            .map(SqlValue::Int)
            .ok_or_else(|| not_representable(column, &f.to_string(), "INT")),
        SqlValue::Decimal(d) => match d.is_integer().then(|| d.to_i64()).flatten() {
            Some(n) => Ok(SqlValue::Int(n)),
            None => Err(not_representable(column, &d.to_string(), "INT")),
        },
        SqlValue::String(s) => s
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|_| not_representable(column, &s, "INT")),
        other => Ok(other),
    }
}

/// Float column: an `f64` passes through; integer and decimal literals become
/// the `f64` the column stores, and a numeric string is parsed.
///
/// Narrowing a decimal to `f64` is the same rounding PostgreSQL performs
/// assigning `numeric` to `double precision`, so it is not an error — only a
/// decimal with no `f64` image at all is refused.
fn coerce_to_float(column: &str, value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Int(i) => Ok(SqlValue::Float(i as f64)),
        SqlValue::Decimal(d) => d
            .to_f64()
            .map(SqlValue::Float)
            .ok_or_else(|| not_representable(column, &d.to_string(), "FLOAT")),
        SqlValue::String(s) => s
            .parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|_| not_representable(column, &s, "FLOAT")),
        other => Ok(other),
    }
}

/// Timestamp column: every accepted literal becomes the one typed instant
/// the column stores, tagged as the declared kind.
///
/// A typed literal keeps its instant and takes the declared tag, so a
/// `TIMESTAMP` column holds `SqlValue::Timestamp` and a `TIMESTAMPTZ` column
/// holds `SqlValue::Timestamptz` whichever spelling wrote it. Text is parsed
/// as ISO-8601 (`Z`, a `±HH:MM` offset, and fractional seconds are all
/// accepted). A numeric literal is epoch milliseconds — the unit every engine
/// reads an integer time literal as — and a fractional one contributes its
/// integer part, mirroring the columnar engine. `NULL` is left alone:
/// nullability is enforced elsewhere. Any other literal kind is refused
/// naming the column and the kind, because no instant can be read from it.
fn coerce_to_instant(column: &str, value: SqlValue, declared: &SqlDataType) -> Result<SqlValue> {
    let declared_name = instant_declared_name(declared);
    let tag = |at: NdbDateTime| match declared {
        SqlDataType::Timestamptz => SqlValue::Timestamptz(at),
        SqlDataType::Int64
        | SqlDataType::Float64
        | SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Timestamp
        | SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry
        | SqlDataType::Unknown => SqlValue::Timestamp(at),
    };
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Timestamp(at) | SqlValue::Timestamptz(at) => Ok(tag(at)),
        SqlValue::String(s) => NdbDateTime::parse(&s)
            .map(tag)
            .ok_or_else(|| not_representable(column, &s, declared_name)),
        SqlValue::Int(millis) => NdbDateTime::from_millis(millis)
            .map(tag)
            .map_err(|_| not_representable(column, &millis.to_string(), declared_name)),
        SqlValue::Float(f) => whole_f64(f.trunc())
            .and_then(|millis| NdbDateTime::from_millis(millis).ok())
            .map(tag)
            .ok_or_else(|| not_representable(column, &f.to_string(), declared_name)),
        SqlValue::Decimal(d) => d
            .trunc()
            .to_i64()
            .and_then(|millis| NdbDateTime::from_millis(millis).ok())
            .map(tag)
            .ok_or_else(|| not_representable(column, &d.to_string(), declared_name)),
        SqlValue::Bool(_) | SqlValue::Bytes(_) | SqlValue::Array(_) => {
            Err(SqlError::TypeMismatch {
                detail: format!(
                    "column '{column}': {} is not representable as {declared_name}",
                    literal_kind(&value)
                ),
            })
        }
    }
}

/// The declared type name an instant coercion error reports.
fn instant_declared_name(declared: &SqlDataType) -> &'static str {
    match declared {
        SqlDataType::Timestamptz => "TIMESTAMPTZ",
        SqlDataType::Int64
        | SqlDataType::Float64
        | SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Timestamp
        | SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry
        | SqlDataType::Unknown => "TIMESTAMP",
    }
}

/// The article-prefixed kind name an error names a refused literal by.
fn literal_kind(value: &SqlValue) -> &'static str {
    match value {
        SqlValue::Null => "null",
        SqlValue::Bool(_) => "a boolean",
        SqlValue::Int(_) => "an integer",
        SqlValue::Float(_) => "a float",
        SqlValue::Decimal(_) => "a decimal",
        SqlValue::String(_) => "text",
        SqlValue::Bytes(_) => "bytes",
        SqlValue::Array(_) => "an array",
        SqlValue::Timestamp(_) => "a timestamp",
        SqlValue::Timestamptz(_) => "a timestamptz",
    }
}

/// `Some(n)` when `f` is a whole number inside `i64`'s range.
fn whole_f64(f: f64) -> Option<i64> {
    (f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64).then_some(f as i64)
}

fn not_representable(column: &str, value: &str, declared: &str) -> SqlError {
    SqlError::TypeMismatch {
        detail: format!("column '{column}': '{value}' is not representable as {declared}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, data_type: SqlDataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn coerced(columns: &[ColumnInfo], name: &str, value: SqlValue) -> Result<SqlValue> {
        coerced_with_exemption(columns, name, value, None)
    }

    fn coerced_with_exemption(
        columns: &[ColumnInfo],
        name: &str,
        value: SqlValue,
        exempt_column: Option<&str>,
    ) -> Result<SqlValue> {
        let mut rows = vec![vec![(name.to_string(), value)]];
        coerce_rows_to_declared_types(columns, &mut rows, exempt_column)?;
        Ok(rows.remove(0).remove(0).1)
    }

    fn decimal(text: &str) -> SqlValue {
        SqlValue::Decimal(
            rust_decimal::Decimal::from_str_exact(text).expect("test decimal literal parses"),
        )
    }

    /// The failing case this module exists for: a fractional literal bound for
    /// a declared float column must reach storage as a number, not as the
    /// decimal's text form.
    #[test]
    fn fractional_literal_becomes_a_float_for_a_float_column() {
        let columns = [column("r", SqlDataType::Float64)];
        assert_eq!(
            coerced(&columns, "r", decimal("1.5")).expect("1.5 fits a float column"),
            SqlValue::Float(1.5)
        );
    }

    /// Integers are coerced by the same rule, in both directions: an integer
    /// literal in a float column widens, and an integer column keeps its
    /// `i64` untouched.
    #[test]
    fn integer_literals_follow_the_declared_family() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::Int(5)).expect("an integer fits a float column"),
            SqlValue::Float(5.0)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Int(5)).expect("an integer fits an integer column"),
            SqlValue::Int(5)
        );
    }

    /// A whole-numbered decimal or float is a valid integer; a fractional one
    /// is refused rather than truncated.
    #[test]
    fn integer_column_accepts_whole_numbers_and_refuses_fractions() {
        let columns = [column("n", SqlDataType::Int64)];
        assert_eq!(
            coerced(&columns, "n", decimal("3.0")).expect("3.0 is a whole number"),
            SqlValue::Int(3)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Float(4.0)).expect("4.0 is a whole number"),
            SqlValue::Int(4)
        );
        assert!(
            coerced(&columns, "n", decimal("1.5")).is_err(),
            "a fractional value must not be silently truncated into an INT column"
        );
    }

    /// Numeric text is parsed into the declared family; text that is not a
    /// number at all is an error rather than a stored string under a numeric
    /// wire type.
    #[test]
    fn numeric_strings_are_parsed_and_non_numeric_text_is_refused() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::String("2.25".into())).expect("numeric text parses"),
            SqlValue::Float(2.25)
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::String("7".into())).expect("numeric text parses"),
            SqlValue::Int(7)
        );
        assert!(coerced(&columns, "r", SqlValue::String("abc".into())).is_err());
    }

    /// NULL is untouched — nullability is enforced elsewhere, and this pass
    /// only fixes representation.
    #[test]
    fn null_passes_through_every_declared_type() {
        let columns = [
            column("r", SqlDataType::Float64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced(&columns, "r", SqlValue::Null).expect("null is not an error"),
            SqlValue::Null
        );
        assert_eq!(
            coerced(&columns, "n", SqlValue::Null).expect("null is not an error"),
            SqlValue::Null
        );
    }

    /// Non-numeric declared types impose no representation of their own: a
    /// `DECIMAL` column keeps its exact decimal, and a `TEXT` column keeps
    /// whatever literal it was given.
    #[test]
    fn non_numeric_declared_types_pass_through_untouched() {
        let columns = [
            column("d", SqlDataType::Decimal),
            column("t", SqlDataType::String),
        ];
        assert_eq!(
            coerced(&columns, "d", decimal("1.5")).expect("decimal columns keep exact decimals"),
            decimal("1.5")
        );
        assert_eq!(
            coerced(&columns, "t", SqlValue::Int(9)).expect("text columns are untouched"),
            SqlValue::Int(9)
        );
    }

    /// `2020-03-05T10:00:00Z` as microseconds since the Unix epoch.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    fn early() -> NdbDateTime {
        NdbDateTime::from_micros(EARLY_MICROS)
    }

    /// A typed literal keeps its instant and takes the declared tag, in both
    /// directions.
    #[test]
    fn typed_instant_literals_are_retagged_to_the_declared_kind() {
        let columns = [
            column("at", SqlDataType::Timestamp),
            column("at_tz", SqlDataType::Timestamptz),
        ];
        assert_eq!(
            coerced(&columns, "at", SqlValue::Timestamptz(early()))
                .expect("a typed instant fits a TIMESTAMP column"),
            SqlValue::Timestamp(early())
        );
        assert_eq!(
            coerced(&columns, "at_tz", SqlValue::Timestamp(early()))
                .expect("a typed instant fits a TIMESTAMPTZ column"),
            SqlValue::Timestamptz(early())
        );
    }

    /// Text is parsed as ISO-8601: a `Z` suffix, an offset, and fractional
    /// seconds all resolve to the one instant, and the offset shifts it.
    #[test]
    fn text_literals_are_parsed_to_the_instant_they_spell() {
        let columns = [column("at", SqlDataType::Timestamp)];
        for spelling in [
            "2020-03-05 10:00:00",
            "2020-03-05T10:00:00Z",
            "2020-03-05T10:00:00.000000Z",
            "2020-03-05T15:30:00+05:30",
        ] {
            assert_eq!(
                coerced(&columns, "at", SqlValue::String(spelling.into()))
                    .unwrap_or_else(|e| panic!("{spelling} parses: {e}")),
                SqlValue::Timestamp(early()),
                "{spelling}"
            );
        }
    }

    /// Text that spells no instant is refused, and the error names the
    /// column and the literal.
    #[test]
    fn non_datetime_text_is_refused_naming_the_column_and_literal() {
        let columns = [column("created_at", SqlDataType::Timestamp)];
        let err = coerced(
            &columns,
            "created_at",
            SqlValue::String("not a date".into()),
        )
        .expect_err("non-datetime text must be refused");
        let detail = err.to_string();
        assert!(
            detail.contains("created_at") && detail.contains("not a date"),
            "error must name the column and the literal: {detail}"
        );
    }

    /// An integer literal is epoch milliseconds; a fractional literal
    /// contributes its integer part.
    #[test]
    fn numeric_literals_are_epoch_milliseconds() {
        let columns = [
            column("at", SqlDataType::Timestamp),
            column("at_tz", SqlDataType::Timestamptz),
        ];
        assert_eq!(
            coerced(&columns, "at", SqlValue::Int(1_583_402_400_000))
                .expect("an integer is epoch milliseconds"),
            SqlValue::Timestamp(early())
        );
        assert_eq!(
            coerced(&columns, "at_tz", SqlValue::Int(1_583_402_400_000))
                .expect("an integer is epoch milliseconds"),
            SqlValue::Timestamptz(early())
        );
        assert_eq!(
            coerced(&columns, "at", SqlValue::Float(1_583_402_400_000.7))
                .expect("a float contributes its integer part"),
            SqlValue::Timestamp(early())
        );
        assert_eq!(
            coerced(&columns, "at", decimal("1583402400000.9"))
                .expect("a decimal contributes its integer part"),
            SqlValue::Timestamp(early())
        );
    }

    /// A numeric literal whose milliseconds overflow the instant's range, or
    /// that is not a finite number, is refused rather than wrapped.
    #[test]
    fn out_of_range_numeric_literals_are_refused() {
        let columns = [column("at", SqlDataType::Timestamp)];
        assert!(coerced(&columns, "at", SqlValue::Int(i64::MAX)).is_err());
        assert!(coerced(&columns, "at", SqlValue::Float(f64::NAN)).is_err());
        assert!(coerced(&columns, "at", SqlValue::Float(f64::INFINITY)).is_err());
        assert!(coerced(&columns, "at", decimal("99999999999999999999999")).is_err());
    }

    /// NULL is untouched, and every literal kind that carries no instant is
    /// refused with an error naming the column and the kind.
    #[test]
    fn null_passes_and_non_instant_kinds_are_refused() {
        let columns = [column("created_at", SqlDataType::Timestamptz)];
        assert_eq!(
            coerced(&columns, "created_at", SqlValue::Null).expect("null is not an error"),
            SqlValue::Null
        );
        for (literal, kind) in [
            (SqlValue::Bool(true), "a boolean"),
            (SqlValue::Bytes(vec![1, 2]), "bytes"),
            (SqlValue::Array(vec![SqlValue::Int(1)]), "an array"),
        ] {
            let err = coerced(&columns, "created_at", literal)
                .expect_err("a literal with no instant must be refused");
            let detail = err.to_string();
            assert!(
                detail.contains("created_at") && detail.contains(kind),
                "error must name the column and the kind: {detail}"
            );
        }
    }

    /// `SET at = <literal>` runs the same instant coercion as `VALUES`.
    #[test]
    fn assignments_coerce_instants() {
        let columns = [column("at", SqlDataType::Timestamp)];
        let mut assignments = vec![(
            "at".to_string(),
            SqlExpr::Literal(SqlValue::Int(1_583_402_400_000)),
        )];
        coerce_assignments_to_declared_types(&columns, &mut assignments, None)
            .expect("an integer assignment is epoch milliseconds");
        match &assignments[0].1 {
            SqlExpr::Literal(value) => assert_eq!(*value, SqlValue::Timestamp(early())),
            other => panic!("expected a literal assignment, got {other:?}"),
        }
    }

    /// The primary key keeps its literal exactly as written even when its
    /// declared type would otherwise re-type it: the engines derive a row's
    /// identity from the literal's own rendering on both the write and the
    /// read side, so coercing one side would make the row unfindable.
    #[test]
    fn the_primary_key_column_is_exempt() {
        let columns = [
            column("id", SqlDataType::Int64),
            column("n", SqlDataType::Int64),
        ];
        assert_eq!(
            coerced_with_exemption(&columns, "id", decimal("5.0"), Some("id"))
                .expect("an exempt key is never re-typed"),
            decimal("5.0")
        );
        // The exemption is by name and applies to that column only — a
        // non-key column of the same declared type still coerces.
        assert_eq!(
            coerced_with_exemption(&columns, "n", decimal("5.0"), Some("id"))
                .expect("5.0 is a whole number"),
            SqlValue::Int(5)
        );
    }

    /// `SET pk = <literal>` carries the same exemption, and a non-key
    /// assignment beside it is still coerced.
    #[test]
    fn assignments_honour_the_primary_key_exemption() {
        let columns = [
            column("id", SqlDataType::Float64),
            column("r", SqlDataType::Float64),
        ];
        let mut assignments = vec![
            ("id".to_string(), SqlExpr::Literal(decimal("1.5"))),
            ("r".to_string(), SqlExpr::Literal(decimal("1.5"))),
        ];
        coerce_assignments_to_declared_types(&columns, &mut assignments, Some("id"))
            .expect("both assignments are representable");
        let literal = |expr: &SqlExpr| match expr {
            SqlExpr::Literal(value) => value.clone(),
            other => panic!("expected a literal assignment, got {other:?}"),
        };
        assert_eq!(literal(&assignments[0].1), decimal("1.5"));
        assert_eq!(literal(&assignments[1].1), SqlValue::Float(1.5));
    }

    /// The public write-side entry coerces, then range-checks: a numeric
    /// literal on a TIMESTAMP column becomes the instant, text that spells
    /// no instant is refused naming the column, and an integer past the
    /// declared SMALLINT width is refused naming the column.
    #[test]
    fn write_literal_coerces_then_range_checks() {
        let at = column("at", SqlDataType::Timestamp);
        assert_eq!(
            coerce_write_literal(&at, SqlValue::Int(1_583_402_400_000))
                .expect("an integer is epoch milliseconds"),
            SqlValue::Timestamp(early())
        );
        let err = coerce_write_literal(&at, SqlValue::String("not a date".into()))
            .expect_err("text with no instant is refused");
        assert!(err.to_string().contains("'at'"), "{err}");

        let mut small = column("s", SqlDataType::Int64);
        small.int_width = Some(nodedb_types::columnar::IntWidth::I16);
        assert_eq!(
            coerce_write_literal(&small, SqlValue::Int(7)).expect("7 fits SMALLINT"),
            SqlValue::Int(7)
        );
        let err = coerce_write_literal(&small, SqlValue::Int(999_999))
            .expect_err("999999 does not fit SMALLINT");
        assert!(
            matches!(err, SqlError::IntegerOutOfRange { ref column, .. } if column == "s"),
            "{err}"
        );
    }

    /// The write-side entry keeps the primary key as written, like every
    /// `VALUES` and `SET` path.
    #[test]
    fn write_literal_exempts_the_primary_key() {
        let mut id = column("id", SqlDataType::Int64);
        id.is_primary_key = true;
        assert_eq!(
            coerce_write_literal(&id, decimal("5.0")).expect("an exempt key is never re-typed"),
            decimal("5.0")
        );
    }

    /// A column the catalog does not declare (the KV `key`/`value`
    /// convention, a `ttl` pseudo-column) is left exactly as written.
    #[test]
    fn undeclared_columns_are_left_alone() {
        let columns = [column("r", SqlDataType::Float64)];
        assert_eq!(
            coerced(&columns, "ttl", decimal("1.5")).expect("undeclared columns are untouched"),
            decimal("1.5")
        );
    }
}
