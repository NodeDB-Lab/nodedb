// SPDX-License-Identifier: Apache-2.0

//! Literal, cast, array, and interval conversion.

use sqlparser::ast::{Array, DataType, Expr, Interval, TypedString, Value, ValueWithSpan};

use crate::error::{Result, SqlError};
use crate::resolver::ColumnScope;
use crate::resolver::expr::value::{convert_value, parse_interval_to_micros};
use crate::types::*;

use super::entry::convert_expr_depth;
use super::typed_literal::TypedKind;

pub(super) fn convert_value_expr(val: &ValueWithSpan) -> Result<SqlExpr> {
    Ok(SqlExpr::Literal(convert_value(&val.value)?))
}

/// `TYPE '...'`. A timestamp, UUID, bytea or decimal type resolves to its
/// typed value, refusing text that spells none. Any other type keeps the
/// text.
pub(super) fn convert_typed_string(ts: &TypedString) -> Result<SqlExpr> {
    let raw = match &ts.value.value {
        Value::SingleQuotedString(s) => s.clone(),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!("typed string value: {other}"),
            });
        }
    };
    match TypedKind::of(&ts.data_type)? {
        Some(kind) => Ok(SqlExpr::Literal(kind.parse_text(&raw)?)),
        None => Ok(SqlExpr::Literal(SqlValue::String(raw))),
    }
}

pub(super) fn convert_cast(
    expr: &Expr,
    data_type: &DataType,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    // `::tsvector` and `::tsquery` casts are PG surface notation; the
    // inner expression is the actual text value.  Elide the cast and
    // return the inner expression directly — no runtime type change is
    // needed since we operate on plain strings internally.
    let type_str = format!("{data_type}").to_ascii_lowercase();
    if type_str == "tsvector" || type_str == "tsquery" {
        return convert_expr_depth(expr, depth, scope);
    }
    let inner = convert_expr_depth(expr, depth, scope)?;
    // Resolved for every cast, so a `DECIMAL(p,s)` typmod that is not valid
    // is refused here, never at run time.
    let kind = TypedKind::of(data_type)?;
    // A literal cast to a timestamp, UUID, bytea or decimal type resolves to
    // the typed value here, so `'...'::uuid` stores a UUID, never its text,
    // and `12.50::decimal` the exact decimal, never a float.
    if let SqlExpr::Literal(value) = &inner
        && let Some(kind) = kind
        && let Some(typed) = kind.cast(value.clone())?
    {
        return Ok(SqlExpr::Literal(typed));
    }
    Ok(SqlExpr::Cast {
        expr: Box::new(inner),
        to_type: format!("{data_type}"),
    })
}

pub(super) fn convert_array(
    array: &Array,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    let elems = array
        .elem
        .iter()
        .map(|e| convert_expr_depth(e, depth, scope))
        .collect::<Result<_>>()?;
    Ok(SqlExpr::ArrayLiteral(elems))
}

pub(super) fn convert_wildcard() -> Result<SqlExpr> {
    Ok(SqlExpr::Wildcard)
}

pub(super) fn convert_interval(interval: &Interval) -> Result<SqlExpr> {
    // INTERVAL '1 hour' → microseconds as i64 literal.
    // The interval value is typically a string literal.
    let interval_str = match interval.value.as_ref() {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => s.clone(),
            Value::Number(n, _) => {
                // INTERVAL 5 HOUR → combine number with leading_field.
                if let Some(ref field) = interval.leading_field {
                    format!("{n} {field}")
                } else {
                    n.clone()
                }
            }
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!("INTERVAL value: {}", interval.value),
                });
            }
        },
        _ => {
            return Err(SqlError::Unsupported {
                detail: format!("INTERVAL expression: {}", interval.value),
            });
        }
    };

    // If leading_field is specified, append it: INTERVAL '5' HOUR → "5 HOUR"
    let full_str = if interval_str.chars().all(|c| c.is_ascii_digit())
        && let Some(ref field) = interval.leading_field
    {
        format!("{interval_str} {field}")
    } else {
        interval_str
    };

    let micros = parse_interval_to_micros(&full_str).ok_or_else(|| SqlError::Parse {
        detail: format!("cannot parse INTERVAL '{full_str}'"),
    })?;

    Ok(SqlExpr::Literal(SqlValue::Int(micros)))
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Expr, Value};

    use crate::error::SqlError;
    use crate::resolver::expr::convert::entry::tests::{first_select_expr, select_expr_lowered};
    use crate::resolver::expr::value::convert_value;
    use crate::types::*;

    /// `'userId'` is a single-quoted string literal.
    #[test]
    fn single_quoted_is_string_literal() {
        let expr = first_select_expr("SELECT 'userId' FROM users");
        match &expr {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) => assert_eq!(s, "userId"),
                other => panic!("expected SingleQuotedString, got {other:?}"),
            },
            other => panic!("expected Value, got {other:?}"),
        }
        // And convert_value maps it to SqlValue::String.
        let Expr::Value(v) = expr else { unreachable!() };
        assert!(matches!(
            convert_value(&v.value),
            Ok(SqlValue::String(s)) if s == "userId"
        ));
    }

    /// `Value::DoubleQuotedString` (non-Postgres dialect) falls through
    /// `convert_value` to `SqlError::Unsupported`. With PostgreSQL dialect
    /// this variant is never produced, but constructing it directly verifies
    /// the arm is absent and not silently accepted.
    #[test]
    fn double_quoted_string_value_unsupported() {
        // Construct the variant directly — it cannot be produced by parsing
        // with PostgreSqlDialect, which is exactly why the arm is dead code.
        let val = Value::DoubleQuotedString("userId".into());
        assert!(
            matches!(convert_value(&val), Err(SqlError::Unsupported { .. })),
            "DoubleQuotedString should be Unsupported, not silently accepted"
        );
    }

    #[test]
    fn tsvector_cast_elided() {
        // 'foo'::tsvector → Literal("foo")
        let expr = select_expr_lowered("SELECT 'foo'::tsvector FROM t");
        assert!(
            matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "foo"),
            "expected Literal(\"foo\"), got {expr:?}"
        );
    }

    #[test]
    fn tsquery_cast_elided() {
        // 'rust'::tsquery → Literal("rust")
        let expr = select_expr_lowered("SELECT 'rust'::tsquery FROM t");
        assert!(
            matches!(expr, SqlExpr::Literal(SqlValue::String(ref s)) if s == "rust"),
            "expected Literal(\"rust\"), got {expr:?}"
        );
    }
}
