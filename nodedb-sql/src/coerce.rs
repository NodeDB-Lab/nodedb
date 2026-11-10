// SPDX-License-Identifier: Apache-2.0

//! Postgres-semantic value coercion for planner use-sites.
//!
//! The planner matches `sqlparser::ast::Value` in numeric contexts
//! (LIMIT, OFFSET, FETCH, fusion weights, …). When a parameter was sent
//! over the pgwire Parse message with `Type::UNKNOWN` — the default for
//! drivers that don't pre-fetch OIDs, e.g. `postgres-js` with
//! `fetch_types: false` — our bind layer emits it as
//! `Value::SingleQuotedString` (we have no type information to do
//! otherwise at bind time, and a guess-and-coerce approach would
//! silently corrupt string parameters bound into string columns).
//!
//! Postgres' model: UNKNOWN literals stay uncoerced until the planner
//! has context, and the planner then resolves them by the surrounding
//! operator / column type. These helpers are the single chokepoint
//! implementing that resolution for numeric contexts. Any future
//! numeric use-site must route through here — a raw
//! `match Value::Number` ignores UNKNOWN-coerced literals and
//! re-introduces the silent match-failure bug class.
//!
//! Row-bound extraction (LIMIT / OFFSET / FETCH) returns
//! `Result<RowBound, _>`. `RowBound` has two states: `Rows(n)` for a
//! literal in `[0, usize::MAX]`, and `Unbounded` for a `NULL` argument
//! — PostgreSQL treats `NULL` as no bound at all, the same as an absent
//! clause. `LIMIT ALL` is PostgreSQL's other unbounded spelling, but
//! `sqlparser` parses it as an absent LIMIT expression, so it never
//! reaches `checked_row_bound` at all — callers see it as `None` before
//! this module runs. A malformed literal (negative, fractional,
//! non-numeric, overflowing `usize`) is a third case: the statement
//! fails. `checked_row_bound` is the chokepoint every
//! LIMIT/OFFSET/FETCH site calls.

use sqlparser::ast;

/// A row bound the planner resolved.
#[derive(Debug, PartialEq)]
pub enum RowBound {
    /// A literal count the planner applies.
    Rows(usize),
    /// `NULL`. PostgreSQL reads it as no bound at all, the same as an
    /// absent clause or `LIMIT ALL` (which never reaches this type —
    /// see the module doc).
    Unbounded,
}

impl RowBound {
    /// The LIMIT this bound names. `Rows(n)` maps to `Some(n)`.
    /// `Unbounded` maps to `None` — no limit applies.
    pub fn limit(self) -> Option<usize> {
        match self {
            RowBound::Rows(n) => Some(n),
            RowBound::Unbounded => None,
        }
    }

    /// The OFFSET this bound names. `Rows(n)` maps to `n`.
    /// `Unbounded` maps to `0` — no rows skip.
    pub fn offset(self) -> usize {
        match self {
            RowBound::Rows(n) => n,
            RowBound::Unbounded => 0,
        }
    }
}

/// A row-bound literal that did not resolve to `[0, usize::MAX]`.
///
/// Carries the value as written so the caller names it without
/// re-deriving text from the AST.
#[derive(Debug, PartialEq)]
pub struct InvalidRowBoundLiteral {
    pub raw: String,
}

/// Resolve a `Value` into a `usize` row bound.
///
/// Accepts:
/// - `Value::Number(n, _)` — the typed-parameter and explicit-literal path.
/// - `Value::SingleQuotedString(s)` where `s` parses as `usize` — the
///   UNKNOWN-param bind path (pgwire drivers that send `Type::UNKNOWN`).
///
/// `Value::DoubleQuotedString` is NOT accepted: with the PostgreSQL dialect
/// double-quoted tokens parse as `Expr::Identifier`, never as
/// `Expr::Value(Value::DoubleQuotedString)`, so that variant is unreachable
/// in practice and routing it here would silently accept non-numeric text.
///
/// # Bounds
///
/// Valid outputs are `[0, usize::MAX]` (64-bit on typical targets).
/// A negative number, a fractional value, a value exceeding `usize::MAX`,
/// or non-numeric text returns `Err(InvalidRowBoundLiteral)` carrying the
/// value as written.
///
/// Does not perform saturating or wrapping coercion — values that
/// overflow `usize` are rejected, not silently truncated.
pub fn as_usize_literal(value: &ast::Value) -> Result<usize, InvalidRowBoundLiteral> {
    match value {
        ast::Value::Number(n, _) => n
            .parse::<usize>()
            .map_err(|_| InvalidRowBoundLiteral { raw: n.clone() }),
        ast::Value::SingleQuotedString(s) => s
            .parse::<usize>()
            .map_err(|_| InvalidRowBoundLiteral { raw: s.clone() }),
        other => Err(InvalidRowBoundLiteral {
            raw: other.to_string(),
        }),
    }
}

/// Resolve an `Expr::Value` into a `usize` row bound. Thin wrapper that
/// unpacks the `Expr` → `Value` layer so callers reading LIMIT/OFFSET/FETCH
/// clauses don't each re-write the unpack. A non-`Expr::Value` expression
/// (a column reference, a function call, an arithmetic expression) is not
/// a literal the planner can read — it fails with the expression's
/// `Display` text as `raw`.
///
/// A whole `NUMERIC` parameter binds as the typed literal `NUMERIC '10'`,
/// so that form reads as its digits.
pub fn expr_as_usize_literal(expr: &ast::Expr) -> Result<usize, InvalidRowBoundLiteral> {
    match expr {
        ast::Expr::Value(v) => as_usize_literal(&v.value),
        ast::Expr::TypedString(ts)
            if matches!(
                ts.data_type,
                ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_)
            ) =>
        {
            as_usize_literal(&ts.value.value)
        }
        other => Err(InvalidRowBoundLiteral {
            raw: other.to_string(),
        }),
    }
}

/// Resolve a LIMIT/OFFSET/FETCH bound. Names `clause` and the value as
/// written on failure. Every row-bound site routes through here.
///
/// `Expr::Value(Value::Null)` resolves to `RowBound::Unbounded` —
/// PostgreSQL treats a `NULL` LIMIT/OFFSET/FETCH argument as no bound
/// at all. A prepared `LIMIT $1` reaches this shape at Parse-time
/// schema inference, before the real bound is bound at Execute time.
pub fn checked_row_bound(clause: &'static str, expr: &ast::Expr) -> crate::error::Result<RowBound> {
    if matches!(expr, ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)) {
        return Ok(RowBound::Unbounded);
    }
    expr_as_usize_literal(expr)
        .map(RowBound::Rows)
        .map_err(|e| crate::error::SqlError::InvalidLimitValue {
            clause,
            value: e.raw,
        })
}

/// Resolve a `Value` into an `f64` if numeric-shaped.
///
/// Same UNKNOWN-coercion behavior as `as_usize_literal` but for
/// floating-point contexts (fusion weights, scoring thresholds,
/// confidence intervals).
///
/// # Bounds
///
/// `f64::from_str` accepts `NaN`, `inf`, subnormals, and values that
/// overflow to `±inf` via the IEEE-754 rules. Callers that need to
/// reject those (e.g. fusion weights outside `[0, 1]`) must validate
/// the returned value themselves — this helper is purely a literal
/// extractor, not a domain validator.
pub fn as_f64_literal(value: &ast::Value) -> Option<f64> {
    match value {
        ast::Value::Number(n, _) => n.parse::<f64>().ok(),
        ast::Value::SingleQuotedString(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usize_from_a_whole_numeric_param() {
        let expr = ast::Expr::TypedString(ast::TypedString {
            data_type: ast::DataType::Numeric(ast::ExactNumberInfo::None),
            value: ast::ValueWithSpan {
                value: ast::Value::SingleQuotedString("10".into()),
                span: sqlparser::tokenizer::Span::empty(),
            },
            uses_odbc_syntax: false,
        });
        assert_eq!(expr_as_usize_literal(&expr), Ok(10));
    }

    #[test]
    fn usize_from_number() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("42".into(), false)),
            Ok(42)
        );
    }

    /// Untyped pgwire param: `Type::UNKNOWN` → `ParamValue::Text` →
    /// `Value::SingleQuotedString`. LIMIT still has to work.
    #[test]
    fn usize_from_unknown_param_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("42".into())),
            Ok(42)
        );
    }

    #[test]
    fn usize_rejects_non_numeric_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("abc".into())),
            Err(InvalidRowBoundLiteral { raw: "abc".into() })
        );
    }

    #[test]
    fn usize_rejects_negative() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("-1".into())),
            Err(InvalidRowBoundLiteral { raw: "-1".into() })
        );
    }

    #[test]
    fn f64_from_unknown_param_text() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("1.5".into())),
            Some(1.5)
        );
    }

    // ── bounds / overflow ──────────────────────────────────────────

    /// Values larger than `usize::MAX` are rejected, not wrapped or
    /// truncated. Silent truncation would reproduce the pattern of
    /// "untyped param drops silently" — the exact bug class this
    /// module exists to close.
    #[test]
    fn usize_rejects_overflow_number() {
        let huge = format!("{}0", usize::MAX);
        assert_eq!(
            as_usize_literal(&ast::Value::Number(huge.clone(), false)),
            Err(InvalidRowBoundLiteral { raw: huge })
        );
    }

    #[test]
    fn usize_rejects_overflow_text() {
        let huge = format!("{}0", usize::MAX);
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString(huge.clone())),
            Err(InvalidRowBoundLiteral { raw: huge })
        );
    }

    #[test]
    fn usize_rejects_fractional_number() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("1.5".into(), false)),
            Err(InvalidRowBoundLiteral { raw: "1.5".into() })
        );
    }

    #[test]
    fn usize_rejects_fractional_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("1.5".into())),
            Err(InvalidRowBoundLiteral { raw: "1.5".into() })
        );
    }

    #[test]
    fn usize_rejects_scientific_notation() {
        // `1e3` is not a usize literal — Postgres treats it as a float.
        assert_eq!(
            as_usize_literal(&ast::Value::Number("1e3".into(), false)),
            Err(InvalidRowBoundLiteral { raw: "1e3".into() })
        );
    }

    #[test]
    fn usize_accepts_zero() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("0".into(), false)),
            Ok(0)
        );
    }

    #[test]
    fn usize_accepts_max() {
        let max_str = usize::MAX.to_string();
        assert_eq!(
            as_usize_literal(&ast::Value::Number(max_str, false)),
            Ok(usize::MAX)
        );
    }

    #[test]
    fn f64_accepts_negative() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("-1.5".into())),
            Some(-1.5)
        );
    }

    #[test]
    fn f64_overflow_produces_infinity() {
        // IEEE-754 semantics — documented contract. Callers that
        // can't tolerate `inf` must validate.
        let out = as_f64_literal(&ast::Value::Number("1e400".into(), false));
        assert!(matches!(out, Some(f) if f.is_infinite()));
    }

    #[test]
    fn f64_rejects_non_numeric_text() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("foo".into())),
            None
        );
    }
}
