// SPDX-License-Identifier: Apache-2.0

//! Binary, unary, and `ANY` operator conversion.

use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

use crate::error::{Result, SqlError};
use crate::resolver::ColumnScope;
use crate::resolver::expr::binary_ops::{convert_binary_op, convert_unary_op};
use crate::resolver::expr::int_width::{int_width_of, interval_operand_as_bigint};
use crate::types::*;

use super::entry::convert_expr_depth;

pub(super) fn convert_binary(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    // JSON and FTS operators are lowered to function calls before the
    // generic binary-op path so they are never passed to
    // convert_binary_op.
    let json_fn: Option<&str> = match op {
        BinaryOperator::Arrow => Some("pg_json_get"),
        BinaryOperator::LongArrow => Some("pg_json_get_text"),
        BinaryOperator::HashArrow => Some("pg_json_path_get"),
        BinaryOperator::HashLongArrow => Some("pg_json_path_get_text"),
        BinaryOperator::AtArrow => Some("pg_json_contains"),
        BinaryOperator::ArrowAt => Some("pg_json_contained_by"),
        BinaryOperator::Question => Some("pg_json_has_key"),
        BinaryOperator::QuestionAnd => Some("pg_json_has_all_keys"),
        BinaryOperator::QuestionPipe => Some("pg_json_has_any_key"),
        _ => None,
    };
    if let Some(name) = json_fn {
        return Ok(SqlExpr::Function {
            name: name.into(),
            args: vec![
                convert_expr_depth(left, depth, scope)?,
                convert_expr_depth(right, depth, scope)?,
            ],
            distinct: false,
        });
    }
    // `col @@ query` → pg_fts_match(col, query)
    if matches!(op, BinaryOperator::AtAt) {
        let col_expr = convert_expr_depth(left, depth, scope)?;
        let query_expr = convert_expr_depth(right, depth, scope)?;
        return Ok(crate::functions::fts_ops::pg_fts_funcs::lower_pg_fts_match(
            col_expr, query_expr,
        ));
    }
    let left_expr = convert_expr_depth(left, depth, scope)?;
    let op = convert_binary_op(op)?;
    let right_expr = convert_expr_depth(right, depth, scope)?;
    // An interval with an integer is `bigint` arithmetic. An interval
    // beside an instant or any other non-integer keeps its literal.
    let (left_expr, right_expr) = if is_arithmetic(op) {
        let left_is_int = int_width_of(&left_expr, scope).is_some();
        let right_is_int = int_width_of(&right_expr, scope).is_some();
        (
            if right_is_int {
                interval_operand_as_bigint(left, left_expr)
            } else {
                left_expr
            },
            if left_is_int {
                interval_operand_as_bigint(right, right_expr)
            } else {
                right_expr
            },
        )
    } else {
        (left_expr, right_expr)
    };
    Ok(SqlExpr::BinaryOp {
        left: Box::new(left_expr),
        op,
        right: Box::new(right_expr),
    })
}

fn is_arithmetic(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
    )
}

/// A negative integer literal reaches sqlparser as unary minus applied
/// to a *positive* number, so the most negative `BIGINT` arrives as
/// `-(9223372036854775808)` — and that operand does not fit an `i64`.
/// Converting the operand on its own therefore falls back to `Float`
/// and silently turns an exact integer into an approximate one. Folding
/// the sign into the literal before parsing keeps the whole `i64` range
/// exact; anything that still does not fit takes the general path.
pub(super) fn convert_unary(
    op: &UnaryOperator,
    inner: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    if matches!(op, UnaryOperator::Minus)
        && let Expr::Value(v) = inner
        && let Value::Number(n, _) = &v.value
    {
        return match format!("-{n}").parse::<i64>() {
            Ok(i) => Ok(SqlExpr::Literal(SqlValue::Int(i))),
            Err(_) => Ok(SqlExpr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(convert_expr_depth(inner, depth, scope)?),
            }),
        };
    }
    let op = convert_unary_op(op)?;
    let converted = convert_expr_depth(inner, depth, scope)?;
    let converted = match op {
        UnaryOp::Neg => interval_operand_as_bigint(inner, converted),
        UnaryOp::Not => {
            if let Some(type_name) = numeric_type_name(&converted, scope) {
                return Err(SqlError::DatatypeMismatch {
                    detail: format!("argument of NOT must be type boolean, not type {type_name}"),
                });
            }
            converted
        }
    };
    Ok(SqlExpr::UnaryOp {
        op,
        expr: Box::new(converted),
    })
}

/// The SQL type name of `expr` when it is a number at plan time: a numeric
/// literal, a numeric cast, a declared numeric column, or a negation or
/// arithmetic over one. `None` when the type is boolean, NULL, or not known
/// before execution.
fn numeric_type_name(expr: &SqlExpr, scope: &ColumnScope<'_>) -> Option<&'static str> {
    if let Some(width) = int_width_of(expr, scope) {
        return Some(width.type_name());
    }
    match expr {
        SqlExpr::Literal(SqlValue::Float(_)) => Some("double precision"),
        SqlExpr::Literal(SqlValue::Decimal(_)) => Some("numeric"),
        SqlExpr::Cast { to_type, .. } => match nodedb_query::CastType::from_type_name(to_type)? {
            nodedb_query::CastType::Float => Some("double precision"),
            nodedb_query::CastType::Decimal(_) => Some("numeric"),
            _ => None,
        },
        SqlExpr::Column { table, name } => {
            match scope.declared_column(table.as_deref(), name)?.data_type {
                SqlDataType::Float64 => Some("double precision"),
                SqlDataType::Decimal(_) => Some("numeric"),
                _ => None,
            }
        }
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => numeric_type_name(expr, scope),
        SqlExpr::BinaryOp { left, op, right } if is_arithmetic(*op) => {
            numeric_type_name(left, scope).or_else(|| numeric_type_name(right, scope))
        }
        _ => None,
    }
}

/// `left = ANY(right)` — desugar into InList over array elements.
/// When `right` resolves to an ArrayLiteral (or a function call that
/// the bridge/evaluator will fold to an array), emit InList so the
/// downstream scan filter path handles it natively.
pub(super) fn convert_any_op(
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    // Only support `=` comparison for now; reject other operators
    // with a clear, non-AST-leaking message.
    if !matches!(compare_op, BinaryOperator::Eq) {
        return Err(SqlError::Unsupported {
            detail: "ANY operator with non-equality comparison is not supported".into(),
        });
    }
    let left_expr = convert_expr_depth(left, depth, scope)?;
    let right_expr = convert_expr_depth(right, depth, scope)?;
    // Expand the right-hand side into a list if it is an array literal;
    // otherwise wrap as a single-element list so InList still evaluates.
    let list = match right_expr {
        SqlExpr::ArrayLiteral(elems) => elems,
        other => vec![other],
    };
    Ok(SqlExpr::InList {
        expr: Box::new(left_expr),
        list,
        negated: false,
    })
}

#[cfg(test)]
mod tests {
    use crate::resolver::expr::convert::entry::tests::{select_expr_lowered, where_sql_expr};
    use crate::types::*;

    /// `"col" = 'literal'` — double-quoted identifier on the left, single-quoted
    /// string literal on the right — must lower to `BinaryOp(Column("col"), Eq,
    /// Literal(String("literal")))`.  This is the canonical mixed-quotation form
    /// used in WHERE clauses (e.g. WHERE "userId" = 'alice').
    #[test]
    fn double_quoted_col_eq_single_quoted_literal() {
        let expr = where_sql_expr(r#"SELECT * FROM t WHERE "col" = 'literal'"#);
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                assert!(
                    matches!(*left, SqlExpr::Column { ref name, .. } if name == "col"),
                    "left should be Column(col), got {left:?}"
                );
                assert!(
                    matches!(*right, SqlExpr::Literal(SqlValue::String(ref s)) if s == "literal"),
                    "right should be Literal(String(\"literal\")), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    /// `"colA" = "colB"` — both sides are double-quoted identifiers; both must
    /// resolve as column references, not string literals.
    #[test]
    fn double_quoted_col_eq_double_quoted_col() {
        let expr = where_sql_expr(r#"SELECT * FROM t WHERE "colA" = "colB""#);
        match expr {
            SqlExpr::BinaryOp { left, right, .. } => {
                assert!(
                    matches!(*left, SqlExpr::Column { ref name, .. } if name == "colA"),
                    "left should be Column(colA), got {left:?}"
                );
                assert!(
                    matches!(*right, SqlExpr::Column { ref name, .. } if name == "colB"),
                    "right should be Column(colB), got {right:?}"
                );
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    fn convert_select(sql: &str) -> crate::error::Result<SqlExpr> {
        let raw = crate::resolver::expr::convert::entry::tests::first_select_expr(sql);
        crate::resolver::expr::convert::entry::convert_expr(
            &raw,
            &crate::resolver::ColumnScope::Unchecked,
        )
    }

    #[test]
    fn not_refuses_a_numeric_operand() {
        for (sql, type_name) in [
            ("SELECT NOT 5", "integer"),
            ("SELECT NOT -5", "integer"),
            ("SELECT NOT 5000000000", "bigint"),
            ("SELECT NOT 1.5", "numeric"),
            ("SELECT NOT (1 + 2)", "integer"),
        ] {
            match convert_select(sql) {
                Err(crate::error::SqlError::DatatypeMismatch { detail }) => assert_eq!(
                    detail,
                    format!("argument of NOT must be type boolean, not type {type_name}"),
                    "{sql}"
                ),
                other => panic!("{sql}: expected a datatype mismatch, got {other:?}"),
            }
        }
    }

    #[test]
    fn not_keeps_boolean_null_and_unknown_operands() {
        for sql in [
            "SELECT NOT true",
            "SELECT NOT false",
            "SELECT NOT NULL",
            "SELECT NOT (x = 1)",
            "SELECT NOT x",
        ] {
            assert!(
                matches!(
                    convert_select(sql),
                    Ok(SqlExpr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    })
                ),
                "{sql}"
            );
        }
        assert!(matches!(
            convert_select("SELECT -x"),
            Ok(SqlExpr::UnaryOp {
                op: UnaryOp::Neg,
                ..
            })
        ));
    }

    // ── JSON operator lowering tests ───────────────────────────────────────

    fn assert_json_fn(sql: &str, expected_fn: &str) {
        let expr = select_expr_lowered(sql);
        match expr {
            SqlExpr::Function { name, args, .. } => {
                assert_eq!(name, expected_fn, "wrong function name");
                assert_eq!(args.len(), 2, "expected 2 args");
            }
            other => panic!("expected Function, got {other:?}"),
        }
    }

    #[test]
    fn arrow_lowers_to_pg_json_get() {
        assert_json_fn("SELECT data->'key' FROM t", "pg_json_get");
    }

    #[test]
    fn long_arrow_lowers_to_pg_json_get_text() {
        assert_json_fn("SELECT data->>'key' FROM t", "pg_json_get_text");
    }

    #[test]
    fn hash_arrow_lowers_to_pg_json_path_get() {
        assert_json_fn("SELECT data#>'{a,b}' FROM t", "pg_json_path_get");
    }

    #[test]
    fn hash_long_arrow_lowers_to_pg_json_path_get_text() {
        assert_json_fn("SELECT data#>>'{a,b}' FROM t", "pg_json_path_get_text");
    }

    #[test]
    fn at_arrow_lowers_to_pg_json_contains() {
        assert_json_fn("SELECT data @> '{\"a\":1}' FROM t", "pg_json_contains");
    }

    #[test]
    fn arrow_at_lowers_to_pg_json_contained_by() {
        assert_json_fn("SELECT '{\"a\":1}' <@ data FROM t", "pg_json_contained_by");
    }

    #[test]
    fn question_lowers_to_pg_json_has_key() {
        assert_json_fn("SELECT data ? 'key' FROM t", "pg_json_has_key");
    }

    #[test]
    fn question_and_lowers_to_pg_json_has_all_keys() {
        assert_json_fn(
            "SELECT data ?& ARRAY['a','b'] FROM t",
            "pg_json_has_all_keys",
        );
    }

    #[test]
    fn question_pipe_lowers_to_pg_json_has_any_key() {
        assert_json_fn(
            "SELECT data ?| ARRAY['a','b'] FROM t",
            "pg_json_has_any_key",
        );
    }

    #[test]
    fn chained_arrow_lowers_nested() {
        // data->'a'->'b' → pg_json_get(pg_json_get(data, 'a'), 'b')
        let expr = select_expr_lowered("SELECT data->'a'->'b' FROM t");
        match expr {
            SqlExpr::Function { name, ref args, .. } => {
                assert_eq!(name, "pg_json_get", "outer fn should be pg_json_get");
                match &args[0] {
                    SqlExpr::Function {
                        name: inner_name, ..
                    } => {
                        assert_eq!(inner_name, "pg_json_get", "inner fn should be pg_json_get");
                    }
                    other => panic!("expected inner pg_json_get, got {other:?}"),
                }
            }
            other => panic!("expected outer pg_json_get, got {other:?}"),
        }
    }

    // ── FTS operator / function lowering tests ────────────────────────────────

    fn where_fn(sql: &str) -> SqlExpr {
        where_sql_expr(sql)
    }

    #[test]
    fn at_at_lowers_to_pg_fts_match() {
        // col @@ to_tsquery('rust & lang') → pg_fts_match(col, pg_to_tsquery('rust & lang'))
        let expr = where_fn("SELECT * FROM t WHERE body @@ to_tsquery('rust & lang')");
        match expr {
            SqlExpr::Function {
                ref name, ref args, ..
            } => {
                assert_eq!(
                    name, "pg_fts_match",
                    "operator @@ should lower to pg_fts_match"
                );
                assert_eq!(args.len(), 2, "expected 2 args");
                match &args[1] {
                    SqlExpr::Function { name: inner, .. } => {
                        assert_eq!(inner, "pg_to_tsquery");
                    }
                    other => panic!("expected pg_to_tsquery as right arg, got {other:?}"),
                }
            }
            other => panic!("expected pg_fts_match Function, got {other:?}"),
        }
    }

    #[test]
    fn at_at_with_plainto_tsquery() {
        // col @@ plainto_tsquery('rust lang') → pg_fts_match(col, pg_plainto_tsquery(...))
        let expr = where_fn("SELECT * FROM t WHERE body @@ plainto_tsquery('rust lang')");
        match expr {
            SqlExpr::Function {
                ref name, ref args, ..
            } => {
                assert_eq!(name, "pg_fts_match");
                match &args[1] {
                    SqlExpr::Function { name: inner, .. } => {
                        assert_eq!(inner, "pg_plainto_tsquery");
                    }
                    other => panic!("expected pg_plainto_tsquery, got {other:?}"),
                }
            }
            other => panic!("expected pg_fts_match, got {other:?}"),
        }
    }
}
