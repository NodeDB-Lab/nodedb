// SPDX-License-Identifier: Apache-2.0

//! Entry points and per-variant dispatch for AST expression conversion.

use sqlparser::ast::Expr;

use crate::error::{Result, SqlError};
use crate::resolver::ColumnScope;
use crate::resolver::expr::functions::convert_function_depth;
use crate::resolver::expr::int_width::check_at_int_width;
use crate::types::*;

use super::{builtins, identifier, literals, operators, predicates};

/// Maximum AST nesting depth accepted by `convert_expr`.
/// Exceeding this limit returns `Err` instead of overflowing the stack.
const MAX_CONVERT_DEPTH: usize = 128;

/// Convert a sqlparser `Expr` to our `SqlExpr`.
///
/// An integer operation whose PostgreSQL result type is `smallint` or
/// `integer` converts to a cast of itself to that type, so its result is
/// checked at that width.
pub fn convert_expr(expr: &Expr, scope: &ColumnScope<'_>) -> Result<SqlExpr> {
    convert_expr_depth(expr, &mut 0, scope)
}

/// Internal recursive helper that carries a depth counter to enforce
/// `MAX_CONVERT_DEPTH` and prevent stack overflow on malformed ASTs.
pub(in crate::resolver::expr) fn convert_expr_depth(
    expr: &Expr,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    *depth += 1;
    if *depth > MAX_CONVERT_DEPTH {
        return Err(SqlError::Unsupported {
            detail: format!("expression nesting depth exceeds maximum of {MAX_CONVERT_DEPTH}"),
        });
    }
    let result = convert_expr_inner(expr, depth, scope)
        .map(|converted| check_at_int_width(converted, scope));
    *depth -= 1;
    result
}

fn convert_expr_inner(expr: &Expr, depth: &mut usize, scope: &ColumnScope<'_>) -> Result<SqlExpr> {
    match expr {
        Expr::Identifier(ident) => identifier::convert_identifier(ident, scope),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            identifier::convert_compound_identifier(parts, scope)
        }
        Expr::Value(val) => literals::convert_value_expr(val),
        Expr::BinaryOp { left, op, right } => {
            operators::convert_binary(left, op, right, depth, scope)
        }
        Expr::UnaryOp { op, expr } => operators::convert_unary(op, expr, depth, scope),
        Expr::Function(func) => convert_function_depth(func, depth, scope),
        Expr::Nested(inner) => convert_expr_depth(inner, depth, scope),
        Expr::IsNull(inner) => predicates::convert_is_null(inner, false, depth, scope),
        Expr::IsNotNull(inner) => predicates::convert_is_null(inner, true, depth, scope),
        Expr::InList {
            expr,
            list,
            negated,
        } => predicates::convert_in_list(expr, list, *negated, depth, scope),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => predicates::convert_between(expr, low, high, *negated, depth, scope),
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => predicates::convert_like(expr, pattern, *negated, false, depth, scope),
        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => predicates::convert_like(expr, pattern, *negated, true, depth, scope),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => predicates::convert_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            depth,
            scope,
        ),
        Expr::TypedString(ts) => literals::convert_typed_string(ts),
        Expr::Cast {
            expr, data_type, ..
        } => literals::convert_cast(expr, data_type, depth, scope),
        Expr::Array(array) => literals::convert_array(array, depth, scope),
        Expr::Wildcard(_) => literals::convert_wildcard(),
        Expr::Trim { expr, .. } => builtins::convert_trim(expr, depth, scope),
        Expr::Ceil { expr, .. } => builtins::convert_ceil(expr, depth, scope),
        Expr::Floor { expr, .. } => builtins::convert_floor(expr, depth, scope),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => builtins::convert_substring(
            expr,
            substring_from.as_deref(),
            substring_for.as_deref(),
            depth,
            scope,
        ),
        Expr::Interval(interval) => literals::convert_interval(interval),
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => operators::convert_any_op(left, compare_op, right, depth, scope),
        _ => Err(SqlError::Unsupported {
            detail: format!("expression: {expr}"),
        }),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use sqlparser::ast::{Expr, SelectItem, Statement};

    use super::convert_expr;
    use crate::error::SqlError;
    use crate::parser::statement::parse_sql;
    use crate::resolver::ColumnScope;
    use crate::types::*;

    /// Extract the first SELECT item expression from a simple `SELECT <expr> FROM <tbl>`.
    pub(in crate::resolver::expr::convert) fn first_select_expr(sql: &str) -> Expr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        match &sel.projection[0] {
            SelectItem::UnnamedExpr(e) => e.clone(),
            SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            other => panic!("unexpected projection item: {other:?}"),
        }
    }

    /// Extract and convert the WHERE predicate from a simple
    /// `SELECT * FROM tbl WHERE <expr>` statement.
    pub(in crate::resolver::expr::convert) fn where_sql_expr(sql: &str) -> SqlExpr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = sel.selection.as_ref().expect("expected WHERE clause");
        convert_expr(raw, &ColumnScope::Unchecked).expect("convert_expr failed")
    }

    /// Parses `SELECT <expr> FROM t` and returns the lowered `SqlExpr` for `<expr>`.
    pub(in crate::resolver::expr::convert) fn select_expr_lowered(sql: &str) -> SqlExpr {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = &sel.projection[0];
        let raw_expr = match raw {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        };
        convert_expr(raw_expr, &ColumnScope::Unchecked).expect("convert_expr failed")
    }

    #[test]
    fn ts_rank_cd_is_unsupported() {
        use crate::parser::statement::parse_sql;
        let sql = "SELECT ts_rank_cd(body, to_tsquery('rust')) FROM t";
        let stmts = parse_sql(sql).expect("parse ok");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        let raw = match &sel.projection[0] {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection: {other:?}"),
        };
        let err = convert_expr(raw, &ColumnScope::Unchecked).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "ts_rank_cd should be Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("ts_rank_cd"),
            "error should mention ts_rank_cd: {msg}"
        );
    }

    #[test]
    fn to_tsquery_lowers_to_pg_to_tsquery() {
        let expr = select_expr_lowered("SELECT to_tsquery('rust & lang') FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_to_tsquery");
            }
            other => panic!("expected pg_to_tsquery Function, got {other:?}"),
        }
    }

    #[test]
    fn plainto_tsquery_lowers_correctly() {
        let expr = select_expr_lowered("SELECT plainto_tsquery('rust lang') FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_plainto_tsquery");
            }
            other => panic!("expected pg_plainto_tsquery, got {other:?}"),
        }
    }

    #[test]
    fn ts_rank_lowers_to_pg_ts_rank() {
        let expr = select_expr_lowered("SELECT ts_rank(body, to_tsquery('rust')) FROM t");
        match expr {
            SqlExpr::Function { ref name, .. } => {
                assert_eq!(name, "pg_ts_rank");
            }
            other => panic!("expected pg_ts_rank, got {other:?}"),
        }
    }
}
