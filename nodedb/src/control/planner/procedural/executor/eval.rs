// SPDX-License-Identifier: BUSL-1.1

//! Expression evaluation helpers for the statement executor.
//!
//! Evaluates SQL expressions and converts results to Rust types (bool, i64,
//! nodedb_types::Value). Used by ASSIGN, IF/WHILE conditions, FOR bounds,
//! and OUT parameter capture.
//!
//! Uses sqlparser for expression parsing and a simple tree-walk evaluator
//! for constant expressions.

use crate::control::state::SharedState;
use crate::types::TenantId;

/// Signal from constant-expression evaluation that a `/` or `%` reached a
/// zero divisor.
///
/// Kept local to this module — the procedural executor's constant folder is
/// a separate, sqlparser-based tree-walk evaluator with no dependency on
/// `nodedb_query` (whose own `EvalError::DivisionByZero`, from the
/// DataFusion-backed row-expression evaluator, covers a different code path
/// entirely). Every other historically `None`-folding case (unknown
/// identifiers, unsupported operators, overflow, non-finite float results)
/// is unaffected and keeps folding to `Ok(None)` — see `try_eval_constant`,
/// `evaluate_to_value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum ConstEvalError {
    #[error("division by zero")]
    DivisionByZero,
}

impl From<ConstEvalError> for crate::Error {
    fn from(e: ConstEvalError) -> Self {
        match e {
            ConstEvalError::DivisionByZero => Self::DivisionByZero,
        }
    }
}

/// Evaluate a SQL boolean condition.
///
/// Fast path for constant TRUE/FALSE/1/0/NULL. Falls back to constant
/// expression evaluation for arithmetic/comparison.
pub async fn evaluate_condition(
    _state: &SharedState,
    _tenant_id: TenantId,
    condition_sql: &str,
) -> crate::Result<bool> {
    if let Some(result) = crate::control::trigger::try_eval_simple_condition(condition_sql) {
        return Ok(result);
    }

    match try_eval_constant(condition_sql)? {
        Some(nodedb_types::Value::Bool(b)) => Ok(b),
        Some(nodedb_types::Value::Integer(i)) => Ok(i != 0),
        Some(nodedb_types::Value::Float(f)) => Ok(f != 0.0),
        Some(_) => Ok(false),
        None => Err(crate::Error::PlanError {
            detail: format!("cannot evaluate condition: {condition_sql}"),
        }),
    }
}

/// Evaluate a SQL expression as an integer.
///
/// Fast path for literal integers. Falls back to constant evaluation.
pub async fn evaluate_int(
    _state: &SharedState,
    _tenant_id: TenantId,
    sql: &str,
) -> crate::Result<i64> {
    if let Ok(v) = sql.trim().parse::<i64>() {
        return Ok(v);
    }

    match try_eval_constant(sql)? {
        Some(nodedb_types::Value::Integer(i)) => Ok(i),
        Some(nodedb_types::Value::Float(f)) => Ok(f as i64),
        _ => Err(crate::Error::PlanError {
            detail: format!("could not evaluate '{sql}' as integer"),
        }),
    }
}

/// Evaluate a SQL expression and return the result as a `nodedb_types::Value`.
///
/// Fast path for literal values (NULL, TRUE, FALSE, integers, floats, strings).
/// Falls back to constant expression evaluation.
pub async fn evaluate_to_value(
    _state: &SharedState,
    _tenant_id: TenantId,
    expr: &str,
) -> crate::Result<nodedb_types::Value> {
    let trimmed = expr.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(nodedb_types::Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Ok(nodedb_types::Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Ok(nodedb_types::Value::Bool(false));
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return Ok(nodedb_types::Value::Integer(n));
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        return Ok(nodedb_types::Value::Float(n));
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(nodedb_types::Value::String(inner.replace("''", "'")));
    }

    match try_eval_constant(trimmed)? {
        Some(val) => Ok(val),
        None => Ok(nodedb_types::Value::Null),
    }
}

/// Try to evaluate a constant SQL expression without a query dispatch.
///
/// Handles basic arithmetic (+, -, *, /), comparisons, boolean logic,
/// and string concatenation on literal values.
///
/// Returns `Ok(None)` for anything this mini-evaluator doesn't handle
/// (unparseable input, unknown identifiers, unsupported operators,
/// overflow) — callers fold that to `Value::Null`, matching historical
/// behavior. Returns `Err(ConstEvalError::DivisionByZero)` only when
/// evaluation reaches a `/` or `%` with a zero divisor.
fn try_eval_constant(sql: &str) -> Result<Option<nodedb_types::Value>, ConstEvalError> {
    let trimmed = sql.trim();
    let expr_str = if trimmed.to_uppercase().starts_with("SELECT ") {
        trimmed[7..].trim()
    } else {
        trimmed
    };

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let select_sql = format!("SELECT {expr_str}");
    // reconstructed-sql: parser-only evaluates one constant-expression AST without dispatch
    let Ok(stmts) = sqlparser::parser::Parser::parse_sql(&dialect, &select_sql) else {
        return Ok(None);
    };
    let Some(stmt) = stmts.first() else {
        return Ok(None);
    };

    if let sqlparser::ast::Statement::Query(query) = stmt
        && let sqlparser::ast::SetExpr::Select(select) = &*query.body
        && let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) = select.projection.first()
    {
        return eval_expr(expr);
    }
    Ok(None)
}

/// Recursively evaluate a sqlparser expression tree.
fn eval_expr(expr: &sqlparser::ast::Expr) -> Result<Option<nodedb_types::Value>, ConstEvalError> {
    use sqlparser::ast::{Expr, UnaryOperator, Value};

    match expr {
        Expr::Value(v) => Ok(match &v.value {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(nodedb_types::Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(nodedb_types::Value::Float(f))
                } else {
                    None
                }
            }
            Value::SingleQuotedString(s) => Some(nodedb_types::Value::String(s.clone())),
            Value::Boolean(b) => Some(nodedb_types::Value::Bool(*b)),
            Value::Null => Some(nodedb_types::Value::Null),
            _ => None,
        }),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => Ok(match eval_expr(inner)? {
            Some(nodedb_types::Value::Integer(i)) => Some(nodedb_types::Value::Integer(-i)),
            Some(nodedb_types::Value::Float(f)) => Some(nodedb_types::Value::Float(-f)),
            _ => None,
        }),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(match eval_expr(inner)? {
            Some(nodedb_types::Value::Bool(b)) => Some(nodedb_types::Value::Bool(!b)),
            _ => None,
        }),
        Expr::BinaryOp { left, op, right } => match (eval_expr(left)?, eval_expr(right)?) {
            (Some(l), Some(r)) => eval_binary_op(&l, op, &r),
            _ => Ok(None),
        },
        Expr::Nested(inner) => eval_expr(inner),
        _ => Ok(None),
    }
}

fn eval_binary_op(
    l: &nodedb_types::Value,
    op: &sqlparser::ast::BinaryOperator,
    r: &nodedb_types::Value,
) -> Result<Option<nodedb_types::Value>, ConstEvalError> {
    use nodedb_types::Value;
    use sqlparser::ast::BinaryOperator::*;

    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            Plus => Ok(a.checked_add(*b).map(Value::Integer)),
            Minus => Ok(a.checked_sub(*b).map(Value::Integer)),
            Multiply => Ok(a.checked_mul(*b).map(Value::Integer)),
            Divide if *b != 0 => Ok(a.checked_div(*b).map(Value::Integer)),
            Divide => Err(ConstEvalError::DivisionByZero),
            Modulo if *b != 0 => Ok(a.checked_rem(*b).map(Value::Integer)),
            Modulo => Err(ConstEvalError::DivisionByZero),
            Gt => Ok(Some(Value::Bool(a > b))),
            GtEq => Ok(Some(Value::Bool(a >= b))),
            Lt => Ok(Some(Value::Bool(a < b))),
            LtEq => Ok(Some(Value::Bool(a <= b))),
            Eq => Ok(Some(Value::Bool(a == b))),
            NotEq => Ok(Some(Value::Bool(a != b))),
            _ => Ok(None),
        },
        (Value::Float(a), Value::Float(b)) => match op {
            Plus => Ok(Some(Value::Float(a + b))),
            Minus => Ok(Some(Value::Float(a - b))),
            Multiply => Ok(Some(Value::Float(a * b))),
            // Non-finite divisor (NaN/Infinity) is unsupported input, not a
            // zero-divisor — keeps folding to `None` as before.
            Divide if !b.is_finite() => Ok(None),
            Divide if *b == 0.0 => Err(ConstEvalError::DivisionByZero),
            Divide => {
                let result = a / b;
                if result.is_finite() {
                    Ok(Some(Value::Float(result)))
                } else {
                    Ok(None)
                }
            }
            Gt => Ok(Some(Value::Bool(a > b))),
            GtEq => Ok(Some(Value::Bool(a >= b))),
            Lt => Ok(Some(Value::Bool(a < b))),
            LtEq => Ok(Some(Value::Bool(a <= b))),
            Eq => Ok(Some(Value::Bool(a == b))),
            NotEq => Ok(Some(Value::Bool(a != b))),
            _ => Ok(None),
        },
        (Value::Integer(a), Value::Float(b)) => {
            eval_binary_op(&Value::Float(*a as f64), op, &Value::Float(*b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            eval_binary_op(&Value::Float(*a), op, &Value::Float(*b as f64))
        }
        (Value::Bool(a), Value::Bool(b)) => Ok(match op {
            And => Some(Value::Bool(*a && *b)),
            Or => Some(Value::Bool(*a || *b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        }),
        (Value::String(a), Value::String(b)) => Ok(match op {
            StringConcat => Some(Value::String(format!("{a}{b}"))),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        }),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Division/modulo by zero ─────────────────────────────────────────────

    #[test]
    fn integer_division_by_zero_signals_error() {
        assert_eq!(
            try_eval_constant("10 / 0"),
            Err(ConstEvalError::DivisionByZero)
        );
    }

    #[test]
    fn integer_modulo_by_zero_signals_error() {
        assert_eq!(
            try_eval_constant("10 % 0"),
            Err(ConstEvalError::DivisionByZero)
        );
    }

    #[test]
    fn float_division_by_zero_signals_error() {
        assert_eq!(
            try_eval_constant("10.0 / 0.0"),
            Err(ConstEvalError::DivisionByZero)
        );
    }

    /// Control: a non-zero divisor still computes normally.
    #[test]
    fn integer_division_by_nonzero_still_computes() {
        assert_eq!(
            try_eval_constant("10 / 2"),
            Ok(Some(nodedb_types::Value::Integer(5)))
        );
    }

    /// Control: an unrelated `None` case (unknown identifier) is unaffected
    /// by the division-by-zero signal and still folds to `Ok(None)`.
    #[test]
    fn unknown_identifier_still_folds_to_none() {
        assert_eq!(try_eval_constant("some_unbound_identifier"), Ok(None));
    }
}
