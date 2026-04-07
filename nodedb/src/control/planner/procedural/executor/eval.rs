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

    match try_eval_constant(condition_sql) {
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

    match try_eval_constant(sql) {
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

    match try_eval_constant(trimmed) {
        Some(val) => Ok(val),
        None => Ok(nodedb_types::Value::Null),
    }
}

/// Try to evaluate a constant SQL expression without a query dispatch.
///
/// Handles basic arithmetic (+, -, *, /), comparisons, boolean logic,
/// and string concatenation on literal values.
fn try_eval_constant(sql: &str) -> Option<nodedb_types::Value> {
    let trimmed = sql.trim();
    let expr_str = if trimmed.to_uppercase().starts_with("SELECT ") {
        trimmed[7..].trim()
    } else {
        trimmed
    };

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let select_sql = format!("SELECT {expr_str}");
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &select_sql).ok()?;
    let stmt = stmts.first()?;

    if let sqlparser::ast::Statement::Query(query) = stmt {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            if let Some(sqlparser::ast::SelectItem::UnnamedExpr(expr)) =
                select.projection.first()
            {
                return eval_expr(expr);
            }
        }
    }
    None
}

/// Recursively evaluate a sqlparser expression tree.
fn eval_expr(expr: &sqlparser::ast::Expr) -> Option<nodedb_types::Value> {
    use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(nodedb_types::Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(nodedb_types::Value::Float(f))
                } else {
                    None
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Some(nodedb_types::Value::String(s.clone()))
            }
            Value::Boolean(b) => Some(nodedb_types::Value::Bool(*b)),
            Value::Null => Some(nodedb_types::Value::Null),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match eval_expr(inner)? {
            nodedb_types::Value::Integer(i) => Some(nodedb_types::Value::Integer(-i)),
            nodedb_types::Value::Float(f) => Some(nodedb_types::Value::Float(-f)),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => match eval_expr(inner)? {
            nodedb_types::Value::Bool(b) => Some(nodedb_types::Value::Bool(!b)),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left)?;
            let r = eval_expr(right)?;
            eval_binary_op(&l, op, &r)
        }
        Expr::Nested(inner) => eval_expr(inner),
        _ => None,
    }
}

fn eval_binary_op(
    l: &nodedb_types::Value,
    op: &sqlparser::ast::BinaryOperator,
    r: &nodedb_types::Value,
) -> Option<nodedb_types::Value> {
    use nodedb_types::Value;
    use sqlparser::ast::BinaryOperator::*;

    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            Plus => Some(Value::Integer(a + b)),
            Minus => Some(Value::Integer(a - b)),
            Multiply => Some(Value::Integer(a * b)),
            Divide if *b != 0 => Some(Value::Integer(a / b)),
            Modulo if *b != 0 => Some(Value::Integer(a % b)),
            Gt => Some(Value::Bool(a > b)),
            GtEq => Some(Value::Bool(a >= b)),
            Lt => Some(Value::Bool(a < b)),
            LtEq => Some(Value::Bool(a <= b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        (Value::Float(a), Value::Float(b)) => match op {
            Plus => Some(Value::Float(a + b)),
            Minus => Some(Value::Float(a - b)),
            Multiply => Some(Value::Float(a * b)),
            Divide if *b != 0.0 => Some(Value::Float(a / b)),
            Gt => Some(Value::Bool(a > b)),
            GtEq => Some(Value::Bool(a >= b)),
            Lt => Some(Value::Bool(a < b)),
            LtEq => Some(Value::Bool(a <= b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        (Value::Integer(a), Value::Float(b)) => {
            eval_binary_op(&Value::Float(*a as f64), op, &Value::Float(*b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            eval_binary_op(&Value::Float(*a), op, &Value::Float(*b as f64))
        }
        (Value::Bool(a), Value::Bool(b)) => match op {
            And => Some(Value::Bool(*a && *b)),
            Or => Some(Value::Bool(*a || *b)),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        (Value::String(a), Value::String(b)) => match op {
            StringConcat => Some(Value::String(format!("{a}{b}"))),
            Eq => Some(Value::Bool(a == b)),
            NotEq => Some(Value::Bool(a != b)),
            _ => None,
        },
        _ => None,
    }
}
