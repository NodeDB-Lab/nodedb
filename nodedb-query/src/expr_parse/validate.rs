// SPDX-License-Identifier: Apache-2.0

//! Checks on a parsed expression: determinism, and the columns it reads.

use super::error::ExprParseError;
use crate::expr::SqlExpr;

const NON_DETERMINISTIC: &[&str] = &[
    "now",
    "current_timestamp",
    "random",
    "nextval",
    "uuid",
    "uuid_v4",
    "uuid_v7",
    "gen_random_uuid",
    "ulid",
    "cuid2",
    "nanoid",
];

pub(super) fn validate_deterministic(expr: &SqlExpr) -> Result<(), ExprParseError> {
    match expr {
        SqlExpr::Function { name, args } => {
            if NON_DETERMINISTIC.contains(&name.as_str()) {
                return Err(ExprParseError::InvalidLiteral {
                    detail: format!(
                        "non-deterministic function '{name}()' not allowed in GENERATED ALWAYS AS"
                    ),
                });
            }
            for arg in args {
                validate_deterministic(arg)?;
            }
            Ok(())
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            validate_deterministic(left)?;
            validate_deterministic(right)
        }
        SqlExpr::Negate(inner) => validate_deterministic(inner),
        SqlExpr::Coalesce(args) => {
            for arg in args {
                validate_deterministic(arg)?;
            }
            Ok(())
        }
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(op) = operand {
                validate_deterministic(op)?;
            }
            for (cond, then) in when_thens {
                validate_deterministic(cond)?;
                validate_deterministic(then)?;
            }
            if let Some(e) = else_expr {
                validate_deterministic(e)?;
            }
            Ok(())
        }
        SqlExpr::Cast { expr, .. } => validate_deterministic(expr),
        SqlExpr::NullIf(a, b) => {
            validate_deterministic(a)?;
            validate_deterministic(b)
        }
        SqlExpr::IsNull { expr, .. } => validate_deterministic(expr),
        SqlExpr::Column(_)
        | SqlExpr::Literal(_)
        | SqlExpr::OldColumn(_)
        | SqlExpr::ExcludedColumn(_) => Ok(()),
    }
}

/// The columns `expr` reads, each once, in first-seen order.
pub fn referenced_columns(expr: &SqlExpr) -> Vec<String> {
    let mut deps = Vec::new();
    collect_columns(expr, &mut deps);
    let mut seen = Vec::with_capacity(deps.len());
    for column in deps {
        if !seen.contains(&column) {
            seen.push(column);
        }
    }
    seen
}

pub(super) fn collect_columns(expr: &SqlExpr, deps: &mut Vec<String>) {
    match expr {
        SqlExpr::Column(name) => deps.push(name.clone()),
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, deps);
            collect_columns(right, deps);
        }
        SqlExpr::Negate(inner) => collect_columns(inner, deps),
        SqlExpr::Function { args, .. } => {
            for arg in args {
                collect_columns(arg, deps);
            }
        }
        SqlExpr::Coalesce(args) => {
            for arg in args {
                collect_columns(arg, deps);
            }
        }
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_columns(op, deps);
            }
            for (cond, then) in when_thens {
                collect_columns(cond, deps);
                collect_columns(then, deps);
            }
            if let Some(e) = else_expr {
                collect_columns(e, deps);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_columns(expr, deps),
        SqlExpr::NullIf(a, b) => {
            collect_columns(a, deps);
            collect_columns(b, deps);
        }
        SqlExpr::IsNull { expr, .. } => collect_columns(expr, deps),
        SqlExpr::Literal(_) | SqlExpr::OldColumn(_) | SqlExpr::ExcludedColumn(_) => {}
    }
}
