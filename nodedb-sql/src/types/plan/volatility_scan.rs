// SPDX-License-Identifier: Apache-2.0

//! Volatility scanning over plan expressions and column DEFAULT strings.
//!
//! A plan holding a volatile call must re-plan per execution, so the plan
//! cache never replays a frozen value.

use crate::planner::const_fold::default_registry;
use crate::types_expr::SqlExpr;

/// Whether an expression tree contains a call to a `Volatile` function.
pub fn expr_is_volatile(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Function { name, args, .. } => {
            default_registry().is_volatile(name) || args.iter().any(expr_is_volatile)
        }
        SqlExpr::BinaryOp { left, right, .. } => expr_is_volatile(left) || expr_is_volatile(right),
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. } => expr_is_volatile(expr),
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_is_volatile)
                || when_then
                    .iter()
                    .any(|(when, then)| expr_is_volatile(when) || expr_is_volatile(then))
                || else_expr.as_deref().is_some_and(expr_is_volatile)
        }
        SqlExpr::InList { expr, list, .. } => {
            expr_is_volatile(expr) || list.iter().any(expr_is_volatile)
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => expr_is_volatile(expr) || expr_is_volatile(low) || expr_is_volatile(high),
        SqlExpr::Like { expr, pattern, .. } => expr_is_volatile(expr) || expr_is_volatile(pattern),
        SqlExpr::ArrayLiteral(items) => items.iter().any(expr_is_volatile),
        SqlExpr::Column { .. } | SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => {
            false
        }
    }
}

/// Column DEFAULT spellings that generate a fresh value per row but name no
/// registered function. Kept in step with the generator arms of
/// `crate::planner::defaults`.
const VOLATILE_DEFAULT_ALIASES: &[&str] =
    &["uuidv7", "uuidv4", "gen_uuid_v7", "gen_uuid_v4", "gen_ulid"];

/// Whether a stored column DEFAULT expression re-evaluates per execution.
///
/// The catalog stores a DEFAULT as text, in either a bare form (`UUID_V7`) or
/// a call form (`uuid_v7()`, `nextval('s')`), so the leading identifier is
/// checked before the string is parsed as an expression.
pub fn default_expr_is_volatile(expr: &str) -> bool {
    let trimmed = expr.trim();
    let head: String = trimmed
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_ascii_lowercase();
    if !head.is_empty()
        && (default_registry().is_volatile(&head)
            || VOLATILE_DEFAULT_ALIASES.contains(&head.as_str()))
    {
        return true;
    }
    crate::parse_expr_string(trimmed)
        .ok()
        .is_some_and(|parsed| expr_is_volatile(&parsed))
}

/// Whether any `(column, default_expr)` pair re-evaluates per execution.
pub fn defaults_are_volatile(defaults: &[(String, String)]) -> bool {
    defaults
        .iter()
        .any(|(_, expr)| default_expr_is_volatile(expr))
}
