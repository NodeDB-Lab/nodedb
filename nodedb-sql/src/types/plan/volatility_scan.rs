// SPDX-License-Identifier: Apache-2.0

//! Volatility scanning over plan expressions.
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
