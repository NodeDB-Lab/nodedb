// SPDX-License-Identifier: Apache-2.0

//! Group-row column qualification.

use crate::types::SqlExpr;

/// `expr` with every column reference stripped of its table qualifier. A
/// finalized group row keys its columns by bare name.
pub(super) fn unqualify_columns(expr: SqlExpr) -> SqlExpr {
    match expr {
        SqlExpr::Column { name, .. } => SqlExpr::Column { table: None, name },
        SqlExpr::Function {
            name,
            args,
            distinct,
        } => SqlExpr::Function {
            name,
            args: args.into_iter().map(unqualify_columns).collect(),
            distinct,
        },
        SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
            left: Box::new(unqualify_columns(*left)),
            op,
            right: Box::new(unqualify_columns(*right)),
        },
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: Box::new(unqualify_columns(*expr)),
        },
        SqlExpr::Cast { expr, to_type } => SqlExpr::Cast {
            expr: Box::new(unqualify_columns(*expr)),
            to_type,
        },
        SqlExpr::IsNull { expr, negated } => SqlExpr::IsNull {
            expr: Box::new(unqualify_columns(*expr)),
            negated,
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => SqlExpr::Case {
            operand: operand.map(|e| Box::new(unqualify_columns(*e))),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| (unqualify_columns(when), unqualify_columns(then)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(unqualify_columns(*e))),
        },
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => SqlExpr::InList {
            expr: Box::new(unqualify_columns(*expr)),
            list: list.into_iter().map(unqualify_columns).collect(),
            negated,
        },
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => SqlExpr::Between {
            expr: Box::new(unqualify_columns(*expr)),
            low: Box::new(unqualify_columns(*low)),
            high: Box::new(unqualify_columns(*high)),
            negated,
        },
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => SqlExpr::Like {
            expr: Box::new(unqualify_columns(*expr)),
            pattern: Box::new(unqualify_columns(*pattern)),
            negated,
            case_insensitive,
        },
        SqlExpr::ArrayLiteral(items) => {
            SqlExpr::ArrayLiteral(items.into_iter().map(unqualify_columns).collect())
        }
        SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => expr,
    }
}
