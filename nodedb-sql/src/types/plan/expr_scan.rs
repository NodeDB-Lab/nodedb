// SPDX-License-Identifier: Apache-2.0

//! Structural scans over plan expressions: sequence-accessor detection and
//! column-reference collection.
//!
//! A SELECT-list expression holding a sequence accessor is evaluated by the
//! Control Plane once per output row. The planner needs to know that an
//! expression holds one, and which base columns it reads, so the Data Plane
//! projects those columns and the response stage evaluates the expression.

use crate::functions::sequence_accessor::is_sequence_accessor;
use crate::types::query::Projection;
use crate::types_expr::SqlExpr;

/// The name of the first sequence accessor called in `expr`, when one is.
///
/// A `SqlExpr::Subquery` is opaque here: its body resolves in its own scope,
/// which refuses an accessor of its own.
pub fn first_sequence_accessor(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Function { name, args, .. } => {
            if is_sequence_accessor(name) {
                return Some(name.as_str());
            }
            args.iter().find_map(first_sequence_accessor)
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            first_sequence_accessor(left).or_else(|| first_sequence_accessor(right))
        }
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. } => first_sequence_accessor(expr),
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => operand
            .as_deref()
            .and_then(first_sequence_accessor)
            .or_else(|| {
                when_then.iter().find_map(|(when, then)| {
                    first_sequence_accessor(when).or_else(|| first_sequence_accessor(then))
                })
            })
            .or_else(|| else_expr.as_deref().and_then(first_sequence_accessor)),
        SqlExpr::InList { expr, list, .. } => {
            first_sequence_accessor(expr).or_else(|| list.iter().find_map(first_sequence_accessor))
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => first_sequence_accessor(expr)
            .or_else(|| first_sequence_accessor(low))
            .or_else(|| first_sequence_accessor(high)),
        SqlExpr::Like { expr, pattern, .. } => {
            first_sequence_accessor(expr).or_else(|| first_sequence_accessor(pattern))
        }
        SqlExpr::ArrayLiteral(items) => items.iter().find_map(first_sequence_accessor),
        SqlExpr::Column { .. } | SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => {
            None
        }
    }
}

/// Whether `expr` calls a sequence accessor anywhere outside a subquery.
pub fn contains_sequence_accessor(expr: &SqlExpr) -> bool {
    first_sequence_accessor(expr).is_some()
}

/// Every column `expr` references, qualified as `table.name` when the
/// reference carries a table, deduplicated, in first-seen order.
///
/// A `SqlExpr::Subquery` contributes nothing: its columns belong to its own
/// relation set.
pub fn referenced_columns(expr: &SqlExpr) -> Vec<String> {
    let mut out = Vec::new();
    collect_columns(expr, &mut out);
    out
}

fn collect_columns(expr: &SqlExpr, out: &mut Vec<String>) {
    match expr {
        SqlExpr::Column { table, name } => {
            let qualified = match table {
                Some(table) => format!("{table}.{name}"),
                None => name.clone(),
            };
            if !out.contains(&qualified) {
                out.push(qualified);
            }
        }
        SqlExpr::Function { args, .. } | SqlExpr::ArrayLiteral(args) => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::IsNull { expr, .. } => collect_columns(expr, out),
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_columns(operand, out);
            }
            for (when, then) in when_then {
                collect_columns(when, out);
                collect_columns(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_columns(else_expr, out);
            }
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_columns(expr, out);
            for item in list {
                collect_columns(item, out);
            }
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_columns(expr, out);
            collect_columns(low, out);
            collect_columns(high, out);
        }
        SqlExpr::Like { expr, pattern, .. } => {
            collect_columns(expr, out);
            collect_columns(pattern, out);
        }
        SqlExpr::Literal(_) | SqlExpr::Subquery(_) | SqlExpr::Wildcard => {}
    }
}

/// Whether `projection` holds an entry the Control Plane evaluates per row.
pub fn projection_is_cp_computed(projection: &[Projection]) -> bool {
    projection
        .iter()
        .any(|p| matches!(p, Projection::CpComputed { .. }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types_expr::{BinaryOp, SqlValue};

    fn column(table: Option<&str>, name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: table.map(str::to_string),
            name: name.into(),
        }
    }

    fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
        SqlExpr::Function {
            name: name.into(),
            args,
            distinct: false,
        }
    }

    fn nextval() -> SqlExpr {
        call(
            "nextval",
            vec![SqlExpr::Literal(SqlValue::String("s".into()))],
        )
    }

    #[test]
    fn accessor_found_at_top_level_and_nested() {
        assert_eq!(first_sequence_accessor(&nextval()), Some("nextval"));
        let nested = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
            op: BinaryOp::Mul,
            right: Box::new(SqlExpr::Cast {
                expr: Box::new(call("coalesce", vec![nextval()])),
                to_type: "text".into(),
            }),
        };
        assert!(contains_sequence_accessor(&nested));
        let case = SqlExpr::Case {
            operand: None,
            when_then: vec![(column(None, "flag"), call("currval", Vec::new()))],
            else_expr: None,
        };
        assert_eq!(first_sequence_accessor(&case), Some("currval"));
    }

    #[test]
    fn plain_expressions_hold_no_accessor() {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(column(Some("t"), "a")),
            op: BinaryOp::Add,
            right: Box::new(call("upper", vec![column(None, "b")])),
        };
        assert!(!contains_sequence_accessor(&expr));
        assert!(!contains_sequence_accessor(&SqlExpr::Wildcard));
    }

    #[test]
    fn subquery_is_opaque() {
        let subquery = SqlExpr::Subquery(Box::new(crate::types::SqlPlan::ConstantResult {
            columns: vec!["n".into()],
            values: vec![SqlValue::Int(1)],
            volatile: true,
        }));
        assert!(!contains_sequence_accessor(&subquery));
        assert!(referenced_columns(&subquery).is_empty());
    }

    #[test]
    fn referenced_columns_are_qualified_deduplicated_and_ordered() {
        let expr = SqlExpr::Case {
            operand: Some(Box::new(column(Some("t"), "kind"))),
            when_then: vec![(
                SqlExpr::InList {
                    expr: Box::new(column(None, "b")),
                    list: vec![column(None, "a"), column(Some("t"), "kind")],
                    negated: false,
                },
                SqlExpr::Between {
                    expr: Box::new(column(None, "c")),
                    low: Box::new(column(None, "a")),
                    high: Box::new(SqlExpr::Literal(SqlValue::Int(9))),
                    negated: false,
                },
            )],
            else_expr: Some(Box::new(SqlExpr::Like {
                expr: Box::new(column(None, "d")),
                pattern: Box::new(SqlExpr::Literal(SqlValue::String("x%".into()))),
                negated: false,
                case_insensitive: false,
            })),
        };
        assert_eq!(referenced_columns(&expr), ["t.kind", "b", "a", "c", "d"]);
    }

    #[test]
    fn cp_computed_projection_is_detected() {
        let plain = vec![
            Projection::Column("id".into()),
            Projection::Computed {
                expr: nextval(),
                alias: "n".into(),
            },
        ];
        assert!(!projection_is_cp_computed(&plain));
        let cp = vec![Projection::CpComputed {
            expr: nextval(),
            alias: "n".into(),
        }];
        assert!(projection_is_cp_computed(&cp));
    }
}
