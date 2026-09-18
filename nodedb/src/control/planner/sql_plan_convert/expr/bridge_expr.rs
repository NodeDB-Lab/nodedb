// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::SqlExpr;

use super::super::value::sql_value_to_nodedb_value;

/// Convert a `nodedb_sql::types::SqlExpr` (parser AST) to a
/// `nodedb_query::expr::SqlExpr` (bridge evaluation type).
///
/// Column references use the **bare** name (no table qualifier) for
/// single-collection evaluation contexts (WHERE, CHECK, GENERATED).
/// For join contexts where the merged document uses qualified keys
/// (`"t1.col"`), use [`sql_expr_to_bridge_expr_qualified`] instead.
pub(in crate::control::planner::sql_plan_convert) fn sql_expr_to_bridge_expr(
    expr: &SqlExpr,
) -> crate::bridge::expr_eval::SqlExpr {
    convert_expr_inner(expr, false)
}

/// Like [`sql_expr_to_bridge_expr`] but qualifies column references
/// with their table name (`t.col` → `"t.col"`) for join merged docs.
pub(in crate::control::planner::sql_plan_convert) fn sql_expr_to_bridge_expr_qualified(
    expr: &SqlExpr,
) -> crate::bridge::expr_eval::SqlExpr {
    convert_expr_inner(expr, true)
}

fn convert_expr_inner(expr: &SqlExpr, qualify: bool) -> crate::bridge::expr_eval::SqlExpr {
    use crate::bridge::expr_eval::SqlExpr as BExpr;
    match expr {
        SqlExpr::Column { table, name } => {
            // `EXCLUDED.col` references the row proposed for insertion in
            // `INSERT ... ON CONFLICT DO UPDATE`. Emit the dedicated
            // variant so the upsert handler can resolve against the
            // incoming row via `eval_with_excluded`. The table qualifier
            // comes in already-normalized (lowercased) from the parser.
            if table
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case("excluded"))
            {
                return BExpr::ExcludedColumn(name.clone());
            }
            if qualify {
                BExpr::Column(nodedb_sql::planner::qualified_name(table.as_deref(), name))
            } else {
                BExpr::Column(name.clone())
            }
        }
        SqlExpr::Literal(v) => BExpr::Literal(sql_value_to_nodedb_value(v)),
        SqlExpr::BinaryOp { left, op, right } => BExpr::BinaryOp {
            left: Box::new(convert_expr_inner(left, qualify)),
            op: match op {
                nodedb_sql::types::BinaryOp::Add => crate::bridge::expr_eval::BinaryOp::Add,
                nodedb_sql::types::BinaryOp::Sub => crate::bridge::expr_eval::BinaryOp::Sub,
                nodedb_sql::types::BinaryOp::Mul => crate::bridge::expr_eval::BinaryOp::Mul,
                nodedb_sql::types::BinaryOp::Div => crate::bridge::expr_eval::BinaryOp::Div,
                nodedb_sql::types::BinaryOp::Mod => crate::bridge::expr_eval::BinaryOp::Mod,
                nodedb_sql::types::BinaryOp::Eq => crate::bridge::expr_eval::BinaryOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => crate::bridge::expr_eval::BinaryOp::NotEq,
                nodedb_sql::types::BinaryOp::Gt => crate::bridge::expr_eval::BinaryOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => crate::bridge::expr_eval::BinaryOp::GtEq,
                nodedb_sql::types::BinaryOp::Lt => crate::bridge::expr_eval::BinaryOp::Lt,
                nodedb_sql::types::BinaryOp::Le => crate::bridge::expr_eval::BinaryOp::LtEq,
                nodedb_sql::types::BinaryOp::And => crate::bridge::expr_eval::BinaryOp::And,
                nodedb_sql::types::BinaryOp::Or => crate::bridge::expr_eval::BinaryOp::Or,
                nodedb_sql::types::BinaryOp::Concat => crate::bridge::expr_eval::BinaryOp::Concat,
            },
            right: Box::new(convert_expr_inner(right, qualify)),
        },
        SqlExpr::Function { name, args, .. } => BExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| convert_expr_inner(a, qualify))
                .collect(),
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => BExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(convert_expr_inner(e, qualify))),
            when_thens: when_then
                .iter()
                .map(|(w, t)| {
                    (
                        convert_expr_inner(w, qualify),
                        convert_expr_inner(t, qualify),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(convert_expr_inner(e, qualify))),
        },
        SqlExpr::Cast { expr, to_type } => {
            let cast_type = match to_type.to_uppercase().as_str() {
                "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => {
                    crate::bridge::expr_eval::CastType::Int
                }
                "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => {
                    crate::bridge::expr_eval::CastType::Float
                }
                "BOOL" | "BOOLEAN" => crate::bridge::expr_eval::CastType::Bool,
                _ => crate::bridge::expr_eval::CastType::String,
            };
            BExpr::Cast {
                expr: Box::new(convert_expr_inner(expr, qualify)),
                to_type: cast_type,
            }
        }
        SqlExpr::Wildcard => BExpr::Column("*".into()),

        // NOT e / -e → evaluator's Negate (handles both bool and numeric).
        SqlExpr::UnaryOp { expr, .. } => BExpr::Negate(Box::new(convert_expr_inner(expr, qualify))),

        // `e IS NULL` / `e IS NOT NULL` — direct passthrough.
        SqlExpr::IsNull { expr, negated } => BExpr::IsNull {
            expr: Box::new(convert_expr_inner(expr, qualify)),
            negated: *negated,
        },

        // `e BETWEEN low AND high` desugars to `e >= low AND e <= high`
        // (or `e < low OR e > high` when negated). The evaluator has no
        // native Between variant, so the planner must lower it here.
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = convert_expr_inner(expr, qualify);
            let l = convert_expr_inner(low, qualify);
            let h = convert_expr_inner(high, qualify);
            if *negated {
                let lt = BExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: crate::bridge::expr_eval::BinaryOp::Lt,
                    right: Box::new(l),
                };
                let gt = BExpr::BinaryOp {
                    left: Box::new(e),
                    op: crate::bridge::expr_eval::BinaryOp::Gt,
                    right: Box::new(h),
                };
                BExpr::BinaryOp {
                    left: Box::new(lt),
                    op: crate::bridge::expr_eval::BinaryOp::Or,
                    right: Box::new(gt),
                }
            } else {
                let ge = BExpr::BinaryOp {
                    left: Box::new(e.clone()),
                    op: crate::bridge::expr_eval::BinaryOp::GtEq,
                    right: Box::new(l),
                };
                let le = BExpr::BinaryOp {
                    left: Box::new(e),
                    op: crate::bridge::expr_eval::BinaryOp::LtEq,
                    right: Box::new(h),
                };
                BExpr::BinaryOp {
                    left: Box::new(ge),
                    op: crate::bridge::expr_eval::BinaryOp::And,
                    right: Box::new(le),
                }
            }
        }

        // `e IN (a, b, c)` desugars to `e = a OR e = b OR e = c` — each
        // element may itself be a non-literal expression, so we must
        // recursively convert and OR the comparisons together. `NOT IN`
        // is `e <> a AND e <> b AND e <> c`.
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let target = convert_expr_inner(expr, qualify);
            if list.is_empty() {
                // Empty list: `e IN ()` = false, `e NOT IN ()` = true.
                return BExpr::Literal(nodedb_types::Value::Bool(*negated));
            }
            let (eq_op, combine_op) = if *negated {
                (
                    crate::bridge::expr_eval::BinaryOp::NotEq,
                    crate::bridge::expr_eval::BinaryOp::And,
                )
            } else {
                (
                    crate::bridge::expr_eval::BinaryOp::Eq,
                    crate::bridge::expr_eval::BinaryOp::Or,
                )
            };
            // Empty list is handled above, so `list` is guaranteed non-empty
            // here: we reduce `(target eq list[0]) op (target eq list[1]) op ...`
            // without touching `.unwrap()` or `.expect()`.
            list.iter()
                .map(|item| BExpr::BinaryOp {
                    left: Box::new(target.clone()),
                    op: eq_op,
                    right: Box::new(convert_expr_inner(item, qualify)),
                })
                .reduce(|acc, next| BExpr::BinaryOp {
                    left: Box::new(acc),
                    op: combine_op,
                    right: Box::new(next),
                })
                // Unreachable: `list.is_empty()` returns early above.
                .unwrap_or(BExpr::Literal(nodedb_types::Value::Bool(*negated)))
        }

        // `e LIKE pattern` — no direct evaluator variant; route through a
        // function call so the shared function dispatcher handles it.
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let fn_name = if *case_insensitive { "ilike" } else { "like" };
            let call = BExpr::Function {
                name: fn_name.into(),
                args: vec![
                    convert_expr_inner(expr, qualify),
                    convert_expr_inner(pattern, qualify),
                ],
            };
            if *negated {
                BExpr::Negate(Box::new(call))
            } else {
                call
            }
        }

        // `ARRAY['a', 'b', ...]` — lower each element and, when all resolve to
        // `BExpr::Literal`, fold into a single `Value::Array` literal so that
        // functions like `pg_json_has_any_key` / `pg_json_has_all_keys` receive
        // a proper `Value::Array` argument rather than `Value::Null`.
        SqlExpr::ArrayLiteral(elems) => {
            let mut values = Vec::with_capacity(elems.len());
            let mut all_literal = true;
            for elem in elems {
                match convert_expr_inner(elem, qualify) {
                    BExpr::Literal(v) => values.push(v),
                    other => {
                        all_literal = false;
                        // Non-literal element: fall back to Null for that slot.
                        let _ = other;
                        values.push(nodedb_types::Value::Null);
                    }
                }
            }
            if all_literal {
                BExpr::Literal(nodedb_types::Value::Array(values))
            } else {
                BExpr::Literal(nodedb_types::Value::Null)
            }
        }

        _ => BExpr::Literal(nodedb_types::Value::Null),
    }
}
