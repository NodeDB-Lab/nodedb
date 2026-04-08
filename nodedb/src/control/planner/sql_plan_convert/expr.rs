//! Expression conversion and CTE inlining.

use nodedb_sql::types::{SortKey, SqlExpr, SqlPlan};

use super::value::sql_value_to_nodedb_value;

/// Convert a `nodedb_sql::types::SqlExpr` (parser AST) to a
/// `nodedb_query::expr::SqlExpr` (bridge evaluation type).
pub(super) fn sql_expr_to_bridge_expr(expr: &SqlExpr) -> crate::bridge::expr_eval::SqlExpr {
    use crate::bridge::expr_eval::SqlExpr as BExpr;
    match expr {
        SqlExpr::Column { name, .. } => BExpr::Column(name.clone()),
        SqlExpr::Literal(v) => BExpr::Literal(sql_value_to_nodedb_value(v)),
        SqlExpr::BinaryOp { left, op, right } => BExpr::BinaryOp {
            left: Box::new(sql_expr_to_bridge_expr(left)),
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
            right: Box::new(sql_expr_to_bridge_expr(right)),
        },
        SqlExpr::Function { name, args, .. } => BExpr::Function {
            name: name.clone(),
            args: args.iter().map(sql_expr_to_bridge_expr).collect(),
        },
        SqlExpr::Case {
            operand,
            when_then,
            else_expr,
        } => BExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(sql_expr_to_bridge_expr(e))),
            when_thens: when_then
                .iter()
                .map(|(w, t)| (sql_expr_to_bridge_expr(w), sql_expr_to_bridge_expr(t)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|e| Box::new(sql_expr_to_bridge_expr(e))),
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
                expr: Box::new(sql_expr_to_bridge_expr(expr)),
                to_type: cast_type,
            }
        }
        SqlExpr::Wildcard => BExpr::Column("*".into()),
        _ => BExpr::Literal(nodedb_types::Value::Null),
    }
}

pub(super) fn convert_sort_keys(keys: &[SortKey]) -> Vec<(String, bool)> {
    keys.iter()
        .filter_map(|k| match &k.expr {
            SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
            _ => None,
        })
        .collect()
}

/// Replace scans on `cte_name` with the CTE's actual subquery plan.
pub(super) fn inline_cte(plan: &SqlPlan, cte_name: &str, cte_plan: &SqlPlan) -> SqlPlan {
    match plan {
        // Direct scan on CTE name → replace with CTE plan.
        SqlPlan::Scan {
            collection,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            ..
        } if collection == cte_name => {
            // If the outer query adds filters/sort/limit, wrap the CTE plan.
            // For simple SELECT * FROM cte, just return the CTE plan directly.
            if filters.is_empty()
                && sort_keys.is_empty()
                && limit.is_none()
                && !distinct
                && projection.is_empty()
            {
                cte_plan.clone()
            } else {
                // Merge outer constraints onto the CTE plan if it's also a Scan.
                if let SqlPlan::Scan {
                    collection: inner_col,
                    engine: inner_eng,
                    filters: inner_f,
                    projection: inner_p,
                    sort_keys: inner_s,
                    limit: inner_l,
                    offset: inner_o,
                    distinct: inner_d,
                    window_functions: inner_w,
                } = cte_plan
                {
                    let mut merged_filters = inner_f.clone();
                    merged_filters.extend(filters.iter().cloned());
                    SqlPlan::Scan {
                        collection: inner_col.clone(),
                        engine: *inner_eng,
                        filters: merged_filters,
                        // Outer projection overrides inner; empty means "inherit from CTE".
                        projection: if projection.is_empty() {
                            inner_p.clone()
                        } else {
                            projection.clone()
                        },
                        sort_keys: if sort_keys.is_empty() {
                            inner_s.clone()
                        } else {
                            sort_keys.clone()
                        },
                        limit: limit.or(*inner_l),
                        // offset 0 = unspecified → inherit CTE's offset.
                        offset: if *offset > 0 { *offset } else { *inner_o },
                        distinct: *distinct || *inner_d,
                        window_functions: inner_w.clone(),
                    }
                } else {
                    cte_plan.clone()
                }
            }
        }

        // Aggregate referencing CTE → inline into the input.
        SqlPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            limit,
        } => SqlPlan::Aggregate {
            input: Box::new(inline_cte(input, cte_name, cte_plan)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            having: having.clone(),
            limit: *limit,
        },

        // JOIN referencing CTE on either side.
        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            filters,
        } => SqlPlan::Join {
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
            on: on.clone(),
            join_type: *join_type,
            condition: condition.clone(),
            limit: *limit,
            projection: projection.clone(),
            filters: filters.clone(),
        },

        // Union referencing CTE → inline into all inputs.
        SqlPlan::Union { inputs, distinct } => SqlPlan::Union {
            inputs: inputs
                .iter()
                .map(|i| inline_cte(i, cte_name, cte_plan))
                .collect(),
            distinct: *distinct,
        },

        // Intersect referencing CTE → inline into both sides.
        SqlPlan::Intersect { left, right, all } => SqlPlan::Intersect {
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
            all: *all,
        },

        // Except referencing CTE → inline into both sides.
        SqlPlan::Except { left, right, all } => SqlPlan::Except {
            left: Box::new(inline_cte(left, cte_name, cte_plan)),
            right: Box::new(inline_cte(right, cte_name, cte_plan)),
            all: *all,
        },

        // INSERT ... SELECT referencing CTE → inline into the source subquery.
        SqlPlan::InsertSelect {
            target,
            source,
            limit,
        } => SqlPlan::InsertSelect {
            target: target.clone(),
            source: Box::new(inline_cte(source, cte_name, cte_plan)),
            limit: *limit,
        },

        // No CTE reference — return as-is.
        _ => plan.clone(),
    }
}
