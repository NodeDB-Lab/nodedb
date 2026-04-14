//! Filter serialization: SqlPlan filters → ScanFilter msgpack bytes.
//!
//! This is the boundary between the Control Plane planner and the Data Plane
//! scan evaluator. Filter expressions the planner can reduce to simple
//! `(field, op, value)` triples travel as native `ScanFilter` records; any
//! expression the planner cannot reduce — scalar functions in WHERE,
//! non-literal BETWEEN bounds, column arithmetic, `NOT(...)`, IN with
//! computed elements — is shipped verbatim as a `FilterOp::Expr` carrying
//! a `nodedb_query::expr::SqlExpr`. The Data Plane evaluates that against
//! each candidate row via the shared evaluator.

use nodedb_sql::planner::qualified_name;
use nodedb_sql::types::{Filter, FilterExpr, SqlExpr, SqlValue};

use super::expr::sql_expr_to_bridge_expr;
use super::value::sql_value_to_nodedb_value;

/// Convert SqlPlan filters to ScanFilter msgpack bytes.
pub(super) fn serialize_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters: Vec<nodedb_query::scan_filter::ScanFilter> = filters
        .iter()
        .flat_map(|f| filter_to_scan_filters(&f.expr))
        .collect();
    if scan_filters.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

fn filter_to_scan_filters(expr: &FilterExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        FilterExpr::Comparison { field, op, value } => {
            let filter_op = match op {
                nodedb_sql::types::CompareOp::Eq => FilterOp::Eq,
                nodedb_sql::types::CompareOp::Ne => FilterOp::Ne,
                nodedb_sql::types::CompareOp::Gt => FilterOp::Gt,
                nodedb_sql::types::CompareOp::Ge => FilterOp::Gte,
                nodedb_sql::types::CompareOp::Lt => FilterOp::Lt,
                nodedb_sql::types::CompareOp::Le => FilterOp::Lte,
            };
            vec![ScanFilter {
                field: field.clone(),
                op: filter_op,
                value: sql_value_to_nodedb_value(value),
                clauses: Vec::new(),
                expr: None,
            }]
        }
        FilterExpr::Like { field, pattern } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::Like,
                value: nodedb_types::Value::String(pattern.clone()),
                clauses: Vec::new(),
                expr: None,
            }]
        }
        FilterExpr::InList { field, values } => {
            let arr = values.iter().map(sql_value_to_nodedb_value).collect();
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::In,
                value: nodedb_types::Value::Array(arr),
                clauses: Vec::new(),
                expr: None,
            }]
        }
        FilterExpr::IsNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
                expr: None,
            }]
        }
        FilterExpr::IsNotNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNotNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
                expr: None,
            }]
        }
        FilterExpr::And(filters) => filters
            .iter()
            .flat_map(|f| filter_to_scan_filters(&f.expr))
            .collect(),
        FilterExpr::Or(filters) => {
            let clauses: Vec<Vec<ScanFilter>> = filters
                .iter()
                .map(|f| filter_to_scan_filters(&f.expr))
                .collect();
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses,
                expr: None,
            }]
        }
        FilterExpr::Expr(sql_expr) => sql_expr_to_scan_filters(sql_expr),
        _ => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::MatchAll,
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
            expr: None,
        }],
    }
}

/// Build a `ScanFilter` carrying a full expression predicate. Used whenever
/// the planner cannot reduce the WHERE expression to a simple
/// `(field, op, value)` tuple.
fn expr_filter(expr: &SqlExpr) -> nodedb_query::scan_filter::ScanFilter {
    nodedb_query::scan_filter::ScanFilter {
        field: String::new(),
        op: nodedb_query::scan_filter::FilterOp::Expr,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
        expr: Some(sql_expr_to_bridge_expr(expr)),
    }
}

/// Convert a raw `SqlExpr` (from WHERE clause) to a `ScanFilter` list.
///
/// Tries to produce simple, field-indexed filters for common cases (direct
/// comparisons, BETWEEN with literals, IN with literals) so the scanner can
/// use its fast pre-filtered path. Anything that doesn't fit — scalar
/// functions on the LHS, arithmetic, NOT, non-literal bounds — is shipped
/// as a single `FilterOp::Expr` carrying the whole expression tree.
fn sql_expr_to_scan_filters(root: &SqlExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match root {
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::And,
            right,
        } => {
            let mut filters = sql_expr_to_scan_filters(left);
            filters.extend(sql_expr_to_scan_filters(right));
            filters
        }
        SqlExpr::BinaryOp {
            left,
            op: nodedb_sql::types::BinaryOp::Or,
            right,
        } => {
            let left_filters = sql_expr_to_scan_filters(left);
            let right_filters = sql_expr_to_scan_filters(right);
            vec![ScanFilter {
                field: String::new(),
                op: FilterOp::Or,
                value: nodedb_types::Value::Null,
                clauses: vec![left_filters, right_filters],
                expr: None,
            }]
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let field = match left.as_ref() {
                SqlExpr::Column { table, name } => qualified_name(table.as_deref(), name),
                SqlExpr::Function { name, args, .. } => {
                    // HAVING fast path: COUNT(*) > 2 → field = "count(*)".
                    // Any other function goes through the generic evaluator.
                    if !is_aggregate_function(name) {
                        return vec![expr_filter(root)];
                    }
                    let arg = args
                        .first()
                        .map(|a| match a {
                            SqlExpr::Column { table, name } => {
                                qualified_name(table.as_deref(), name)
                            }
                            SqlExpr::Literal(nodedb_sql::types::SqlValue::String(s)) => s.clone(),
                            _ => "*".to_string(),
                        })
                        .unwrap_or_else(|| "*".to_string());
                    nodedb_query::agg_key::canonical_agg_key(name, &arg)
                }
                _ => return vec![expr_filter(root)],
            };
            let value = match right.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                SqlExpr::Column { table, name } => {
                    let col_op = match op {
                        nodedb_sql::types::BinaryOp::Gt => FilterOp::GtColumn,
                        nodedb_sql::types::BinaryOp::Ge => FilterOp::GteColumn,
                        nodedb_sql::types::BinaryOp::Lt => FilterOp::LtColumn,
                        nodedb_sql::types::BinaryOp::Le => FilterOp::LteColumn,
                        nodedb_sql::types::BinaryOp::Eq => FilterOp::EqColumn,
                        nodedb_sql::types::BinaryOp::Ne => FilterOp::NeColumn,
                        _ => return vec![expr_filter(root)],
                    };
                    return vec![ScanFilter {
                        field,
                        op: col_op,
                        value: nodedb_types::Value::String(qualified_name(table.as_deref(), name)),
                        clauses: Vec::new(),
                        expr: None,
                    }];
                }
                _ => return vec![expr_filter(root)],
            };
            let filter_op = match op {
                nodedb_sql::types::BinaryOp::Eq => FilterOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => FilterOp::Ne,
                nodedb_sql::types::BinaryOp::Gt => FilterOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => FilterOp::Gte,
                nodedb_sql::types::BinaryOp::Lt => FilterOp::Lt,
                nodedb_sql::types::BinaryOp::Le => FilterOp::Lte,
                _ => return vec![expr_filter(root)],
            };
            vec![ScanFilter {
                field,
                op: filter_op,
                value,
                clauses: Vec::new(),
                expr: None,
            }]
        }
        SqlExpr::IsNull { expr, negated } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { table, name } => qualified_name(table.as_deref(), name),
                _ => return vec![expr_filter(root)],
            };
            let op = if *negated {
                FilterOp::IsNotNull
            } else {
                FilterOp::IsNull
            };
            vec![ScanFilter {
                field,
                op,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
                expr: None,
            }]
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { table, name } => qualified_name(table.as_deref(), name),
                _ => return vec![expr_filter(root)],
            };
            // If any list element is non-literal, the IN set cannot be
            // pre-materialized — fall back to expression evaluation so the
            // computed values are honoured per row.
            if list.iter().any(|e| !matches!(e, SqlExpr::Literal(_))) {
                return vec![expr_filter(root)];
            }
            let values: Vec<nodedb_types::Value> = list
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Literal(v) => Some(sql_value_to_nodedb_value(v)),
                    _ => None,
                })
                .collect();
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotIn
                } else {
                    FilterOp::In
                },
                value: nodedb_types::Value::Array(values),
                clauses: Vec::new(),
                expr: None,
            }]
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { table, name } => qualified_name(table.as_deref(), name),
                _ => return vec![expr_filter(root)],
            };
            let pat = match pattern.as_ref() {
                SqlExpr::Literal(SqlValue::String(s)) => s.clone(),
                _ => return vec![expr_filter(root)],
            };
            vec![ScanFilter {
                field,
                op: if *negated {
                    FilterOp::NotLike
                } else {
                    FilterOp::Like
                },
                value: nodedb_types::Value::String(pat),
                clauses: Vec::new(),
                expr: None,
            }]
        }
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { table, name } => qualified_name(table.as_deref(), name),
                _ => return vec![expr_filter(root)],
            };
            let low_val = match low.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![expr_filter(root)],
            };
            let high_val = match high.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![expr_filter(root)],
            };
            if *negated {
                // NOT BETWEEN → lt OR gt (outside the range)
                vec![ScanFilter {
                    field: String::new(),
                    op: FilterOp::Or,
                    value: nodedb_types::Value::Null,
                    clauses: vec![
                        vec![ScanFilter {
                            field: field.clone(),
                            op: FilterOp::Lt,
                            value: low_val,
                            clauses: Vec::new(),
                            expr: None,
                        }],
                        vec![ScanFilter {
                            field,
                            op: FilterOp::Gt,
                            value: high_val,
                            clauses: Vec::new(),
                            expr: None,
                        }],
                    ],
                    expr: None,
                }]
            } else {
                // BETWEEN → gte AND lte
                vec![
                    ScanFilter {
                        field: field.clone(),
                        op: FilterOp::Gte,
                        value: low_val,
                        clauses: Vec::new(),
                        expr: None,
                    },
                    ScanFilter {
                        field,
                        op: FilterOp::Lte,
                        value: high_val,
                        clauses: Vec::new(),
                        expr: None,
                    },
                ]
            }
        }
        _ => vec![expr_filter(root)],
    }
}

/// Aggregate function names — these are the only functions whose reduction
/// through the HAVING fast path is sound. Anything else on the LHS of a
/// comparison must go through the generic expression evaluator.
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}
