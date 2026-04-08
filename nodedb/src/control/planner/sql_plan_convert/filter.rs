//! Filter serialization: SqlPlan filters → ScanFilter msgpack bytes.

use nodedb_sql::types::{Filter, FilterExpr, SqlExpr, SqlValue};

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
            }]
        }
        FilterExpr::Like { field, pattern } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::Like,
                value: nodedb_types::Value::String(pattern.clone()),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::InList { field, values } => {
            let arr = values.iter().map(sql_value_to_nodedb_value).collect();
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::In,
                value: nodedb_types::Value::Array(arr),
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
            }]
        }
        FilterExpr::IsNotNull { field } => {
            vec![ScanFilter {
                field: field.clone(),
                op: FilterOp::IsNotNull,
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
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
            }]
        }
        FilterExpr::Expr(sql_expr) => {
            // Convert SqlExpr to ScanFilter via pattern matching.
            sql_expr_to_scan_filters(sql_expr)
        }
        _ => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::MatchAll,
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
        }],
    }
}

/// Convert a raw SqlExpr (from WHERE clause) to ScanFilter list.
fn sql_expr_to_scan_filters(expr: &SqlExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
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
            }]
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let field = match left.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                SqlExpr::Function { name, args, .. } => {
                    // HAVING: COUNT(*) > 2 → field = "count_all"
                    let arg = args
                        .first()
                        .map(|a| match a {
                            SqlExpr::Column { name, .. } => name.clone(),
                            SqlExpr::Literal(nodedb_sql::types::SqlValue::String(s)) => s.clone(),
                            _ => "*".to_string(),
                        })
                        .unwrap_or_else(|| "*".to_string());
                    format!("{name}_{arg}").replace('*', "all")
                }
                _ => return vec![match_all()],
            };
            let value = match right.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                SqlExpr::Column { name, .. } => {
                    // Column-vs-column comparison (e.g. scalar subquery result).
                    let col_op = match op {
                        nodedb_sql::types::BinaryOp::Gt => FilterOp::GtColumn,
                        nodedb_sql::types::BinaryOp::Ge => FilterOp::GteColumn,
                        nodedb_sql::types::BinaryOp::Lt => FilterOp::LtColumn,
                        nodedb_sql::types::BinaryOp::Le => FilterOp::LteColumn,
                        nodedb_sql::types::BinaryOp::Eq => FilterOp::EqColumn,
                        nodedb_sql::types::BinaryOp::Ne => FilterOp::NeColumn,
                        _ => return vec![match_all()],
                    };
                    return vec![ScanFilter {
                        field,
                        op: col_op,
                        value: nodedb_types::Value::String(name.clone()),
                        clauses: Vec::new(),
                    }];
                }
                _ => return vec![match_all()],
            };
            let filter_op = match op {
                nodedb_sql::types::BinaryOp::Eq => FilterOp::Eq,
                nodedb_sql::types::BinaryOp::Ne => FilterOp::Ne,
                nodedb_sql::types::BinaryOp::Gt => FilterOp::Gt,
                nodedb_sql::types::BinaryOp::Ge => FilterOp::Gte,
                nodedb_sql::types::BinaryOp::Lt => FilterOp::Lt,
                nodedb_sql::types::BinaryOp::Le => FilterOp::Lte,
                _ => return vec![match_all()],
            };
            vec![ScanFilter {
                field,
                op: filter_op,
                value,
                clauses: Vec::new(),
            }]
        }
        SqlExpr::IsNull { expr, negated } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
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
            }]
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
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
            }]
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let pat = match pattern.as_ref() {
                SqlExpr::Literal(SqlValue::String(s)) => s.clone(),
                _ => return vec![match_all()],
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
            }]
        }
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let field = match expr.as_ref() {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => return vec![match_all()],
            };
            let low_val = match low.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
            };
            let high_val = match high.as_ref() {
                SqlExpr::Literal(v) => sql_value_to_nodedb_value(v),
                _ => return vec![match_all()],
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
                        }],
                        vec![ScanFilter {
                            field,
                            op: FilterOp::Gt,
                            value: high_val,
                            clauses: Vec::new(),
                        }],
                    ],
                }]
            } else {
                // BETWEEN → gte AND lte
                vec![
                    ScanFilter {
                        field: field.clone(),
                        op: FilterOp::Gte,
                        value: low_val,
                        clauses: Vec::new(),
                    },
                    ScanFilter {
                        field,
                        op: FilterOp::Lte,
                        value: high_val,
                        clauses: Vec::new(),
                    },
                ]
            }
        }
        _ => vec![match_all()],
    }
}

fn match_all() -> nodedb_query::scan_filter::ScanFilter {
    nodedb_query::scan_filter::ScanFilter {
        field: String::new(),
        op: nodedb_query::scan_filter::FilterOp::MatchAll,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
    }
}
