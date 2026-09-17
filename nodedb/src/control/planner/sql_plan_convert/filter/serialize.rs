// SPDX-License-Identifier: BUSL-1.1

//! `Filter` tree → `ScanFilter` list → msgpack bytes.
//!
//! A `FilterExpr` the planner already reduced to a field/op/value triple maps
//! one-to-one onto a `ScanFilter`; a `FilterExpr::Expr` is handed to
//! [`super::expr_lower`] for reduction.

use nodedb_sql::types::{Filter, FilterExpr};

use super::expr_lower::{sql_expr_to_join_scan_filters, sql_expr_to_scan_filters};
use crate::control::planner::sql_plan_convert::value::sql_value_to_nodedb_value;

/// Convert SqlPlan filters to ScanFilter msgpack bytes.
pub(crate) fn serialize_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
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
    encode_scan_filters(&scan_filters)
}

/// Serialize post-join WHERE filters, preserving table qualifiers inside full
/// expression predicates so they resolve against alias-prefixed merged rows.
pub(crate) fn serialize_join_post_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters = filters
        .iter()
        .flat_map(|filter| filter_to_join_scan_filters(&filter.expr))
        .collect::<Vec<_>>();
    encode_scan_filters(&scan_filters)
}

pub(crate) fn encode_scan_filters(
    filters: &Vec<nodedb_query::scan_filter::ScanFilter>,
) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("filter serialization: {e}"),
    })
}

fn filter_to_join_scan_filters(expr: &FilterExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        FilterExpr::And(filters) => filters
            .iter()
            .flat_map(|filter| filter_to_join_scan_filters(&filter.expr))
            .collect(),
        FilterExpr::Or(filters) => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::Or,
            value: nodedb_types::Value::Null,
            clauses: filters
                .iter()
                .map(|filter| filter_to_join_scan_filters(&filter.expr))
                .collect(),
            expr: None,
        }],
        FilterExpr::Expr(sql_expr) => sql_expr_to_join_scan_filters(sql_expr),
        _ => filter_to_scan_filters(expr),
    }
}

pub(crate) fn filter_to_scan_filters(
    expr: &FilterExpr,
) -> Vec<nodedb_query::scan_filter::ScanFilter> {
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
