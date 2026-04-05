//! Scan filter extraction from DataFusion expressions.

use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{LogicalPlan, Operator};
use datafusion::prelude::*;
use tracing::warn;

use crate::bridge::scan_filter::ScanFilter;

use super::super::expr_convert::expr_to_json_value;

/// Convert a DataFusion expression to scan filter predicates.
pub(in crate::control::planner) fn expr_to_scan_filters(expr: &Expr) -> Vec<ScanFilter> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut filters = expr_to_scan_filters(&binary.left);
            filters.extend(expr_to_scan_filters(&binary.right));
            filters
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left_filters = expr_to_scan_filters(&binary.left);
            let right_filters = expr_to_scan_filters(&binary.right);

            // If either branch is unsupported, the OR as a whole can't be
            // converted to a ScanFilter. Emit a "match_all" filter so the
            // Data Plane scans all documents. This is correct (superset of
            // matches) because the unsupported branch might match anything.
            //
            // SAFETY: Previously this returned Vec::new() which meant "no
            // filters" — same as match_all — but callers in AND chains would
            // silently DROP the entire WHERE clause. Now we return an explicit
            // match_all ScanFilter that always evaluates to true, preserving
            // any sibling AND filters.
            if left_filters.is_empty() || right_filters.is_empty() {
                warn!(
                    "OR predicate has unsupported branch (left={}, right={}); emitting match_all for this OR",
                    left_filters.len(),
                    right_filters.len()
                );
                return vec![ScanFilter {
                    op: "match_all".into(),
                    ..Default::default()
                }];
            }

            vec![ScanFilter {
                op: "or".into(),
                clauses: vec![left_filters, right_filters],
                ..Default::default()
            }]
        }
        Expr::BinaryExpr(binary) => {
            let op_str = match binary.op {
                Operator::Eq => "eq",
                Operator::NotEq => "ne",
                Operator::Gt => "gt",
                Operator::GtEq => "gte",
                Operator::Lt => "lt",
                Operator::LtEq => "lte",
                _ => return Vec::new(),
            };

            let (field, value) = match (&*binary.left, &*binary.right) {
                (Expr::Column(col), Expr::Literal(lit, meta)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone(), meta.clone())),
                ),
                (Expr::Literal(lit, meta), Expr::Column(col)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone(), meta.clone())),
                ),
                _ => return Vec::new(),
            };

            vec![ScanFilter {
                field,
                op: op_str.into(),
                value,
                clauses: Vec::new(),
            }]
        }
        Expr::Like(like) => {
            if let Expr::Column(col) = &*like.expr {
                let pattern = expr_to_json_value(&like.pattern);
                let op = if like.case_insensitive {
                    if like.negated { "not_ilike" } else { "ilike" }
                } else if like.negated {
                    "not_like"
                } else {
                    "like"
                };
                vec![ScanFilter {
                    field: col.name.clone(),
                    op: op.into(),
                    value: pattern,
                    clauses: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Expr::IsNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![ScanFilter {
                    field: col.name.clone(),
                    op: "is_null".into(),
                    value: serde_json::Value::Null,
                    clauses: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![ScanFilter {
                    field: col.name.clone(),
                    op: "is_not_null".into(),
                    value: serde_json::Value::Null,
                    clauses: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Expr::Between(between) => {
            if let Expr::Column(col) = &*between.expr {
                let low = expr_to_json_value(&between.low);
                let high = expr_to_json_value(&between.high);
                if between.negated {
                    vec![ScanFilter {
                        op: "or".into(),
                        clauses: vec![
                            vec![ScanFilter {
                                field: col.name.clone(),
                                op: "lt".into(),
                                value: low,
                                clauses: Vec::new(),
                            }],
                            vec![ScanFilter {
                                field: col.name.clone(),
                                op: "gt".into(),
                                value: high,
                                clauses: Vec::new(),
                            }],
                        ],
                        ..Default::default()
                    }]
                } else {
                    vec![
                        ScanFilter {
                            field: col.name.clone(),
                            op: "gte".into(),
                            value: low,
                            clauses: Vec::new(),
                        },
                        ScanFilter {
                            field: col.name.clone(),
                            op: "lte".into(),
                            value: high,
                            clauses: Vec::new(),
                        },
                    ]
                }
            } else {
                Vec::new()
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            if let Expr::Column(col) = expr.as_ref() {
                let values: Vec<serde_json::Value> = list.iter().map(expr_to_json_value).collect();
                vec![ScanFilter {
                    field: col.name.clone(),
                    op: if *negated { "not_in" } else { "in" }.into(),
                    value: serde_json::Value::Array(values),
                    clauses: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Expr::ScalarFunction(func) if func.name() == "text_match" => {
            if func.args.len() >= 2 {
                vec![ScanFilter {
                    field: "__text_match".into(),
                    op: "text_match".into(),
                    value: expr_to_json_value(&func.args[1]),
                    clauses: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        // EXISTS (SELECT ...) / NOT EXISTS (SELECT ...)
        // IN (SELECT ...) / NOT IN (SELECT ...)
        // Both encode the sub-table and sub-filters into a ScanFilter value.
        // The Data Plane handler evaluates the sub-scan and filters based on the result.
        Expr::Exists(exists) => {
            subquery_to_scan_filter(&exists.subquery.subquery, exists.negated, "EXISTS")
        }
        Expr::InSubquery(in_sub) => {
            subquery_to_scan_filter(&in_sub.subquery.subquery, in_sub.negated, "IN")
        }
        // Scalar subquery (correlated): DataFusion usually decorrelates these.
        // If one reaches here, emit match_all as safe fallback.
        Expr::ScalarSubquery(_) => {
            warn!("correlated scalar subquery not fully supported; emitting match_all");
            vec![ScanFilter {
                op: "match_all".into(),
                ..Default::default()
            }]
        }
        _ => Vec::new(),
    }
}

/// Shared handler for EXISTS and IN subquery expressions.
///
/// Extracts the sub-collection and sub-filters, then encodes as a ScanFilter
/// with `"exists"` or `"not_exists"` operator.
fn subquery_to_scan_filter(
    subquery_plan: &LogicalPlan,
    negated: bool,
    label: &str,
) -> Vec<ScanFilter> {
    let (sub_collection, sub_filter_bytes) = extract_exists_source(subquery_plan);
    if let Some((coll, filters)) = sub_collection.zip(sub_filter_bytes) {
        let op = if negated { "not_exists" } else { "exists" };
        vec![ScanFilter {
            field: String::new(),
            op: op.into(),
            value: serde_json::json!({
                "collection": coll,
                "filters": match serde_json::from_slice::<serde_json::Value>(&filters) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("failed to deserialize {label} subquery filters: {e}");
                        serde_json::Value::Object(Default::default())
                    }
                },
            }),
            clauses: Vec::new(),
        }]
    } else {
        warn!("{label} subquery too complex; emitting match_all");
        vec![ScanFilter {
            op: "match_all".into(),
            ..Default::default()
        }]
    }
}

/// Extract collection name and filters from an EXISTS subquery plan.
fn extract_exists_source(plan: &LogicalPlan) -> (Option<String>, Option<Vec<u8>>) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let coll = scan.table_name.to_string().to_lowercase();
            let filter_bytes = if !scan.filters.is_empty() {
                let mut all = Vec::new();
                for f in &scan.filters {
                    all.extend(expr_to_scan_filters(f));
                }
                zerompk::to_msgpack_vec(&all).ok()
            } else {
                Some(Vec::new())
            };
            (Some(coll), filter_bytes)
        }
        LogicalPlan::Filter(filter) => {
            let (coll, _) = extract_exists_source(&filter.input);
            let filters = expr_to_scan_filters(&filter.predicate);
            let filter_bytes = zerompk::to_msgpack_vec(&filters).ok();
            (coll, filter_bytes)
        }
        LogicalPlan::Projection(proj) => extract_exists_source(&proj.input),
        LogicalPlan::SubqueryAlias(alias) => extract_exists_source(&alias.input),
        _ => (None, None),
    }
}

/// Extract WHERE predicate from a DML plan as serialized ScanFilters.
pub(in crate::control::planner) fn extract_where_filters(
    plan: &LogicalPlan,
) -> crate::Result<Vec<u8>> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let scan_filters = expr_to_scan_filters(&filter.predicate);
            if scan_filters.is_empty() {
                return Err(crate::Error::PlanError {
                    detail: "WHERE clause contains unsupported predicates for bulk operation"
                        .into(),
                });
            }
            zerompk::to_msgpack_vec(&scan_filters).map_err(|e| crate::Error::PlanError {
                detail: format!("failed to serialize scan filters: {e}"),
            })
        }
        LogicalPlan::TableScan(_) => Err(crate::Error::PlanError {
            detail: "bulk operation requires a WHERE clause".into(),
        }),
        _ => {
            if let Some(child) = plan.inputs().first() {
                extract_where_filters(child)
            } else {
                Err(crate::Error::PlanError {
                    detail: "could not find WHERE predicate in plan".into(),
                })
            }
        }
    }
}
