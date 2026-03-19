//! Filter and expression extraction helpers for the plan converter.
//!
//! These are standalone functions used by `PlanConverter` to translate
//! DataFusion `Expr` trees into NodeDB scan filters, update assignments,
//! and scalar values.

use datafusion::logical_expr::{LogicalPlan, Operator};
use datafusion::prelude::*;

/// Convert a DataFusion expression to scan filter predicates.
///
/// Supports: eq, ne, gt, gte, lt, lte on any field.
/// Supports AND/OR combinations (flattened to a list of AND predicates).
pub(super) fn expr_to_scan_filters(expr: &Expr) -> Vec<serde_json::Value> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut filters = expr_to_scan_filters(&binary.left);
            filters.extend(expr_to_scan_filters(&binary.right));
            filters
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
                (Expr::Column(col), Expr::Literal(lit)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone())),
                ),
                (Expr::Literal(lit), Expr::Column(col)) => (
                    col.name.clone(),
                    expr_to_json_value(&Expr::Literal(lit.clone())),
                ),
                _ => return Vec::new(),
            };

            vec![serde_json::json!({
                "field": field,
                "op": op_str,
                "value": value,
            })]
        }
        Expr::IsNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![serde_json::json!({
                    "field": col.name,
                    "op": "is_null",
                    "value": null,
                })]
            } else {
                Vec::new()
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                vec![serde_json::json!({
                    "field": col.name,
                    "op": "is_not_null",
                    "value": null,
                })]
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    }
}

/// Extract a usize from an Expr (for OFFSET values).
pub(super) fn expr_to_usize(expr: &Expr) -> crate::Result<usize> {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            s.parse::<usize>().map_err(|_| crate::Error::PlanError {
                detail: format!("expected integer for OFFSET, got: {s}"),
            })
        }
        _ => Err(crate::Error::PlanError {
            detail: format!("expected literal for OFFSET, got: {expr}"),
        }),
    }
}

/// Extract SET field assignments from an UPDATE DML input plan.
///
/// DataFusion represents UPDATE SET as a projection with assignment expressions.
/// Returns `Vec<(field_name, json_value_bytes)>`.
pub(super) fn extract_update_assignments(
    plan: &LogicalPlan,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let mut updates = Vec::new();
            let schema = proj.schema.fields();
            for (i, expr) in proj.expr.iter().enumerate() {
                let field_name = if i < schema.len() {
                    schema[i].name().clone()
                } else {
                    continue;
                };
                // Skip the id column — it's the WHERE target, not a SET.
                if field_name == "id" || field_name == "document_id" {
                    continue;
                }
                let value = expr_to_json_value(expr);
                let value_bytes =
                    serde_json::to_vec(&value).map_err(|e| crate::Error::Serialization {
                        format: "json".into(),
                        detail: format!("filter serialization: {e}"),
                    })?;
                updates.push((field_name, value_bytes));
            }
            Ok(updates)
        }
        LogicalPlan::Filter(filter) => extract_update_assignments(&filter.input),
        _ => {
            // DataFusion may wrap UPDATE in various ways. Return empty if we can't parse.
            Ok(Vec::new())
        }
    }
}

/// Collect document IDs from equality predicates (id = 'value' OR id = 'value2').
pub(super) fn collect_eq_ids(expr: &Expr, ids: &mut Vec<String>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (col_name, value) = match (&*binary.left, &*binary.right) {
                (Expr::Column(col), Expr::Literal(lit)) => (col.name.as_str(), lit.to_string()),
                (Expr::Literal(lit), Expr::Column(col)) => (col.name.as_str(), lit.to_string()),
                _ => return,
            };
            if col_name == "id" || col_name == "document_id" {
                ids.push(value.trim_matches('\'').trim_matches('"').to_string());
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_eq_ids(&binary.left, ids);
            collect_eq_ids(&binary.right, ids);
        }
        _ => {}
    }
}

/// Convert an expression to a string value (for document IDs).
pub(super) fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            s.trim_matches('\'').trim_matches('"').to_string()
        }
        _ => format!("{expr}"),
    }
}

/// Convert an expression to a JSON value (for document fields).
pub(super) fn expr_to_json_value(expr: &Expr) -> serde_json::Value {
    match expr {
        Expr::Literal(lit) => {
            let s = lit.to_string();
            // Try parsing as number first.
            if let Ok(n) = s.parse::<i64>() {
                return serde_json::Value::Number(n.into());
            }
            if let Ok(n) = s.parse::<f64>() {
                if let Some(num) = serde_json::Number::from_f64(n) {
                    return serde_json::Value::Number(num);
                }
            }
            if s == "true" {
                return serde_json::Value::Bool(true);
            }
            if s == "false" {
                return serde_json::Value::Bool(false);
            }
            if s == "NULL" || s == "null" {
                return serde_json::Value::Null;
            }
            // String value — strip quotes.
            serde_json::Value::String(s.trim_matches('\'').trim_matches('"').to_string())
        }
        _ => serde_json::Value::String(format!("{expr}")),
    }
}

/// Extract (document_id, value_bytes) pairs from an INSERT input plan.
///
/// DataFusion represents `INSERT INTO t VALUES (...)` as a projection of
/// literal values. The first column is the document ID; remaining columns
/// are serialized as a JSON object.
pub(super) fn extract_insert_values(plan: &LogicalPlan) -> crate::Result<Vec<(String, Vec<u8>)>> {
    match plan {
        LogicalPlan::Values(values) => {
            let schema = values.schema.fields();
            let mut results = Vec::with_capacity(values.values.len());
            for row in &values.values {
                let doc_id = if let Some(first) = row.first() {
                    expr_to_string(first)
                } else {
                    continue;
                };
                let mut obj = serde_json::Map::new();
                for (i, expr) in row.iter().enumerate() {
                    let field_name = if i < schema.len() {
                        schema[i].name().clone()
                    } else {
                        format!("column{i}")
                    };
                    let val = expr_to_json_value(expr);
                    obj.insert(field_name, val);
                }
                let value_bytes =
                    serde_json::to_vec(&obj).map_err(|e| crate::Error::PlanError {
                        detail: format!("failed to serialize insert values: {e}"),
                    })?;
                results.push((doc_id, value_bytes));
            }
            Ok(results)
        }
        LogicalPlan::Projection(proj) => extract_insert_values(&proj.input),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported INSERT input plan type: {}", plan.display()),
        }),
    }
}

/// Extract document IDs to delete from a DELETE plan's filter.
pub(super) fn extract_delete_targets(
    plan: &LogicalPlan,
    _collection: &str,
) -> crate::Result<Vec<String>> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let mut ids = Vec::new();
            collect_eq_ids(&filter.predicate, &mut ids);
            Ok(ids)
        }
        LogicalPlan::TableScan(_) => Err(crate::Error::PlanError {
            detail: "DELETE without WHERE clause is not supported. Use DROP COLLECTION to remove all data.".into(),
        }),
        _ => Err(crate::Error::PlanError {
            detail: format!("unsupported DELETE input plan: {}", plan.display()),
        }),
    }
}
