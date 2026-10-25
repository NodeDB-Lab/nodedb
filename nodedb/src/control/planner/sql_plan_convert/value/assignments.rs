// SPDX-License-Identifier: BUSL-1.1

//! UPDATE assignment serialization: `(field, SqlExpr)` pairs → wire-ready
//! `UpdateValue` payloads.
//!
//! Literal RHS is pre-encoded as msgpack. Non-literal RHS (arithmetic,
//! functions, CASE, concatenation, ...) is converted to the shared evaluator
//! type `bridge::expr_eval::SqlExpr` and shipped to the Data Plane, where
//! it is evaluated against the current row at apply time.

use nodedb_sql::types::SqlExpr;

use nodedb_physical::physical_plan::UpdateValue;

use super::super::expr::{sql_expr_to_bridge_expr, sql_expr_to_bridge_expr_qualified};
use super::convert::sql_value_to_msgpack;

pub(crate) fn assignments_to_update_values(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<(String, UpdateValue)>> {
    let mut result = Vec::with_capacity(assignments.len());
    for (field, expr) in assignments {
        let value = match expr {
            SqlExpr::Literal(v) => UpdateValue::Literal(sql_value_to_msgpack(v)?),
            _ => UpdateValue::Expr(sql_expr_to_bridge_expr(expr)),
        };
        result.push((field.clone(), value));
    }
    Ok(result)
}

/// Like [`assignments_to_update_values`] but qualifies column references with
/// their table name (`src.col` → `Column("src.col")`) so the Data Plane can
/// resolve them against a merged `{target_fields, "src.field": ...}` document.
pub(crate) fn assignments_to_update_values_qualified(
    assignments: &[(String, SqlExpr)],
) -> crate::Result<Vec<(String, UpdateValue)>> {
    let mut result = Vec::with_capacity(assignments.len());
    for (field, expr) in assignments {
        let value = match expr {
            SqlExpr::Literal(v) => UpdateValue::Literal(sql_value_to_msgpack(v)?),
            _ => UpdateValue::Expr(sql_expr_to_bridge_expr_qualified(expr)),
        };
        result.push((field.clone(), value));
    }
    Ok(result)
}
