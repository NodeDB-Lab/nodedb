// SPDX-License-Identifier: BUSL-1.1

//! Projection-name, computed-column, and window-function serialization helpers.

use nodedb_sql::types::{Projection, SqlExpr, WindowSpec};

use nodedb_physical::physical_plan::JoinProjection;

use super::super::expr::{sql_expr_to_bridge_expr, sql_expr_to_bridge_expr_qualified};

pub(in crate::control::planner::sql_plan_convert) fn extract_projection_names(
    proj: &[Projection],
    window_functions: &[WindowSpec],
) -> Vec<String> {
    proj.iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            Projection::Computed { alias, .. }
                if window_functions.iter().any(|spec| spec.alias == *alias) =>
            {
                Some(alias.clone())
            }
            _ => None,
        })
        .collect()
}

pub(in crate::control::planner::sql_plan_convert) fn extract_join_projection_specs(
    proj: &[Projection],
) -> Vec<JoinProjection> {
    proj.iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(JoinProjection {
                source: name.clone(),
                output: name.clone(),
            }),
            Projection::Computed {
                expr: SqlExpr::Column { table, name },
                alias,
            } => Some(JoinProjection {
                source: table
                    .as_deref()
                    .map_or_else(|| name.clone(), |table| format!("{table}.{name}")),
                output: alias.clone(),
            }),
            _ => None,
        })
        .collect()
}

pub(in crate::control::planner::sql_plan_convert) fn serialize_join_computed_projection(
    proj: &[Projection],
) -> crate::Result<Vec<u8>> {
    let has_expression = proj.iter().any(|item| {
        matches!(
            item,
            Projection::Computed { expr, .. }
                if !matches!(expr, SqlExpr::Column { .. })
        )
    });
    if !has_expression {
        return Ok(Vec::new());
    }
    if proj
        .iter()
        .any(|item| matches!(item, Projection::Star | Projection::QualifiedStar(_)))
    {
        return Err(crate::Error::BadRequest {
            detail: "join projections cannot combine a wildcard with a computed expression; list the projected columns explicitly".into(),
        });
    }

    let mut computed = Vec::new();
    for item in proj {
        match item {
            Projection::Column(name) => computed.push(crate::bridge::expr_eval::ComputedColumn {
                alias: name.clone(),
                expr: crate::bridge::expr_eval::SqlExpr::Column(name.clone()),
            }),
            Projection::Computed { expr, alias } => {
                computed.push(crate::bridge::expr_eval::ComputedColumn {
                    alias: alias.clone(),
                    expr: sql_expr_to_bridge_expr_qualified(expr),
                })
            }
            // A sequence stamp reads nothing and evaluates nothing on the
            // Data Plane; the control plane fills the cell after shaping.
            Projection::Sequence { .. } => {}
            Projection::Star | Projection::QualifiedStar(_) => {
                return Err(crate::Error::BadRequest {
                    detail: "wildcard join projection reached computed-expression lowering".into(),
                });
            }
        }
    }
    encode_computed_columns(computed, "join computed projection")
}

/// Encode a `(target_column, source_expression)` binding as the same
/// `Vec<ComputedColumn>` payload [`serialize_join_computed_projection`]
/// produces. Every pair is kept: a bare column binding still names the target
/// column it writes. Column references stay unqualified — the rows this map
/// shapes come from one collection, so their fields carry bare names.
pub(in crate::control::planner::sql_plan_convert) fn serialize_column_map(
    column_map: &[(String, SqlExpr)],
) -> crate::Result<Vec<u8>> {
    if column_map.is_empty() {
        return Ok(Vec::new());
    }
    let computed: Vec<crate::bridge::expr_eval::ComputedColumn> = column_map
        .iter()
        .map(|(name, expr)| crate::bridge::expr_eval::ComputedColumn {
            alias: name.clone(),
            expr: sql_expr_to_bridge_expr(expr),
        })
        .collect();
    encode_computed_columns(computed, "insert-select column map")
}

/// Encode a computed-column list as its MessagePack payload. `context` names
/// the caller, so an encode error says which payload failed.
fn encode_computed_columns(
    computed: Vec<crate::bridge::expr_eval::ComputedColumn>,
    context: &str,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&computed).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{context}: {e}"),
    })
}

pub(in crate::control::planner::sql_plan_convert) fn extract_computed_columns(
    proj: &[Projection],
    window_functions: &[WindowSpec],
) -> crate::Result<Vec<u8>> {
    let computed: Vec<crate::bridge::expr_eval::ComputedColumn> = proj
        .iter()
        .filter_map(|p| match p {
            Projection::Computed { expr, alias }
                if !window_functions.iter().any(|spec| spec.alias == *alias) =>
            {
                Some(crate::bridge::expr_eval::ComputedColumn {
                    alias: alias.clone(),
                    expr: sql_expr_to_bridge_expr(expr),
                })
            }
            _ => None,
        })
        .collect();
    if computed.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::to_msgpack_vec(&computed).map_err(|e| crate::Error::Internal {
        detail: format!("serialize computed columns: {e}"),
    })
}

pub(in crate::control::planner::sql_plan_convert) fn serialize_window_functions(
    specs: &[nodedb_sql::types::WindowSpec],
) -> crate::Result<Vec<u8>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let bridge_specs: Vec<crate::bridge::window_func::WindowFuncSpec> = specs
        .iter()
        .map(|s| crate::bridge::window_func::WindowFuncSpec {
            alias: s.alias.clone(),
            func_name: s.function.clone(),
            args: s.args.iter().map(sql_expr_to_bridge_expr).collect(),
            partition_by: s.partition_by.iter().map(sql_expr_to_bridge_expr).collect(),
            order_by: s
                .order_by
                .iter()
                .map(|k| (sql_expr_to_bridge_expr(&k.expr), k.ascending))
                .collect(),
            frame: s.frame.clone(),
        })
        .collect();
    zerompk::to_msgpack_vec(&bridge_specs).map_err(|e| crate::Error::Internal {
        detail: format!("serialize window functions: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::SqlValue;

    #[test]
    fn computed_join_projection_rejects_mixed_wildcards() {
        let projection = vec![
            Projection::Star,
            Projection::Computed {
                expr: SqlExpr::Literal(SqlValue::Int(1)),
                alias: "one".into(),
            },
        ];
        assert!(matches!(
            serialize_join_computed_projection(&projection),
            Err(crate::Error::BadRequest { .. })
        ));
    }
}
