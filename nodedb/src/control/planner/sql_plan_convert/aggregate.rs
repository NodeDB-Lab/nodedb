//! Aggregate plan conversion and projection/window helpers.

use nodedb_sql::types::{
    AggregateExpr, EngineType, Filter, Projection, SqlExpr, SqlPlan, WindowSpec,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::convert::{ConvertContext, convert_one};
use super::expr::sql_expr_to_bridge_expr;
use super::filter::serialize_filters;
use super::value::extract_time_range;

pub(super) fn convert_aggregate(
    input: &SqlPlan,
    group_by: &[SqlExpr],
    aggregates: &[AggregateExpr],
    having: &[Filter],
    limit: usize,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // Check if aggregating over a join.
    if let SqlPlan::Join {
        left,
        right,
        on,
        join_type,
        limit: join_limit,
        ..
    } = input
    {
        let mut left_collection = extract_collection_name(left);
        let mut right_collection = extract_collection_name(right);
        let mut left_alias = extract_scan_alias(left);
        let mut right_alias = extract_scan_alias(right);

        let group_strs = group_by_to_strings(group_by);
        let agg_pairs = aggregates.iter().map(agg_expr_to_pair).collect();
        let inline_left = inline_join_side(left, tenant_id, ctx)?;
        let inline_right = inline_join_side(right, tenant_id, ctx)?;

        // RIGHT JOIN → swap sides and convert to LEFT JOIN.
        let mut on_keys = on.to_vec();
        let effective_join_type = if join_type.as_str() == "right" {
            std::mem::swap(&mut left_collection, &mut right_collection);
            std::mem::swap(&mut left_alias, &mut right_alias);
            on_keys = on_keys.into_iter().map(|(l, r)| (r, l)).collect();
            "left".to_string()
        } else {
            join_type.as_str().to_string()
        };

        let vshard = VShardId::from_collection(&left_collection);

        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on: on_keys,
                join_type: effective_join_type,
                limit: *join_limit,
                post_group_by: group_strs,
                post_aggregates: agg_pairs,
                projection: Vec::new(),
                post_filters: Vec::new(),
                inline_left,
                inline_right,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    // Standard aggregate on a single collection.
    let collection = extract_collection_name(input);
    let vshard = VShardId::from_collection(&collection);
    let (filters_ref, engine) = match input {
        SqlPlan::Scan {
            filters, engine, ..
        } => (filters.as_slice(), Some(*engine)),
        _ => (&[][..], None),
    };
    let filter_bytes = serialize_filters(filters_ref)?;
    let having_bytes = serialize_filters(having)?;

    let group_strs = group_by_to_strings(group_by);
    let agg_specs: Vec<AggregateSpec> = aggregates.iter().map(agg_expr_to_spec).collect();
    let agg_pairs: Vec<(String, String)> = aggregates.iter().map(agg_expr_to_pair).collect();

    // Timeseries aggregates: route through TimeseriesOp::Scan with time_range + aggregates.
    if engine == Some(EngineType::Timeseries) {
        let time_range = extract_time_range(filters_ref);
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection,
                time_range,
                projection: Vec::new(),
                limit,
                filters: filter_bytes,
                bucket_interval_ms: 0,
                group_by: group_strs,
                aggregates: agg_pairs,
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            group_by: group_strs,
            aggregates: agg_specs,
            filters: filter_bytes,
            having: having_bytes,
            limit,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
    }])
}

// ── Helpers ──

pub(super) fn inline_join_side(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    if matches!(plan, SqlPlan::Scan { .. } | SqlPlan::PointGet { .. }) {
        return Ok(None);
    }

    let mut tasks = convert_one(plan, tenant_id, ctx)?;
    if tasks.len() > 1 {
        return Err(crate::Error::PlanError {
            detail: format!(
                "inline join side must produce exactly 1 task, got {}",
                tasks.len()
            ),
        });
    }
    Ok(tasks.pop().map(|t| Box::new(t.plan)))
}

pub(super) fn extract_collection_name(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        SqlPlan::PointGet { collection, .. } => collection.clone(),
        SqlPlan::Join { left, .. } => extract_collection_name(left),
        SqlPlan::Aggregate { input, .. } => extract_collection_name(input),
        _ => String::new(),
    }
}

pub(super) fn extract_scan_alias(plan: &SqlPlan) -> Option<String> {
    match plan {
        SqlPlan::Scan { alias, .. } => alias.clone(),
        SqlPlan::PointGet { alias, .. } => alias.clone(),
        SqlPlan::Join { left, .. } => extract_scan_alias(left),
        SqlPlan::Aggregate { input, .. } => extract_scan_alias(input),
        _ => None,
    }
}

/// Convert an `AggregateExpr` to the Data Plane aggregate spec.
pub(super) fn agg_expr_to_spec(a: &AggregateExpr) -> AggregateSpec {
    let (field, expr) = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => (name.clone(), None),
            SqlExpr::Wildcard => ("*".into(), None),
            _ => ("*".into(), Some(sql_expr_to_bridge_expr(arg))),
        })
        .unwrap_or_else(|| ("*".into(), None));

    let function = aggregate_function_name(a);
    let canonical = nodedb_query::agg_key::canonical_agg_key(&function, &field);
    let user_alias = if a.alias.eq_ignore_ascii_case(&canonical) {
        None
    } else {
        Some(a.alias.clone())
    };

    AggregateSpec {
        function,
        alias: canonical,
        user_alias,
        field,
        expr,
    }
}

/// Convert an `AggregateExpr` to the legacy `(op, field)` pair used by
/// non-QueryOp aggregate paths (timeseries, post-join aggregation).
pub(super) fn agg_expr_to_pair(a: &AggregateExpr) -> (String, String) {
    let field = a
        .args
        .first()
        .map(|arg| match arg {
            SqlExpr::Column { name, .. } => name.clone(),
            SqlExpr::Wildcard => "*".into(),
            _ => format!("{arg:?}"),
        })
        .unwrap_or_else(|| "*".into());
    (aggregate_function_name(a), field)
}

fn aggregate_function_name(a: &AggregateExpr) -> String {
    if a.distinct {
        match a.function.as_str() {
            "count" => "count_distinct".into(),
            "array_agg" => "array_agg_distinct".into(),
            _ => a.function.clone(),
        }
    } else {
        a.function.clone()
    }
}

fn group_by_to_strings(exprs: &[SqlExpr]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            SqlExpr::Column { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn extract_projection_names(
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

pub(super) fn extract_join_projection_specs(proj: &[Projection]) -> Vec<JoinProjection> {
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

pub(super) fn extract_computed_columns(
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

pub(super) fn serialize_window_functions(
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
            partition_by: s
                .partition_by
                .iter()
                .filter_map(|e| match e {
                    SqlExpr::Column { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect(),
            order_by: s
                .order_by
                .iter()
                .filter_map(|k| match &k.expr {
                    SqlExpr::Column { name, .. } => Some((name.clone(), k.ascending)),
                    _ => None,
                })
                .collect(),
            frame: crate::bridge::window_func::WindowFrame::default(),
        })
        .collect();
    zerompk::to_msgpack_vec(&bridge_specs).map_err(|e| crate::Error::Internal {
        detail: format!("serialize window functions: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::{agg_expr_to_spec, extract_computed_columns, extract_projection_names};
    use nodedb_sql::types::{AggregateExpr, BinaryOp, Projection, SqlExpr, SqlValue, WindowSpec};

    #[test]
    fn aggregate_spec_preserves_alias_and_case_expression() {
        let agg = AggregateExpr {
            function: "sum".into(),
            args: vec![SqlExpr::Case {
                operand: None,
                when_then: vec![(
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column {
                            table: None,
                            name: "category".into(),
                        }),
                        op: BinaryOp::Eq,
                        right: Box::new(SqlExpr::Literal(SqlValue::String("tools".into()))),
                    },
                    SqlExpr::Literal(SqlValue::Int(1)),
                )],
                else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::Int(0)))),
            }],
            alias: "tools_count".into(),
            distinct: false,
        };

        let spec = agg_expr_to_spec(&agg);

        assert_eq!(spec.function, "sum");
        assert_eq!(spec.alias, "sum(*)");
        assert_eq!(spec.user_alias.as_deref(), Some("tools_count"));
        assert_eq!(spec.field, "*");
        assert!(matches!(
            spec.expr,
            Some(crate::bridge::expr_eval::SqlExpr::Case { .. })
        ));
    }

    #[test]
    fn window_aliases_stay_in_projection_and_out_of_computed_columns() {
        let projection = vec![
            Projection::Column("name".into()),
            Projection::Computed {
                expr: SqlExpr::Function {
                    name: "row_number".into(),
                    args: Vec::new(),
                    distinct: false,
                },
                alias: "rn".into(),
            },
            Projection::Computed {
                expr: SqlExpr::Column {
                    table: None,
                    name: "age".into(),
                },
                alias: "age_copy".into(),
            },
        ];
        let window_functions = vec![WindowSpec {
            function: "row_number".into(),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            alias: "rn".into(),
        }];

        assert_eq!(
            extract_projection_names(&projection, &window_functions),
            vec!["name".to_string(), "rn".to_string()]
        );

        let computed_bytes =
            extract_computed_columns(&projection, &window_functions).expect("serialize computed");
        let computed: Vec<crate::bridge::expr_eval::ComputedColumn> =
            zerompk::from_msgpack(&computed_bytes).expect("deserialize computed");

        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0].alias, "age_copy");
    }
}
