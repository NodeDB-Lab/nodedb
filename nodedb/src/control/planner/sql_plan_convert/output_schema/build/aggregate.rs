// SPDX-License-Identifier: BUSL-1.1

//! Aggregate output ordering and catalog-derived types.

use super::super::columns::{column_types_for, group_by_key_column};
use crate::control::planner::sql_plan_convert::aggregate::agg_expr_to_pair;
use crate::control::planner::sql_plan_convert::lateral::collection_name_from_plan;
use crate::control::planner::sql_plan_convert::output_schema_types::infer_aggregate_type;
use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::query::{AggOutputSlot, AggregateExpr};
use nodedb_sql::types_expr::SqlExpr;
use std::collections::HashMap;

/// The grouped (non-`time_bucket`) timeseries scan schema: GROUP BY keys
/// typed from the catalog, then aggregates as `Text`.
pub(super) fn timeseries_group_schema<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
    group_by: &[String],
    aggregates: &[AggregateExpr],
) -> OutputSchema {
    let types = column_types_for(catalog, database_id, collection);
    let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
    for key in group_by {
        columns.push(OutputColumn {
            display_name: key.clone(),
            lookup_key: key.clone(),
            ty: types.get(key).copied().unwrap_or(DdlColType::Text),
        });
    }
    for agg in aggregates {
        let (function, field) = agg_expr_to_pair(agg);
        let key = canonical_agg_key(&function, &field);
        columns.push(OutputColumn {
            display_name: key.clone(),
            lookup_key: key,
            ty: DdlColType::Text,
        });
    }
    OutputSchema {
        columns,
        is_star: false,
        cp_computed: Vec::new(),
    }
}

/// The fields of an `SqlPlan::Aggregate` that shape its output.
pub(super) struct AggregateShape<'a> {
    pub input: &'a SqlPlan,
    pub group_by: &'a [SqlExpr],
    pub group_by_aliases: &'a [Option<String>],
    pub output_order: &'a [AggOutputSlot],
    pub aggregates: &'a [AggregateExpr],
}

/// The schema of an `SqlPlan::Aggregate`, in SELECT-list order.
pub(super) fn aggregate_schema<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    shape: AggregateShape<'_>,
) -> OutputSchema {
    let AggregateShape {
        input,
        group_by,
        group_by_aliases,
        output_order,
        aggregates,
    } = shape;
    // Catalog types of the aggregate's underlying columns, resolved from
    // the input plan's single source collection when it has one (Scan /
    // point-get style). GROUP BY bare keys and MIN/MAX/SUM/AVG argument
    // columns are typed against this; anything unresolvable stays Text.
    let types = match collection_name_from_plan(input) {
        Some(collection) => column_types_for(catalog, database_id, &collection),
        None => HashMap::new(),
    };
    // Derives the `OutputColumn` for one GROUP BY key: `group_by_aliases`
    // is parallel to `group_by` when populated, but may be empty when the
    // plan was built without a projection in scope — treat a
    // missing/`None` entry as "no alias".
    let key_column = |index: usize| {
        group_by.get(index).map(|key| {
            let alias = group_by_aliases.get(index).and_then(|a| a.as_deref());
            group_by_key_column(key, index, alias, &types)
        })
    };
    // Derives the `OutputColumn` for one aggregate. `AggregateExpr::alias`
    // is always populated by the planner: either the user's explicit
    // alias, or (for unnamed projections) the lowercased unparsed
    // expression text — e.g. `count(*)` — matching this module's own
    // lowercasing of non-column expressions. So the alias is already the
    // canonical name; no separate derivation needed. The result type is
    // inferred conservatively (COUNT -> Int8, MIN/MAX preserve the input
    // column type, SUM/AVG of a float -> Float8, else Text).
    let agg_column = |index: usize| {
        aggregates.get(index).map(|agg| OutputColumn {
            display_name: agg.alias.clone(),
            lookup_key: agg.alias.clone(),
            ty: infer_aggregate_type(agg, &types),
        })
    };
    let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
    if output_order.is_empty() {
        // Built without a projection in scope: fall back to
        // group-keys-first, then aggregates.
        for index in 0..group_by.len() {
            columns.extend(key_column(index));
        }
        for index in 0..aggregates.len() {
            columns.extend(agg_column(index));
        }
    } else {
        // Emit columns in the recorded SELECT-list order.
        for slot in output_order {
            match slot {
                AggOutputSlot::GroupKey(index) => columns.extend(key_column(*index)),
                AggOutputSlot::Aggregate(index) => columns.extend(agg_column(*index)),
            }
        }
    }
    OutputSchema {
        columns,
        is_star: false,
        cp_computed: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{NoCatalog, TypedCatalog, agg_expr, metrics_column, scan_plan};
    use super::super::schema::build_output_schema;
    use crate::control::server::response_shape::types::DdlColType;
    use nodedb_sql::types::SqlPlan;
    use nodedb_sql::types_expr::SqlExpr;
    #[test]
    fn aggregate_outputs_group_keys_then_aggregates_in_order() {
        use nodedb_sql::types::query::AggregateExpr;

        let plans = vec![SqlPlan::Aggregate {
            input: Box::new(scan_plan("orders", vec![])),
            group_by: vec![SqlExpr::Column {
                table: None,
                name: "status".to_string(),
            }],
            // `SELECT status AS state ...` — the group-key output name is the
            // SELECT-list alias, while the value lookup key stays the raw
            // grouped column name.
            group_by_aliases: vec![Some("state".to_string())],
            // Empty output_order exercises the group-keys-first fallback.
            output_order: Vec::new(),
            aggregates: vec![
                AggregateExpr {
                    function: "sum".to_string(),
                    args: vec![SqlExpr::Column {
                        table: None,
                        name: "x".to_string(),
                    }],
                    alias: "total".to_string(),
                    distinct: false,
                    grouping_col_index: None,
                },
                AggregateExpr {
                    function: "count".to_string(),
                    args: vec![SqlExpr::Wildcard],
                    alias: "count(*)".to_string(),
                    distinct: false,
                    grouping_col_index: None,
                },
            ],
            having: Vec::new(),
            limit: 0,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }];

        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_eq!(schema.columns.len(), 3);
        // Group-key display_name is the SELECT-list alias; lookup_key stays
        // the raw grouped column name (the executor's emitted value key).
        assert_eq!(schema.columns[0].display_name, "state");
        assert_eq!(schema.columns[0].lookup_key, "status");
        assert_eq!(schema.columns[1].display_name, "total");
        assert_eq!(schema.columns[1].lookup_key, "total");
        // sum(x) stays TEXT here because this test has no catalog, so the
        // argument's numeric type is unresolvable and falls back to TEXT.
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        assert_eq!(schema.columns[2].display_name, "count(*)");
        assert_eq!(schema.columns[2].lookup_key, "count(*)");
        // count(*) is always Postgres bigint (Int8), independent of catalog.
        assert_eq!(schema.columns[2].ty, DdlColType::Int8);
        assert!(!schema.is_star);
    }

    /// Group keys and aggregate arguments retain catalog types. Computed keys remain Text.
    #[test]
    fn aggregate_types_resolve_against_catalog() {
        let plans = vec![SqlPlan::Aggregate {
            input: Box::new(scan_plan("metrics", vec![])),
            group_by: vec![metrics_column("region")],
            group_by_aliases: vec![None],
            output_order: Vec::new(),
            aggregates: vec![
                agg_expr("count", vec![SqlExpr::Wildcard], "count(*)"),
                agg_expr("min", vec![metrics_column("n")], "min_n"),
                agg_expr("sum", vec![metrics_column("amount")], "sum_amount"),
                agg_expr("sum", vec![metrics_column("n")], "sum_n"),
            ],
            having: Vec::new(),
            limit: 0,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        );
        // group-keys-first fallback (empty output_order): region, then aggs.
        assert_eq!(schema.columns.len(), 5);
        // GROUP BY text column -> the text column's catalog type.
        assert_eq!(schema.columns[0].display_name, "region");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
        // COUNT(*) -> Int8 (Postgres bigint).
        assert_eq!(schema.columns[1].display_name, "count(*)");
        assert_eq!(schema.columns[1].ty, DdlColType::Int8);
        // MIN(int_col) preserves the integer input type.
        assert_eq!(schema.columns[2].display_name, "min_n");
        assert_eq!(schema.columns[2].ty, DdlColType::Int8);
        // SUM(float_col) -> Float8.
        assert_eq!(schema.columns[3].display_name, "sum_amount");
        assert_eq!(schema.columns[3].ty, DdlColType::Float8);
        // SUM(int_col) stays Text (numeric promotion, no regression).
        assert_eq!(schema.columns[4].display_name, "sum_n");
        assert_eq!(schema.columns[4].ty, DdlColType::Text);
    }

    /// A computed GROUP BY key (`UPPER(region)`) is not a bare column, so it
    /// defaults to Text.
    #[test]
    fn computed_group_by_key_is_text() {
        let upper = SqlExpr::Function {
            name: "upper".to_string(),
            args: vec![metrics_column("region")],
            distinct: false,
        };
        let plans = vec![SqlPlan::Aggregate {
            input: Box::new(scan_plan("metrics", vec![])),
            group_by: vec![upper],
            group_by_aliases: vec![Some("u".to_string())],
            output_order: Vec::new(),
            aggregates: Vec::new(),
            having: Vec::new(),
            limit: 0,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        );
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "u");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
    }
}
