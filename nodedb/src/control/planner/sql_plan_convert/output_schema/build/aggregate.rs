// SPDX-License-Identifier: BUSL-1.1

//! Aggregate output ordering and catalog-derived types.

use super::super::columns::{column_types_for, group_by_key_column};
use crate::control::planner::sql_plan_convert::lateral::collection_name_from_plan;
use crate::control::planner::sql_plan_convert::output_schema_types::{
    infer_aggregate_type, time_bucket_type,
};
use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;
use nodedb_sql::SqlError;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::{AggOutputSlot, AggregateExpr};
use nodedb_sql::types::{SqlPlan, TimeseriesAggOutput, TimeseriesScanPlan};
use nodedb_sql::types_expr::SqlExpr;
use std::collections::HashMap;

/// The schema of an aggregating timeseries scan, grouped, bucketed, or not.
///
/// Columns follow the plan's recorded SELECT-list order, under their
/// SELECT-list names. The scan writes each group row under those names, so
/// each column reads its own name. Each column is typed exactly as
/// [`aggregate_schema`] types the generic aggregate.
///
/// A plan with no recorded order falls back to the encoder's own order and
/// keys: `bucket`, each GROUP BY column, then each aggregate's canonical key.
pub(super) fn timeseries_group_schema<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    scan: &TimeseriesScanPlan,
) -> Result<OutputSchema, SqlError> {
    let types = column_types_for(catalog, database_id, &scan.collection)?;
    let renamed = !scan.output.is_empty();
    let fallback;
    let output: &[TimeseriesAggOutput] = if renamed {
        &scan.output
    } else {
        fallback = encoder_order(scan);
        &fallback
    };
    let mut columns = Vec::with_capacity(output.len());
    for slot in output {
        let Some((key, name)) = slot.key_and_name(&scan.aggregates) else {
            continue;
        };
        let ty = match slot {
            // The bucket renders in the kind of the column it buckets.
            TimeseriesAggOutput::Bucket { source, .. } => {
                time_bucket_type(source.as_deref().and_then(|s| types.get(s).copied()))
            }
            TimeseriesAggOutput::GroupColumn { name, .. } => {
                types.get(name).copied().unwrap_or(DdlColType::Text)
            }
            TimeseriesAggOutput::Aggregate(index) => {
                scan.aggregates.get(*index).map_or(DdlColType::Text, |agg| {
                    gap_filled_type(scan, agg, infer_aggregate_type(agg, &types))
                })
            }
        };
        columns.push(OutputColumn {
            lookup_key: if renamed { name.clone() } else { key },
            display_name: name,
            ty,
        });
    }
    Ok(OutputSchema {
        columns,
        is_star: false,
        cp_computed: Vec::new(),
        declared_key: None,
    })
}

/// The type of aggregate `agg` of `scan`, typed `ty`, when its buckets are
/// gap-filled.
///
/// A `prev`, `linear` or literal fill writes the neighbouring buckets' AVG,
/// or the literal, into each value aggregate of a filled bucket. Over an
/// integer column that AVG is a numeric, so an integer-typed value
/// aggregate is `Numeric`: an integer cell renders as valid `numeric` text
/// too. A count stays a count. A `null` fill, or none, keeps `ty`.
fn gap_filled_type(scan: &TimeseriesScanPlan, agg: &AggregateExpr, ty: DdlColType) -> DdlColType {
    if matches!(agg.function.as_str(), "count" | "approx_count_distinct") {
        return ty;
    }
    use crate::engine::timeseries::gap_fill::GapFillStrategy;
    let fills_values = matches!(
        GapFillStrategy::parse(&scan.gap_fill),
        Some(GapFillStrategy::Prev | GapFillStrategy::Linear | GapFillStrategy::Literal(_))
    );
    match ty {
        DdlColType::Int2 | DdlColType::Int4 | DdlColType::Int8 if fills_values => {
            DdlColType::Numeric
        }
        other => other,
    }
}

/// The columns a native grouped timeseries scan encodes, in its own order:
/// the bucket, each GROUP BY column, then each aggregate.
fn encoder_order(scan: &TimeseriesScanPlan) -> Vec<TimeseriesAggOutput> {
    let bucket = (scan.bucket_interval_ms > 0).then_some(TimeseriesAggOutput::Bucket {
        alias: None,
        source: None,
    });
    let groups = scan
        .group_by
        .iter()
        .map(|name| TimeseriesAggOutput::GroupColumn {
            name: name.clone(),
            alias: None,
        });
    let aggregates = (0..scan.aggregates.len()).map(TimeseriesAggOutput::Aggregate);
    bucket.into_iter().chain(groups).chain(aggregates).collect()
}

/// The collection whose catalog types the aggregate's input columns.
///
/// A timeseries scan feeds the generic aggregate whenever the native grouped
/// scan cannot compute the query, and its rows carry the collection's types.
fn aggregate_input_collection(input: &SqlPlan) -> Option<String> {
    match input {
        SqlPlan::TimeseriesScan(TimeseriesScanPlan { collection, .. }) => Some(collection.clone()),
        other => collection_name_from_plan(other),
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
) -> Result<OutputSchema, SqlError> {
    let AggregateShape {
        input,
        group_by,
        group_by_aliases,
        output_order,
        aggregates,
    } = shape;
    // Catalog types of the aggregate's underlying columns, resolved from
    // the input plan's single source collection when it has one (Scan /
    // point-get / timeseries-scan style). GROUP BY bare keys and
    // MIN/MAX/SUM/AVG argument columns are typed against this; anything
    // unresolvable stays Text.
    let types = match aggregate_input_collection(input) {
        Some(collection) => column_types_for(catalog, database_id, &collection)?,
        None => HashMap::new(),
    };
    // Derives the `OutputColumn` for one GROUP BY key: `group_by_aliases`
    // is parallel to `group_by` when populated, but may be empty when the
    // plan was built without a projection in scope — treat a
    // missing/`None` entry as "no alias".
    // Each key's row key is decided by the rule the aggregate's rename
    // applies, so an alias another cell already holds reads its row name.
    let row_names: Vec<String> = group_by
        .iter()
        .enumerate()
        .map(|(index, key)| nodedb_sql::planner::agg_naming::group_key_row_name(key, index))
        .collect();
    let aggregate_names: Vec<&str> = aggregates.iter().map(|agg| agg.alias.as_str()).collect();
    let final_names = nodedb_sql::planner::agg_naming::group_key_final_names(
        &row_names,
        group_by_aliases,
        &aggregate_names,
    );
    let key_column = |index: usize| {
        group_by
            .get(index)
            .zip(final_names.get(index))
            .map(|(key, final_name)| {
                let alias = group_by_aliases.get(index).and_then(|a| a.as_deref());
                group_by_key_column(key, index, alias, final_name, &types)
            })
    };
    // Derives the `OutputColumn` for one aggregate. `AggregateExpr::alias`
    // is always populated by the planner: either the user's explicit
    // alias, or (for unnamed projections) the lowercased unparsed
    // expression text — e.g. `count(*)` — matching this module's own
    // lowercasing of non-column expressions. So the alias is already the
    // canonical name; no separate derivation needed. The result type is
    // inferred conservatively (COUNT -> Int8, MIN/MAX preserve the input
    // column type, SUM of a float -> Float8, AVG of a float -> Float8 and of
    // anything else -> Numeric, else Text).
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
    Ok(OutputSchema {
        columns,
        is_star: false,
        cp_computed: Vec::new(),
        declared_key: None,
    })
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
            // SELECT-list alias.
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
                    filter_text: None,
                    arg_text: None,
                },
                AggregateExpr {
                    function: "count".to_string(),
                    args: vec![SqlExpr::Wildcard],
                    alias: "count(*)".to_string(),
                    distinct: false,
                    grouping_col_index: None,
                    filter_text: None,
                    arg_text: None,
                },
            ],
            having: Vec::new(),
            limit: 0,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }];

        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        assert_eq!(schema.columns.len(), 3);
        // An aliased group key shows and reads its alias: the aggregate
        // renames the key to it in every result row.
        assert_eq!(schema.columns[0].display_name, "state");
        assert_eq!(schema.columns[0].lookup_key, "state");
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
        )
        .expect("schema derives");
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
        // SUM(bigint_col) -> Numeric, as PostgreSQL's sum(int8).
        assert_eq!(schema.columns[4].display_name, "sum_n");
        assert_eq!(schema.columns[4].ty, DdlColType::Numeric);
    }

    fn timeseries_scan(
        output: Vec<nodedb_sql::types::TimeseriesAggOutput>,
    ) -> nodedb_sql::types::TimeseriesScanPlan {
        nodedb_sql::types::TimeseriesScanPlan {
            collection: "metrics".to_string(),
            time_range: (i64::MIN, i64::MAX),
            bucket_interval_ms: 3_600_000,
            group_by: vec!["region".to_string()],
            aggregates: vec![agg_expr("avg", vec![metrics_column("amount")], "mean")],
            filters: Vec::new(),
            having: Vec::new(),
            projection: Vec::new(),
            gap_fill: String::new(),
            limit: usize::MAX,
            sort_keys: Vec::new(),
            tiered: false,
            temporal: nodedb_sql::temporal::TemporalScope::default(),
            output,
            group_keys: Vec::new(),
        }
    }

    /// A native grouped timeseries scan announces SELECT-list order and
    /// aliases. Each column reads its own name, which the encoder writes, and
    /// takes the generic aggregate's type.
    #[test]
    fn timeseries_scan_follows_select_list_order_and_aliases() {
        use nodedb_sql::types::TimeseriesAggOutput;
        let plans = vec![SqlPlan::TimeseriesScan(timeseries_scan(vec![
            TimeseriesAggOutput::Aggregate(0),
            TimeseriesAggOutput::Bucket {
                alias: Some("b".to_string()),
                source: Some("n".to_string()),
            },
            TimeseriesAggOutput::GroupColumn {
                name: "region".to_string(),
                alias: Some("r".to_string()),
            },
        ]))];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        )
        .expect("schema derives");
        let shape: Vec<(&str, &str, DdlColType)> = schema
            .columns
            .iter()
            .map(|c| (c.display_name.as_str(), c.lookup_key.as_str(), c.ty))
            .collect();
        assert_eq!(
            shape,
            [
                ("mean", "mean", DdlColType::Float8),
                ("b", "b", DdlColType::Int8),
                ("r", "r", DdlColType::Text),
            ]
        );
    }

    /// With no recorded order the scan announces its encoder's order: the
    /// bucket, each GROUP BY column, then each aggregate.
    #[test]
    fn timeseries_scan_without_select_list_uses_encoder_order() {
        let plans = vec![SqlPlan::TimeseriesScan(timeseries_scan(Vec::new()))];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        )
        .expect("schema derives");
        let names: Vec<&str> = schema
            .columns
            .iter()
            .map(|c| c.display_name.as_str())
            .collect();
        assert_eq!(names, ["bucket", "region", "mean"]);
    }

    /// The generic aggregate over a timeseries scan types its columns from
    /// the scanned collection.
    #[test]
    fn aggregate_over_a_timeseries_scan_takes_catalog_types() {
        let plans = vec![SqlPlan::Aggregate {
            input: Box::new(SqlPlan::TimeseriesScan(timeseries_scan(Vec::new()))),
            group_by: vec![metrics_column("region")],
            group_by_aliases: vec![None],
            output_order: Vec::new(),
            aggregates: vec![agg_expr("avg", vec![metrics_column("amount")], "mean")],
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
        )
        .expect("schema derives");
        assert_eq!(schema.columns[1].display_name, "mean");
        assert_eq!(schema.columns[1].ty, DdlColType::Float8);
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
        )
        .expect("schema derives");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "u");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
    }
}
