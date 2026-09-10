// SPDX-License-Identifier: BUSL-1.1

//! Derives the planner-authoritative [`OutputSchema`] from a compiled
//! `SqlPlan` list, threaded into response shaping so the pgwire encoder can
//! advertise correct RowDescription type OIDs.
//!
//! A read plan announces its projection. A write plan announces the columns
//! its `RETURNING` clause projects, and nothing when it carries none — see
//! [`build_returning_schema`](super::returning::build_returning_schema).

use std::collections::HashMap;

use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::query::AggOutputSlot;

use crate::control::planner::sql_plan_convert::aggregate::agg_expr_to_pair;
use crate::control::planner::sql_plan_convert::lateral::collection_name_from_plan;
use crate::control::planner::sql_plan_convert::output_schema_types::infer_aggregate_type;
use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;

use super::columns::{
    column_types_for, group_by_key_column, ordered_columns_for, schema_from_projection,
};
use super::returning::build_returning_schema;

/// Derives the planner-authoritative output schema of a compiled plan list.
///
/// A read plan announces the columns its projection names. A write plan
/// announces the columns `returning` projects: the clause is stripped from the
/// statement text before planning, so the plan itself carries no column list
/// and the caller supplies the parsed spec. `None` means the statement carries
/// no `RETURNING` clause, and a write then announces nothing.
pub fn build_output_schema<C: SqlCatalog + ?Sized>(
    plans: &[SqlPlan],
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    returning: Option<&ReturningSpec>,
) -> OutputSchema {
    let Some(plan) = plans.first() else {
        return OutputSchema {
            columns: Vec::new(),
            is_star: false,
        };
    };

    match plan {
        // A grouped timeseries scan announces the columns its aggregate
        // encoder emits, in that encoder's order: each GROUP BY key, then
        // each aggregate. A GROUP BY key carries its own catalog type, so one
        // stored instant renders the same grouped as it does through a plain
        // `SELECT`. An aggregate result stays `Text`: the timeseries plan
        // carries no SELECT-list alias for it, so its type cannot be resolved
        // with certainty.
        //
        // A `time_bucket` query is excluded: its encoder prepends a `bucket`
        // boundary column that the plan's GROUP BY list does not name, so the
        // announced shape would not be the emitted one.
        SqlPlan::TimeseriesScan {
            collection,
            group_by,
            aggregates,
            bucket_interval_ms,
            ..
        } if !group_by.is_empty() && *bucket_interval_ms == 0 => {
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
            }
        }
        SqlPlan::Scan {
            collection,
            projection,
            ..
        }
        | SqlPlan::DocumentIndexLookup {
            collection,
            projection,
            ..
        }
        | SqlPlan::SpatialScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::TimeseriesScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::PointGet {
            collection,
            projection,
            ..
        }
        | SqlPlan::RangeScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::RecursiveScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::VectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::MultiVectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::SparseSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::TextSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::HybridSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::HybridSearchTriple {
            collection,
            projection,
            ..
        } => {
            let types = column_types_for(catalog, database_id, collection);
            let ordered_cols = ordered_columns_for(catalog, database_id, collection);
            schema_from_projection(projection, &types, &ordered_cols)
        }
        SqlPlan::Join {
            left,
            right,
            projection,
            ..
        } => {
            // Each projected column resolves against the catalog of the side
            // it came from, keyed on the qualified name the join executor
            // emits (`orders.ts`). A bare name two sides declare differently
            // cannot be attributed and stays `Text`. A star here has no
            // single catalog to expand against, so no ordered columns are
            // supplied.
            let types = super::join_types::join_column_types(left, right, catalog, database_id);
            schema_from_projection(projection, &types, &[])
        }
        SqlPlan::ConstantResult { columns, .. } => OutputSchema {
            columns: columns
                .iter()
                .map(|c| OutputColumn {
                    display_name: c.clone(),
                    lookup_key: c.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            ..
        } => {
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
            }
        }
        // Set operations take their column names/types from the first
        // (left) branch, matching standard SQL set-op semantics.
        SqlPlan::Union { inputs, .. } => match inputs.first() {
            Some(first) => {
                build_output_schema(std::slice::from_ref(first), catalog, database_id, returning)
            }
            None => OutputSchema::default(),
        },
        SqlPlan::Intersect { left, .. } | SqlPlan::Except { left, .. } => build_output_schema(
            std::slice::from_ref(left.as_ref()),
            catalog,
            database_id,
            returning,
        ),
        SqlPlan::RecursiveValue { columns, .. } => OutputSchema {
            columns: columns
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        // The outer query determines the final projected shape; the CTE
        // definitions themselves are only inputs to it.
        SqlPlan::Cte { outer, .. } => build_output_schema(
            std::slice::from_ref(outer.as_ref()),
            catalog,
            database_id,
            returning,
        ),
        // A post-processor's projected shape is its outer projection; an empty
        // projection (SELECT *) inherits the body's columns. (Synthesized during
        // conversion, so this is normally unreached — the schema is derived from
        // the pre-inline `Cte` above — but the arm keeps derivation correct if a
        // `Subquery` is ever schema-derived directly.)
        SqlPlan::Subquery {
            input, projection, ..
        } => {
            if projection.is_empty() {
                build_output_schema(
                    std::slice::from_ref(input.as_ref()),
                    catalog,
                    database_id,
                    returning,
                )
            } else {
                let types = HashMap::new();
                schema_from_projection(projection, &types, &[])
            }
        }
        SqlPlan::LateralTopK { projection, .. } | SqlPlan::LateralLoop { projection, .. } => {
            // No single source collection spans both outer and inner rows;
            // default every projected field to `Text` rather than picking
            // one side's catalog arbitrarily. A star has no single catalog to
            // expand against, so no ordered columns are supplied.
            let types = HashMap::new();
            schema_from_projection(projection, &types, &[])
        }
        SqlPlan::ArraySlice {
            attr_projection, ..
        }
        | SqlPlan::ArrayProject {
            attr_projection, ..
        } => OutputSchema {
            columns: attr_projection
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        // A write announces exactly what its `RETURNING` clause projects, from
        // the target collection's declared columns. `RETURNING` is a
        // projection, so it is typed like one: a `SELECT ts, host, v` and an
        // `INSERT ... RETURNING ts, host, v` announce the same three types and
        // render the same stored row identically.
        SqlPlan::Insert { collection, .. }
        | SqlPlan::KvInsert { collection, .. }
        | SqlPlan::Upsert { collection, .. }
        | SqlPlan::Update { collection, .. }
        | SqlPlan::UpdateFrom { collection, .. }
        | SqlPlan::Delete { collection, .. }
        | SqlPlan::TimeseriesIngest { collection, .. }
        | SqlPlan::VectorPrimaryInsert { collection, .. } => {
            build_returning_schema(returning, collection, catalog, database_id)
        }
        // Same rule, for the two writes that name their target `target`.
        SqlPlan::Merge { target, .. } | SqlPlan::InsertSelect { target, .. } => {
            build_returning_schema(returning, target, catalog, database_id)
        }
        // No rows to shape. `TRUNCATE`, index DDL, and the whole `CREATE ARRAY`
        // family answer with a command tag; the array DML ops answer with an
        // affected count, and `inject_returning_spec` attaches no spec to them,
        // so announcing columns for one would hold a count payload to a row
        // shape it does not have.
        SqlPlan::Truncate { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. } => OutputSchema::default(),
        // `ArrayAgg` / `ArrayElementwise` compile to `ArrayOp::Aggregate` /
        // `ArrayOp::Elementwise`, which `describe_plan` classifies as
        // `PlanKind::MultiRow`. `MultiRow` responses are shaped by
        // `shape_generic_rows` -> `shape_decoded_rows`, which derives column
        // names from the decoded JSON payload itself, not from
        // `OutputSchema`. An empty schema here is therefore correct.
        SqlPlan::ArrayAgg { .. } | SqlPlan::ArrayElementwise { .. } => OutputSchema::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::query::Projection;
    use nodedb_sql::types_expr::SqlExpr;

    /// Catalog stub whose `get_collection` is never called by the
    /// `ConstantResult` branch under test; only required to satisfy the
    /// generic `SqlCatalog` bound on `build_output_schema`.
    struct NoCatalog;

    impl SqlCatalog for NoCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            Ok(None)
        }
    }

    #[test]
    fn constant_result_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::ConstantResult {
            columns: vec!["a".to_string(), "b".to_string()],
            values: vec![],
            volatile: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].display_name, "a");
        assert_eq!(schema.columns[0].lookup_key, "a");
        assert_eq!(schema.columns[1].display_name, "b");
        assert!(!schema.is_star);
    }

    /// Minimal `Scan` plan against `collection`, used only to exercise
    /// recursion (Union/Intersect/Except/Cte); the catalog is `NoCatalog`
    /// so every column falls back to `DdlColType::Text`.
    fn scan_plan(collection: &str, projection: Vec<Projection>) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: None,
            engine: nodedb_sql::types::query::EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection,
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        }
    }

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

    #[test]
    fn union_takes_schema_from_first_input() {
        let plans = vec![SqlPlan::Union {
            inputs: vec![
                scan_plan("a", vec![Projection::Column("id".to_string())]),
                scan_plan("b", vec![Projection::Column("other".to_string())]),
            ],
            distinct: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "id");
    }

    #[test]
    fn recursive_value_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::RecursiveValue {
            cte_name: "c".to_string(),
            columns: vec!["n".to_string()],
            init_exprs: vec!["1".to_string()],
            step_exprs: vec!["n + 1".to_string()],
            condition: None,
            max_depth: 100,
            distinct: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "n");
        assert_eq!(schema.columns[0].lookup_key, "n");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
        assert!(!schema.is_star);
    }

    /// Shared projection for the leaf-variant tests below: a bare `id`
    /// column plus a computed `dist` alias.
    fn id_and_dist_projection() -> Vec<Projection> {
        vec![
            Projection::Column("id".to_string()),
            Projection::Computed {
                expr: SqlExpr::Wildcard,
                alias: "dist".to_string(),
            },
        ]
    }

    fn assert_id_and_dist_schema(schema: &OutputSchema) {
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].display_name, "id");
        assert_eq!(schema.columns[0].lookup_key, "id");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
        assert_eq!(schema.columns[1].display_name, "dist");
        assert_eq!(schema.columns[1].lookup_key, "dist");
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        assert!(!schema.is_star);
    }

    #[test]
    fn point_get_uses_its_own_projection() {
        let plans = vec![SqlPlan::PointGet {
            collection: "users".to_string(),
            alias: None,
            engine: nodedb_sql::types::query::EngineType::DocumentSchemaless,
            key_column: "id".to_string(),
            key_value: nodedb_sql::types_expr::SqlValue::Null,
            projection: id_and_dist_projection(),
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn vector_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::VectorSearch {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            query_vector: vec![0.0, 1.0],
            top_k: 10,
            ef_search: 64,
            metric: nodedb_sql::types::DistanceMetric::L2,
            filters: Vec::new(),
            array_prefilter: None,
            ann_options: nodedb_sql::types::VectorAnnOptions::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            projection: id_and_dist_projection(),
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn hybrid_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::HybridSearch {
            collection: "docs".to_string(),
            query_vector: vec![0.0, 1.0],
            query_text: "hello".to_string(),
            top_k: 10,
            ef_search: 64,
            vector_weight: 0.5,
            fuzzy: false,
            score_alias: None,
            projection: id_and_dist_projection(),
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn text_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::TextSearch {
            collection: "docs".to_string(),
            query: nodedb_sql::types::FtsQuery::Plain {
                text: "hello".to_string(),
                fuzzy: false,
            },
            top_k: 10,
            filters: Vec::new(),
            score_alias: None,
            projection: id_and_dist_projection(),
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_id_and_dist_schema(&schema);
    }

    /// Catalog stub exposing a single `metrics` collection with a text
    /// `region`, integer `n`, and float `amount` column — used to exercise
    /// catalog-backed type resolution for GROUP BY keys, aggregate arguments,
    /// and bare-column computed projections.
    struct TypedCatalog;

    impl SqlCatalog for TypedCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            use nodedb_sql::types::collection::ColumnInfo;
            use nodedb_sql::types::query::EngineType;
            use nodedb_sql::types_expr::SqlDataType;

            if name != "metrics" {
                return Ok(None);
            }
            let col = |n: &str, t: SqlDataType| ColumnInfo {
                name: n.to_string(),
                data_type: t,
                nullable: true,
                is_primary_key: false,
                default: None,
                raw_type: None,
                int_width: None,
                float_width: None,
            };
            Ok(Some(nodedb_sql::types::CollectionInfo {
                name: "metrics".to_string(),
                engine: EngineType::DocumentStrict,
                columns: vec![
                    col("region", SqlDataType::String),
                    col("n", SqlDataType::Int64),
                    col("amount", SqlDataType::Float64),
                ],
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: nodedb_sql::types::CollectionInfo::open_schema_for(
                    EngineType::DocumentStrict,
                ),
            }))
        }
    }

    fn agg_expr(
        function: &str,
        args: Vec<SqlExpr>,
        alias: &str,
    ) -> nodedb_sql::types::query::AggregateExpr {
        nodedb_sql::types::query::AggregateExpr {
            function: function.to_string(),
            args,
            alias: alias.to_string(),
            distinct: false,
            grouping_col_index: None,
        }
    }

    fn metrics_column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.to_string(),
        }
    }

    /// GROUP BY a bare text column plus MIN/SUM/COUNT aggregates resolve to
    /// their real catalog-derived types, while SUM over an integer column and
    /// a computed GROUP BY key stay Text.
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

    /// A computed SELECT expression that is really a bare column reference
    /// carries that column's catalog type; a boolean comparison is `Bool`.
    #[test]
    fn computed_projection_types_resolve_against_catalog() {
        let projection = vec![
            Projection::Computed {
                expr: metrics_column("n"),
                alias: "aliased_n".to_string(),
            },
            Projection::Computed {
                expr: SqlExpr::BinaryOp {
                    left: Box::new(metrics_column("n")),
                    op: nodedb_sql::types_expr::BinaryOp::Gt,
                    right: Box::new(SqlExpr::Literal(nodedb_sql::types_expr::SqlValue::Int(0))),
                },
                alias: "positive".to_string(),
            },
        ];
        let plans = vec![scan_plan("metrics", projection)];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        );
        assert_eq!(schema.columns.len(), 2);
        // Bare-column-passthrough computed expr -> the column's catalog type.
        assert_eq!(schema.columns[0].display_name, "aliased_n");
        assert_eq!(schema.columns[0].lookup_key, "n");
        assert_eq!(schema.columns[0].ty, DdlColType::Int8);
        // Boolean comparison expression -> Bool.
        assert_eq!(schema.columns[1].display_name, "positive");
        assert_eq!(schema.columns[1].ty, DdlColType::Bool);
    }
}
