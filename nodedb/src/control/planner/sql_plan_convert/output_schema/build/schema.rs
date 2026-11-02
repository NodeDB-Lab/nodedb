// SPDX-License-Identifier: BUSL-1.1

//! Derives the planner-authoritative [`OutputSchema`] from a compiled
//! `SqlPlan` list, threaded into response shaping so the pgwire encoder can
//! advertise correct RowDescription type OIDs.
//!
//! A read plan announces its projection. A write plan announces the columns
//! its `RETURNING` clause projects, and nothing when it carries none — see
//! [`build_returning_schema`](super::super::returning::build_returning_schema).

use std::collections::HashMap;

use nodedb_sql::SqlError;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types::{
    CtePlan, DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, InsertPlan,
    KvInsertPlan, LateralLoopPlan, LateralTopKPlan, MergePlan, RangeScanPlan, RecursiveScanPlan,
    RecursiveValuePlan, SqlPlan, TextSearchPlan, TimeseriesIngestPlan, TimeseriesScanPlan,
    UpsertPlan, VectorPrimaryDeletePlan, VectorPrimaryInsertPlan, VectorPrimaryUpdatePlan,
};

use crate::control::server::response_shape::schema::{
    OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type_with_width,
};
use crate::control::server::response_shape::types::DdlColType;

use super::super::columns::{
    column_types_for, declared_key_for, ordered_columns_for, schema_from_projection,
};
use super::super::cte_types::{CatalogRef, CteTypesCatalog};
use super::super::returning::build_returning_schema;
use super::aggregate::{AggregateShape, aggregate_schema, timeseries_group_schema};
use super::constant::constant_schema;

/// Derives the planner-authoritative output schema of a compiled plan list.
///
/// A read plan announces the columns its projection names. A write plan
/// announces the columns `returning` projects: the clause is stripped from the
/// statement text before planning, so the plan itself carries no column list
/// and the caller supplies the projection resolved against the target. `None`
/// means the statement carries no `RETURNING` clause, and a write then
/// announces nothing.
///
/// A catalog lookup error is returned. A schema derived past one names the
/// wrong identity column and the wrong types.
pub fn build_output_schema<C: SqlCatalog + ?Sized>(
    plans: &[SqlPlan],
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    returning: Option<&[Projection]>,
) -> Result<OutputSchema, SqlError> {
    let Some(plan) = plans.first() else {
        return Ok(OutputSchema::default());
    };

    Ok(match plan {
        // A grouped, bucketed, or aggregating timeseries scan announces its
        // SELECT-list columns in SELECT-list order, under their aliases, typed
        // as the generic aggregate types them. An ungrouped aggregate takes
        // this arm too: the raw-scan arm below announces no column for it.
        SqlPlan::TimeseriesScan(scan)
            if !scan.group_by.is_empty()
                || !scan.aggregates.is_empty()
                || scan.bucket_interval_ms > 0 =>
        {
            timeseries_group_schema(catalog, database_id, scan)?
        }
        SqlPlan::Scan {
            collection,
            projection,
            ..
        }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::SpatialScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::PointGet {
            collection,
            projection,
            ..
        }
        | SqlPlan::RangeScan(RangeScanPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::RecursiveScan(RecursiveScanPlan {
            collection,
            projection,
            ..
        })
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
        | SqlPlan::TextSearch(TextSearchPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::HybridSearch(HybridSearchPlan {
            collection,
            projection,
            ..
        })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
            collection,
            projection,
            ..
        }) => {
            let types = column_types_for(catalog, database_id, collection)?;
            let ordered_cols = ordered_columns_for(catalog, database_id, collection)?;
            OutputSchema {
                declared_key: declared_key_for(catalog, database_id, collection)?,
                ..schema_from_projection(projection, &types, &ordered_cols)
            }
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
            let types =
                super::super::join_types::join_column_types(left, right, catalog, database_id)?;
            schema_from_projection(projection, &types, &[])
        }
        SqlPlan::ConstantResult {
            columns, values, ..
        } => constant_schema(columns, values),
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            ..
        } => aggregate_schema(
            catalog,
            database_id,
            AggregateShape {
                input,
                group_by,
                group_by_aliases,
                output_order,
                aggregates,
            },
        )?,
        // Set operations take their column names/types from the first
        // (left) branch, matching standard SQL set-op semantics.
        SqlPlan::Union { inputs, .. } => match inputs.first() {
            Some(first) => {
                build_output_schema(std::slice::from_ref(first), catalog, database_id, returning)?
            }
            None => OutputSchema::default(),
        },
        SqlPlan::Intersect { left, .. } | SqlPlan::Except { left, .. } => build_output_schema(
            std::slice::from_ref(left.as_ref()),
            catalog,
            database_id,
            returning,
        )?,
        SqlPlan::RecursiveValue(RecursiveValuePlan { columns, .. }) => OutputSchema {
            columns: columns
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
            cp_computed: Vec::new(),
            declared_key: None,
        },
        // The outer query determines the final projected shape. It reads each
        // definition by name, and each name resolves to the columns its body
        // announces, so a column read through a CTE or a derived table keeps
        // the body's type.
        SqlPlan::Cte(CtePlan { definitions, outer }) => {
            let stored = CatalogRef(catalog);
            let overlay = CteTypesCatalog::build(&stored, definitions, database_id)?;
            let schema = build_output_schema(
                std::slice::from_ref(outer.as_ref()),
                &overlay,
                database_id,
                returning,
            )?;
            // An outer query that runs as a body unchanged reads each column
            // under the key the body's rows carry it.
            overlay.read_through(outer, schema)
        }
        // A post-processor's projected shape is its outer projection over the
        // body's announced columns. An empty projection (SELECT *) inherits
        // the body's columns.
        SqlPlan::Subquery {
            input, projection, ..
        } => {
            let body = build_output_schema(
                std::slice::from_ref(input.as_ref()),
                catalog,
                database_id,
                returning,
            )?;
            if projection.is_empty() {
                body
            } else {
                // The tail names a body column by its output name or by the
                // key the body emits it under. A name announced twice keeps
                // its first type.
                let mut types = HashMap::new();
                for column in &body.columns {
                    types
                        .entry(column.display_name.clone())
                        .or_insert(column.ty);
                    types.entry(column.lookup_key.clone()).or_insert(column.ty);
                }
                schema_from_projection(projection, &types, &body.columns)
            }
        }
        SqlPlan::LateralTopK(LateralTopKPlan { projection, .. })
        | SqlPlan::LateralLoop(LateralLoopPlan { projection, .. }) => {
            // No single source collection spans both outer and inner rows;
            // default every projected field to `Text` rather than picking
            // one side's catalog arbitrarily. A star has no single catalog to
            // expand against, so no ordered columns are supplied.
            let types = HashMap::new();
            schema_from_projection(projection, &types, &[])
        }
        // A cell read announces the columns its rows carry: every dim, then
        // the requested attrs, each typed from the array schema.
        SqlPlan::ArraySlice(slice) => {
            let view = array_view(catalog, &slice.name)?;
            array_columns_schema(&nodedb_sql::resolver::array_tvf::array_slice_columns(
                &view, slice,
            ))
        }
        // A write announces exactly what its `RETURNING` clause projects, from
        // the target collection's declared columns. `RETURNING` is a
        // projection, so it is typed like one: a `SELECT ts, host, v` and an
        // `INSERT ... RETURNING ts, host, v` announce the same three types and
        // render the same stored row identically.
        SqlPlan::Insert(InsertPlan { collection, .. })
        | SqlPlan::KvInsert(KvInsertPlan { collection, .. })
        | SqlPlan::Upsert(UpsertPlan { collection, .. })
        | SqlPlan::Update { collection, .. }
        | SqlPlan::UpdateFrom { collection, .. }
        | SqlPlan::Delete { collection, .. }
        | SqlPlan::TimeseriesIngest(TimeseriesIngestPlan { collection, .. })
        | SqlPlan::VectorPrimaryInsert(VectorPrimaryInsertPlan { collection, .. })
        | SqlPlan::VectorPrimaryDelete(VectorPrimaryDeletePlan { collection, .. })
        | SqlPlan::VectorPrimaryUpdate(VectorPrimaryUpdatePlan { collection, .. }) => {
            build_returning_schema(returning, collection, catalog, database_id)?
        }
        // Same rule, for the two writes that name their target `target`.
        SqlPlan::Merge(MergePlan { target, .. }) | SqlPlan::InsertSelect { target, .. } => {
            build_returning_schema(returning, target, catalog, database_id)?
        }
        // No rows to shape. `TRUNCATE`, index DDL, and the whole `CREATE ARRAY`
        // family answer with a command tag; the array DML ops answer with an
        // affected count, and `inject_returning_spec` attaches no spec to them,
        // so announcing columns for one would hold a count payload to a row
        // shape it does not have.
        SqlPlan::Truncate { .. }
        | SqlPlan::VectorPrimaryTruncate { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. } => OutputSchema::default(),
        // An aggregate announces `group`, typed as its group dim the way a
        // slice types that dim, then `result`, typed by the reducer. A
        // temporal aggregate ends with a summary row, so it also announces
        // the row's `truncated_before_horizon` flag.
        SqlPlan::ArrayAgg(agg) => {
            let view = array_view(catalog, &agg.name)?;
            array_columns_schema(&nodedb_sql::resolver::array_tvf::array_agg_row_columns(
                &view, agg,
            )?)
        }
        // `ArrayElementwise` compiles to `ClusterArrayOp::Elementwise`, which
        // `describe_plan` classifies as `PlanKind::MultiRow`. `MultiRow`
        // responses are shaped by `shape_generic_rows` ->
        // `shape_decoded_rows`, which derives column names from the decoded
        // payload when the schema is empty.
        SqlPlan::ArrayElementwise { .. } => OutputSchema::default(),
    })
}

/// The array `name` names, or the unknown-table error a plan over a dropped
/// array reports.
fn array_view<C: SqlCatalog + ?Sized>(
    catalog: &C,
    name: &str,
) -> Result<nodedb_sql::types::ArrayCatalogView, SqlError> {
    catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })
}

/// One output column per relation column, in order, under its own name and
/// its wire type. A narrow attr announces its own width: `int2`, `int4`, or
/// `float4`.
fn array_columns_schema(columns: &[nodedb_sql::types::ColumnInfo]) -> OutputSchema {
    OutputSchema {
        columns: columns
            .iter()
            .map(|column| OutputColumn {
                display_name: column.name.clone(),
                lookup_key: column.name.clone(),
                ty: sql_data_type_to_ddl_col_type_with_width(
                    &column.data_type,
                    column.int_width,
                    column.float_width,
                ),
            })
            .collect(),
        is_star: false,
        cp_computed: Vec::new(),
        declared_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{
        FailingCatalog, NoCatalog, TypedCatalog, metrics_column, scan_plan,
    };
    use super::build_output_schema;
    use crate::control::server::response_shape::schema::OutputSchema;
    use crate::control::server::response_shape::types::DdlColType;
    use nodedb_sql::types::query::Projection;
    use nodedb_sql::types::{HybridSearchPlan, RecursiveValuePlan, SqlPlan, TextSearchPlan};
    use nodedb_sql::types_expr::SqlExpr;
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
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "id");
    }

    #[test]
    fn recursive_value_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::RecursiveValue(RecursiveValuePlan {
            cte_name: "c".to_string(),
            columns: vec!["n".to_string()],
            init_exprs: vec!["1".to_string()],
            step_exprs: vec!["n + 1".to_string()],
            condition: None,
            max_depth: 100,
            distinct: false,
        })];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
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
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
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
            pk_prefilter: None,
            projection: id_and_dist_projection(),
            temporal: nodedb_sql::TemporalScope::default(),
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn hybrid_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::HybridSearch(HybridSearchPlan {
            collection: "docs".to_string(),
            vector_field: "emb".to_string(),
            query_vector: vec![0.0, 1.0],
            text_field: None,
            query_text: "hello".to_string(),
            filters: Vec::new(),
            top_k: 10,
            ef_search: 64,
            vector_weight: 0.5,
            mode: nodedb_types::text_search::QueryMode::Or,
            fuzzy: false,
            score_alias: None,
            projection: id_and_dist_projection(),
            temporal: nodedb_sql::TemporalScope::default(),
        })];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn text_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::TextSearch(TextSearchPlan {
            collection: "docs".to_string(),
            shape: nodedb_sql::types::TextSearchShape::Match {
                field: None,
                query: nodedb_sql::types::FtsQuery::Plain {
                    text: "hello".to_string(),
                    fuzzy: false,
                },
                mode: nodedb_types::text_search::QueryMode::Or,
                top_k: Some(10),
            },
            filters: Vec::new(),
            scores: Vec::new(),
            projection: id_and_dist_projection(),
            temporal: nodedb_sql::TemporalScope::default(),
        })];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        assert_id_and_dist_schema(&schema);
    }

    /// A catalog lookup error reaches the caller. Read as "no collection",
    /// it renders the identity under `id` and types every column `Text`.
    #[test]
    fn a_catalog_error_is_returned_not_swallowed() {
        let plans = vec![scan_plan("items", vec![Projection::Star])];
        let err = build_output_schema(
            &plans,
            &FailingCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        )
        .expect_err("a catalog error is returned");
        assert!(
            matches!(err, nodedb_sql::SqlError::RetryableSchemaChanged { .. }),
            "{err:?}"
        );
    }

    /// Computed column projections retain catalog types. Boolean comparisons use Bool.
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
        )
        .expect("schema derives");
        assert_eq!(schema.columns.len(), 2);
        // Bare-column-passthrough computed expr -> the column's catalog type.
        assert_eq!(schema.columns[0].display_name, "aliased_n");
        assert_eq!(schema.columns[0].lookup_key, "n");
        assert_eq!(schema.columns[0].ty, DdlColType::Int8);
        // Boolean comparison expression -> Bool.
        assert_eq!(schema.columns[1].display_name, "positive");
        assert_eq!(schema.columns[1].ty, DdlColType::Bool);
    }

    fn columns(names: &[&str]) -> Vec<Projection> {
        names
            .iter()
            .map(|name| Projection::Column((*name).to_string()))
            .collect()
    }

    fn types_of(schema: &OutputSchema) -> Vec<DdlColType> {
        schema.columns.iter().map(|c| c.ty).collect()
    }

    /// A column read through a CTE carries the type its definition body
    /// announces. A later definition reads an earlier one with its types.
    #[test]
    fn a_cte_reference_takes_its_body_types() {
        let plans = vec![SqlPlan::Cte(nodedb_sql::types::CtePlan {
            definitions: vec![
                (
                    "c".to_string(),
                    scan_plan("metrics", columns(&["n", "amount", "region"])),
                ),
                ("d".to_string(), scan_plan("c", columns(&["amount", "n"]))),
            ],
            outer: Box::new(scan_plan("d", columns(&["n", "amount"]))),
        })];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        )
        .expect("schema derives");
        assert_eq!(types_of(&schema), [DdlColType::Int8, DdlColType::Float8]);
        assert_eq!(schema.declared_key, None);
    }

    /// A post-processor tail types its projection from the body it reads.
    #[test]
    fn a_subquery_tail_takes_its_body_types() {
        let plans = vec![SqlPlan::Subquery {
            input: Box::new(scan_plan("metrics", columns(&["n", "region"]))),
            filters: Vec::new(),
            projection: vec![
                Projection::Column("region".to_string()),
                Projection::Computed {
                    expr: metrics_column("n"),
                    alias: "count".to_string(),
                },
            ],
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        }];
        let schema = build_output_schema(
            &plans,
            &TypedCatalog,
            nodedb_types::DatabaseId::DEFAULT,
            None,
        )
        .expect("schema derives");
        assert_eq!(types_of(&schema), [DdlColType::Text, DdlColType::Int8]);
    }
}
