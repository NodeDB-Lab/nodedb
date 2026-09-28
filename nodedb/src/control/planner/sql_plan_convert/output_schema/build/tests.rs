// SPDX-License-Identifier: BUSL-1.1

//! Output-schema derivation for read, constant, search, and write plans.

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::{HybridSearchPlan, RecursiveValuePlan, SqlPlan};

use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::DdlColType;

use super::build_output_schema;
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
fn constant_result_columns_are_typed_from_their_values() {
    use nodedb_sql::types_expr::SqlValue;
    let plans = vec![SqlPlan::ConstantResult {
        columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        values: vec![SqlValue::Int(1), SqlValue::String("x".into())],
        volatile: false,
    }];
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].display_name, "a");
    assert_eq!(schema.columns[0].lookup_key, "a");
    assert_eq!(schema.columns[0].ty, DdlColType::Int8);
    assert_eq!(schema.columns[1].display_name, "b");
    assert_eq!(schema.columns[1].ty, DdlColType::Text);
    // A column without a value keeps its slot and types as text.
    assert_eq!(schema.columns[2].display_name, "c");
    assert_eq!(schema.columns[2].ty, DdlColType::Text);
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

    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
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
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
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
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
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
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
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
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
    assert_id_and_dist_schema(&schema);
}

#[test]
fn hybrid_search_uses_its_own_projection() {
    let plans = vec![SqlPlan::HybridSearch(HybridSearchPlan {
        collection: "docs".to_string(),
        query_vector: vec![0.0, 1.0],
        query_text: "hello".to_string(),
        top_k: 10,
        ef_search: 64,
        vector_weight: 0.5,
        fuzzy: false,
        score_alias: None,
        projection: id_and_dist_projection(),
    })];
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
    assert_id_and_dist_schema(&schema);
}

#[test]
fn text_search_uses_its_own_projection() {
    let plans = vec![SqlPlan::TextSearch {
        collection: "docs".to_string(),
        field: None,
        query: nodedb_sql::types::FtsQuery::Plain {
            text: "hello".to_string(),
            fuzzy: false,
        },
        top_k: 10,
        filters: Vec::new(),
        score_alias: None,
        projection: id_and_dist_projection(),
    }];
    let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
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
