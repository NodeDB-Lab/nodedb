// SPDX-License-Identifier: Apache-2.0

//! Top-level query entry: CTE handling and UNION dispatch. ORDER BY and
//! search-trigger detection live in `order_by.rs`; LIMIT / OFFSET application
//! lives in `limit.rs`.

use nodedb_types::DatabaseId;
use sqlparser::ast::{Query, SetExpr};

use super::cte_catalog::CteCatalog;
use super::limit::apply_limit;
use super::order_by::{apply_order_by, try_hybrid_from_projection};
use super::query_tail::QueryTail;
use super::select_stmt::plan_select;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::reserved::check_ast_identifier;
use crate::resolver::derived::{infer_subquery_relation, rename_output_columns};
use crate::temporal::TemporalScope;
use crate::types::{Projection, SqlExpr, *};

/// Returns `true` when every projection item is either:
/// - a plain column reference to the surrogate/PK column (`id` or `document_id`), or
/// - a `vector_distance(...)` function call (any alias).
///
/// Anything else — a payload field, `*`, or an unrecognised expression — returns `false`.
fn is_pure_vector_projection(projection: &[Projection]) -> bool {
    if projection.is_empty() {
        return false;
    }
    for item in projection {
        match item {
            Projection::Column(name) => {
                let lower = name.to_ascii_lowercase();
                if lower != "id" && lower != "document_id" {
                    return false;
                }
            }
            Projection::Computed { expr, .. } => {
                // Accept any of the three vector distance function names.
                let SqlExpr::Function { name, .. } = expr else {
                    return false;
                };
                if !name.eq_ignore_ascii_case("vector_distance")
                    && !name.eq_ignore_ascii_case("vector_cosine_distance")
                    && !name.eq_ignore_ascii_case("vector_neg_inner_product")
                {
                    return false;
                }
            }
            Projection::Star | Projection::QualifiedStar(_) => return false,
        }
    }
    true
}

/// Plan a SELECT query.
pub fn plan_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    // Handle CTEs (WITH clause).
    if let Some(with) = &query.with
        && with.recursive
    {
        return crate::planner::cte::plan_recursive_cte(query, catalog, functions, temporal);
    }
    // Non-recursive CTEs: plan each CTE subquery and the outer query.
    if let Some(with) = &query.with
        && !with.cte_tables.is_empty()
    {
        let inner_query = Query {
            with: None,
            body: query.body.clone(),
            order_by: query.order_by.clone(),
            limit_clause: query.limit_clause.clone(),
            fetch: query.fetch.clone(),
            locks: query.locks.clone(),
            for_clause: query.for_clause.clone(),
            settings: query.settings.clone(),
            format_clause: query.format_clause.clone(),
            pipe_operators: query.pipe_operators.clone(),
        };

        // Plan each CTE subquery and infer the relation it exposes.
        let mut definitions = Vec::new();
        let mut relations = Vec::new();
        for cte in &with.cte_tables {
            let name = check_ast_identifier(&cte.alias.name)?;
            let declared: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|column| check_ast_identifier(&column.name))
                .collect::<Result<_>>()?;
            let cte_plan = plan_query(&cte.query, catalog, functions, temporal)?;
            let info = infer_subquery_relation(catalog, &name, &cte.query)?;
            definitions.push((name.clone(), cte_plan));
            relations.push((name, rename_output_columns(info, &declared)));
        }

        // Build CTE-aware catalog so the outer query can reference CTE names.
        let cte_catalog = CteCatalog {
            inner: catalog,
            relations,
        };
        let outer = plan_query(&inner_query, &cte_catalog, functions, temporal)?;

        return Ok(SqlPlan::Cte {
            definitions,
            outer: Box::new(outer),
        });
    }

    // Handle UNION.
    match &*query.body {
        SetExpr::Select(select) => {
            // ORDER BY / LIMIT belong to the query, not to its SELECT body,
            // but the scan planner needs them to pick an access path that can
            // honour them — so they travel down with the SELECT.
            let tail = QueryTail {
                order_by: query.order_by.as_ref(),
                limit_clause: &query.limit_clause,
                fetch: query.fetch.as_ref(),
            };
            let planned = plan_select(select, catalog, functions, temporal, &tail)?;
            let scope = planned.scope;
            let mut plan = planned.plan;
            // Snapshot the projection before ORDER BY transforms the plan,
            // in case `apply_order_by` converts a Scan into VectorSearch.
            let pre_order_by_projection: Option<Vec<Projection>> = match &plan {
                SqlPlan::Scan { projection, .. } => Some(projection.clone()),
                _ => None,
            };
            let pre_order_by_collection: Option<String> = match &plan {
                SqlPlan::Scan { collection, .. } => Some(collection.clone()),
                _ => None,
            };
            if let Some(order_by) = &query.order_by {
                plan = apply_order_by(&plan, order_by, functions, &select.projection, &scope)?;
            }
            // Fall back to a SELECT-projection scan for hybrid-search and
            // text-search triggers. The `SELECT id, rrf_score(...) AS score
            // FROM c WHERE ... LIMIT N` shape has no ORDER BY, so
            // `apply_order_by` cannot fire. The same applies to
            // `SELECT id, bm25_score(field, term) FROM c ORDER BY id` where
            // ORDER BY does not contain a search trigger.
            //
            // Also fires when the plan is already `TextSearch` (set by the
            // WHERE `text_match(...)` path) and the SELECT list additionally
            // contains `bm25_score(...)` — in that case we attach the
            // `score_alias` so the executor knows to inject the score column.
            //
            // `apply_order_by` may have wrapped a search plan in a
            // post-processing tail to carry the sort, so the upgrade inspects
            // the body and is re-wrapped in place — otherwise the score column
            // the SELECT list asked for would never be attached.
            let upgrade = {
                let leaf = match &plan {
                    SqlPlan::Subquery { input, .. } => input.as_ref(),
                    other => other,
                };
                if matches!(leaf, SqlPlan::Scan { .. } | SqlPlan::TextSearch { .. }) {
                    try_hybrid_from_projection(leaf, &select.projection, functions)?
                } else {
                    None
                }
            };
            if let Some(upgraded_leaf) = upgrade {
                plan = match plan {
                    SqlPlan::Subquery {
                        filters,
                        projection,
                        window_functions,
                        sort_keys,
                        offset,
                        distinct,
                        limit,
                        ..
                    } => SqlPlan::Subquery {
                        input: Box::new(upgraded_leaf),
                        filters,
                        projection,
                        window_functions,
                        sort_keys,
                        offset,
                        distinct,
                        limit,
                    },
                    _ => upgraded_leaf,
                };
            }
            // After ORDER BY: if we now have a VectorSearch, check whether
            // the collection is vector-primary and the projection is
            // payload-free. If so, set `skip_payload_fetch`.
            if let SqlPlan::VectorSearch {
                ref collection,
                ref mut skip_payload_fetch,
                ref mut filters,
                ref mut payload_filters,
                ..
            } = plan
            {
                let info = catalog
                    .get_collection(DatabaseId::DEFAULT, collection)
                    .ok()
                    .flatten();
                let is_vector_primary = info
                    .as_ref()
                    .map(|c| c.primary == nodedb_types::PrimaryEngine::Vector)
                    .unwrap_or(false);
                if is_vector_primary {
                    if let Some(ref proj) = pre_order_by_projection
                        && pre_order_by_collection.as_deref() == Some(collection.as_str())
                    {
                        *skip_payload_fetch = is_pure_vector_projection(proj);
                    }
                    if let Some(vp) = info.as_ref().and_then(|c| c.vector_primary.as_ref()) {
                        let mut peeled: Vec<SqlPayloadAtom> = Vec::new();
                        let is_indexed = |name: &str| {
                            vp.payload_indexes
                                .iter()
                                .any(|(p, _)| p.eq_ignore_ascii_case(name))
                        };
                        filters.retain(|f| match &f.expr {
                            FilterExpr::Comparison {
                                field,
                                op: CompareOp::Eq,
                                value,
                            } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::Eq(field.clone(), value.clone()));
                                false
                            }
                            FilterExpr::InList { field, values } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::In(field.clone(), values.clone()));
                                false
                            }
                            FilterExpr::Between { field, low, high } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::Range {
                                    field: field.clone(),
                                    low: Some(low.clone()),
                                    low_inclusive: true,
                                    high: Some(high.clone()),
                                    high_inclusive: true,
                                });
                                false
                            }
                            FilterExpr::Comparison { field, op, value }
                                if matches!(
                                    op,
                                    CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                                ) && is_indexed(field) =>
                            {
                                let inclusive = matches!(op, CompareOp::Le | CompareOp::Ge);
                                let upper = matches!(op, CompareOp::Lt | CompareOp::Le);
                                peeled.push(SqlPayloadAtom::Range {
                                    field: field.clone(),
                                    low: if upper { None } else { Some(value.clone()) },
                                    low_inclusive: !upper && inclusive,
                                    high: if upper { Some(value.clone()) } else { None },
                                    high_inclusive: upper && inclusive,
                                });
                                false
                            }
                            FilterExpr::Expr(SqlExpr::BinaryOp {
                                left,
                                op: BinaryOp::Eq,
                                right,
                            }) => match (&**left, &**right) {
                                (SqlExpr::Column { name, .. }, SqlExpr::Literal(v))
                                    if is_indexed(name) =>
                                {
                                    peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                                    false
                                }
                                (SqlExpr::Literal(v), SqlExpr::Column { name, .. })
                                    if is_indexed(name) =>
                                {
                                    peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                                    false
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::InList {
                                expr,
                                list,
                                negated: false,
                            }) => match &**expr {
                                SqlExpr::Column { name, .. } if is_indexed(name) => {
                                    let mut lits = Vec::with_capacity(list.len());
                                    let all_lit = list.iter().all(|e| {
                                        if let SqlExpr::Literal(v) = e {
                                            lits.push(v.clone());
                                            true
                                        } else {
                                            false
                                        }
                                    });
                                    if all_lit {
                                        peeled.push(SqlPayloadAtom::In(name.clone(), lits));
                                        false
                                    } else {
                                        true
                                    }
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::Between {
                                expr,
                                low,
                                high,
                                negated: false,
                            }) => match (&**expr, &**low, &**high) {
                                (
                                    SqlExpr::Column { name, .. },
                                    SqlExpr::Literal(lo),
                                    SqlExpr::Literal(hi),
                                ) if is_indexed(name) => {
                                    peeled.push(SqlPayloadAtom::Range {
                                        field: name.clone(),
                                        low: Some(lo.clone()),
                                        low_inclusive: true,
                                        high: Some(hi.clone()),
                                        high_inclusive: true,
                                    });
                                    false
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::BinaryOp { left, op, right })
                                if matches!(
                                    op,
                                    BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                                ) =>
                            {
                                match (&**left, &**right) {
                                    (SqlExpr::Column { name, .. }, SqlExpr::Literal(v))
                                        if is_indexed(name) =>
                                    {
                                        let inclusive = matches!(op, BinaryOp::Le | BinaryOp::Ge);
                                        let upper = matches!(op, BinaryOp::Lt | BinaryOp::Le);
                                        peeled.push(SqlPayloadAtom::Range {
                                            field: name.clone(),
                                            low: if upper { None } else { Some(v.clone()) },
                                            low_inclusive: !upper && inclusive,
                                            high: if upper { Some(v.clone()) } else { None },
                                            high_inclusive: upper && inclusive,
                                        });
                                        false
                                    }
                                    _ => true,
                                }
                            }
                            _ => true,
                        });
                        *payload_filters = peeled;
                    }
                }
            }
            apply_limit(plan, &tail)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => crate::planner::union::plan_set_operation(
            op,
            left,
            right,
            set_quantifier,
            catalog,
            functions,
            temporal,
        ),
        _ => Err(SqlError::Unsupported {
            detail: format!("query body type: {}", query.body),
        }),
    }
}

/// Unit tests for SELECT query planning.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::preprocess::pipeline::preprocess;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    struct TestCatalog;

    impl SqlCatalog for TestCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            let info = match name {
                "products" => Some(CollectionInfo {
                    name: "products".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                "users" => Some(CollectionInfo {
                    name: "users".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                "orders" => Some(CollectionInfo {
                    name: "orders".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                "docs" => Some(CollectionInfo {
                    name: "docs".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                "tags" => Some(CollectionInfo {
                    name: "tags".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                "user_prefs" => Some(CollectionInfo {
                    name: "user_prefs".into(),
                    engine: EngineType::KeyValue,
                    columns: Vec::new(),
                    primary_key: Some("key".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::KeyValue),
                }),
                "embeddings" => Some(CollectionInfo {
                    name: "embeddings".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }),
                _ => None,
            };
            Ok(info)
        }

        fn lookup_array(&self, name: &str) -> Option<crate::types::ArrayCatalogView> {
            if name == "genome" {
                Some(crate::types::ArrayCatalogView {
                    name: "genome".into(),
                    dims: vec![
                        crate::types_array::ArrayDimAst {
                            name: "chrom".into(),
                            dtype: crate::types_array::ArrayDimType::Int64,
                            lo: crate::types_array::ArrayDomainBound::Int64(1),
                            hi: crate::types_array::ArrayDomainBound::Int64(23),
                        },
                        crate::types_array::ArrayDimAst {
                            name: "pos".into(),
                            dtype: crate::types_array::ArrayDimType::Int64,
                            lo: crate::types_array::ArrayDomainBound::Int64(0),
                            hi: crate::types_array::ArrayDomainBound::Int64(1_000_000),
                        },
                    ],
                    attrs: vec![crate::types_array::ArrayAttrAst {
                        name: "qual".into(),
                        dtype: crate::types_array::ArrayAttrType::Float64,
                        nullable: true,
                    }],
                    tile_extents: vec![1, 1_000_000],
                })
            } else {
                None
            }
        }
    }

    fn plan_select_sql(sql: &str) -> SqlPlan {
        // Run preprocessor so operator rewrites (`<->`, `<=>`, `<#>`) are applied
        // before sqlparser sees the SQL.
        let (preprocessed_sql, temporal) = match preprocess(sql).unwrap() {
            Some(p) => (p.sql, p.temporal),
            None => (sql.to_string(), crate::TemporalScope::default()),
        };
        let statements = parse_sql(&preprocessed_sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        plan_query(query, &TestCatalog, &FunctionRegistry::new(), temporal).unwrap()
    }

    #[test]
    fn aggregate_subquery_join_filters_input_before_aggregation() {
        let plan = plan_select_sql(
            "SELECT AVG(price) FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE qty > 100)",
        );

        let SqlPlan::Aggregate { input, .. } = plan else {
            panic!("expected aggregate plan");
        };

        let SqlPlan::Join {
            left,
            join_type,
            on,
            ..
        } = *input
        else {
            panic!("expected semi-join below aggregate");
        };

        assert_eq!(join_type, JoinType::Semi);
        assert_eq!(on, vec![("category".into(), "category".into())]);
        assert!(matches!(*left, SqlPlan::Scan { .. }));
    }

    #[test]
    fn scalar_subquery_defers_projection_until_after_join_filter() {
        let plan = plan_select_sql(
            "SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
        );

        let SqlPlan::Join {
            left,
            projection,
            filters,
            ..
        } = plan
        else {
            panic!("expected join plan");
        };

        let SqlPlan::Scan {
            projection: scan_projection,
            ..
        } = *left
        else {
            panic!("expected scan on join left");
        };

        assert!(scan_projection.is_empty(), "scan projected too early");
        assert_eq!(projection.len(), 1);
        match &projection[0] {
            Projection::Column(name) => assert_eq!(name, "user_id"),
            other => panic!("expected user_id projection, got {other:?}"),
        }
        assert!(
            !filters.is_empty(),
            "scalar comparison should stay post-join"
        );
    }

    #[test]
    fn chained_join_preserves_qualified_on_keys() {
        let plan = plan_select_sql(
            "SELECT d.name, t.tag, p.theme \
             FROM docs d \
             LEFT JOIN tags t ON d.id = t.doc_id \
             INNER JOIN user_prefs p ON d.id = p.key",
        );

        let SqlPlan::Join { left, on, .. } = plan else {
            panic!("expected outer join plan");
        };
        assert_eq!(on, vec![("d.id".into(), "p.key".into())]);

        let SqlPlan::Join { on: inner_on, .. } = *left else {
            panic!("expected nested left join");
        };
        assert_eq!(inner_on, vec![("d.id".into(), "t.doc_id".into())]);
    }

    #[test]
    fn order_by_vector_distance_with_array_join_fuses_into_vector_search() {
        let plan = plan_select_sql(
            "SELECT v.id FROM embeddings v \
             JOIN ARRAY_SLICE('genome', '{chrom: [1, 1], pos: [0, 50000]}') AS s \
               ON v.id = s.qual \
             ORDER BY vector_distance(v.embedding, [1.0, 0.0, 0.0]) \
             LIMIT 10",
        );

        let SqlPlan::VectorSearch {
            collection,
            top_k,
            array_prefilter,
            ..
        } = plan
        else {
            panic!("expected fused VectorSearch plan");
        };
        assert_eq!(collection, "embeddings");
        assert_eq!(top_k, 10);
        let prefilter = array_prefilter.expect("array_prefilter must be set on fused plan");
        assert_eq!(prefilter.array_name, "genome");
        assert_eq!(prefilter.slice.dim_ranges.len(), 2);
    }

    #[test]
    fn vector_distance_two_args_produces_default_ann_options() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options, VectorAnnOptions::default());
    }

    #[test]
    fn order_by_sparse_score_desc_routes_to_sparse_search() {
        // `ORDER BY sparse_score(field, '{dim: weight, ...}') DESC LIMIT k` must
        // route to `SqlPlan::SparseSearch` exactly as `vector_distance(...)` routes
        // to `SqlPlan::VectorSearch`. The query literal is parsed into sorted
        // `(dimension, weight)` entries and `top_k` tracks the LIMIT.
        let plan = plan_select_sql(
            "SELECT id FROM embeddings \
             ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC LIMIT 5",
        );
        let SqlPlan::SparseSearch {
            collection,
            field,
            query_entries,
            top_k,
            ..
        } = plan
        else {
            panic!("expected SparseSearch plan");
        };
        assert_eq!(collection, "embeddings");
        assert_eq!(field, "terms");
        assert_eq!(top_k, 5);
        assert_eq!(query_entries, vec![(3, 1.0), (7, 0.5)]);
    }

    #[test]
    fn vector_distance_named_args_parses_ann_options() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'rabitq', oversample => 3) LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            ef_search,
            top_k,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::RaBitQ));
        assert_eq!(ann_options.oversample, Some(3));
        // ef_search falls back to top_k * 2 (no ef_search_override supplied).
        assert_eq!(ef_search, top_k * 2);
    }

    #[test]
    fn vector_distance_ef_search_override_applied() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0], ef_search => 150) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ef_search, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ef_search, 150);
    }

    #[test]
    fn arrow_distance_operator_yields_l2_metric() {
        // The <-> operator rewrites to vector_distance(...) via the preprocessor.
        // Use the function form here since sqlparser handles bracket-array syntax.
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::L2);
    }

    #[test]
    fn cosine_distance_operator_yields_cosine_metric() {
        // The <=> operator rewrites to vector_cosine_distance(...).
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_cosine_distance(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::Cosine);
    }

    #[test]
    fn neg_inner_product_operator_yields_inner_product_metric() {
        // The <#> operator rewrites to vector_neg_inner_product(...).
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_neg_inner_product(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::InnerProduct);
    }

    #[test]
    fn vector_distance_function_yields_l2_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::L2);
    }

    #[test]
    fn vector_cosine_distance_function_yields_cosine_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_cosine_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::Cosine);
    }

    #[test]
    fn vector_neg_inner_product_function_yields_inner_product_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_neg_inner_product(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::InnerProduct);
    }
}
