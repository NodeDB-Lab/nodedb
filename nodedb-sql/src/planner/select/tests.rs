//! Unit tests for SELECT query planning.

use super::*;
use crate::functions::registry::FunctionRegistry;
use crate::parser::statement::parse_sql;
use crate::types::*;
use sqlparser::ast::Statement;

struct TestCatalog;

impl SqlCatalog for TestCatalog {
    fn get_collection(
        &self,
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
    let statements = parse_sql(sql).unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query statement");
    };
    plan_query(
        query,
        &TestCatalog,
        &FunctionRegistry::new(),
        crate::TemporalScope::default(),
    )
    .unwrap()
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
         JOIN NDARRAY_SLICE('genome', '{chrom: [1, 1], pos: [0, 50000]}') AS s \
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
fn vector_distance_three_args_parses_ann_options() {
    let plan = plan_select_sql(
        r#"SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], '{"quantization":"rabitq","oversample":3}') LIMIT 5"#,
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
    // ef_search falls back to top_k * 2 (no ef_search_override in this JSON).
    assert_eq!(ef_search, top_k * 2);
}

#[test]
fn vector_distance_ef_search_override_applied() {
    let plan = plan_select_sql(
        r#"SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0], '{"ef_search_override":150}') LIMIT 5"#,
    );
    let SqlPlan::VectorSearch { ef_search, .. } = plan else {
        panic!("expected VectorSearch plan");
    };
    assert_eq!(ef_search, 150);
}
