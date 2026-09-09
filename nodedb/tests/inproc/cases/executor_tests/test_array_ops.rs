// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for array operators and array aggregate functions.

use nodedb::bridge::scan_filter::ScanFilter;
use nodedb_physical::physical_plan::{
    AggregateSpec, DocumentOp, GroupKeySpec, PhysicalPlan, QueryOp,
};

use super::helpers::*;

fn surrogate_for(id: &str) -> nodedb_types::Surrogate {
    let mut h: u32 = 2_166_136_261;
    for &b in id.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    nodedb_types::Surrogate::new(h.max(1))
}

fn insert_product(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    id: &str,
    doc: serde_json::Value,
) {
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            document_id: id.into(),
            value: serde_json::to_vec(&doc).unwrap(),
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
}

fn filter(field: &str, op: &str, value: nodedb_types::Value) -> ScanFilter {
    ScanFilter {
        field: field.into(),
        op: op.into(),
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

fn seed(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
) {
    insert_product(
        core,
        tx,
        rx,
        "p1",
        serde_json::json!({
            "name": "T-Shirt", "brand": "Nike", "tags": ["sale", "new"], "sizes": ["S", "M", "L"], "color": "red"
        }),
    );
    insert_product(
        core,
        tx,
        rx,
        "p2",
        serde_json::json!({
            "name": "Sneaker", "brand": "Nike", "tags": ["new"], "sizes": ["M", "L", "XL"], "color": "blue"
        }),
    );
    insert_product(
        core,
        tx,
        rx,
        "p3",
        serde_json::json!({
            "name": "Cap", "brand": "Adidas", "tags": ["sale", "clearance"], "sizes": ["S"], "color": "red"
        }),
    );
    insert_product(
        core,
        tx,
        rx,
        "p4",
        serde_json::json!({
            "name": "Pants", "brand": "Puma", "tags": ["premium"], "sizes": ["M", "L"], "color": "blue"
        }),
    );
}

#[test]
fn array_contains_filter() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // Products where tags contains "sale".
    let filters = vec![filter(
        "tags",
        "array_contains",
        nodedb_types::Value::String("sale".into()),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            filters: filter_bytes,
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }),
    );

    let val = payload_value(&payload);
    // p1 and p3 have "sale" tag.
    assert_eq!(val["affected"], 2);
}

#[test]
fn array_contains_all_filter() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // Products where sizes contains ALL of ["S", "M"].
    let filters = vec![filter(
        "sizes",
        "array_contains_all",
        nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("S".into()),
            nodedb_types::Value::String("M".into()),
        ]),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            filters: filter_bytes,
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }),
    );

    let val = payload_value(&payload);
    // Only p1 has both S and M.
    assert_eq!(val["affected"], 1);
}

#[test]
fn array_overlap_filter() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // Products where tags overlaps ["sale", "premium"].
    let filters = vec![filter(
        "tags",
        "array_overlap",
        nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("sale".into()),
            nodedb_types::Value::String("premium".into()),
        ]),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            filters: filter_bytes,
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }),
    );

    let val = payload_value(&payload);
    // p1 (sale), p3 (sale), p4 (premium).
    assert_eq!(val["affected"], 3);
}

#[test]
fn array_agg_aggregate() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // GROUP BY brand, ARRAY_AGG(color).
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            input: None,
            group_by: vec![GroupKeySpec::column("brand")],
            aggregates: vec![AggregateSpec {
                function: "array_agg".into(),
                alias: "array_agg_color".into(),
                user_alias: None,
                field: "color".into(),
                expr: None,
            }],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 100,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            sort_keys: Vec::new(),
        }),
    );

    let v = payload_value(&payload);
    let rows: Vec<serde_json::Value> = if let serde_json::Value::Array(arr) = v {
        arr
    } else {
        serde_json::from_str(&payload_json(&payload)).unwrap_or_default()
    };

    // Nike has 2 products: colors ["red", "blue"].
    let nike = rows.iter().find(|r| r["brand"] == "Nike").unwrap();
    let colors = nike["array_agg_color"].as_array().unwrap();
    assert_eq!(colors.len(), 2);
    assert!(colors.contains(&serde_json::json!("red")));
    assert!(colors.contains(&serde_json::json!("blue")));
}

#[test]
fn array_length_function() {
    // Test via expression evaluator directly.
    let expr = nodedb_query::expr_parse::parse_generated_expr("array_length(tags)")
        .unwrap()
        .0;
    let doc = nodedb_types::Value::from(serde_json::json!({"tags": ["a", "b", "c"]}));
    assert_eq!(expr.eval(&doc).unwrap(), nodedb_types::Value::Integer(3));

    let doc2 = nodedb_types::Value::from(serde_json::json!({"tags": []}));
    assert_eq!(expr.eval(&doc2).unwrap(), nodedb_types::Value::Integer(0));
}

#[test]
fn array_append_function() {
    let expr = nodedb_query::expr_parse::parse_generated_expr("array_append(tags, 'new')")
        .unwrap()
        .0;
    let doc = nodedb_types::Value::from(serde_json::json!({"tags": ["a", "b"]}));
    assert_eq!(
        expr.eval(&doc).unwrap(),
        nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("a".into()),
            nodedb_types::Value::String("b".into()),
            nodedb_types::Value::String("new".into()),
        ])
    );
}

#[test]
fn array_remove_function() {
    let expr = nodedb_query::expr_parse::parse_generated_expr("array_remove(tags, 'b')")
        .unwrap()
        .0;
    let doc = nodedb_types::Value::from(serde_json::json!({"tags": ["a", "b", "c"]}));
    assert_eq!(
        expr.eval(&doc).unwrap(),
        nodedb_types::Value::Array(vec![
            nodedb_types::Value::String("a".into()),
            nodedb_types::Value::String("c".into()),
        ])
    );
}

#[test]
fn no_match_returns_zero() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // No product has tag "nonexistent".
    let filters = vec![filter(
        "tags",
        "array_contains",
        nodedb_types::Value::String("nonexistent".into()),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "products",
            ),
            filters: filter_bytes,
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v["affected"], 0);
}
