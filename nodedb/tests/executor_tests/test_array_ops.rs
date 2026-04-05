//! Integration tests for array operators and array aggregate functions.

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{DocumentOp, QueryOp};
use nodedb::bridge::scan_filter::ScanFilter;

use crate::helpers::*;

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
            collection: "products".into(),
            document_id: id.into(),
            value: serde_json::to_vec(&doc).unwrap(),
        }),
    );
}

fn filter(field: &str, op: &str, value: serde_json::Value) -> ScanFilter {
    ScanFilter {
        field: field.into(),
        op: op.into(),
        value,
        clauses: Vec::new(),
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
    let filters = vec![filter("tags", "array_contains", serde_json::json!("sale"))];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates: vec![],
            returning: true,
        }),
    );

    let text = payload_json(&payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    // p1 and p3 have "sale" tag.
    assert_eq!(docs.len(), 2);
}

#[test]
fn array_contains_all_filter() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // Products where sizes contains ALL of ["S", "M"].
    let filters = vec![filter(
        "sizes",
        "array_contains_all",
        serde_json::json!(["S", "M"]),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates: vec![],
            returning: true,
        }),
    );

    let text = payload_json(&payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    // Only p1 has both S and M.
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "T-Shirt");
}

#[test]
fn array_overlap_filter() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // Products where tags overlaps ["sale", "premium"].
    let filters = vec![filter(
        "tags",
        "array_overlap",
        serde_json::json!(["sale", "premium"]),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates: vec![],
            returning: true,
        }),
    );

    let text = payload_json(&payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    // p1 (sale), p3 (sale), p4 (premium).
    assert_eq!(docs.len(), 3);
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
            collection: "products".into(),
            group_by: vec!["brand".into()],
            aggregates: vec![("array_agg".into(), "color".into())],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 100,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
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
    let doc = serde_json::json!({"tags": ["a", "b", "c"]});
    assert_eq!(expr.eval(&doc), serde_json::json!(3));

    let doc2 = serde_json::json!({"tags": []});
    assert_eq!(expr.eval(&doc2), serde_json::json!(0));
}

#[test]
fn array_append_function() {
    let expr = nodedb_query::expr_parse::parse_generated_expr("array_append(tags, 'new')")
        .unwrap()
        .0;
    let doc = serde_json::json!({"tags": ["a", "b"]});
    assert_eq!(expr.eval(&doc), serde_json::json!(["a", "b", "new"]));
}

#[test]
fn array_remove_function() {
    let expr = nodedb_query::expr_parse::parse_generated_expr("array_remove(tags, 'b')")
        .unwrap()
        .0;
    let doc = serde_json::json!({"tags": ["a", "b", "c"]});
    assert_eq!(expr.eval(&doc), serde_json::json!(["a", "c"]));
}

#[test]
fn no_match_returns_zero() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed(&mut core, &mut tx, &mut rx);

    // No product has tag "nonexistent".
    let filters = vec![filter(
        "tags",
        "array_contains",
        serde_json::json!("nonexistent"),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates: vec![],
            returning: false,
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v["affected"], 0);
}
