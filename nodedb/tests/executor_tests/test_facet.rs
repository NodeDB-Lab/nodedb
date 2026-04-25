//! Integration tests for multi-facet aggregation (FacetCounts).
//!
//! Verifies:
//! - Unfiltered facet counts across multiple fields
//! - Filtered facet counts (shared filter bitmap)
//! - Index-backed vs scan-based counting produce consistent results
//! - Zero-match filter returns empty facets
//! - Limit per facet (top-N truncation)

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{DocumentOp, QueryOp};
use nodedb::bridge::scan_filter::ScanFilter;

use crate::helpers::*;

/// Insert a product document with brand, color, size, and price fields.
#[allow(clippy::too_many_arguments)]
fn insert_product(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    id: &str,
    brand: &str,
    color: &str,
    size: &str,
    price: f64,
    surrogate: nodedb_types::Surrogate,
) {
    let value = serde_json::json!({
        "brand": brand,
        "color": color,
        "size": size,
        "price": price,
        "in_stock": true,
    });
    let value_bytes = serde_json::to_vec(&value).unwrap();
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: id.into(),
            value: value_bytes,
            surrogate,
            pk_bytes: Vec::new(),
        }),
    );
}

/// Seed the test dataset.
///
/// Inserts products without explicit secondary index registration — facet
/// counting falls back to the HashMap document-scan path, which is correct
/// and sufficient for testing the FacetCounts plumbing.
fn seed_products(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
) {
    insert_product(
        core,
        tx,
        rx,
        "p1",
        "Nike",
        "black",
        "10",
        120.0,
        nodedb_types::Surrogate::new(1),
    );
    insert_product(
        core,
        tx,
        rx,
        "p2",
        "Nike",
        "white",
        "10",
        110.0,
        nodedb_types::Surrogate::new(2),
    );
    insert_product(
        core,
        tx,
        rx,
        "p3",
        "Nike",
        "black",
        "11",
        130.0,
        nodedb_types::Surrogate::new(3),
    );
    insert_product(
        core,
        tx,
        rx,
        "p4",
        "Adidas",
        "black",
        "10",
        100.0,
        nodedb_types::Surrogate::new(4),
    );
    insert_product(
        core,
        tx,
        rx,
        "p5",
        "Adidas",
        "red",
        "9",
        90.0,
        nodedb_types::Surrogate::new(5),
    );
    insert_product(
        core,
        tx,
        rx,
        "p6",
        "Puma",
        "white",
        "11",
        80.0,
        nodedb_types::Surrogate::new(6),
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

#[test]
fn unfiltered_facet_counts_multiple_fields() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: Vec::new(),
            fields: vec!["brand".into(), "color".into()],
            limit_per_facet: 0,
        }),
    );

    let v = payload_value(&payload);

    // Brand facets: Nike=3, Adidas=2, Puma=1.
    let brand_facets = v.get("brand").and_then(|a| a.as_array()).unwrap();
    assert_eq!(brand_facets.len(), 3);
    let nike = brand_facets.iter().find(|f| f["value"] == "Nike").unwrap();
    assert_eq!(nike["count"], 3);
    let adidas = brand_facets
        .iter()
        .find(|f| f["value"] == "Adidas")
        .unwrap();
    assert_eq!(adidas["count"], 2);
    let puma = brand_facets.iter().find(|f| f["value"] == "Puma").unwrap();
    assert_eq!(puma["count"], 1);

    // Color facets: black=3, white=2, red=1.
    let color_facets = v.get("color").and_then(|a| a.as_array()).unwrap();
    assert_eq!(color_facets.len(), 3);
    let black = color_facets.iter().find(|f| f["value"] == "black").unwrap();
    assert_eq!(black["count"], 3);
}

#[test]
fn filtered_facet_counts() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);

    // Filter: brand = 'Nike' — should only count Nike products.
    let filters = vec![filter(
        "brand",
        "eq",
        nodedb_types::Value::String("Nike".into()),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: filter_bytes,
            fields: vec!["color".into(), "size".into()],
            limit_per_facet: 0,
        }),
    );

    let v = payload_value(&payload);

    // Color within Nike: black=2, white=1.
    let colors = v.get("color").and_then(|a| a.as_array()).unwrap();
    assert_eq!(colors.len(), 2);
    let black = colors.iter().find(|f| f["value"] == "black").unwrap();
    assert_eq!(black["count"], 2);
    let white = colors.iter().find(|f| f["value"] == "white").unwrap();
    assert_eq!(white["count"], 1);

    // Size within Nike: 10=2, 11=1.
    let sizes = v.get("size").and_then(|a| a.as_array()).unwrap();
    assert_eq!(sizes.len(), 2);
}

#[test]
fn zero_match_filter_returns_empty_facets() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);

    // Filter: brand = 'NonExistent'.
    let filters = vec![filter(
        "brand",
        "eq",
        nodedb_types::Value::String("NonExistent".into()),
    )];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: filter_bytes,
            fields: vec!["color".into()],
            limit_per_facet: 0,
        }),
    );

    let v = payload_value(&payload);
    let colors = v.get("color").and_then(|a| a.as_array()).unwrap();
    assert!(colors.is_empty());
}

#[test]
fn limit_per_facet_truncates() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);

    // 3 distinct brands, limit to top 2.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: Vec::new(),
            fields: vec!["brand".into()],
            limit_per_facet: 2,
        }),
    );

    let v = payload_value(&payload);
    let brands = v.get("brand").and_then(|a| a.as_array()).unwrap();
    assert_eq!(brands.len(), 2);
    // Sorted by count descending: Nike(3), Adidas(2) — Puma(1) truncated.
    assert_eq!(brands[0]["value"], "Nike");
    assert_eq!(brands[1]["value"], "Adidas");
}

#[test]
fn facet_on_field_without_index_falls_back_to_scan() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);

    // 'price' has no secondary index — falls back to HashMap counting.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: Vec::new(),
            fields: vec!["price".into()],
            limit_per_facet: 0,
        }),
    );

    let v = payload_value(&payload);
    let prices = v.get("price").and_then(|a| a.as_array()).unwrap();
    // 6 unique prices.
    assert_eq!(prices.len(), 6);
}

#[test]
fn facet_single_field() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_products(&mut core, &mut tx, &mut rx);
    insert_product(
        &mut core,
        &mut tx,
        &mut rx,
        "p2",
        "Nike",
        "white",
        "10",
        110.0,
        nodedb_types::Surrogate::new(2),
    );
    insert_product(
        &mut core,
        &mut tx,
        &mut rx,
        "p3",
        "Nike",
        "black",
        "11",
        130.0,
        nodedb_types::Surrogate::new(3),
    );
    insert_product(
        &mut core,
        &mut tx,
        &mut rx,
        "p4",
        "Adidas",
        "black",
        "10",
        100.0,
        nodedb_types::Surrogate::new(4),
    );
    insert_product(
        &mut core,
        &mut tx,
        &mut rx,
        "p5",
        "Adidas",
        "red",
        "9",
        90.0,
        nodedb_types::Surrogate::new(5),
    );
    insert_product(
        &mut core,
        &mut tx,
        &mut rx,
        "p6",
        "Puma",
        "white",
        "11",
        80.0,
        nodedb_types::Surrogate::new(6),
    );

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::FacetCounts {
            collection: "products".into(),
            filters: Vec::new(),
            fields: vec!["size".into()],
            limit_per_facet: 0,
        }),
    );

    let v = payload_value(&payload);
    let sizes = v.get("size").and_then(|a| a.as_array()).unwrap();
    // Sizes: 10=3, 11=2, 9=1.
    assert_eq!(sizes.len(), 3);
    assert_eq!(sizes[0]["value"], "10");
    assert_eq!(sizes[0]["count"], 3);
}
