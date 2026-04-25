//! Integration tests for stored generated (computed) columns.
//!
//! Verifies:
//! - INSERT materializes generated column values
//! - UPDATE recomputes when source columns change
//! - Direct UPDATE of generated column is rejected
//! - Chained generated columns (A depends on B) evaluate in correct order
//! - Generated columns are readable via PointGet

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{DocumentOp, EnforcementOptions, GeneratedColumnSpec};

use crate::helpers::*;

/// Register a collection with generated columns.
fn register_with_generated(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    collection: &str,
    specs: Vec<GeneratedColumnSpec>,
) {
    let enforcement = EnforcementOptions {
        generated_columns: specs,
        ..Default::default()
    };
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: collection.into(),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: Default::default(),
            enforcement: Box::new(enforcement),
            bitemporal: false,
        }),
    );
}

/// Parse an expression string into SqlExpr.
fn parse_expr(text: &str) -> nodedb::bridge::expr_eval::SqlExpr {
    let (expr, _) = nodedb_query::expr_parse::parse_generated_expr(text).unwrap();
    expr
}

fn get_doc(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    collection: &str,
    id: &str,
) -> serde_json::Value {
    let payload = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: collection.into(),
            document_id: id.into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    payload_value(&payload)
}

#[test]
fn insert_materializes_generated_column() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // price_with_tax = ROUND(price * (1 + tax_rate), 2)
    let (expr, deps) =
        nodedb_query::expr_parse::parse_generated_expr("ROUND(price * (1 + tax_rate), 2)").unwrap();
    register_with_generated(
        &mut core,
        &mut tx,
        &mut rx,
        "products",
        vec![GeneratedColumnSpec {
            name: "price_with_tax".into(),
            expr,
            depends_on: deps,
        }],
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: "p1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "price": 99.99,
                "tax_rate": 0.08
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    let doc = get_doc(&mut core, &mut tx, &mut rx, "products", "p1");
    assert_eq!(
        doc.get("price_with_tax").and_then(|v| v.as_f64()),
        Some(107.99)
    );
}

#[test]
fn insert_materializes_concat() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    register_with_generated(
        &mut core,
        &mut tx,
        &mut rx,
        "products",
        vec![GeneratedColumnSpec {
            name: "search_text".into(),
            expr: parse_expr("CONCAT(name, ' ', brand)"),
            depends_on: vec!["name".into(), "brand".into()],
        }],
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: "p1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "name": "Running Shoe",
                "brand": "Nike"
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    let doc = get_doc(&mut core, &mut tx, &mut rx, "products", "p1");
    assert_eq!(
        doc.get("search_text").and_then(|v| v.as_str()),
        Some("Running Shoe Nike")
    );
}

#[test]
fn update_recomputes_generated_column() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let (expr, deps) =
        nodedb_query::expr_parse::parse_generated_expr("ROUND(price * (1 + tax_rate), 2)").unwrap();
    register_with_generated(
        &mut core,
        &mut tx,
        &mut rx,
        "products",
        vec![GeneratedColumnSpec {
            name: "price_with_tax".into(),
            expr,
            depends_on: deps,
        }],
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: "p1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "price": 100.0,
                "tax_rate": 0.08
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    // Update price — generated column should recompute.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "products".into(),
            document_id: "p1".into(),
            updates: vec![(
                "price".to_string(),
                nodedb::bridge::physical_plan::UpdateValue::Literal(
                    nodedb_types::json_to_msgpack(&serde_json::json!(200.0)).unwrap(),
                ),
            )],
            returning: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    let doc = get_doc(&mut core, &mut tx, &mut rx, "products", "p1");
    assert_eq!(
        doc.get("price_with_tax").and_then(|v| v.as_f64()),
        Some(216.0)
    );
}

#[test]
fn update_generated_column_directly_rejected() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    register_with_generated(
        &mut core,
        &mut tx,
        &mut rx,
        "products",
        vec![GeneratedColumnSpec {
            name: "price_with_tax".into(),
            expr: parse_expr("price * 1.08"),
            depends_on: vec!["price".into()],
        }],
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: "p1".into(),
            value: serde_json::to_vec(&serde_json::json!({"price": 100.0})).unwrap(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    // Direct update of generated column should fail.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "products".into(),
            document_id: "p1".into(),
            updates: vec![(
                "price_with_tax".to_string(),
                nodedb::bridge::physical_plan::UpdateValue::Literal(
                    nodedb_types::json_to_msgpack(&serde_json::json!(999.0)).unwrap(),
                ),
            )],
            returning: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    assert_eq!(resp.status, nodedb::bridge::envelope::Status::Error);
}

#[test]
fn chained_generated_columns() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // total = qty * price
    // total_with_tax = total * (1 + tax_rate)
    // total depends on qty, price; total_with_tax depends on total, tax_rate
    register_with_generated(
        &mut core,
        &mut tx,
        &mut rx,
        "orders",
        vec![
            GeneratedColumnSpec {
                name: "total".into(),
                expr: parse_expr("qty * price"),
                depends_on: vec!["qty".into(), "price".into()],
            },
            GeneratedColumnSpec {
                name: "total_with_tax".into(),
                expr: parse_expr("total * (1 + tax_rate)"),
                depends_on: vec!["total".into(), "tax_rate".into()],
            },
        ],
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "orders".into(),
            document_id: "o1".into(),
            value: serde_json::to_vec(&serde_json::json!({
                "qty": 3,
                "price": 10.0,
                "tax_rate": 0.1
            }))
            .unwrap(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    let doc = get_doc(&mut core, &mut tx, &mut rx, "orders", "o1");
    // total = 3 * 10 = 30
    assert_eq!(doc.get("total").and_then(|v| v.as_f64()), Some(30.0));
    // total_with_tax = 30 * 1.1 = 33
    assert_eq!(
        doc.get("total_with_tax").and_then(|v| v.as_f64()),
        Some(33.0)
    );
}
