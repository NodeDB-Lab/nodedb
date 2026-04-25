//! Integration tests for conditional update guarantees.
//!
//! Verifies:
//! - Affected row count is correctly returned for bulk updates
//! - Conditional UPDATE WHERE with predicates (stock >= N) works atomically
//! - RETURNING flag returns post-update documents
//! - TransactionBatch does not auto-abort on 0-row conditional update
//! - PointUpdate returns affected count

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, MetaOp};
use nodedb::bridge::scan_filter::ScanFilter;

use crate::helpers::*;

fn filter(field: &str, op: &str, value: nodedb_types::Value) -> ScanFilter {
    ScanFilter {
        field: field.into(),
        op: op.into(),
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

/// Hash a string PK to a deterministic non-zero surrogate so each test
/// row lands on its own substrate key. The data plane keys redb rows by
/// `surrogate_to_doc_id(surrogate)`; with a wired catalog the assigner
/// guarantees a stable injection, but executor-direct fixtures bypass
/// the catalog and have to thread their own bindings.
fn surrogate_for(id: &str) -> nodedb_types::Surrogate {
    let mut h: u32 = 2_166_136_261;
    for &b in id.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    // Bias away from `Surrogate::ZERO`, which the document substrate
    // reserves as a sentinel for unbound rows.
    nodedb_types::Surrogate::new(h.max(1))
}

/// Insert a product document with stock field.
fn insert_product(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    id: &str,
    stock: u64,
) {
    let value = format!("{{\"name\":\"product\",\"stock\":{stock}}}");
    send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "products".into(),
            document_id: id.into(),
            value: value.into_bytes(),
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
        }),
    );
}

/// Read a product and return its stock value.
fn get_stock(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    id: &str,
) -> u64 {
    let payload = send_ok(
        core,
        tx,
        rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "products".into(),
            document_id: id.into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: surrogate_for(id),
            pk_bytes: id.as_bytes().to_vec(),
        }),
    );
    let v = payload_value(&payload);
    v.get("stock").and_then(|s| s.as_u64()).unwrap_or_default()
}

#[test]
fn bulk_update_returns_affected_count() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "p1", 10);
    insert_product(&mut core, &mut tx, &mut rx, "p2", 5);
    insert_product(&mut core, &mut tx, &mut rx, "p3", 0);

    // Bulk update: SET stock = 99 WHERE stock > 0 (should match p1 and p2).
    let filters = vec![filter("stock", "gt", nodedb_types::Value::Integer(0))];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();
    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(99)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates,
            returning: false,
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v.get("affected").and_then(|a| a.as_u64()), Some(2));

    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "p1"), 99);
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "p2"), 99);
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "p3"), 0);
}

#[test]
fn conditional_decrement_stops_at_zero() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "flash-deal", 5);

    let mut successes = 0u64;
    for i in 0..10 {
        let current_stock = get_stock(&mut core, &mut tx, &mut rx, "flash-deal");

        let filters = vec![filter("stock", "gte", nodedb_types::Value::Integer(1))];
        let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();

        let new_stock = current_stock.saturating_sub(1);
        let updates = vec![(
            "stock".to_string(),
            nodedb::bridge::physical_plan::UpdateValue::Literal(
                nodedb_types::json_to_msgpack(&serde_json::json!(new_stock)).unwrap(),
            ),
        )];

        let payload = send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: "products".into(),
                filters: filter_bytes,
                updates,
                returning: false,
            }),
        );

        let v = payload_value(&payload);
        let affected = v.get("affected").and_then(|a| a.as_u64()).unwrap_or(0);
        if affected > 0 {
            successes += 1;
        }

        if i >= 5 {
            assert_eq!(
                affected, 0,
                "iteration {i}: expected 0 affected after stock depleted"
            );
        }
    }

    assert_eq!(successes, 5, "exactly 5 decrements should succeed");
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "flash-deal"), 0);
}

#[test]
fn bulk_update_zero_match_returns_zero_affected() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "p1", 0);

    let filters = vec![filter("stock", "gte", nodedb_types::Value::Integer(100))];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();
    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(999)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates,
            returning: false,
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v.get("affected").and_then(|a| a.as_u64()), Some(0));
}

#[test]
fn bulk_update_returning_returns_updated_documents() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "r1", 10);
    insert_product(&mut core, &mut tx, &mut rx, "r2", 20);

    let filters = vec![filter("stock", "gt", nodedb_types::Value::Integer(0))];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();
    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(0)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates,
            returning: true,
        }),
    );

    let text = payload_json(&payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(docs.len(), 2);

    for doc in &docs {
        assert_eq!(doc.get("stock").and_then(|s| s.as_u64()), Some(0));
        assert!(doc.get("id").is_some(), "returned doc should include id");
    }
}

#[test]
fn bulk_update_returning_zero_match_returns_affected_zero() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "p1", 0);

    let filters = vec![filter("stock", "gte", nodedb_types::Value::Integer(100))];
    let filter_bytes = zerompk::to_msgpack_vec(&filters).unwrap();
    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(999)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "products".into(),
            filters: filter_bytes,
            updates,
            returning: true,
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v.get("affected").and_then(|a| a.as_u64()), Some(0));
}

#[test]
fn point_update_returns_affected_count() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "pu1", 10);

    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(5)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "products".into(),
            document_id: "pu1".into(),
            updates,
            returning: false,
            surrogate: surrogate_for("pu1"),
            pk_bytes: b"pu1".to_vec(),
        }),
    );

    let v = payload_value(&payload);
    assert_eq!(v.get("affected").and_then(|a| a.as_u64()), Some(1));
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "pu1"), 5);
}

#[test]
fn point_update_returning_returns_updated_document() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "pu2", 10);

    let updates = vec![(
        "stock".to_string(),
        nodedb::bridge::physical_plan::UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(7)).unwrap(),
        ),
    )];

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "products".into(),
            document_id: "pu2".into(),
            updates,
            returning: true,
            surrogate: surrogate_for("pu2"),
            pk_bytes: b"pu2".to_vec(),
        }),
    );

    let text = payload_json(&payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get("stock").and_then(|s| s.as_u64()), Some(7));
    assert_eq!(docs[0].get("id").and_then(|s| s.as_str()), Some("pu2"));
}

#[test]
fn transaction_batch_does_not_abort_on_zero_row_update() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    insert_product(&mut core, &mut tx, &mut rx, "t1", 1);
    insert_product(&mut core, &mut tx, &mut rx, "t2", 0);

    // Transaction: first update matches (stock >= 1), second doesn't (stock >= 100).
    // Batch should NOT auto-abort on 0-row update.
    let filters_match = zerompk::to_msgpack_vec(&vec![filter(
        "stock",
        "gte",
        nodedb_types::Value::Integer(1),
    )])
    .unwrap();

    let filters_nomatch = zerompk::to_msgpack_vec(&vec![filter(
        "stock",
        "gte",
        nodedb_types::Value::Integer(100),
    )])
    .unwrap();

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![
                PhysicalPlan::Document(DocumentOp::BulkUpdate {
                    collection: "products".into(),
                    filters: filters_match,
                    updates: vec![(
                        "stock".to_string(),
                        nodedb::bridge::physical_plan::UpdateValue::Literal(
                            nodedb_types::json_to_msgpack(&serde_json::json!(0)).unwrap(),
                        ),
                    )],
                    returning: false,
                }),
                PhysicalPlan::Document(DocumentOp::BulkUpdate {
                    collection: "products".into(),
                    filters: filters_nomatch,
                    updates: vec![(
                        "stock".to_string(),
                        nodedb::bridge::physical_plan::UpdateValue::Literal(
                            nodedb_types::json_to_msgpack(&serde_json::json!(999)).unwrap(),
                        ),
                    )],
                    returning: false,
                }),
            ],
        }),
    );

    assert_eq!(
        resp.status,
        Status::Ok,
        "transaction should not abort on 0-row update: {:?}",
        resp.error_code
    );

    // t1 updated to 0, t2 unchanged at 0.
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "t1"), 0);
    assert_eq!(get_stock(&mut core, &mut tx, &mut rx, "t2"), 0);
}
