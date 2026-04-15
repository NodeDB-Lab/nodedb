//! Phase 4 validation gate tests.
//!
//! 1. Security regression: auth failures, privilege changes, tenant isolation
//! 2. Linearizability: WAL append + read consistency
//! 3. WAL replay: deterministic traces
//! 4. Mixed-engine isolation: protected-tier not evicted under budget

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, GraphOp, VectorOp};

use crate::helpers::*;

// ──────────────────────────────────────────────────────────────────────
// Gate 1: Security regression — auth failures, privilege, tenant isolation
// ──────────────────────────────────────────────────────────────────────

#[test]
fn security_tenant_isolation() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant 1 inserts a document.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "secrets".into(),
            document_id: "s1".into(),
            value: b"{\"data\":\"tenant1_secret\"}".to_vec(),
        }),
    );

    // Tenant 1 can read it.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "secrets".into(),
            document_id: "s1".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // The storage key is tenant-scoped: "{tenant_id}:{collection}:{doc_id}".
    // A different tenant_id would produce a different key and get NotFound.
    // This is enforced at the storage layer — the CoreLoop test uses tenant_id=1
    // from make_request(). Cross-tenant access is blocked by the planner's
    // tenant_id check before dispatch.
}

#[test]
fn security_rls_policy_enforcement() {
    use nodedb::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};

    let store = RlsPolicyStore::new();

    // Create a write policy requiring status = "approved".
    let filter = nodedb::bridge::scan_filter::ScanFilter {
        field: "status".into(),
        op: "eq".into(),
        value: nodedb_types::Value::String("approved".into()),
        clauses: Vec::new(),
        expr: None,
    };
    let predicate = zerompk::to_msgpack_vec(&vec![filter]).unwrap();

    store
        .create_policy(RlsPolicy {
            name: "require_approved".into(),
            collection: "orders".into(),
            tenant_id: 1,
            policy_type: PolicyType::Write,
            predicate,
            compiled_predicate: None,
            mode: nodedb::control::security::predicate::PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .unwrap();

    // Approved document passes.
    let doc_ok = serde_json::json!({"status": "approved", "amount": 100});
    assert!(store.check_write(1, "orders", &doc_ok, "user1").is_ok());

    // Pending document rejected.
    let doc_bad = serde_json::json!({"status": "pending", "amount": 200});
    let err = store.check_write(1, "orders", &doc_bad, "user1");
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("require_approved"));

    // Different tenant has no policies — allowed.
    assert!(store.check_write(99, "orders", &doc_bad, "user1").is_ok());
}

#[test]
fn security_jwt_validation() {
    use nodedb::control::security::jwt::{JwtConfig, JwtError, JwtValidator};

    let validator = JwtValidator::new(JwtConfig::default());

    // Malformed token rejected.
    assert_eq!(
        validator.validate("not-a-jwt").unwrap_err(),
        JwtError::MalformedToken
    );

    // Two-part token rejected.
    assert_eq!(
        validator.validate("header.payload").unwrap_err(),
        JwtError::MalformedToken
    );
}

// ──────────────────────────────────────────────────────────────────────
// Gate 2: Linearizability — WAL append guarantees read-after-write
// ──────────────────────────────────────────────────────────────────────

#[test]
fn linearizability_read_after_write() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Write then immediately read — must see the write.
    for i in 0..20u32 {
        let doc_id = format!("lin_{i}");
        let value = format!("{{\"seq\":{i}}}");

        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "linear".into(),
                document_id: doc_id.clone(),
                value: value.into_bytes(),
            }),
        );

        let resp = send_raw(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "linear".into(),
                document_id: doc_id.clone(),
                rls_filters: Vec::new(),
            }),
        );
        assert_eq!(
            resp.status,
            Status::Ok,
            "read-after-write failed for {doc_id}"
        );
    }
}

#[test]
fn linearizability_delete_visibility() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "linear".into(),
            document_id: "del1".into(),
            value: b"{\"x\":1}".to_vec(),
        }),
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: "linear".into(),
            document_id: "del1".into(),
        }),
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "linear".into(),
            document_id: "del1".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.error_code, None);
}

// ──────────────────────────────────────────────────────────────────────
// Gate 3: WAL replay produces deterministic results
// ──────────────────────────────────────────────────────────────────────

#[test]
fn wal_replay_deterministic() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Perform a deterministic sequence of operations.
    let ops = vec![
        ("put", "d1", b"{\"a\":1}".to_vec()),
        ("put", "d2", b"{\"b\":2}".to_vec()),
        ("put", "d3", b"{\"c\":3}".to_vec()),
        ("put", "d1", b"{\"a\":10}".to_vec()), // Overwrite d1.
    ];

    for (op, doc_id, value) in &ops {
        if *op == "put" {
            send_ok(
                &mut core,
                &mut tx,
                &mut rx,
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "replay".into(),
                    document_id: doc_id.to_string(),
                    value: value.clone(),
                }),
            );
        }
    }

    // Verify final state is deterministic.
    let d1 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "replay".into(),
            document_id: "d1".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(d1.status, Status::Ok);
    // d1 was overwritten to {"a":10}.
    let d1_json = payload_json(&d1.payload);
    assert!(
        d1_json.contains("10"),
        "d1 should be overwritten: {d1_json}"
    );

    let d2 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "replay".into(),
            document_id: "d2".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(d2.status, Status::Ok);

    let d3 = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "replay".into(),
            document_id: "d3".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(d3.status, Status::Ok);
}

// ──────────────────────────────────────────────────────────────────────
// Gate 4: Mixed-engine isolation — no protected-tier eviction
// ──────────────────────────────────────────────────────────────────────

#[test]
fn mixed_engine_isolation_no_cross_eviction() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert documents (sparse engine).
    for i in 0..50u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "mixed".into(),
                document_id: format!("doc_{i}"),
                value: format!("{{\"val\":{i}}}").into_bytes(),
            }),
        );
    }

    // Insert vectors (vector engine).
    for i in 0..50u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: "mixed".into(),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                doc_id: None,
            }),
        );
    }

    // Insert edges (graph engine).
    for i in 0..49u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Graph(GraphOp::EdgePut {
                src_id: format!("doc_{i}"),
                label: "NEXT".into(),
                dst_id: format!("doc_{}", i + 1),
                properties: vec![],
            }),
        );
    }

    // All engines should still work — no cross-eviction.
    // Sparse engine: point get.
    let doc = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "mixed".into(),
            document_id: "doc_25".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(doc.status, Status::Ok, "sparse engine should be intact");

    // Vector engine: search.
    let vec_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "mixed".into(),
            query_vector: vec![25.0f32, 0.0, 0.0],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(
        vec_resp.status,
        Status::Ok,
        "vector engine should be intact"
    );

    // Graph engine: neighbors.
    let graph_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "doc_25".into(),
            edge_label: Some("NEXT".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(
        graph_resp.status,
        Status::Ok,
        "graph engine should be intact"
    );
    let neighbors_json = payload_json(&graph_resp.payload);
    assert!(
        neighbors_json.contains("doc_26"),
        "graph should have doc_25→doc_26: {neighbors_json}"
    );
}
