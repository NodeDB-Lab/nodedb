//! Tenant data purge integration test.
//!
//! Creates data across multiple engines as Tenant A, purges, then verifies
//! zero remaining data. Tenant B's data must be unaffected.

use nodedb::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, GraphOp, KvOp, MetaOp, TimeseriesOp};

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn purge_removes_all_tenant_data() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // ── Populate Tenant A across engines ──

    // Sparse: documents.
    for i in 0..5u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "users".into(),
                document_id: format!("u{i}"),
                value: format!("{{\"name\":\"user_{i}\"}}").into_bytes(),
            }),
        );
    }

    // Graph: edges.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            src_id: "u0".into(),
            label: "KNOWS".into(),
            dst_id: "u1".into(),
            properties: vec![],
        }),
    );

    // KV: entries.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"sess_1".to_vec(),
            value: b"session_data".to_vec(),
            ttl_ms: 0,
        }),
    );

    // Timeseries: ingest.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".into(),
            payload: b"cpu,host=a value=1.0 1000000000\n".to_vec(),
            format: "ilp".into(),
            wal_lsn: None,
        }),
    );

    // ── Populate Tenant B (must survive purge) ──

    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "users".into(),
            document_id: "u0".into(),
            value: b"{\"name\":\"tenant_b_user\"}".to_vec(),
        }),
    );

    // ── Purge Tenant A ──

    let purge_resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Meta(MetaOp::PurgeTenant {
            tenant_id: TENANT_A,
        }),
    );
    assert_eq!(purge_resp.status, Status::Ok, "purge should succeed");

    // Verify purge summary.
    let summary = payload_value(&purge_resp.payload);
    assert!(
        summary["documents_removed"].as_u64().unwrap_or(0) > 0,
        "should have removed documents"
    );
    assert!(
        summary["edges_removed"].as_u64().unwrap_or(0) > 0,
        "should have removed edges"
    );
    assert!(
        summary["kv_tables_removed"].as_u64().unwrap_or(0) > 0,
        "should have removed KV tables"
    );

    // ── Verify Tenant A data is gone ──

    // Sparse: all documents gone.
    let resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "u0".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(
        resp.error_code,
        Some(ErrorCode::NotFound),
        "Tenant A documents should be purged"
    );

    // Graph: edges gone.
    let graph_resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "u0".into(),
            edge_label: Some("KNOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(graph_resp.status, Status::Ok);
    let graph_json = payload_json(&graph_resp.payload);
    assert!(
        !graph_json.contains("u1"),
        "Tenant A graph edges should be purged, got: {graph_json}"
    );

    // KV: gone.
    let kv_resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "sessions".into(),
            key: b"sess_1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    let kv_empty = kv_resp.payload.is_empty()
        || kv_resp.error_code == Some(ErrorCode::NotFound)
        || payload_json(&kv_resp.payload).contains("null");
    assert!(kv_empty, "Tenant A KV data should be purged");

    // Timeseries: gone.
    let ts_resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "metrics".into(),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 100,
            filters: vec![],
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            rls_filters: vec![],
        }),
    );
    assert_eq!(ts_resp.status, Status::Ok);
    let ts_json = payload_json(&ts_resp.payload);
    let ts_val: serde_json::Value =
        serde_json::from_str(&ts_json).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let ts_arr = ts_val.as_array().unwrap_or(&empty);
    assert!(
        ts_arr.is_empty(),
        "Tenant A timeseries data should be purged, got: {ts_json}"
    );

    // ── Verify Tenant B data is intact ──

    let b_resp = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "u0".into(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(
        b_resp.status,
        Status::Ok,
        "Tenant B data must survive Tenant A's purge"
    );
    let b_json = payload_json(&b_resp.payload);
    assert!(
        b_json.contains("tenant_b_user"),
        "Tenant B document should be intact: {b_json}"
    );
}

#[test]
fn purge_is_idempotent() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert some data.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "test".into(),
            document_id: "d1".into(),
            value: b"{\"x\":1}".to_vec(),
        }),
    );

    // First purge.
    let r1 = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Meta(MetaOp::PurgeTenant {
            tenant_id: TENANT_A,
        }),
    );
    assert_eq!(r1.status, Status::Ok);

    // Second purge (idempotent — no data to delete).
    let r2 = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Meta(MetaOp::PurgeTenant {
            tenant_id: TENANT_A,
        }),
    );
    assert_eq!(r2.status, Status::Ok, "second purge should succeed");
    let summary = payload_value(&r2.payload);
    assert_eq!(
        summary["documents_removed"].as_u64().unwrap_or(99),
        0,
        "second purge should remove nothing"
    );
}
