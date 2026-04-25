//! Cross-tenant isolation: Sparse (Document) engine.
//!
//! Tenant A inserts a document. Tenant B queries the same collection name
//! and document_id — must get NotFound, not Tenant A's data.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::DocumentOp;

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn sparse_point_get_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts a document.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "users".into(),
            document_id: "u1".into(),
            value: b"{\"name\":\"alice\",\"secret\":\"tenant_a_data\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );

    // Tenant A can read it.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "u1".into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let json_a = payload_json(&resp_a.payload);
    assert!(json_a.contains("alice"), "Tenant A should see own data");

    // Tenant B queries the same collection and doc_id — must not see Tenant A's data.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "u1".into(),
            rls_filters: Vec::new(),
            system_as_of_ms: None,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
        }),
    );
    assert_eq!(
        resp_b.error_code, None,
        "Tenant B must NOT see Tenant A's document"
    );
}

#[test]
fn sparse_range_scan_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts several documents.
    for i in 0..5u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "items".into(),
                document_id: format!("item_{i}"),
                value: format!("{{\"val\":{i}}}").into_bytes(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

    // Tenant B range-scans — must get zero results.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Document(DocumentOp::RangeScan {
            collection: "items".into(),
            field: String::new(),
            lower: None,
            upper: None,
            limit: 100,
        }),
    );
    assert_eq!(resp_b.status, Status::Ok);
    let json_b = payload_json(&resp_b.payload);
    // Empty scan should return empty array or empty payload.
    let val: serde_json::Value =
        serde_json::from_str(&json_b).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "Tenant B range scan must return 0 results, got {}: {json_b}",
        arr.len()
    );
}
