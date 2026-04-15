//! Cross-tenant isolation: Vector engine.
//!
//! Tenant A inserts vectors. Tenant B searches — must get zero results.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::VectorOp;

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn vector_search_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts vectors.
    for i in 0..10u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: "embeddings".into(),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                doc_id: None,
            }),
        );
    }

    // Tenant A can search and find results.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "embeddings".into(),
            query_vector: vec![5.0f32, 0.0, 0.0],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    assert!(!resp_a.payload.is_empty(), "Tenant A should find vectors");

    // Tenant B searches the same collection — must get zero results.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "embeddings".into(),
            query_vector: vec![5.0f32, 0.0, 0.0],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
        }),
    );
    // Tenant B has no vector index for this collection — the engine returns
    // either Ok with empty results or Error (no index found). Both are correct
    // isolation: Tenant B cannot see Tenant A's vectors.
    let is_isolated = resp_b.status == Status::Error || resp_b.payload.is_empty() || {
        let json_b = payload_json(&resp_b.payload);
        let val: serde_json::Value =
            serde_json::from_str(&json_b).unwrap_or(serde_json::Value::Array(vec![]));
        let empty = vec![];
        val.as_array().unwrap_or(&empty).is_empty()
    };
    assert!(
        is_isolated,
        "Tenant B vector search must return 0 results or error, got: {:?}",
        resp_b.status
    );
}
