//! Integration tests for vector engine operations.

use std::sync::Arc;

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::{ErrorCode, PhysicalPlan, Status};

use crate::helpers::*;

#[test]
fn vector_insert_and_search() {
    let (mut core, mut tx, mut rx) = make_core();

    for i in 0..10u32 {
        tx.try_push(BridgeRequest {
            inner: make_request_with_id(
                100 + i as u64,
                PhysicalPlan::VectorInsert {
                    collection: "embeddings".into(),
                    vector: vec![i as f32, 0.0, 0.0],
                    dim: 3,
                },
            ),
        })
        .unwrap();
    }

    let processed = core.tick();
    assert_eq!(processed, 10);
    for _ in 0..10 {
        let resp = rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::VectorSearch {
            collection: "embeddings".into(),
            query_vector: Arc::from([5.0f32, 0.0, 0.0].as_slice()),
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
        },
    );

    let json = payload_json(&payload);
    assert!(json.contains("\"id\""), "payload: {json}");
    assert!(json.contains("\"distance\""), "payload: {json}");
}

#[test]
fn vector_search_no_index_returns_not_found() {
    let (mut core, mut tx, mut rx) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::VectorSearch {
            collection: "nonexistent".into(),
            query_vector: Arc::from([1.0f32, 0.0, 0.0].as_slice()),
            top_k: 5,
            ef_search: 0,
            filter_bitmap: None,
        },
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code, Some(ErrorCode::NotFound));
}
