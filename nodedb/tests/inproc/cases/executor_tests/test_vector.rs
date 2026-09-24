// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for vector engine operations.

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

#[test]
fn vector_insert_and_search() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for i in 0..10u32 {
        tx.try_push(BridgeRequest::unfloored(make_request_with_id(
            100 + i as u64,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: None,
                provenance: None,
            }),
        )))
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
        PhysicalPlan::Vector(VectorOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
            query_vector: vec![5.0f32, 0.0, 0.0],
            top_k: 3,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );

    let json = payload_json(&payload);
    assert!(json.contains("\"id\""), "payload: {json}");
    assert!(json.contains("\"distance\""), "payload: {json}");
}

#[test]
fn vector_search_no_index_returns_not_found() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "nonexistent"),
            query_vector: vec![1.0f32, 0.0, 0.0],
            top_k: 5,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }),
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
}
