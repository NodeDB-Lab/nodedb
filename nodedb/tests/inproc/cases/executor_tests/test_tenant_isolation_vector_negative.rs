// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Vector engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B inserting vectors into the same collection as Tenant A
//! cannot contaminate Tenant A's search results.  After Tenant B's inserts,
//! Tenant A's search must return the same count of results it returned before.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{PhysicalPlan, VectorOp};
use nodedb_types::vector_distance::DistanceMetric;
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

/// Tenant B inserting vectors must not inflate Tenant A's search results.
#[test]
fn vector_cross_tenant_insert_does_not_contaminate_search() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts exactly 5 vectors.
    for i in 0..5u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: nodedb_types::Surrogate::new(i + 1),
                pk_bytes: None,
                provenance: None,
            }),
        );
    }

    // Establish baseline: Tenant A sees ≤ 5 results for top_k=10.
    let resp_baseline = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
            query_vector: vec![2.0f32, 0.0, 0.0],
            top_k: 10,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
    );
    assert_eq!(resp_baseline.status, Status::Ok);
    let json_baseline = payload_json(&resp_baseline.payload);
    let baseline_count = serde_json::from_str::<serde_json::Value>(&json_baseline)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    // Tenant B inserts 20 additional vectors into the same collection name.
    for i in 0..20u32 {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_B,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
                vector: vec![i as f32, 1.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: nodedb_types::Surrogate::new(i + 1001),
                pk_bytes: None,
                provenance: None,
            }),
        );
    }

    // Tenant A's search must return the same number of results as the baseline.
    let resp_after = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
            query_vector: vec![2.0f32, 0.0, 0.0],
            top_k: 10,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
    );
    assert_eq!(resp_after.status, Status::Ok);
    let json_after = payload_json(&resp_after.payload);
    let after_count = serde_json::from_str::<serde_json::Value>(&json_after)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    assert_eq!(
        after_count, baseline_count,
        "Tenant B's inserts must not inflate Tenant A's vector search results \
         (baseline={baseline_count}, after={after_count})"
    );
}
