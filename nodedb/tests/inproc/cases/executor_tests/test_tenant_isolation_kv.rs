// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Key-Value engine.
//!
//! Tenant A puts a key. Tenant B gets the same key — must get NotFound.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

#[test]
fn kv_get_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A puts a key.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"session_abc".to_vec(),
            value: b"tenant_a_session_data".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        }),
    );

    // Tenant A can read it.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"session_abc".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    assert!(
        !resp_a.payload.is_empty(),
        "Tenant A should see own KV data"
    );

    // Tenant B gets the same key — must be empty or NotFound.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: b"session_abc".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    // KV engine returns Ok with empty payload or NotFound for missing keys.
    let is_empty = resp_b.payload.is_empty()
        || resp_b.error_code.as_deref() == Some(&ErrorCode::NotFound)
        || payload_json(&resp_b.payload).contains("null");
    assert!(
        is_empty,
        "Tenant B must NOT see Tenant A's KV data, got: {:?}",
        payload_json(&resp_b.payload)
    );
}
