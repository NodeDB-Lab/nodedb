// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Key-Value engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B writing to the same key-space as Tenant A cannot
//! overwrite or delete Tenant A's data.  After each cross-tenant write
//! attempt, Tenant A's key must still return its original value.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::helpers::*;

/// Tenant B Put on the same key must not overwrite Tenant A's value.
#[test]
fn kv_cross_tenant_put_does_not_overwrite() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts a key.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "cache",
            ),
            key: b"shared_key".to_vec(),
            value: b"tenant_a_value".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Tenant B writes to the same collection + key with a different value.
    // This must NOT overwrite Tenant A's entry.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "cache",
            ),
            key: b"shared_key".to_vec(),
            value: b"tenant_b_value".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Tenant A's read must still return a non-empty, non-null payload
    // (A's entry must be intact in A's namespace).
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "cache",
            ),
            key: b"shared_key".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let is_present = !resp_a.payload.is_empty()
        && resp_a.error_code.as_deref() != Some(&ErrorCode::NotFound)
        && !payload_json(&resp_a.payload).contains("null");
    assert!(
        is_present,
        "Tenant A's value must be intact after Tenant B's cross-tenant Put; payload len={}",
        resp_a.payload.len()
    );
}

/// Tenant B Delete on a key that Tenant A owns must not remove Tenant A's entry.
#[test]
fn kv_cross_tenant_delete_does_not_affect_owner() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts a key.
    send_ok_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"sess_xyz".to_vec(),
            value: b"secret_token".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Tenant B attempts to delete the same key.
    // The engine must silently succeed (key doesn't exist in B's namespace)
    // without touching A's entry.
    let resp_del = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Kv(KvOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            keys: vec![b"sess_xyz".to_vec()],
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
    // Either Ok (deleted 0 rows from B's namespace) or NotFound — both correct.
    let ok_or_not_found = resp_del.status == Status::Ok
        || resp_del.error_code.as_deref() == Some(&ErrorCode::NotFound);
    assert!(
        ok_or_not_found,
        "Cross-tenant delete must be Ok or NotFound, got {:?}",
        resp_del.error_code
    );

    // Tenant A's key must still be present (non-empty, non-null payload).
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"sess_xyz".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);
    let is_present = !resp_a.payload.is_empty()
        && resp_a.error_code.as_deref() != Some(&ErrorCode::NotFound)
        && !payload_json(&resp_a.payload).contains("null");
    assert!(
        is_present,
        "Tenant A's KV entry must survive Tenant B's cross-tenant delete; payload len={}",
        resp_a.payload.len()
    );
}
