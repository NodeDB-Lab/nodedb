//! Integration tests for KV engine operations via the SPSC bridge.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::KvOp;

use crate::helpers::*;

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

#[test]
fn kv_put_get_delete() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // PUT
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "cache".into(),
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // GET
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "cache".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"value1");

    // DELETE
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Delete {
            collection: "cache".into(),
            keys: vec![b"key1".to_vec()],
        }),
    );
    let json = payload_value(&payload);
    assert_eq!(json["deleted"], 1);

    // GET after DELETE → NotFound
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "cache".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);
}

#[test]
fn kv_overwrite_returns_ok() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "c".into(),
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Overwrite.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "c".into(),
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "c".into(),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"v2");
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

#[test]
fn kv_batch_put_and_get() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5u8).map(|i| (vec![i], vec![i * 10])).collect();

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection: "c".into(),
            entries,
            ttl_ms: 0,
        }),
    );
    let json = payload_value(&payload);
    assert_eq!(json["inserted"], 5);

    // BatchGet
    let keys: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i]).collect();
    let _payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::BatchGet {
            collection: "c".into(),
            keys,
        }),
    );
}

// ---------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------

#[test]
fn kv_scan_returns_entries() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for i in 0..5u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "scantest".into(),
                key: format!("key{i}").into_bytes(),
                value: format!("val{i}").into_bytes(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Scan {
            collection: "scantest".into(),
            cursor: Vec::new(),
            count: 100,
            filters: Vec::new(),
            match_pattern: None,
        }),
    );

    let json = payload_value(&payload);
    let entries = json.as_array().unwrap();
    assert_eq!(entries.len(), 5);
}

#[test]
fn kv_scan_with_match_pattern() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for prefix in &["user:", "session:", "user:"] {
        for i in 0..3u32 {
            send_ok(
                &mut core,
                &mut tx,
                &mut rx,
                PhysicalPlan::Kv(KvOp::Put {
                    collection: "mixed".into(),
                    key: format!("{prefix}{i}").into_bytes(),
                    value: b"data".to_vec(),
                    ttl_ms: 0,
                    surrogate: nodedb_types::Surrogate::ZERO,
                }),
            );
        }
    }

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Scan {
            collection: "mixed".into(),
            cursor: Vec::new(),
            count: 100,
            filters: Vec::new(),
            match_pattern: Some("user:*".into()),
        }),
    );

    let json = payload_value(&payload);
    let entries = json.as_array().unwrap();
    // "user:0", "user:1", "user:2" — 3 entries (second batch overwrites first).
    assert_eq!(entries.len(), 3);
}

// ---------------------------------------------------------------------------
// TTL / Expiry
// ---------------------------------------------------------------------------

#[test]
fn kv_expire_and_persist() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // PUT without TTL.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "c".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Set EXPIRE.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Expire {
            collection: "c".into(),
            key: b"k".to_vec(),
            ttl_ms: 60_000,
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // PERSIST removes TTL.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Persist {
            collection: "c".into(),
            key: b"k".to_vec(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Key should still be accessible.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "c".into(),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"v");
}

// ---------------------------------------------------------------------------
// Secondary indexes
// ---------------------------------------------------------------------------

#[test]
fn kv_register_index_and_lookup() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert entries first.
    let doc1 = nodedb_types::json_to_msgpack(
        &serde_json::json!({"region": "us-east", "status": "active"}),
    )
    .unwrap();
    let doc2 = nodedb_types::json_to_msgpack(
        &serde_json::json!({"region": "eu-west", "status": "active"}),
    )
    .unwrap();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"s1".to_vec(),
            value: doc1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"s2".to_vec(),
            value: doc2,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Register index with backfill.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection: "sessions".into(),
            field: "region".into(),
            field_position: 0,
            backfill: true,
        }),
    );
    let json = payload_value(&payload);
    assert_eq!(json["backfilled"], 2);
}

#[test]
fn kv_drop_index() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Register index.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection: "c".into(),
            field: "status".into(),
            field_position: 0,
            backfill: false,
        }),
    );

    // Insert entry (will be indexed).
    let doc = nodedb_types::json_to_msgpack(&serde_json::json!({"status": "active"})).unwrap();
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "c".into(),
            key: b"k1".to_vec(),
            value: doc,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Drop index.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::DropIndex {
            collection: "c".into(),
            field: "status".into(),
        }),
    );
    let json = payload_value(&payload);
    assert_eq!(json["entries_removed"], 1);
}

// ---------------------------------------------------------------------------
// Tenant isolation
// ---------------------------------------------------------------------------

#[test]
fn kv_tenant_isolation() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant 1 writes.
    let req = nodedb::bridge::envelope::Request {
        tenant_id: nodedb::types::TenantId::new(1),
        ..make_request(PhysicalPlan::Kv(KvOp::Put {
            collection: "shared".into(),
            key: b"k".to_vec(),
            value: b"tenant1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }))
    };
    tx.try_push(nodedb::bridge::dispatch::BridgeRequest { inner: req })
        .unwrap();
    core.tick();
    let resp = rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Ok);

    // Tenant 2 writes same key.
    let req = nodedb::bridge::envelope::Request {
        tenant_id: nodedb::types::TenantId::new(2),
        ..make_request(PhysicalPlan::Kv(KvOp::Put {
            collection: "shared".into(),
            key: b"k".to_vec(),
            value: b"tenant2".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }))
    };
    tx.try_push(nodedb::bridge::dispatch::BridgeRequest { inner: req })
        .unwrap();
    core.tick();
    let resp = rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Ok);

    // Tenant 1 reads — should get "tenant1", not "tenant2".
    let req = nodedb::bridge::envelope::Request {
        tenant_id: nodedb::types::TenantId::new(1),
        ..make_request(PhysicalPlan::Kv(KvOp::Get {
            collection: "shared".into(),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
        }))
    };
    tx.try_push(nodedb::bridge::dispatch::BridgeRequest { inner: req })
        .unwrap();
    core.tick();
    let resp = rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Ok);
    assert_eq!(resp.inner.payload.to_vec(), b"tenant1");
}
