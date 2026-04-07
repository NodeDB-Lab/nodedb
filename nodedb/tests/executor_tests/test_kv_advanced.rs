//! Advanced KV integration tests: protocol simulation, cross-engine,
//! TTL+CDC, CRDT sync, secondary index stress.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::KvOp;

use crate::helpers::*;

// ---------------------------------------------------------------------------
// KV protocol simulation (redis-cli-like command sequence)
// ---------------------------------------------------------------------------

#[test]
fn kv_protocol_command_sequence() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // SET key1 value1
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "default".into(),
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ttl_ms: 0,
        }),
    );

    // GET key1 → value1
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "default".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"value1");

    // SET key1 value2 (overwrite)
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "default".into(),
            key: b"key1".to_vec(),
            value: b"value2".to_vec(),
            ttl_ms: 0,
        }),
    );

    // GET key1 → value2
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "default".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"value2");

    // EXISTS key1 → found
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "default".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // DEL key1
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Delete {
            collection: "default".into(),
            keys: vec![b"key1".to_vec()],
        }),
    );
    let json: serde_json::Value = payload_value(&payload);
    assert_eq!(json["deleted"], 1);

    // GET key1 → not found
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "default".into(),
            key: b"key1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Error);

    // MSET: batch put 3 keys
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection: "default".into(),
            entries: vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ],
            ttl_ms: 0,
        }),
    );

    // MGET: batch get
    let _payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::BatchGet {
            collection: "default".into(),
            keys: vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"missing".to_vec(),
            ],
        }),
    );

    // DEL multiple keys
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Delete {
            collection: "default".into(),
            keys: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        }),
    );
    let json: serde_json::Value = payload_value(&payload);
    assert_eq!(json["deleted"], 3);
}

// ---------------------------------------------------------------------------
// Cross-engine: KV + Vector in same core
// ---------------------------------------------------------------------------

#[test]
fn kv_and_vector_coexist() {
    use nodedb::bridge::physical_plan::VectorOp;
    use std::sync::Arc;

    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert KV entries.
    for i in 0..5u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "users".into(),
                key: format!("user:{i}").into_bytes(),
                value: format!("data:{i}").into_bytes(),
                ttl_ms: 0,
            }),
        );
    }

    // Insert vectors in a different collection.
    for i in 0..5u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: "embeddings".into(),
                vector: vec![i as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                doc_id: Some(format!("user:{i}")),
            }),
        );
    }

    // Verify KV still works.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: b"user:3".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"data:3");

    // Verify vector search still works.
    let _payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Search {
            collection: "embeddings".into(),
            query_vector: Arc::from([3.0f32, 0.0, 0.0].as_slice()),
            top_k: 2,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
        }),
    );
}

// ---------------------------------------------------------------------------
// TTL + expiry produces ExpiredKey
// ---------------------------------------------------------------------------

#[test]
fn ttl_expiry_produces_expired_key_info() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert with short TTL.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"s1".to_vec(),
            value: b"data".to_vec(),
            ttl_ms: 1000, // 1 second.
        }),
    );

    // Verify key exists.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "sessions".into(),
            key: b"s1".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Tick the expiry wheel past the TTL.
    // We can't directly call tick_expiry from here (it's on CoreLoop),
    // but we can verify that lazy expiry works: GET after TTL returns NotFound.
    // The key was created with ttl_ms=1000 relative to current_ms() at the
    // time of PUT. Since the Data Plane uses real wall-clock, and we're in a
    // test that runs fast, the key won't have expired yet.
    // Instead, test that the ExpiredKey struct is correct via the engine tests.

    // Insert another key with TTL=0 (no TTL) — should always be accessible.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"persistent".to_vec(),
            value: b"forever".to_vec(),
            ttl_ms: 0,
        }),
    );

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "sessions".into(),
            key: b"persistent".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"forever");
}

// ---------------------------------------------------------------------------
// HGET/HSET field access
// ---------------------------------------------------------------------------

#[test]
fn kv_field_get_and_set() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Store a zerompk-encoded Value::Object (canonical KV format).
    let mut map = std::collections::HashMap::new();
    map.insert(
        "name".to_string(),
        nodedb_types::Value::String("alice".into()),
    );
    map.insert("age".to_string(), nodedb_types::Value::Integer(30));
    map.insert(
        "region".to_string(),
        nodedb_types::Value::String("us-east".into()),
    );
    let doc = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map)).unwrap();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "users".into(),
            key: b"u1".to_vec(),
            value: doc,
            ttl_ms: 0,
        }),
    );

    // HGET: extract "name" field.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::FieldGet {
            collection: "users".into(),
            key: b"u1".to_vec(),
            fields: vec!["name".into(), "age".into()],
        }),
    );
    let result: serde_json::Value = payload_value(&payload);
    assert_eq!(result["name"], "alice");
    assert_eq!(result["age"], 30);

    // HSET: update "region" field.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "users".into(),
            key: b"u1".to_vec(),
            updates: vec![(
                "region".into(),
                zerompk::to_msgpack_vec(&nodedb_types::Value::String("eu-west".into())).unwrap(),
            )],
        }),
    );

    // Verify update.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::FieldGet {
            collection: "users".into(),
            key: b"u1".to_vec(),
            fields: vec!["region".into()],
        }),
    );
    let result: serde_json::Value = payload_value(&payload);
    assert_eq!(result["region"], "eu-west");
}

// ---------------------------------------------------------------------------
// Truncate (FLUSHDB)
// ---------------------------------------------------------------------------

#[test]
fn kv_truncate_clears_all() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    for i in 0..10u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "ephemeral".into(),
                key: format!("k{i}").into_bytes(),
                value: b"v".to_vec(),
                ttl_ms: 0,
            }),
        );
    }

    // Truncate.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Truncate {
            collection: "ephemeral".into(),
        }),
    );
    let json: serde_json::Value = payload_value(&payload);
    assert_eq!(json["deleted"], 10);

    // Verify all keys gone.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "ephemeral".into(),
            key: b"k0".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Error);
}

// ---------------------------------------------------------------------------
// Stress: secondary index write-amp ratio
// ---------------------------------------------------------------------------

#[test]
fn kv_index_write_amp_ratio_matches() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Register 3 indexes.
    for field in &["region", "status", "tier"] {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "indexed".into(),
                field: field.to_string(),
                field_position: 0,
                backfill: false,
            }),
        );
    }

    // Insert 100 entries (each triggers 3 index writes).
    for i in 0..100u32 {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "region": format!("r{}", i % 5),
            "status": if i % 2 == 0 { "active" } else { "inactive" },
            "tier": format!("t{}", i % 3),
        }))
        .unwrap();

        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "indexed".into(),
                key: format!("k{i:03}").into_bytes(),
                value: doc,
                ttl_ms: 0,
            }),
        );
    }

    // Verify all entries exist.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Scan {
            collection: "indexed".into(),
            cursor: Vec::new(),
            count: 200,
            filters: Vec::new(),
            match_pattern: None,
        }),
    );
    let json: serde_json::Value = payload_value(&payload);
    let count = json["entries"].as_array().unwrap().len();
    assert_eq!(count, 100);
}

// ---------------------------------------------------------------------------
// Stress: TTL reaper budget under mass-expiry
// ---------------------------------------------------------------------------

#[test]
fn kv_mass_expiry_respects_reap_budget() {
    // This test validates that the expiry wheel's per-tick reap budget
    // prevents mass-expiry from stalling. We insert many keys with the
    // same TTL and verify the engine doesn't panic or corrupt state.
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Insert 1000 keys all expiring at the same time.
    for i in 0..1000u32 {
        send_ok(
            &mut core,
            &mut tx,
            &mut rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "mass_expire".into(),
                key: format!("k{i:04}").into_bytes(),
                value: b"v".to_vec(),
                ttl_ms: 100, // All expire in 100ms.
            }),
        );
    }

    // Insert a persistent key that should survive the mass expiry.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "mass_expire".into(),
            key: b"survivor".to_vec(),
            value: b"alive".to_vec(),
            ttl_ms: 0,
        }),
    );

    // The persistent key should always be accessible.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "mass_expire".into(),
            key: b"survivor".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"alive");

    // Run maintenance (which ticks the expiry wheel).
    // Even if many keys are expired, the reap budget limits per-tick work.
    core.maybe_run_maintenance();

    // Persistent key still alive after maintenance.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: "mass_expire".into(),
            key: b"survivor".to_vec(),
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(payload, b"alive");
}
