//! Integration smoke tests for `Gateway::execute` and `Gateway::execute_sql`.
//!
//! Tests:
//! 1. Single-node: `Gateway::execute` on a `KvOp::Put` then `KvOp::Get`
//!    succeeds, proving the gateway + dispatcher wire through to the Data Plane.
//! 2. Plan cache: two identical `execute_sql` calls → second returns from
//!    cache (cache length grows to 1 after first call, stays 1 after second).
//!
//! These tests run in the `cluster` nextest group (single-threaded, no
//! parallel cluster interference) because they bring up a full NodeDB node.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::physical_plan::{KvOp, PhysicalPlan};
use nodedb::control::gateway::core::QueryContext;
use nodedb::control::gateway::plan_cache::PlanCacheKey;
use nodedb::control::gateway::plan_cache::{hash_placeholder_types, hash_sql};
use nodedb::control::gateway::version_set::GatewayVersionSet;
use nodedb::control::gateway::{Gateway, PlanCache};
use nodedb::types::TenantId;

use common::cluster_harness::TestClusterNode;

/// Minimal query context for tests.
fn test_ctx() -> QueryContext {
    QueryContext {
        tenant_id: TenantId::new(0),
        trace_id: 0xCAFE_1234,
    }
}

/// Encode a string value as a minimal MessagePack scalar.
fn mp_string(s: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&nodedb_types::Value::String(s.into())).expect("encode string value")
}

// ---------------------------------------------------------------------------
// Test 1: single-node Put → Get round-trip
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_execute_kv_put_get_single_node() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node cluster");

    // Wait for the node to elect itself leader.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create the collection so the Data Plane knows about it.
    node.exec("CREATE COLLECTION gw_kv_smoke")
        .await
        .expect("CREATE COLLECTION");

    // Give the Data Plane a moment to register the new collection.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Build a Gateway on top of the node's SharedState.
    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx();

    // Put.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_kv_smoke".into(),
        key: b"smoke-key".to_vec(),
        value: mp_string("smoke-value"),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    let put_result = gateway.execute(&ctx, put_plan).await;
    assert!(
        put_result.is_ok(),
        "KvOp::Put failed: {:?}",
        put_result.unwrap_err()
    );

    // Get.
    let get_plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_kv_smoke".into(),
        key: b"smoke-key".to_vec(),
        rls_filters: vec![],
    });
    let get_result = gateway.execute(&ctx, get_plan).await;
    assert!(
        get_result.is_ok(),
        "KvOp::Get failed: {:?}",
        get_result.unwrap_err()
    );

    let payloads = get_result.unwrap();
    assert!(!payloads.is_empty(), "Get returned no payload");

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 2: plan cache populates on execute_sql and does not grow unboundedly
// ---------------------------------------------------------------------------
//
// The speculative cache key uses an empty version set (we don't parse SQL to
// extract collections). The actual key is computed from the plan after
// planning. Two calls with the same SQL and the same descriptor state produce
// the same actual key, so the second insert is a no-op and cache length stays
// at 1.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_execute_sql_plan_cache_populated() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node cluster");

    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_cache_smoke")
        .await
        .expect("CREATE COLLECTION");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx();

    let sql = "GET gw_cache_smoke smoke-key";
    let make_plan = || {
        Ok(PhysicalPlan::Kv(KvOp::Get {
            collection: "gw_cache_smoke".into(),
            key: b"smoke-key".to_vec(),
            rls_filters: vec![],
        }))
    };

    // Cache starts empty.
    assert_eq!(gateway.plan_cache.len(), 0);

    // First call: cache miss — plan_fn is invoked; cache grows to 1.
    let _ = gateway
        .execute_sql(&ctx, sql, &[], make_plan)
        .await
        .expect("first execute_sql");

    assert_eq!(
        gateway.plan_cache.len(),
        1,
        "expected 1 entry after first call"
    );

    // Second call with same SQL + same descriptor versions: the actual key is
    // identical, so insert is a no-op and len stays 1.
    let _ = gateway
        .execute_sql(&ctx, sql, &[], make_plan)
        .await
        .expect("second execute_sql");

    assert_eq!(
        gateway.plan_cache.len(),
        1,
        "cache grew on second call with same key — duplicate inserted"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 3: plan cache key stable-hash consistency (pure unit logic, no node)
// ---------------------------------------------------------------------------

#[test]
fn plan_cache_key_construction_and_lookup() {
    let cache = Arc::new(PlanCache::new(8));

    let vs = GatewayVersionSet::from_pairs(vec![("gw_kv_smoke".into(), 1)]);
    let key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_kv_smoke smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: vs.clone(),
    };

    assert!(
        cache.get(&key).is_none(),
        "unexpected cache hit on empty cache"
    );

    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_kv_smoke".into(),
        key: b"smoke-key".to_vec(),
        rls_filters: vec![],
    });
    cache.insert(key.clone(), Arc::new(plan));

    assert!(cache.get(&key).is_some(), "cache miss after insert");
    assert_eq!(cache.len(), 1);
}
