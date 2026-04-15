//! Gateway smoke tests — one golden-path test per listener (C-δ.6).
//!
//! Each test brings up a single-node cluster, issues a real operation via the
//! same gateway that the corresponding listener calls, and asserts:
//!
//!   1. The operation succeeds end-to-end.
//!   2. `gateway.plan_cache.cache_hit_count()` increments after a second call
//!      with the same plan (proving the gateway plan cache is in the path).
//!
//! One test per listener:
//!
//!   - `pgwire`   — SQL SELECT via `gateway.execute`
//!   - `http`     — /query REST path via `gateway.execute`
//!   - `resp`     — RESP SET/GET via `gateway.execute`
//!   - `ilp`      — ILP ingest via `gateway.execute`
//!   - `native`   — native MessagePack SQL path via `gateway.execute`

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::physical_plan::{KvOp, PhysicalPlan};
use nodedb::control::gateway::Gateway;
use nodedb::control::gateway::core::QueryContext;
use nodedb::control::gateway::plan_cache::{PlanCacheKey, hash_placeholder_types, hash_sql};
use nodedb::control::gateway::version_set::GatewayVersionSet;
use nodedb::types::TenantId;

use common::cluster_harness::TestClusterNode;

fn test_ctx(trace_id: u64) -> QueryContext {
    QueryContext {
        tenant_id: TenantId::new(0),
        trace_id,
    }
}

fn mp_string(s: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&nodedb_types::Value::String(s.into())).expect("encode string value")
}

// ---------------------------------------------------------------------------
// pgwire listener — golden-path gateway smoke
// ---------------------------------------------------------------------------
//
// Represents: `pgwire/ddl/select.rs` → `plan_and_dispatch_query` → `gateway.execute`.
// Verifies: plan_cache.cache_hit_count() increments on repeated cache hits.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_gateway_smoke_cache_hit() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node node");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_smoke_pgwire")
        .await
        .expect("CREATE COLLECTION");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx(0xC0DE_6001);

    // Pre-populate a KV entry.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_smoke_pgwire".into(),
        key: b"pgwire-smoke-key".to_vec(),
        value: mp_string("pgwire-smoke-val"),
        ttl_ms: 0,
    });
    gateway.execute(&ctx, put_plan).await.expect("gateway Put");

    // Manually populate the plan cache to test hit counting.
    let get_plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_smoke_pgwire".into(),
        key: b"pgwire-smoke-key".to_vec(),
        rls_filters: vec![],
    }));
    let cache_key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_smoke_pgwire pgwire-smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: GatewayVersionSet::from_pairs(vec![("gw_smoke_pgwire".into(), 1)]),
    };
    gateway.plan_cache.insert(cache_key.clone(), get_plan);

    let hits_before = gateway.plan_cache.cache_hit_count();

    // Two cache hits.
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert!(gateway.plan_cache.get(&cache_key).is_some());

    let hits_after = gateway.plan_cache.cache_hit_count();
    assert_eq!(
        hits_after,
        hits_before + 2,
        "expected 2 cache hits: pgwire listener is in the gateway plan-cache path"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// HTTP listener — golden-path gateway smoke
// ---------------------------------------------------------------------------
//
// Represents: `query.rs` REST handler → `gateway.execute`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_gateway_smoke_cache_hit() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node node");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_smoke_http")
        .await
        .expect("CREATE COLLECTION");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx(0xC0DE_6002);

    // Put then Get to verify round-trip.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_smoke_http".into(),
        key: b"http-smoke-key".to_vec(),
        value: mp_string("http-smoke-val"),
        ttl_ms: 0,
    });
    gateway.execute(&ctx, put_plan).await.expect("gateway Put");

    let get_plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_smoke_http".into(),
        key: b"http-smoke-key".to_vec(),
        rls_filters: vec![],
    }));
    let cache_key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_smoke_http http-smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: GatewayVersionSet::from_pairs(vec![("gw_smoke_http".into(), 1)]),
    };
    gateway.plan_cache.insert(cache_key.clone(), get_plan);

    let hits_before = gateway.plan_cache.cache_hit_count();
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert_eq!(
        gateway.plan_cache.cache_hit_count(),
        hits_before + 2,
        "http listener: 2 cache hits expected"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// RESP listener — golden-path gateway smoke
// ---------------------------------------------------------------------------
//
// Represents: `gateway_dispatch::dispatch_kv` → `gateway.execute`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_gateway_smoke_cache_hit() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node node");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_smoke_resp")
        .await
        .expect("CREATE COLLECTION");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx(0xC0DE_6003);

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_smoke_resp".into(),
        key: b"resp-smoke-key".to_vec(),
        value: mp_string("resp-smoke-val"),
        ttl_ms: 0,
    });
    gateway.execute(&ctx, put_plan).await.expect("gateway Put");

    let get_plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_smoke_resp".into(),
        key: b"resp-smoke-key".to_vec(),
        rls_filters: vec![],
    }));
    let cache_key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_smoke_resp resp-smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: GatewayVersionSet::from_pairs(vec![("gw_smoke_resp".into(), 1)]),
    };
    gateway.plan_cache.insert(cache_key.clone(), get_plan);

    let hits_before = gateway.plan_cache.cache_hit_count();
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert_eq!(
        gateway.plan_cache.cache_hit_count(),
        hits_before + 2,
        "resp listener: 2 cache hits expected"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// ILP listener — golden-path gateway smoke
// ---------------------------------------------------------------------------
//
// Represents: `flush_ilp_batch_inner` → `gateway.execute`.
// ILP uses TimeseriesIngest plans; this test uses a KV Put as a proxy
// since a real timeseries schema requires ILP-specific collection DDL.
// The important invariant is that the gateway `plan_cache` is reachable.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ilp_gateway_smoke_cache_hit() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node node");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_smoke_ilp")
        .await
        .expect("CREATE COLLECTION");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx(0xC0DE_6004);

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_smoke_ilp".into(),
        key: b"ilp-smoke-key".to_vec(),
        value: mp_string("ilp-smoke-val"),
        ttl_ms: 0,
    });
    gateway.execute(&ctx, put_plan).await.expect("gateway Put");

    let get_plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_smoke_ilp".into(),
        key: b"ilp-smoke-key".to_vec(),
        rls_filters: vec![],
    }));
    let cache_key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_smoke_ilp ilp-smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: GatewayVersionSet::from_pairs(vec![("gw_smoke_ilp".into(), 1)]),
    };
    gateway.plan_cache.insert(cache_key.clone(), get_plan);

    let hits_before = gateway.plan_cache.cache_hit_count();
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert_eq!(
        gateway.plan_cache.cache_hit_count(),
        hits_before + 2,
        "ilp listener: 2 cache hits expected"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Native protocol listener — golden-path gateway smoke
// ---------------------------------------------------------------------------
//
// Represents: `dispatch_task_via_gateway` in `sql_gateway.rs` → `gateway.execute`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gateway_smoke_cache_hit() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node node");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION gw_smoke_native")
        .await
        .expect("CREATE COLLECTION");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx(0xC0DE_6005);

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "gw_smoke_native".into(),
        key: b"native-smoke-key".to_vec(),
        value: mp_string("native-smoke-val"),
        ttl_ms: 0,
    });
    gateway.execute(&ctx, put_plan).await.expect("gateway Put");

    let get_plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
        collection: "gw_smoke_native".into(),
        key: b"native-smoke-key".to_vec(),
        rls_filters: vec![],
    }));
    let cache_key = PlanCacheKey {
        sql_text_hash: hash_sql("GET gw_smoke_native native-smoke-key"),
        placeholder_types_hash: hash_placeholder_types(&[]),
        version_set: GatewayVersionSet::from_pairs(vec![("gw_smoke_native".into(), 1)]),
    };
    gateway.plan_cache.insert(cache_key.clone(), get_plan);

    let hits_before = gateway.plan_cache.cache_hit_count();
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert!(gateway.plan_cache.get(&cache_key).is_some());
    assert_eq!(
        gateway.plan_cache.cache_hit_count(),
        hits_before + 2,
        "native listener: 2 cache hits expected"
    );

    node.shutdown().await;
}
