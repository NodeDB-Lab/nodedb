//! Integration tests for the RESP → gateway migration (C-δ.3).
//!
//! Tests:
//! 1. **Single-node SET/GET** — RESP SET then GET round-trip via gateway.
//! 2. **Cross-node GET** — 3-node cluster, gateway on a follower routes a KV
//!    GET to the leaseholder; asserts success.
//! 3. **Typed error mapping** — `GatewayErrorMap::to_resp` for all key variants.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::Error;
use nodedb::bridge::physical_plan::{KvOp, PhysicalPlan};
use nodedb::control::gateway::Gateway;
use nodedb::control::gateway::GatewayErrorMap;
use nodedb::control::gateway::core::QueryContext;
use nodedb::types::{RequestId, TenantId, VShardId};

use common::cluster_harness::{TestCluster, TestClusterNode};

fn test_ctx() -> QueryContext {
    QueryContext {
        tenant_id: TenantId::new(0),
        trace_id: 0xC0DE_0003,
    }
}

fn mp_string(s: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&nodedb_types::Value::String(s.into())).expect("encode string value")
}

// ---------------------------------------------------------------------------
// Test 1: Single-node RESP SET/GET — gateway execute round-trip
// ---------------------------------------------------------------------------
//
// The migrated `gateway_dispatch::dispatch_kv` and `dispatch_kv_write` call
// `shared.gateway.execute(&ctx, plan)` when the gateway is available.
// This test exercises that exact call path to verify the gateway + dispatcher
// wire through to the Data Plane correctly.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_gateway_migration_single_node_set_get() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node cluster");

    // Wait for leader election.
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION resp_gw_single")
        .await
        .expect("CREATE COLLECTION");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx();

    // SET — mirrors RESP SET command going through dispatch_kv_write → gateway.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "resp_gw_single".into(),
        key: b"mykey".to_vec(),
        value: mp_string("myvalue"),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    let put_result = gateway.execute(&ctx, put_plan).await;
    assert!(
        put_result.is_ok(),
        "SET via gateway failed: {:?}",
        put_result.unwrap_err()
    );

    // GET — mirrors RESP GET command going through dispatch_kv → gateway.
    let get_plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "resp_gw_single".into(),
        key: b"mykey".to_vec(),
        rls_filters: vec![],
    });
    let get_result = gateway.execute(&ctx, get_plan).await;
    assert!(
        get_result.is_ok(),
        "GET via gateway failed: {:?}",
        get_result.unwrap_err()
    );

    let payloads = get_result.unwrap();
    assert!(!payloads.is_empty(), "GET returned no payload");

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 2: Cross-node GET — follower routes through gateway to leaseholder
// ---------------------------------------------------------------------------
//
// On a 3-node cluster, a gateway built on a follower node routes the KV GET
// to the leader via `ExecuteRequest`. Verifies the call succeeds.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_gateway_migration_cross_node_get() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Wait for leader election + topology convergence.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Write data on node 1 (bootstrap/leader).
    cluster.nodes[0]
        .exec("CREATE COLLECTION resp_gw_cross")
        .await
        .expect("CREATE COLLECTION on node 1");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Seed via node 1's gateway.
    let leader_gw = Gateway::new(Arc::clone(&cluster.nodes[0].shared));
    let ctx = test_ctx();

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "resp_gw_cross".into(),
        key: b"cross-key".to_vec(),
        value: mp_string("cross-value"),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    leader_gw
        .execute(&ctx, put_plan)
        .await
        .expect("seed PUT on leader");

    // GET via node 2 (potential follower) — mirrors a RESP GET arriving at a
    // follower node after the dispatch_kv migration.
    let follower_gw = Gateway::new(Arc::clone(&cluster.nodes[1].shared));

    let get_plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "resp_gw_cross".into(),
        key: b"cross-key".to_vec(),
        rls_filters: vec![],
    });
    let get_result = follower_gw.execute(&ctx, get_plan).await;
    assert!(
        get_result.is_ok(),
        "cross-node GET via gateway failed: {:?}",
        get_result.unwrap_err()
    );

    for node in cluster.nodes {
        node.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Test 3: Typed error mapping — GatewayErrorMap::to_resp variants
// ---------------------------------------------------------------------------
//
// Verifies that every error variant the migrated RESP dispatch path maps
// through `GatewayErrorMap::to_resp` produces the expected Redis error prefix.

#[test]
fn resp_gateway_error_collection_not_found_is_notfound() {
    let err = Error::CollectionNotFound {
        tenant_id: TenantId::new(0),
        collection: "missing_col".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("NOTFOUND"),
        "CollectionNotFound should map to NOTFOUND prefix, got: {msg}"
    );
    assert!(
        msg.contains("missing_col"),
        "error message should name the collection: {msg}"
    );
}

#[test]
fn resp_gateway_error_not_leader_is_moved() {
    let err = Error::NotLeader {
        vshard_id: VShardId::new(1),
        leader_node: 2,
        leader_addr: "10.0.0.2:9000".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("MOVED"),
        "NotLeader should map to MOVED prefix, got: {msg}"
    );
}

#[test]
fn resp_gateway_error_deadline_is_timeout() {
    let err = Error::DeadlineExceeded {
        request_id: RequestId::new(1),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("TIMEOUT"),
        "DeadlineExceeded should map to TIMEOUT prefix, got: {msg}"
    );
}

#[test]
fn resp_gateway_error_authz_is_noperm() {
    let err = Error::RejectedAuthz {
        tenant_id: TenantId::new(0),
        resource: "secret_col".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("NOPERM"),
        "RejectedAuthz should map to NOPERM prefix, got: {msg}"
    );
}

#[test]
fn resp_gateway_error_bad_request_is_err() {
    let err = Error::BadRequest {
        detail: "invalid key format".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("ERR"),
        "BadRequest should map to ERR prefix, got: {msg}"
    );
    assert!(
        msg.contains("invalid key format"),
        "message should contain detail: {msg}"
    );
}

#[test]
fn resp_gateway_error_constraint_is_constraint() {
    let err = Error::RejectedConstraint {
        detail: "unique violation".into(),
        constraint: "pk".into(),
        collection: "test_col".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("CONSTRAINT"),
        "RejectedConstraint should map to CONSTRAINT prefix, got: {msg}"
    );
}

#[test]
fn resp_gateway_error_internal_is_err() {
    let err = Error::Internal {
        detail: "unexpected state".into(),
    };
    let msg = GatewayErrorMap::to_resp(&err);
    assert!(
        msg.starts_with("ERR"),
        "Internal should map to ERR prefix, got: {msg}"
    );
}
