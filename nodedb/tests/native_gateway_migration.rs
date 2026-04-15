//! Integration tests for the native protocol → gateway migration (C-δ.5).
//!
//! Tests:
//! 1. **Single-node SELECT** — bring up server, issue a SELECT via gateway,
//!    assert rows returned.
//! 2. **Cross-node SELECT** — 3-node cluster, gateway on follower routes a
//!    KV GET to the leaseholder; asserts success.
//! 3. **Typed error → native code** — trigger `CollectionNotFound`, assert the
//!    native error code matches `GatewayErrorMap::to_native` mapping (code 40).

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
        trace_id: 0xC0DE_0005,
    }
}

fn mp_string(s: &str) -> Vec<u8> {
    zerompk::to_msgpack_vec(&nodedb_types::Value::String(s.into())).expect("encode string value")
}

// ---------------------------------------------------------------------------
// Test 1: Single-node SELECT via gateway (mirrors native SQL dispatch)
// ---------------------------------------------------------------------------
//
// The migrated `dispatch_task_via_gateway` in `sql_gateway.rs` calls
// `shared.gateway.execute(&ctx, plan)` when the gateway is present.
// This test exercises that path directly by constructing a gateway over the
// node's `SharedState`, writing a KV entry, and reading it back.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gateway_migration_single_node_select() {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node cluster");

    // Wait for leader election.
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec("CREATE COLLECTION native_gw_single")
        .await
        .expect("CREATE COLLECTION");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = Gateway::new(Arc::clone(&node.shared));
    let ctx = test_ctx();

    // INSERT — mirrors native SQL INSERT going through dispatch_task_via_gateway.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "native_gw_single".into(),
        key: b"native-key".to_vec(),
        value: mp_string("native-value"),
        ttl_ms: 0,
    });
    gateway
        .execute(&ctx, put_plan)
        .await
        .expect("INSERT via gateway");

    // SELECT (GET) — mirrors native SQL SELECT going through dispatch_task_via_gateway.
    let get_plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "native_gw_single".into(),
        key: b"native-key".to_vec(),
        rls_filters: vec![],
    });
    let payloads = gateway
        .execute(&ctx, get_plan)
        .await
        .expect("SELECT via gateway");

    assert!(
        !payloads.is_empty(),
        "SELECT returned no payload — expected at least one row"
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 2: Cross-node SELECT — follower gateway routes to leaseholder
// ---------------------------------------------------------------------------
//
// On a 3-node cluster, a gateway built on a follower node routes a KV GET
// to the leader via `ExecuteRequest`. Verifies the call succeeds end-to-end.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gateway_migration_cross_node_select() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Wait for leader election + topology convergence.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Write data on node 1 (bootstrap/leader).
    cluster.nodes[0]
        .exec("CREATE COLLECTION native_gw_cross")
        .await
        .expect("CREATE COLLECTION on node 1");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let leader_gw = Gateway::new(Arc::clone(&cluster.nodes[0].shared));
    let ctx = test_ctx();

    // Seed a KV entry on the leader.
    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "native_gw_cross".into(),
        key: b"cross-native-key".to_vec(),
        value: mp_string("cross-native-value"),
        ttl_ms: 0,
    });
    leader_gw
        .execute(&ctx, put_plan)
        .await
        .expect("seed PUT on leader");

    // GET via node 2 (potential follower) — mirrors a native SQL SELECT
    // arriving at a follower after the dispatch_task_via_gateway migration.
    let follower_gw = Gateway::new(Arc::clone(&cluster.nodes[1].shared));

    let get_plan = PhysicalPlan::Kv(KvOp::Get {
        collection: "native_gw_cross".into(),
        key: b"cross-native-key".to_vec(),
        rls_filters: vec![],
    });
    let get_result = follower_gw.execute(&ctx, get_plan).await;
    assert!(
        get_result.is_ok(),
        "cross-node SELECT via gateway failed: {:?}",
        get_result.unwrap_err()
    );

    for node in cluster.nodes {
        node.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Test 3: Typed error → native code mapping
// ---------------------------------------------------------------------------
//
// `GatewayErrorMap::to_native` maps each error variant to a numeric code.
// The migrated `direct_ops.rs` and `sql_gateway.rs` call this mapper.
// These tests verify the codes align with the constants defined in error_map.rs.

#[test]
fn native_gateway_error_collection_not_found_is_code_40() {
    let err = Error::CollectionNotFound {
        tenant_id: TenantId::new(0),
        collection: "missing_native_col".into(),
    };
    let (code, msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(
        code, 40,
        "CollectionNotFound should map to code 40, got {code}"
    );
    assert!(
        msg.contains("missing_native_col"),
        "error message should name the collection: {msg}"
    );
}

#[test]
fn native_gateway_error_not_leader_is_code_10() {
    let err = Error::NotLeader {
        vshard_id: VShardId::new(1),
        leader_node: 2,
        leader_addr: "10.0.0.1:9000".into(),
    };
    let (code, msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(code, 10, "NotLeader should map to code 10, got {code}");
    assert!(
        msg.contains("hint:"),
        "not-leader message should contain hint: {msg}"
    );
}

#[test]
fn native_gateway_error_deadline_is_code_20() {
    let err = Error::DeadlineExceeded {
        request_id: RequestId::new(1),
    };
    let (code, _msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(
        code, 20,
        "DeadlineExceeded should map to code 20, got {code}"
    );
}

#[test]
fn native_gateway_error_schema_changed_is_code_30() {
    let err = Error::RetryableSchemaChanged {
        descriptor: "users".into(),
    };
    let (code, msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(
        code, 30,
        "RetryableSchemaChanged should map to code 30, got {code}"
    );
    assert!(
        msg.contains("users"),
        "message should name descriptor: {msg}"
    );
}

#[test]
fn native_gateway_error_authz_is_code_50() {
    let err = Error::RejectedAuthz {
        tenant_id: TenantId::new(0),
        resource: "secret".into(),
    };
    let (code, _msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(code, 50, "RejectedAuthz should map to code 50, got {code}");
}

#[test]
fn native_gateway_error_bad_request_is_code_60() {
    let err = Error::BadRequest {
        detail: "invalid plan".into(),
    };
    let (code, msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(code, 60, "BadRequest should map to code 60, got {code}");
    assert!(
        msg.contains("invalid plan"),
        "message should contain detail: {msg}"
    );
}

#[test]
fn native_gateway_error_constraint_is_code_70() {
    let err = Error::RejectedConstraint {
        detail: "unique violation".into(),
        constraint: "pk".into(),
        collection: "orders".into(),
    };
    let (code, _msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(
        code, 70,
        "RejectedConstraint should map to code 70, got {code}"
    );
}

#[test]
fn native_gateway_error_internal_is_code_99() {
    let err = Error::Internal {
        detail: "unexpected state".into(),
    };
    let (code, _msg) = GatewayErrorMap::to_native(&err);
    assert_eq!(code, 99, "Internal should map to code 99, got {code}");
}
