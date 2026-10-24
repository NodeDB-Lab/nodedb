// SPDX-License-Identifier: BUSL-1.1

//! Array statements sent over WebSocket RPC work in a cluster.
//!
//! One node runs `CREATE ARRAY`. Another node, which ran no DDL, takes an
//! `INSERT INTO ARRAY` and an `ARRAY_AGG` through the WebSocket RPC SQL entry
//! point. That node's array coordinator fans the insert out to the shards
//! that own the cells, and gathers the aggregate back from them. A third
//! node reads the same sum over pgwire.
//!
//! Before the transport intercepted cluster array ops, the insert went to
//! the gateway, which cannot encode a `ClusterArray` plan for the wire.

use crate::common;

use common::cluster_harness::TestCluster;
use common::cluster_harness::TestClusterNode;
use common::cluster_harness::node::lifecycle::HARNESS_SUPERUSER;
use nodedb::control::planner::context::QueryContext;
use nodedb::control::security::identity::AuthMethod;
use nodedb::control::server::http::routes::ws_rpc::execute_sql::execute_sql;
use nodedb::types::{DatabaseId, TraceId};

const CREATE_ARRAY: &str = "CREATE ARRAY wsgrid \
     DIMS (x INT64 [0..63], y INT64 [0..63]) \
     ATTRS (v FLOAT64) \
     TILE_EXTENTS (8, 8) \
     CELL_ORDER HILBERT";

/// Cells spread across the grid, so they land on several shards.
const INSERT: &str = "INSERT INTO ARRAY wsgrid \
     COORDS (1, 1) VALUES (1.5), \
     COORDS (40, 3) VALUES (2.5), \
     COORDS (60, 60) VALUES (3.0)";

const EXPECTED_SUM: f64 = 7.0;

/// Run `sql` through `node`'s WebSocket RPC SQL entry point as the harness
/// superuser.
async fn ws_rpc_sql(node: &TestClusterNode, sql: &str) -> serde_json::Value {
    let identity = node
        .shared
        .credentials
        .to_identity(HARNESS_SUPERUSER, AuthMethod::Trust)
        .expect("the harness superuser exists on every node");
    let query_ctx = QueryContext::for_state_with_lease(&node.shared);
    // Boxed: the statement future is deep enough to exceed the layout query
    // depth of this test crate when held inline.
    Box::pin(execute_sql(
        &node.shared,
        &query_ctx,
        &identity,
        DatabaseId::DEFAULT,
        sql,
        TraceId::generate(),
        "127.0.0.1:40000",
    ))
    .await
    .unwrap_or_else(|e| panic!("ws_rpc {sql} on node {}: {e:?}", node.node_id))
}

/// Whether any number anywhere in `value` equals `expected`.
fn holds_number(value: &serde_json::Value, expected: f64) -> bool {
    match value {
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|f| (f - expected).abs() < 1e-9),
        serde_json::Value::Array(items) => items.iter().any(|v| holds_number(v, expected)),
        serde_json::Value::Object(map) => map.values().any(|v| holds_number(v, expected)),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            false
        }
    }
}

/// The `result` column of a one-row `ARRAY_AGG` read over pgwire.
async fn pgwire_sum(node: &TestClusterNode) -> f64 {
    let msgs = node
        .client
        .simple_query("SELECT * FROM ARRAY_AGG('wsgrid', 'v', 'sum')")
        .await
        .unwrap_or_else(|e| panic!("ARRAY_AGG on node {}: {e:?}", node.node_id));
    let rows: Vec<tokio_postgres::SimpleQueryRow> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "a scalar ARRAY_AGG returns one row");
    let text = rows[0]
        .get("result")
        .unwrap_or_else(|| panic!("ARRAY_AGG row carries no result column"));
    text.parse()
        .unwrap_or_else(|e| panic!("ARRAY_AGG result {text} is not a float: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_rpc_array_insert_and_agg_on_a_node_that_ran_no_ddl() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(CREATE_ARRAY)
        .await
        .expect("CREATE ARRAY wsgrid");
    let writer = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
    let reader = &cluster.nodes[(ddl_idx + 2) % cluster.nodes.len()];

    let inserted = ws_rpc_sql(writer, INSERT).await;
    assert!(
        holds_number(&inserted, 3.0),
        "the insert reports its 3 cells: {inserted}"
    );

    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;

    let summed = ws_rpc_sql(writer, "SELECT * FROM ARRAY_AGG('wsgrid', 'v', 'sum')").await;
    assert!(
        holds_number(&summed, EXPECTED_SUM),
        "ws_rpc ARRAY_AGG sums every shard's cells to {EXPECTED_SUM}: {summed}"
    );

    let over_pgwire = pgwire_sum(reader).await;
    assert!(
        (over_pgwire - EXPECTED_SUM).abs() < 1e-9,
        "pgwire on node {} reads the cells ws_rpc wrote: sum {over_pgwire}",
        reader.node_id
    );

    cluster.shutdown().await;
}
