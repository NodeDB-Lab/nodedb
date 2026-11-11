// SPDX-License-Identifier: BUSL-1.1

//! A request of several statements that feeds a materialized sum, sent to a
//! 3-node cluster over native SQL, WebSocket RPC and NDJSON.
//!
//! Each transport goes to a different node. A node has a native and a pgwire
//! listener only, so the cell starts an HTTP listener on the NDJSON node and
//! calls the WebSocket RPC entry point directly.
//!
//! - Errored request: two inserts into the source, then a read past
//!   `max_query_result_bytes`. The request errors. No source row and no
//!   balance delta applies on any node.
//! - Control request: the same two inserts alone. They apply, and the
//!   balance equals the source sum on every node. The reply is one summed
//!   answer with no `statements` field. Over native it counts both rows.

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::planner::context::QueryContext;
use nodedb::control::security::identity::AuthMethod;
use nodedb::control::server::http::routes::ws_rpc::execute_sql::execute_sql;
use nodedb::types::{DatabaseId, TraceId};
use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::calvin_multishard_fixture::row_count;
use super::materialized_sum_cross_shard::{SOURCE, balance_of, declare_binding, pg_detail, seed};
use crate::common;
use common::cluster_harness::node::lifecycle::HARNESS_SUPERUSER;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(50);
/// The result limit every node runs.
const RESULT_LIMIT: u64 = 16 * 1024;
const BIG: &str = "xs_req_big";
/// Rows of [`BIG`]. Together they pass [`RESULT_LIMIT`].
const BIG_ROWS: usize = 60;
const ROW_PAD: usize = 500;
/// The balance the account starts with.
const START: i64 = 100;

/// A way to send one request.
#[derive(Clone, Copy, Debug)]
enum Via {
    Native,
    Ws,
    Ndjson,
}

impl Via {
    const ALL: [Via; 3] = [Via::Native, Via::Ws, Via::Ndjson];

    /// The node that takes the request: one transport per node.
    fn node(self) -> usize {
        match self {
            Via::Native => 0,
            Via::Ws => 1,
            Via::Ndjson => 2,
        }
    }
}

/// The answer to one request.
struct Sent {
    errored: bool,
    text: String,
    /// The affected count of a native reply. `None` for WS and NDJSON.
    affected: Option<u64>,
}

/// An HTTP listener serving `node`'s state in trust mode. Returns its port.
async fn start_http(node: &TestClusterNode) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http listener");
    let port = listener.local_addr().expect("http local addr").port();
    let shared = Arc::clone(&node.shared);
    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    tokio::spawn(async move {
        let _ = nodedb::control::server::http::server::run_with_listener(
            listener,
            shared,
            AuthMode::Trust,
            None,
            bus,
        )
        .await;
    });
    port
}

/// Send `sql` as one request on `via`. `http_port` is the NDJSON node's
/// listener.
async fn send(cluster: &TestCluster, http_port: u16, via: Via, sql: &str) -> Sent {
    let node = &cluster.nodes[via.node()];
    match via {
        Via::Native => {
            let mut stream = open_trust_session(node.native_port, "nodedb").await;
            let response = send_sql(&mut stream, 2, sql).await;
            Sent {
                errored: response.error.is_some(),
                text: serde_json::to_string(&response)
                    .unwrap_or_else(|e| format!("unserializable response: {e}")),
                affected: response.rows_affected,
            }
        }
        Via::Ws => {
            let identity = node
                .shared
                .credentials
                .to_identity(HARNESS_SUPERUSER, AuthMethod::Trust)
                .expect("the harness superuser exists on every node");
            let query_ctx = QueryContext::for_state_with_lease(&node.shared);
            // Boxed: the statement future is deep enough to exceed the layout
            // query depth of this test crate when held inline.
            let answer = Box::pin(execute_sql(
                &node.shared,
                &query_ctx,
                &identity,
                DatabaseId::DEFAULT,
                sql,
                TraceId::generate(),
                "127.0.0.1:40000",
            ))
            .await;
            Sent {
                errored: answer.is_err(),
                text: format!("{answer:?}"),
                affected: None,
            }
        }
        Via::Ndjson => {
            let response = reqwest::Client::new()
                .post(format!("http://127.0.0.1:{http_port}/v1/query/stream"))
                .json(&serde_json::json!({ "sql": sql }))
                .send()
                .await
                .unwrap_or_else(|e| panic!("POST {sql}: {e}"));
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            Sent {
                errored: status != 200 || text.contains("\"error\""),
                text: format!("{status}: {text}"),
                affected: None,
            }
        }
    }
}

/// Two inserts into the source for `acc-1`, ids `{tag}a` and `{tag}b`, worth
/// 10 and 5.
fn two_inserts(tag: &str) -> String {
    format!(
        "INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('{tag}a', 'acc-1', '10'); \
         INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('{tag}b', 'acc-1', '5')"
    )
}

/// Check, on every node, the balance of `acc-1` and the number of source
/// rows.
async fn assert_state(cluster: &TestCluster, balance: i64, source_rows: usize, what: &str) {
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    for (index, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            balance_of(&node.client, "acc-1").await,
            balance.to_string(),
            "node {index}: balance {what}"
        );
        assert_eq!(
            row_count(&node.client, &format!("SELECT id FROM {SOURCE}")).await,
            source_rows,
            "node {index}: source rows {what}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_errored_sum_request_applies_no_row_and_no_delta_on_any_node() {
    let cluster = TestCluster::spawn_three_with_result_limit(3, RESULT_LIMIT)
        .await
        .expect("spawn 3-node cluster with a small result limit");
    cluster.wait_for_preferred_leaders(CONVERGE).await;
    declare_binding(&cluster).await;
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {BIG} WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {BIG}: {e}"));
    wait_for(
        "every node sees the three collections",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 3)
        },
    )
    .await;
    let pad = "x".repeat(ROW_PAD);
    for n in 0..BIG_ROWS {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {BIG} (id, pad) VALUES ('r{n}', '{pad}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {BIG}: {}", pg_detail(&e)));
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    seed(&cluster, "acc-1", &START.to_string(), &[]).await;
    let http_port = start_http(&cluster.nodes[Via::Ndjson.node()]).await;

    for via in Via::ALL {
        let read = send(&cluster, http_port, via, &format!("SELECT * FROM {BIG}")).await;
        assert!(
            read.errored,
            "{via:?}: a full read of {BIG} must pass the {RESULT_LIMIT} byte limit: {}",
            read.text
        );
    }

    for (index, via) in Via::ALL.into_iter().enumerate() {
        let request = format!("{}; SELECT * FROM {BIG}", two_inserts(&format!("x{index}")));
        let sent = send(&cluster, http_port, via, &request).await;
        assert!(
            sent.errored,
            "{via:?}: the trailing read must error: {}",
            sent.text
        );
        assert_state(
            &cluster,
            START,
            0,
            &format!("after the errored {via:?} request"),
        )
        .await;
    }

    let mut balance = START;
    for (index, via) in Via::ALL.into_iter().enumerate() {
        let sent = send(&cluster, http_port, via, &two_inserts(&format!("c{index}"))).await;
        assert!(!sent.errored, "{via:?}: the control request: {}", sent.text);
        assert!(
            !sent.text.contains("\"statements\""),
            "{via:?}: the reply is one summed answer: {}",
            sent.text
        );
        if let Via::Native = via {
            assert_eq!(
                sent.affected,
                Some(2),
                "{via:?}: one count sums both inserts: {}",
                sent.text
            );
        }
        balance += 15;
        assert_state(
            &cluster,
            balance,
            2 * (index + 1),
            &format!("after the control {via:?} request"),
        )
        .await;
    }
    cluster.shutdown().await;
}
