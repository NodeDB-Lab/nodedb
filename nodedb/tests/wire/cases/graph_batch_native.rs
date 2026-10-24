// SPDX-License-Identifier: BUSL-1.1

//! The native client's batch edge methods, end to end against a real server.
//!
//! `batch_graph_insert_edges` and `batch_graph_delete_edges` each send ONE
//! `GRAPH ... EDGES` statement. These tests write through the native client and
//! read back through both the native client and pgwire. They also check that a
//! batch outside the statement bounds is refused on the client, before any
//! connection is made.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb};
use nodedb_types::graph::Direction;
use nodedb_types::id::NodeId;
use nodedb_types::protocol::{AuthMethod, MAX_EDGES_PER_BATCH_STATEMENT};

use crate::harness::TestServer;

/// The trust identity the wire harness bootstraps for its own pgwire client.
const TRUST_USER: &str = "nodedb";

const EDGES: [(&str, &str, &str); 3] = [("a", "b", "l"), ("a", "c", "l"), ("b", "c", "l")];

fn native_client_with(
    server: &TestServer,
    configure: impl FnOnce(PoolConfig) -> PoolConfig,
) -> NativeClient {
    let base = PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        AuthMethod::Trust {
            username: TRUST_USER.to_string(),
        },
    );
    NativeClient::new(configure(base))
}

/// Every outbound edge of `a`, `b` and `c`, read through the native client.
/// The three walks overlap on edges between visited nodes, so repeats are dropped.
async fn native_edges(client: &NativeClient, collection: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for node in ["a", "b", "c"] {
        let start = NodeId::try_new(node).expect("valid node id");
        let graph = client
            .graph_traverse(collection, &start, 1, Direction::Out, None)
            .await
            .unwrap_or_else(|e| panic!("native traverse from {node}: {e}"));
        out.extend(graph.edges.iter().map(|e| {
            (
                e.from.as_str().to_owned(),
                e.to.as_str().to_owned(),
                e.label.clone(),
            )
        }));
    }
    out.sort();
    out.dedup();
    out
}

fn expected_edges() -> Vec<(String, String, String)> {
    let mut v: Vec<_> = EDGES
        .iter()
        .map(|(f, t, l)| ((*f).to_owned(), (*t).to_owned(), (*l).to_owned()))
        .collect();
    v.sort();
    v
}

/// Outbound neighbor node ids of `node` through pgwire `GRAPH NEIGHBORS`.
async fn pgwire_out_neighbors(server: &TestServer, collection: &str, node: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION out"
        ))
        .await
        .unwrap_or_else(|e| panic!("pgwire neighbors of {node}: {e}"));
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&rows.join("")).expect("neighbors JSON");
    let mut out: Vec<String> = parsed
        .iter()
        .map(|e| e["node"].as_str().expect("node field").to_owned())
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_batch_insert_and_delete_round_trip() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION nat_batch_rt")
        .await
        .expect("create collection");
    let client = native_client_with(&server, |base| base);

    client
        .batch_graph_insert_edges("nat_batch_rt", &EDGES)
        .await
        .expect("native batch insert");

    assert_eq!(
        native_edges(&client, "nat_batch_rt").await,
        expected_edges()
    );
    assert_eq!(
        pgwire_out_neighbors(&server, "nat_batch_rt", "a").await,
        vec!["b".to_owned(), "c".to_owned()]
    );
    assert_eq!(
        pgwire_out_neighbors(&server, "nat_batch_rt", "b").await,
        vec!["c".to_owned()]
    );

    client
        .batch_graph_delete_edges("nat_batch_rt", &EDGES)
        .await
        .expect("native batch delete");

    assert!(native_edges(&client, "nat_batch_rt").await.is_empty());
    for node in ["a", "b", "c"] {
        assert!(
            pgwire_out_neighbors(&server, "nat_batch_rt", node)
                .await
                .is_empty(),
            "node {node} must have no outbound edge after the batch delete"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_batch_inside_a_transaction_reads_its_own_writes_and_rolls_back() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION nat_batch_txn")
        .await
        .expect("create collection");
    // One pooled connection, so begin, the batch, the reads and rollback all
    // ride the same session.
    let client = native_client_with(&server, |base| PoolConfig {
        max_size: 1,
        ..base
    });

    client.begin().await.expect("begin");
    client
        .batch_graph_insert_edges("nat_batch_txn", &EDGES)
        .await
        .expect("batch insert in transaction");
    assert_eq!(
        native_edges(&client, "nat_batch_txn").await,
        expected_edges(),
        "the transaction must read its own batch"
    );

    client.rollback().await.expect("rollback");
    assert!(
        native_edges(&client, "nat_batch_txn").await.is_empty(),
        "a rolled-back batch must leave no edge"
    );
    assert!(
        pgwire_out_neighbors(&server, "nat_batch_txn", "a")
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_batch_bounds_are_refused_before_the_round_trip() {
    // Nothing listens on port 1. A batch that reached the connection step would
    // fail with a connect error, not with the bound text asserted below.
    let client = NativeClient::new(PoolConfig::new(
        "127.0.0.1:1",
        AuthMethod::Trust {
            username: TRUST_USER.to_string(),
        },
    ));

    let empty: [(&str, &str, &str); 0] = [];
    let too_many: Vec<(&str, &str, &str)> =
        vec![("a", "b", "l"); MAX_EDGES_PER_BATCH_STATEMENT + 1];
    assert_eq!(too_many.len(), 1001);

    let insert_empty = client
        .batch_graph_insert_edges("c", &empty)
        .await
        .expect_err("an empty insert batch is refused");
    assert!(insert_empty.to_string().contains("at least one edge"));

    let insert_many = client
        .batch_graph_insert_edges("c", &too_many)
        .await
        .expect_err("an oversized insert batch is refused");
    assert!(insert_many.to_string().contains("at most 1000 edges"));

    let delete_empty = client
        .batch_graph_delete_edges("c", &empty)
        .await
        .expect_err("an empty delete batch is refused");
    assert!(delete_empty.to_string().contains("at least one edge"));

    let delete_many = client
        .batch_graph_delete_edges("c", &too_many)
        .await
        .expect_err("an oversized delete batch is refused");
    assert!(delete_many.to_string().contains("at most 1000 edges"));
}
