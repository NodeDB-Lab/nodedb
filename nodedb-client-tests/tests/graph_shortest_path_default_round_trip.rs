// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDbGraph::graph_shortest_path` returns the
//! actual path between connected nodes.
//!
//! With edges A→B→C in place, `graph_shortest_path("social", A, C)`
//! must return `Ok(Some([A, B, C]))`. A `None` answer must reflect a
//! real disconnected graph — not a trait default that short-circuits
//! before consulting the graph (the silent-fake pattern this test
//! guards against), and not a wire-format mismatch between the client
//! and the server's `GRAPH PATH` DSL.

use nodedb_client::{NodeDb, NodeDbGraph, NodeDbRemote, NodeId};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn graph_shortest_path_returns_real_path_when_connected() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Seed a graph collection. The server's GRAPH overlay attaches to
    // any collection — the trait passes the collection name through to
    // `GRAPH INSERT EDGE IN '<collection>' ...` on the wire.
    remote
        .execute_sql("CREATE COLLECTION social", &[])
        .await
        .expect("CREATE COLLECTION social must succeed");

    // Seed a 3-node path A → B → C through the trait's edge insert.
    // The seed exercises a separate code path (GRAPH INSERT EDGE) from
    // the GRAPH PATH operator the shortest-path call uses, so a failure
    // in one does not mask a failure in the other.
    let alice = NodeId::try_new("alice").expect("fixture");
    let bob = NodeId::try_new("bob").expect("fixture");
    let carol = NodeId::try_new("carol").expect("fixture");
    remote
        .graph_insert_edge("social", &alice, &bob, "KNOWS", None)
        .await
        .expect("seed edge alice->bob");
    remote
        .graph_insert_edge("social", &bob, &carol, "KNOWS", None)
        .await
        .expect("seed edge bob->carol");

    // Spec: with edges A→B→C in place, `graph_shortest_path(A, C)`
    // returns `Ok(Some([A, B, C]))`. Negative outcomes (`Ok(None)`,
    // `Err`) are disallowed: each is indistinguishable from the
    // silent-fake pattern where the default or the wire short-circuits
    // before reaching the server's `GRAPH PATH` operator.
    let path = remote
        .graph_shortest_path("social", &alice, &carol, 5, None)
        .await
        .expect("graph_shortest_path must complete against a populated graph")
        .expect("a 2-hop path A→B→C must be discovered");
    assert_eq!(path.first(), Some(&alice), "path must start at `from`");
    assert_eq!(path.last(), Some(&carol), "path must end at `to`");

    server.graceful_shutdown().await;
}
