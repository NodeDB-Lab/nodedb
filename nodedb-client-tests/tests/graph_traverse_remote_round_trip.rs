// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::graph_traverse` returns the subgraph
//! reachable from edges inserted in the same session.
//!
//! After three `graph_insert_edge` calls fanning from a seed
//! (`a → b`, `b → c`, `a → s`), `graph_traverse(seed, depth=2)` must
//! return a non-empty `SubGraph` containing every reachable node.
//! An empty subgraph is indistinguishable from "the wire short-circuits
//! before the server's traversal runs" — the silent-fake pattern this
//! test guards against.
//!
//! The test also checks In/Out/Both direction, edge orientation, reciprocal
//! edges, a self-loop and discovery depths.

use nodedb_client::{NodeDb, NodeDbRemote, NodeId};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn graph_traverse_returns_inserted_subgraph() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    remote
        .execute_sql("CREATE COLLECTION smoke_g", &[])
        .await
        .expect("CREATE COLLECTION smoke_g must succeed");

    let a = NodeId::try_new("chunk_a").expect("fixture");
    let b = NodeId::try_new("chunk_b").expect("fixture");
    let c = NodeId::try_new("chunk_c").expect("fixture");
    let s = NodeId::try_new("sess").expect("fixture");

    remote
        .graph_insert_edge("smoke_g", &a, &b, "next", None)
        .await
        .expect("seed edge a->b");
    remote
        .graph_insert_edge("smoke_g", &b, &c, "next", None)
        .await
        .expect("seed edge b->c");
    remote
        .graph_insert_edge("smoke_g", &a, &s, "in_session", None)
        .await
        .expect("seed edge a->s");

    let sg = remote
        .graph_traverse("smoke_g", &a, 2, nodedb_types::graph::Direction::Out, None)
        .await
        .expect("graph_traverse must complete against a populated graph");

    // A depth-2 outgoing traversal reaches both direct and two-hop neighbors.
    assert!(
        !sg.nodes.is_empty(),
        "depth-2 traversal from seed must surface reachable nodes, got empty subgraph"
    );

    let node_ids: std::collections::HashSet<&str> =
        sg.nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(
        node_ids.contains("chunk_b"),
        "depth-2 traversal must reach direct neighbor chunk_b; nodes={node_ids:?}"
    );
    assert!(
        node_ids.contains("chunk_c"),
        "depth-2 traversal must reach two-hop neighbor chunk_c; nodes={node_ids:?}"
    );
    assert!(
        node_ids.contains("sess"),
        "depth-2 traversal must reach direct neighbor sess; nodes={node_ids:?}"
    );

    // A populated traversal includes the edges it crossed.
    assert!(
        !sg.edges.is_empty(),
        "depth-2 traversal must surface the edges it crossed; got empty edges"
    );

    for (direction, includes_a, includes_c) in [
        (nodedb_types::graph::Direction::In, true, false),
        (nodedb_types::graph::Direction::Out, false, true),
        (nodedb_types::graph::Direction::Both, true, true),
    ] {
        let traversal = remote
            .graph_traverse("smoke_g", &b, 1, direction, None)
            .await
            .expect("depth-1 traversal must complete");
        let nodes: std::collections::HashSet<&str> = traversal
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect();
        assert_eq!(
            nodes.contains(a.as_str()),
            includes_a,
            "direction={direction}, nodes={nodes:?}"
        );
        assert_eq!(
            nodes.contains(c.as_str()),
            includes_c,
            "direction={direction}, nodes={nodes:?}"
        );
        let edges: std::collections::HashSet<(&str, &str, &str)> = traversal
            .edges
            .iter()
            .map(|edge| (edge.from.as_str(), edge.label.as_str(), edge.to.as_str()))
            .collect();
        assert_eq!(
            edges.contains(&(a.as_str(), "next", b.as_str())),
            includes_a
        );
        assert_eq!(
            edges.contains(&(b.as_str(), "next", c.as_str())),
            includes_c
        );
        assert_eq!(
            edges.len(),
            usize::from(includes_a) + usize::from(includes_c)
        );
    }

    remote
        .graph_insert_edge("smoke_g", &b, &a, "next", None)
        .await
        .expect("reciprocal edge b->a");
    remote
        .graph_insert_edge("smoke_g", &b, &b, "self", None)
        .await
        .expect("self loop b->b");
    let reciprocal = remote
        .graph_traverse("smoke_g", &b, 2, nodedb_types::graph::Direction::Both, None)
        .await
        .expect("bidirectional traversal with reciprocal edges");
    let edges: std::collections::HashSet<(&str, &str, &str)> = reciprocal
        .edges
        .iter()
        .map(|edge| (edge.from.as_str(), edge.label.as_str(), edge.to.as_str()))
        .collect();
    assert_eq!(
        edges.len(),
        reciprocal.edges.len(),
        "physical edges appear once"
    );
    assert_eq!(edges.len(), 5);
    for edge in [
        (a.as_str(), "next", b.as_str()),
        (b.as_str(), "next", a.as_str()),
        (b.as_str(), "next", c.as_str()),
        (a.as_str(), "in_session", s.as_str()),
        (b.as_str(), "self", b.as_str()),
    ] {
        assert!(edges.contains(&edge), "missing physical edge {edge:?}");
    }
    for node in &reciprocal.nodes {
        let expected_depth = match node.id.as_str() {
            "chunk_b" => 0,
            "chunk_a" | "chunk_c" => 1,
            "sess" => 2,
            other => panic!("unexpected node {other}"),
        };
        assert_eq!(node.depth, expected_depth);
    }

    // A label set follows every listed label and no other.
    let x = NodeId::try_new("outside").expect("fixture");
    remote
        .graph_insert_edge("smoke_g", &a, &x, "other", None)
        .await
        .expect("seed edge a->outside");
    let labelled = remote
        .graph_traverse(
            "smoke_g",
            &a,
            1,
            nodedb_types::graph::Direction::Out,
            Some(&nodedb_types::filter::EdgeFilter::labels([
                "next",
                "in_session",
            ])),
        )
        .await
        .expect("label-set traversal must complete");
    let nodes: std::collections::BTreeSet<&str> =
        labelled.nodes.iter().map(|node| node.id.as_str()).collect();
    assert_eq!(
        nodes,
        std::collections::BTreeSet::from(["chunk_a", "chunk_b", "sess"]),
        "'other' is not listed"
    );
    assert!(
        labelled.edges.iter().all(|edge| edge.label != "other"),
        "no 'other' edge is crossed: {:?}",
        labelled.edges
    );

    server.graceful_shutdown().await;
}
