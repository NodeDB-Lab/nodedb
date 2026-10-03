// SPDX-License-Identifier: BUSL-1.1

//! End-to-end `graph_traverse` and `graph_shortest_path` with an
//! `EdgeFilter`, over pgwire (`NodeDbRemote`) and the native protocol
//! (`NativeClient`).
//!
//! Both clients send `GRAPH TRAVERSE` / `GRAPH PATH` SQL with every label
//! and the property filters as `EDGE WHERE`, and decode one result shape.
//! The fixture gives each filter one edge it admits and one it rejects, so
//! a dropped label, a dropped predicate, or a dropped property is visible.
//!
//! Fixture, in one collection:
//! - `hub -road{score 9, owner O'Reilly}-> m1 -rail{score 9}-> dst`
//! - `hub -road{score 1}-> cheap -road{score 1}-> dst`
//! - `hub -other{score 9}-> out`

use std::collections::BTreeSet;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbRemote, NodeId, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::Direction;

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("fixture node id")
}

fn properties(score: i64, owner: Option<&str>) -> Document {
    let mut doc = Document::new("edge");
    doc.set("score", Value::Integer(score));
    if let Some(owner) = owner {
        doc.set("owner", Value::String(owner.to_string()));
    }
    doc
}

fn filter(labels: &[&str], property_filters: Vec<MetadataFilter>) -> EdgeFilter {
    EdgeFilter {
        labels: labels.iter().map(|label| label.to_string()).collect(),
        property_filters,
    }
}

fn score_above(n: i64) -> Vec<MetadataFilter> {
    vec![MetadataFilter::Gt {
        field: "score".into(),
        value: Value::Integer(n),
    }]
}

async fn seed<D: NodeDb>(db: &D, collection: &str) {
    db.execute_sql(&format!("CREATE COLLECTION {collection}"), &[])
        .await
        .expect("create collection");
    for (src, dst, label, score, owner) in [
        ("hub", "m1", "road", 9, Some("O'Reilly")),
        ("m1", "dst", "rail", 9, None),
        ("hub", "cheap", "road", 1, None),
        ("cheap", "dst", "road", 1, None),
        ("hub", "out", "other", 9, None),
    ] {
        db.graph_insert_edge(
            collection,
            &node(src),
            &node(dst),
            label,
            Some(properties(score, owner)),
        )
        .await
        .unwrap_or_else(|e| panic!("seed {src}-{label}->{dst}: {e}"));
    }
}

async fn node_set<D: NodeDb>(
    db: &D,
    collection: &str,
    start: &str,
    depth: u8,
    direction: Direction,
    edge_filter: &EdgeFilter,
) -> BTreeSet<String> {
    db.graph_traverse(
        collection,
        &node(start),
        depth,
        direction,
        Some(edge_filter),
    )
    .await
    .expect("traversal completes")
    .nodes
    .into_iter()
    .map(|n| n.id.as_str().to_string())
    .collect()
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

async fn exercise<D: NodeDb>(db: &D, collection: &str) {
    seed(db, collection).await;

    // Every listed label is followed. `other` is not listed.
    let road_rail = filter(&["road", "rail"], Vec::new());
    assert_eq!(
        node_set(db, collection, "hub", 2, Direction::Out, &road_rail).await,
        set(&["cheap", "dst", "hub", "m1"])
    );

    // Each crossed edge carries its properties, string escaping included.
    let sg = db
        .graph_traverse(
            collection,
            &node("hub"),
            1,
            Direction::Out,
            Some(&road_rail),
        )
        .await
        .expect("traversal completes");
    let to_m1 = sg
        .edges
        .iter()
        .find(|edge| edge.to.as_str() == "m1")
        .expect("hub -road-> m1 is crossed");
    assert_eq!(to_m1.label, "road");
    assert_eq!(to_m1.properties.get("score"), Some(&Value::Integer(9)));
    assert_eq!(
        to_m1.properties.get("owner"),
        Some(&Value::String("O'Reilly".into()))
    );
    let to_cheap = sg
        .edges
        .iter()
        .find(|edge| edge.to.as_str() == "cheap")
        .expect("hub -road-> cheap is crossed");
    assert_eq!(to_cheap.properties.get("score"), Some(&Value::Integer(1)));
    assert_eq!(to_cheap.properties.get("owner"), None);

    // The predicate keeps only edges whose properties match.
    let reilly = filter(&[], vec![MetadataFilter::eq("owner", "O'Reilly")]);
    assert_eq!(
        node_set(db, collection, "hub", 1, Direction::Out, &reilly).await,
        set(&["hub", "m1"])
    );
    let scored = filter(&["road", "rail"], score_above(5));
    assert_eq!(
        node_set(db, collection, "hub", 2, Direction::Out, &scored).await,
        set(&["dst", "hub", "m1"])
    );

    // Incoming: from `dst`, only `m1 -rail{9}->` passes `score > 5`.
    let inward = db
        .graph_traverse(
            collection,
            &node("dst"),
            1,
            Direction::In,
            Some(&filter(&[], score_above(5))),
        )
        .await
        .expect("traversal completes");
    let inward_nodes: BTreeSet<&str> = inward.nodes.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(inward_nodes, BTreeSet::from(["dst", "m1"]));
    let inward_edges: Vec<(&str, &str, &str)> = inward
        .edges
        .iter()
        .map(|e| (e.from.as_str(), e.label.as_str(), e.to.as_str()))
        .collect();
    assert_eq!(inward_edges, vec![("m1", "rail", "dst")]);

    // Both ways from `m1` over `road` only reaches `hub`.
    assert_eq!(
        node_set(
            db,
            collection,
            "m1",
            1,
            Direction::Both,
            &filter(&["road"], Vec::new())
        )
        .await,
        set(&["hub", "m1"])
    );

    // A path that needs two labels, under the predicate.
    let path = db
        .graph_shortest_path(collection, &node("hub"), &node("dst"), 4, Some(&scored))
        .await
        .expect("path completes")
        .expect("hub -road-> m1 -rail-> dst passes");
    let ids: Vec<&str> = path.iter().map(NodeId::as_str).collect();
    assert_eq!(ids, vec!["hub", "m1", "dst"]);

    // `road` alone under the predicate reaches only `m1`.
    let road_only = filter(&["road"], score_above(5));
    assert_eq!(
        db.graph_shortest_path(collection, &node("hub"), &node("dst"), 4, Some(&road_only))
            .await
            .expect("path completes"),
        None
    );

    // `road` alone without the predicate takes the cheap route.
    let cheap = db
        .graph_shortest_path(
            collection,
            &node("hub"),
            &node("dst"),
            4,
            Some(&filter(&["road"], Vec::new())),
        )
        .await
        .expect("path completes")
        .expect("hub -road-> cheap -road-> dst");
    let ids: Vec<&str> = cheap.iter().map(NodeId::as_str).collect();
    assert_eq!(ids, vec!["hub", "cheap", "dst"]);
}

#[tokio::test]
async fn remote_edge_filter_round_trip() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");
    exercise(&remote, "ef_remote").await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn native_edge_filter_round_trip() {
    let server = TestServer::start().await;
    let pool = PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    );
    let native = NativeClient::new(pool);
    exercise(&native, "ef_native").await;
    server.graceful_shutdown().await;
}
