// SPDX-License-Identifier: BUSL-1.1

//! End-to-end `graph_insert_edges` and `graph_delete_edges` over pgwire
//! (`NodeDbRemote`, one `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` statement)
//! and the native protocol (`NativeClient`, one `EdgePutBatch` /
//! `EdgeDeleteBatch` request).
//!
//! Both answer with the edge count. Each edge keeps its own properties,
//! a delete counts only the edges that were live, and a call over the edge
//! cap is refused whole.

use std::collections::BTreeSet;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote, NodeId, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::Direction;
use nodedb_types::id::EdgeId;
use nodedb_types::protocol::{EdgeWrite, MAX_EDGES_PER_STATEMENT};

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("valid node id")
}

fn scored(src: &str, dst: &str, score: i64) -> EdgeWrite {
    let mut edge = EdgeWrite::new(node(src), node(dst), "L");
    edge.properties
        .insert("score".to_string(), Value::Integer(score));
    edge
}

fn edge_id(src: &str, dst: &str) -> EdgeId {
    EdgeId::try_first(node(src), node(dst), "L").expect("edge id")
}

async fn reachable<D: NodeDb>(
    db: &D,
    collection: &str,
    filter: Option<&EdgeFilter>,
) -> BTreeSet<String> {
    db.graph_traverse(collection, &node("hub"), 1, Direction::Out, filter)
        .await
        .expect("traversal completes")
        .nodes
        .into_iter()
        .map(|n| n.id.as_str().to_string())
        .filter(|id| id != "hub")
        .collect()
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

async fn exercise<D: NodeDb>(db: &D, collection: &str) {
    db.execute_sql(&format!("CREATE COLLECTION {collection}"), &[])
        .await
        .expect("create collection");

    let written = db
        .graph_insert_edges(
            collection,
            &[
                scored("hub", "a", 9),
                scored("hub", "b", 1),
                scored("hub", "c", 9),
            ],
        )
        .await
        .expect("insert a batch");
    assert_eq!(written, 3, "the call counts every edge it wrote");
    assert_eq!(reachable(db, collection, None).await, set(&["a", "b", "c"]));

    let high = EdgeFilter {
        labels: vec!["L".to_string()],
        property_filters: vec![MetadataFilter::Gt {
            field: "score".into(),
            value: Value::Integer(5),
        }],
    };
    assert_eq!(
        reachable(db, collection, Some(&high)).await,
        set(&["a", "c"]),
        "each edge keeps its own properties"
    );

    let deleted = db
        .graph_delete_edges(
            collection,
            &[
                edge_id("hub", "a"),
                edge_id("hub", "b"),
                edge_id("hub", "nowhere"),
            ],
        )
        .await
        .expect("delete a batch");
    assert_eq!(deleted, 2, "only edges that were live are counted");
    assert_eq!(reachable(db, collection, None).await, set(&["c"]));

    assert_eq!(
        db.graph_insert_edges(collection, &[]).await.expect("empty"),
        0
    );
    assert_eq!(
        db.graph_delete_edges(collection, &[]).await.expect("empty"),
        0
    );

    let over: Vec<EdgeWrite> = (0..=MAX_EDGES_PER_STATEMENT)
        .map(|i| scored("hub", &format!("over{i}"), 1))
        .collect();
    let error = db
        .graph_insert_edges(collection, &over)
        .await
        .expect_err("a call over the edge cap is refused");
    assert!(error.to_string().contains("split the batch"), "{error}");
    assert_eq!(
        reachable(db, collection, None).await,
        set(&["c"]),
        "a refused call writes none of its edges"
    );
}

#[tokio::test]
async fn remote_edge_batch_round_trip() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");
    exercise(&remote, "eb_remote").await;
    server.graceful_shutdown().await;
}

#[tokio::test]
async fn native_edge_batch_round_trip() {
    let server = TestServer::start().await;
    let pool = PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    );
    let native = NativeClient::new(pool);
    exercise(&native, "eb_native").await;
    server.graceful_shutdown().await;
}
