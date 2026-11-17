// SPDX-License-Identifier: BUSL-1.1

//! Edge properties a native client writes keep their exact type.
//!
//! A Decimal `12.50`, a UUID, a timestamp and bytes, written as edge
//! properties through `graph_insert_edge` and `graph_insert_edges`, read back
//! through `graph_traverse` as exactly the typed values written. An `EDGE
//! WHERE` predicate decides on the typed values, and a restart, which
//! rebuilds the CSR from the edge store, keeps every type.

use std::collections::{BTreeSet, HashMap};

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbGraph, NodeId, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::Direction;
use nodedb_types::protocol::EdgeWrite;
use nodedb_types::result::SubGraph;

const COLLECTION: &str = "typed_edges";
const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("fixture node id")
}

/// The four typed properties every fixture edge carries.
fn typed_properties() -> HashMap<String, Value> {
    [
        (
            "price".to_string(),
            Value::Decimal("12.50".parse().expect("decimal literal")),
        ),
        ("uid".to_string(), Value::Uuid(UUID.into())),
        (
            "at".to_string(),
            Value::DateTime(NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant")),
        ),
        ("blob".to_string(), Value::Bytes(vec![0x01, 0x02, 0xff])),
    ]
    .into()
}

fn price_above(n: i64) -> EdgeFilter {
    EdgeFilter {
        labels: Vec::new(),
        property_filters: vec![MetadataFilter::Gt {
            field: "price".into(),
            value: Value::Integer(n),
        }],
    }
}

async fn walk(db: &NativeClient, filter: Option<&EdgeFilter>) -> SubGraph {
    db.graph_traverse(COLLECTION, &node("hub"), 1, Direction::Out, filter)
        .await
        .expect("traversal completes")
}

/// Every crossed edge carries exactly the typed properties written, the
/// Decimal's scale included.
fn assert_typed_edges(sg: &SubGraph, how: &str) {
    let reached: BTreeSet<&str> = sg.edges.iter().map(|edge| edge.to.as_str()).collect();
    assert_eq!(
        reached,
        BTreeSet::from(["one", "two"]),
        "{how}: both edges are crossed"
    );
    let expected = typed_properties();
    for edge in &sg.edges {
        assert_eq!(
            edge.properties, expected,
            "{how}: edge to {} keeps every type",
            edge.to
        );
        let Some(Value::Decimal(price)) = edge.properties.get("price") else {
            panic!("{how}: price is a Decimal");
        };
        assert_eq!(price.to_string(), "12.50", "{how}: the scale survives");
    }
}

/// The predicate decides on the typed Decimal: `price > 12` admits both
/// edges, `price > 13` admits none.
async fn assert_typed_filter(db: &NativeClient, how: &str) {
    assert_eq!(
        walk(db, Some(&price_above(12))).await.edges.len(),
        2,
        "{how}: 12.50 > 12"
    );
    assert!(
        walk(db, Some(&price_above(13))).await.edges.is_empty(),
        "{how}: 12.50 is not > 13"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_edge_properties_keep_every_type_across_a_restart() {
    let server = TestServer::start().await;
    let db = native(&server);
    db.execute_sql(&format!("CREATE COLLECTION {COLLECTION}"), &[])
        .await
        .expect("create collection");

    let mut single = Document::new("edge");
    for (name, value) in typed_properties() {
        single.set(name, value);
    }
    db.graph_insert_edge(COLLECTION, &node("hub"), &node("one"), "L", Some(single))
        .await
        .expect("single edge with typed properties");
    let mut batched = EdgeWrite::new(node("hub"), node("two"), "L");
    batched.properties = typed_properties();
    assert_eq!(
        db.graph_insert_edges(COLLECTION, &[batched])
            .await
            .expect("edge batch with typed properties"),
        1
    );

    assert_typed_edges(&walk(&db, None).await, "live");
    assert_typed_filter(&db, "live").await;

    drop(db);
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    let db = native(&server);

    assert_typed_edges(&walk(&db, None).await, "after restart");
    assert_typed_filter(&db, "after restart").await;

    server.graceful_shutdown().await;
}
