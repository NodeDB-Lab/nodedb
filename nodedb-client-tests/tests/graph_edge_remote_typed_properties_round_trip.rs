// SPDX-License-Identifier: BUSL-1.1

//! Edge properties the pgwire client writes keep their exact type.
//!
//! `NodeDbRemote::graph_insert_edge` and `graph_insert_edges` bind every
//! property as a typed parameter inside `PROPERTIES { key: $n }`. A Decimal
//! `12.50`, a UUID, a timestamp and bytes read back through the native
//! client's `graph_traverse` as exactly the typed values written.

use std::collections::{BTreeSet, HashMap};

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbGraph, NodeDbRemote, NodeId, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;
use nodedb_types::graph::Direction;
use nodedb_types::protocol::EdgeWrite;

const COLLECTION: &str = "remote_typed_edges";
const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

async fn remote(server: &TestServer) -> NodeDbRemote {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed")
}

fn node(id: &str) -> NodeId {
    NodeId::try_new(id).expect("fixture node id")
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_edge_properties_keep_every_type() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql(&format!("CREATE COLLECTION {COLLECTION}"), &[])
        .await
        .expect("create collection");

    let mut single = Document::new("edge");
    for (name, value) in typed_properties() {
        single.set(name, value);
    }
    remote
        .graph_insert_edge(COLLECTION, &node("hub"), &node("one"), "L", Some(single))
        .await
        .expect("remote edge with typed properties");
    let mut batched = EdgeWrite::new(node("hub"), node("two"), "L");
    batched.properties = typed_properties();
    assert_eq!(
        remote
            .graph_insert_edges(COLLECTION, &[batched])
            .await
            .expect("remote edge batch with typed properties"),
        1
    );

    let sg = native
        .graph_traverse(COLLECTION, &node("hub"), 1, Direction::Out, None)
        .await
        .expect("traversal completes");
    let reached: BTreeSet<&str> = sg.edges.iter().map(|edge| edge.to.as_str()).collect();
    assert_eq!(reached, BTreeSet::from(["one", "two"]));
    for edge in &sg.edges {
        assert_eq!(
            edge.properties,
            typed_properties(),
            "edge to {} keeps every type",
            edge.to
        );
        let Some(Value::Decimal(price)) = edge.properties.get("price") else {
            panic!("price is a Decimal");
        };
        assert_eq!(price.to_string(), "12.50", "the scale survives");
    }

    server.graceful_shutdown().await;
}
