// SPDX-License-Identifier: BUSL-1.1

//! MATCH projections keep every value's exact type.
//!
//! A node document and an edge carry a Decimal `12.50`, a UUID, a timestamp
//! and bytes. `RETURN a.<field>` and `RETURN r.<field>` answer the native
//! client with exactly those typed values, and `WHERE r.price > 12` decides on
//! the typed Decimal. The pgwire client reads the same cells as text, and a
//! missing property as SQL NULL, never the text "NULL".

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbGraph, NodeDbRemote, NodeId, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;
use nodedb_types::result::QueryResult;

const COLLECTION: &str = "typed_match";
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

/// The four typed values every fixture node and edge carries.
fn typed() -> HashMap<String, Value> {
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

/// The one cell of `result` in `column`.
fn cell<'r>(result: &'r QueryResult, column: &str) -> &'r Value {
    assert_eq!(result.rows.len(), 1, "one row: {result:?}");
    let index = result
        .columns
        .iter()
        .position(|c| c == column)
        .unwrap_or_else(|| panic!("column {column} in {:?}", result.columns));
    &result.rows[0][index]
}

const PROJECTION: &str = "a.price, a.uid, a.at, a.blob, r.price, r.uid, r.at, r.blob, r.missing";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_projects_typed_node_and_edge_properties() {
    let server = TestServer::start().await;
    let db = native(&server);
    db.execute_sql(&format!("CREATE COLLECTION {COLLECTION}"), &[])
        .await
        .expect("create collection");

    let mut hub = Document::new("hub");
    let mut edge = Document::new("edge");
    for (name, value) in typed() {
        hub.set(name.clone(), value.clone());
        edge.set(name, value);
    }
    db.document_put(COLLECTION, hub)
        .await
        .expect("the hub node's typed document");
    db.graph_insert_edge(COLLECTION, &node("hub"), &node("leaf"), "L", Some(edge))
        .await
        .expect("an edge with typed properties");

    let typed = typed();
    let native_rows = db
        .query(&format!(
            "MATCH (a)-[r:L]->(b) IN '{COLLECTION}' WHERE r.price > 12 RETURN b, {PROJECTION}"
        ))
        .await
        .expect("native MATCH");
    assert_eq!(cell(&native_rows, "b"), &Value::String("leaf".into()));
    for field in ["price", "uid", "at", "blob"] {
        assert_eq!(
            cell(&native_rows, &format!("a.{field}")),
            &typed[field],
            "a.{field}"
        );
        assert_eq!(
            cell(&native_rows, &format!("r.{field}")),
            &typed[field],
            "r.{field}"
        );
    }
    let Value::Decimal(price) = cell(&native_rows, "r.price") else {
        panic!("r.price is a Decimal");
    };
    assert_eq!(price.to_string(), "12.50", "the scale survives");
    assert_eq!(cell(&native_rows, "r.missing"), &Value::Null, "a real NULL");

    let none = db
        .query(&format!(
            "MATCH (a)-[r:L]->(b) IN '{COLLECTION}' WHERE r.price > 13 RETURN b"
        ))
        .await
        .expect("native MATCH");
    assert!(none.rows.is_empty(), "12.50 is not > 13");

    // pgwire reads the same cells as text, and the missing property as NULL.
    let pg = remote(&server).await;
    let text_rows = pg
        .execute_sql(
            &format!("MATCH (a)-[r:L]->(b) IN '{COLLECTION}' RETURN b, {PROJECTION}"),
            &[],
        )
        .await
        .expect("pgwire MATCH");
    assert_eq!(cell(&text_rows, "b"), &Value::String("leaf".into()));
    assert_eq!(cell(&text_rows, "r.price"), &Value::String("12.50".into()));
    assert_eq!(cell(&text_rows, "a.price"), &Value::String("12.50".into()));
    assert_eq!(cell(&text_rows, "r.uid"), &Value::String(UUID.into()));
    assert_eq!(cell(&text_rows, "r.missing"), &Value::Null, "SQL NULL");

    server.graceful_shutdown().await;
}

/// ORDER BY a typed relationship property that RETURN does not project,
/// with no `IN '<collection>'`: NULL sorts last ascending and first
/// descending, and LIMIT caps the merged result, with or without ORDER BY.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_orders_by_an_unprojected_typed_edge_property() {
    let server = TestServer::start().await;
    let db = native(&server);
    let collection = "ordered_match";
    db.execute_sql(&format!("CREATE COLLECTION {collection}"), &[])
        .await
        .expect("create collection");
    let priced = |price: Option<&str>| {
        let mut doc = Document::new("edge");
        if let Some(price) = price {
            doc.set("price", Value::Decimal(price.parse().expect("decimal")));
        }
        doc
    };
    for (dst, price) in [
        ("cheap", Some("3.25")),
        ("dear", Some("12.50")),
        ("unpriced", None),
        ("mid", Some("7")),
    ] {
        db.graph_insert_edge(
            collection,
            &node("hub"),
            &node(dst),
            "L",
            Some(priced(price)),
        )
        .await
        .unwrap_or_else(|e| panic!("edge to {dst}: {e}"));
    }
    let order = |sql: &'static str| {
        let db = &db;
        async move {
            let result = db.query(sql).await.expect("native MATCH");
            assert_eq!(result.columns, vec!["b".to_string()], "{sql}");
            result
                .rows
                .iter()
                .map(|row| row[0].as_str().unwrap_or("?").to_string())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        order("MATCH (a)-[r:L]->(b) RETURN b ORDER BY r.price").await,
        vec!["cheap", "mid", "dear", "unpriced"],
        "typed ascending order, NULL last"
    );
    assert_eq!(
        order("MATCH (a)-[r:L]->(b) RETURN b ORDER BY r.price DESC LIMIT 2").await,
        vec!["unpriced", "dear"],
        "NULL first descending, LIMIT after the merge"
    );
    assert_eq!(
        order("MATCH (a)-[r:L]->(b) RETURN b ORDER BY r.price LIMIT 2 OFFSET 1").await,
        vec!["mid", "dear"]
    );
    assert_eq!(
        order("MATCH (a)-[r:L]->(b) RETURN b LIMIT 3").await.len(),
        3,
        "LIMIT without ORDER BY caps the merged result"
    );

    server.graceful_shutdown().await;
}
