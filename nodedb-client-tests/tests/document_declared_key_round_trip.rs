// SPDX-License-Identifier: BUSL-1.1

//! End-to-end tests for a collection whose declared primary key is not `id`.
//!
//! The key column is the identity column: every write stores the document id
//! under it, and every key read and key restriction names it. Both clients
//! resolve it from the server catalog, so a document put, read, or deleted
//! through either client is the same row SQL sees under that key.

use std::collections::HashSet;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbRemote, SearchResult, Value};
use nodedb_test_support::pgwire_harness::TestServer;

async fn remote(server: &TestServer) -> NodeDbRemote {
    NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed")
}

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

async fn sql(remote: &NodeDbRemote, statement: &str) {
    remote
        .execute_sql(statement, &[])
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
}

fn item(id: &str, name: &str, qty: i64) -> Document {
    let mut doc = Document::new(id);
    doc.set("name", Value::String(name.into()));
    doc.set("qty", Value::Integer(qty));
    doc
}

/// `written` as it reads back: the declared key is a declared field, so it
/// reads back holding the document id.
fn stored(written: &Document) -> Document {
    let mut doc = written.clone();
    doc.set("sku", Value::String(written.id.clone()));
    doc
}

/// Read `id` through both clients and assert each returns `expected`, or
/// nothing when `expected` is `None`.
async fn assert_both_read(
    remote: &NodeDbRemote,
    native: &NativeClient,
    id: &str,
    expected: Option<&Document>,
) {
    let over_pgwire = remote
        .document_get("items", id)
        .await
        .unwrap_or_else(|e| panic!("remote get {id}: {e}"));
    let over_native = native
        .document_get("items", id)
        .await
        .unwrap_or_else(|e| panic!("native get {id}: {e}"));
    assert_eq!(over_pgwire.as_ref(), expected, "remote read of {id}");
    assert_eq!(over_native.as_ref(), expected, "native read of {id}");
}

#[tokio::test]
async fn documents_round_trip_under_a_declared_key() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    sql(
        &remote,
        "CREATE COLLECTION items (sku STRING PRIMARY KEY, name STRING) \
         WITH (engine='document_schemaless')",
    )
    .await;

    let by_remote = item("p1", "pen", 3);
    remote
        .document_put("items", by_remote.clone())
        .await
        .expect("remote put under a declared key");
    let by_native = item("p2", "ink", 7);
    native
        .document_put("items", by_native.clone())
        .await
        .expect("native put under a declared key");

    assert_both_read(&remote, &native, "p1", Some(&stored(&by_remote))).await;
    assert_both_read(&remote, &native, "p2", Some(&stored(&by_native))).await;

    // SQL sees each document under its key column.
    let keys = remote
        .execute_sql("SELECT sku FROM items WHERE name = 'ink'", &[])
        .await
        .expect("SQL reads the native-written row by its key");
    assert_eq!(keys.rows, vec![vec![Value::String("p2".into())]]);

    // A scan on a non-key predicate returns the stored fields only: the key
    // column holds the id, and no `id` column appears beside it.
    let scanned = remote
        .execute_sql("SELECT * FROM items WHERE name = 'ink'", &[])
        .await
        .expect("SELECT * scan of a declared-key collection");
    assert!(
        !scanned.columns.iter().any(|c| c == "id"),
        "a declared-key scan shows no id column: {:?}",
        scanned.columns
    );
    let sku = scanned
        .columns
        .iter()
        .position(|c| c == "sku")
        .expect("the scan shows the key column");
    assert_eq!(scanned.rows.len(), 1);
    assert_eq!(scanned.rows[0][sku], Value::String("p2".into()));
    let documents = remote
        .execute_sql(
            "SELECT to_jsonb(*) AS document FROM items WHERE name = 'ink'",
            &[],
        )
        .await
        .expect("to_jsonb(*) scan of a declared-key collection");
    let [row] = documents.rows.as_slice() else {
        panic!("one row matches: {:?}", documents.rows);
    };
    // A parameterless SELECT decodes by its column type: a `json` column is
    // the decoded object, a text column its JSON text.
    let fields: serde_json::Value = match row.first() {
        Some(Value::String(json)) => sonic_rs::from_str(json).expect("the cell is JSON"),
        Some(object @ Value::Object(_)) => serde_json::Value::from(object.clone()),
        other => panic!("the document cell is a JSON object: {other:?}"),
    };
    assert_eq!(
        fields,
        serde_json::json!({"sku": "p2", "name": "ink", "qty": 7}),
        "a scanned row is exactly its stored fields"
    );

    // A put replaces the document whole, through either client.
    let replacement = item("p1", "pencil", 4);
    native
        .document_put("items", replacement.clone())
        .await
        .expect("native replace of a remote-written document");
    assert_both_read(&remote, &native, "p1", Some(&stored(&replacement))).await;

    // A key field that names another document is refused.
    let mut conflicting = item("p3", "cap", 1);
    conflicting.set("sku", Value::String("other".into()));
    remote
        .document_put("items", conflicting.clone())
        .await
        .expect_err("remote put whose key field names another id");
    native
        .document_put("items", conflicting)
        .await
        .expect_err("native put whose key field names another id");

    remote
        .document_delete("items", "p1")
        .await
        .expect("remote delete under a declared key");
    native
        .document_delete("items", "p2")
        .await
        .expect("native delete under a declared key");
    assert_both_read(&remote, &native, "p1", None).await;
    assert_both_read(&remote, &native, "p2", None).await;

    server.graceful_shutdown().await;
}

fn ids(hits: &[SearchResult]) -> Vec<&str> {
    hits.iter().map(|h| h.id.as_str()).collect()
}

#[tokio::test]
async fn vector_search_restricts_allowed_ids_on_a_declared_key() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    sql(
        &remote,
        "CREATE COLLECTION sku_vecs \
         FIELDS (sku TEXT PRIMARY KEY, embedding VECTOR(2)) \
         WITH (engine='document_schemaless', m=8, ef_construction=50)",
    )
    .await;
    for (sku, x) in [
        ("near1", 0.0),
        ("near2", 0.1),
        ("near3", 0.2),
        ("far1", 10.0),
        ("far2", 20.0),
        ("far3", 30.0),
    ] {
        sql(
            &remote,
            &format!("INSERT INTO sku_vecs (sku, embedding) VALUES ('{sku}', ARRAY[{x}, 0.0])"),
        )
        .await;
    }

    // The nearest vectors lie outside the allowed set. The restriction must
    // apply before the top-k cut, so k rows come back from the allowed set.
    let allowed: HashSet<String> = ["far1", "far2", "far3"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let over_pgwire = remote
        .vector_search("sku_vecs", &[0.0, 0.0], 2, None, Some(&allowed))
        .await
        .expect("remote vector_search with allowed_ids on a declared key");
    assert_eq!(ids(&over_pgwire), vec!["far1", "far2"], "remote");
    let over_native = native
        .vector_search("sku_vecs", &[0.0, 0.0], 2, None, Some(&allowed))
        .await
        .expect("native vector_search with allowed_ids on a declared key");
    assert_eq!(ids(&over_native), vec!["far1", "far2"], "native");

    server.graceful_shutdown().await;
}
