// SPDX-License-Identifier: BUSL-1.1

//! End-to-end document shape tests for `NodeDbRemote` and `NativeClient`.
//!
//! A document's fields are the row's top-level columns under both clients.
//! SQL filters and text-searches each field by name. A document written by
//! one client reads back with the same fields through the other.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::text_search::{QueryMode, TextSearchParams};

/// All-terms fuzzy search: params the remote client renders as named
/// `text_match` options.
fn pgwire_search_params() -> TextSearchParams {
    TextSearchParams {
        mode: QueryMode::And,
        fuzzy: true,
    }
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

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

async fn seed_collection(remote: &NodeDbRemote) {
    remote
        .execute_sql("CREATE COLLECTION docs", &[])
        .await
        .expect("CREATE COLLECTION docs");
    remote
        .execute_sql("CREATE SEARCH INDEX ON docs FIELDS body", &[])
        .await
        .expect("CREATE SEARCH INDEX on body");
}

fn document(id: &str, fields: &[(&str, &str)]) -> Document {
    let mut doc = Document::new(id);
    for (name, value) in fields {
        doc.set(*name, Value::String((*value).into()));
    }
    doc
}

#[tokio::test]
async fn remote_document_round_trips_as_top_level_fields() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    seed_collection(&remote).await;

    let written = document(
        "d1",
        &[("body", "machine learning is everywhere"), ("title", "ml")],
    );
    remote
        .document_put("docs", written.clone())
        .await
        .expect("remote document_put");

    let read = remote
        .document_get("docs", "d1")
        .await
        .expect("remote document_get")
        .expect("the written document exists");
    assert_eq!(read.id, "d1");
    assert_eq!(
        read.fields, written.fields,
        "fields must round-trip unchanged"
    );

    let filtered = remote
        .execute_sql(
            "SELECT body FROM docs WHERE body = 'machine learning is everywhere'",
            &[],
        )
        .await
        .expect("filter on the body field");
    assert_eq!(
        filtered.rows,
        vec![vec![Value::String("machine learning is everywhere".into())]],
        "the body field must be a top-level column SQL can filter and project"
    );

    let hits = remote
        .text_search(
            "docs",
            "body",
            "machine learning",
            10,
            pgwire_search_params(),
            None,
        )
        .await
        .expect("text_search on the body field");
    assert_eq!(
        hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
        vec!["d1"],
        "the body field must be text-searchable under the document id"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn remote_document_put_replaces_the_whole_field_set() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    seed_collection(&remote).await;

    remote
        .document_put("docs", document("d1", &[("body", "one"), ("extra", "x")]))
        .await
        .expect("first put");
    let replacement = document("d1", &[("body", "two")]);
    remote
        .document_put("docs", replacement.clone())
        .await
        .expect("second put of the same id");

    let read = remote
        .document_get("docs", "d1")
        .await
        .expect("document_get")
        .expect("the document exists");
    assert_eq!(
        read.fields, replacement.fields,
        "a put replaces every field; `extra` must be gone"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn documents_cross_between_remote_and_native_unchanged() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    seed_collection(&remote).await;

    let by_remote = document("from-remote", &[("body", "written over pgwire")]);
    remote
        .document_put("docs", by_remote.clone())
        .await
        .expect("remote put");
    let seen_by_native = native
        .document_get("docs", "from-remote")
        .await
        .expect("native get")
        .expect("native sees the remote-written document");
    assert_eq!(seen_by_native.id, "from-remote");
    assert_eq!(seen_by_native.fields, by_remote.fields);

    let by_native = document(
        "from-native",
        &[("body", "written over the native protocol")],
    );
    native
        .document_put("docs", by_native.clone())
        .await
        .expect("native put");
    let seen_by_remote = remote
        .document_get("docs", "from-native")
        .await
        .expect("remote get")
        .expect("remote sees the native-written document");
    assert_eq!(seen_by_remote.id, "from-native");
    assert_eq!(seen_by_remote.fields, by_native.fields);

    let ids = remote
        .execute_sql(
            "SELECT id FROM docs WHERE body = 'written over the native protocol'",
            &[],
        )
        .await
        .expect("SQL reads the native-written row");
    assert_eq!(
        ids.rows,
        vec![vec![Value::String("from-native".into())]],
        "SQL must see a native-written document under its own id"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn native_document_round_trips_typed_fields_arrays_and_nulls() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    seed_collection(&remote).await;

    let mut written = Document::new("typed");
    written.set("count", Value::Integer(42));
    written.set("ratio", Value::Float(1.5));
    written.set("active", Value::Bool(true));
    written.set("name", Value::String("n".into()));
    written.set(
        "tags",
        Value::Array(vec![Value::String("a".into()), Value::Integer(2)]),
    );
    written.set("missing", Value::Null);
    native
        .document_put("docs", written.clone())
        .await
        .expect("native put of typed fields");

    let read = native
        .document_get("docs", "typed")
        .await
        .expect("native get")
        .expect("the typed document exists");
    assert_eq!(read.id, "typed");
    assert_eq!(
        read.fields, written.fields,
        "the native client returns every field with its own type"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn remote_reads_field_values_as_text_while_native_reads_them_typed() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    seed_collection(&remote).await;

    let mut written = Document::new("n1");
    written.set("count", Value::Integer(5));
    remote
        .document_put("docs", written.clone())
        .await
        .expect("remote put of an integer field");

    let over_pgwire = remote
        .document_get("docs", "n1")
        .await
        .expect("remote get")
        .expect("the document exists");
    assert_eq!(
        over_pgwire.fields.get("count"),
        Some(&Value::String("5".into())),
        "pgwire schemaless reads render every cell as text"
    );

    let over_native = native
        .document_get("docs", "n1")
        .await
        .expect("native get")
        .expect("the document exists");
    assert_eq!(
        over_native.fields.get("count"),
        Some(&Value::Integer(5)),
        "the native client reads the stored integer typed"
    );

    server.graceful_shutdown().await;
}
