// SPDX-License-Identifier: BUSL-1.1

//! `NodeDbRemote::document_put` inside and outside a caller's transaction.
//!
//! The put is a `DELETE` then an `INSERT`. Inside the caller's block it runs
//! under a savepoint, so it never commits or rolls back that block. A failed
//! put rolls back to its savepoint and leaves the block usable with every
//! earlier write. Outside a block it runs in its own transaction.

use nodedb_client::{Document, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;

async fn remote(server: &TestServer) -> NodeDbRemote {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed")
}

async fn sql(remote: &NodeDbRemote, statement: &str) {
    remote
        .execute_sql(statement, &[])
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
}

fn document(id: &str, field: &str, value: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set(field, Value::String(value.into()));
    doc
}

async fn exists(remote: &NodeDbRemote, collection: &str, id: &str) -> bool {
    remote
        .document_get(collection, id)
        .await
        .expect("document_get")
        .is_some()
}

#[tokio::test]
async fn a_put_inside_a_rolled_back_block_rolls_back_with_it() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    sql(&remote, "CREATE COLLECTION docs").await;

    sql(&remote, "BEGIN").await;
    sql(&remote, "INSERT INTO docs (id, body) VALUES ('early', 'x')").await;
    remote
        .document_put("docs", document("d1", "body", "in block"))
        .await
        .expect("put inside the block");
    sql(&remote, "ROLLBACK").await;

    assert!(
        !exists(&remote, "docs", "d1").await,
        "the put must not commit the caller's block"
    );
    assert!(
        !exists(&remote, "docs", "early").await,
        "the block's earlier write rolls back with it"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn a_put_inside_a_committed_block_persists_with_it() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    sql(&remote, "CREATE COLLECTION docs").await;

    sql(&remote, "BEGIN").await;
    sql(&remote, "INSERT INTO docs (id, body) VALUES ('early', 'x')").await;
    remote
        .document_put("docs", document("d1", "body", "in block"))
        .await
        .expect("put inside the block");
    sql(&remote, "COMMIT").await;

    assert!(exists(&remote, "docs", "d1").await);
    assert!(exists(&remote, "docs", "early").await);

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn a_failed_put_inside_a_block_leaves_the_block_usable() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    sql(&remote, "CREATE COLLECTION docs").await;
    // A timeseries collection refuses the put's row-level DELETE.
    sql(
        &remote,
        "CREATE COLLECTION metrics COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;

    sql(&remote, "BEGIN").await;
    sql(&remote, "INSERT INTO docs (id, body) VALUES ('early', 'x')").await;
    remote
        .document_put("metrics", document("m1", "value", "1"))
        .await
        .expect_err("a put into a timeseries collection fails");
    // The put rolled back to its savepoint: the block takes more writes.
    sql(&remote, "INSERT INTO docs (id, body) VALUES ('after', 'y')").await;
    sql(&remote, "COMMIT").await;

    assert!(
        exists(&remote, "docs", "early").await,
        "the failed put must not discard the block's earlier write"
    );
    assert!(exists(&remote, "docs", "after").await);

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn a_failed_standalone_put_keeps_the_old_document() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    sql(&remote, "CREATE COLLECTION people").await;
    sql(&remote, "CREATE UNIQUE INDEX ON people(email)").await;

    remote
        .document_put("people", document("a", "email", "a@x"))
        .await
        .expect("put a");
    remote
        .document_put("people", document("b", "email", "b@x"))
        .await
        .expect("put b");

    // The replace deletes `b`, then its insert breaks the unique index.
    remote
        .document_put("people", document("b", "email", "a@x"))
        .await
        .expect_err("a duplicate unique value is refused");

    let b = remote
        .document_get("people", "b")
        .await
        .expect("document_get")
        .expect("the refused replace must not delete the old document");
    assert_eq!(b.fields.get("email"), Some(&Value::String("b@x".into())));

    // The connection is back outside any block: a later put commits alone.
    remote
        .document_put("people", document("c", "email", "c@x"))
        .await
        .expect("put c");
    assert!(exists(&remote, "people", "c").await);

    server.graceful_shutdown().await;
}
