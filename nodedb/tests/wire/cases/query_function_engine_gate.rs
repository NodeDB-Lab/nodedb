// SPDX-License-Identifier: BUSL-1.1

//! Engine gate for the SQL functions and DDL that hand-build a document scan
//! over a caller-named collection.
//!
//! `VERIFY_HASH_CHAIN`, `TEMPORAL_LOOKUP`, `CONVERT_CURRENCY`,
//! `VERIFY_BALANCE`, `BALANCE_AS_OF` and `CREATE GRAPH INDEX` read the sparse
//! store, which holds document rows only. On a KV or columnar-family
//! collection that scan answers with no rows, so each of them refuses such a
//! collection with SQLSTATE `0A000` instead of reporting over an empty set.

use crate::harness::TestServer;

async fn seed_kv(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id STRING PRIMARY KEY, parent STRING, amount INT) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'a', parent: 'root', amount: 5 }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
}

async fn seed_columnar(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id STRING, pair STRING, ts STRING) \
             WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, pair, ts) VALUES ('r1', 'k1', '2024-01-01')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
}

fn assert_engine_refusal(what: &str, result: Result<Vec<String>, String>) {
    match result {
        Err(message) => assert!(
            message.contains("SQLSTATE 0A000") && message.contains("reads document collections"),
            "{what}: expected the engine refusal, got: {message}"
        ),
        Ok(rows) => panic!("{what}: answered over a non-document collection: {rows:?}"),
    }
}

/// `VERIFY_HASH_CHAIN` on a KV collection is refused, never "valid over zero
/// entries".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_hash_chain_refuses_a_kv_collection() {
    let server = TestServer::start().await;
    seed_kv(&server, "qfe_hash_kv").await;

    let result = server
        .query_text("SELECT VERIFY_HASH_CHAIN('qfe_hash_kv')")
        .await;

    assert_engine_refusal("VERIFY_HASH_CHAIN", result);
}

/// `TEMPORAL_LOOKUP` on a columnar collection is refused, never "no row".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_lookup_refuses_a_columnar_collection() {
    let server = TestServer::start().await;
    seed_columnar(&server, "qfe_tl_col").await;

    let result = server
        .query_text("SELECT TEMPORAL_LOOKUP('qfe_tl_col', 'k1', '2024-12-31', 'pair', 'ts')")
        .await;

    assert_engine_refusal("TEMPORAL_LOOKUP", result);
}

/// `CREATE GRAPH INDEX` on a KV collection is refused, never built empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_graph_index_refuses_a_kv_collection() {
    let server = TestServer::start().await;
    seed_kv(&server, "qfe_graph_kv").await;

    let result = server
        .query_text("CREATE GRAPH INDEX qfe_graph_kv_idx ON qfe_graph_kv (parent -> id)")
        .await;

    assert_engine_refusal("CREATE GRAPH INDEX", result);
}

/// A document collection passes the gate: the same call answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_hash_chain_answers_on_a_document_collection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION qfe_hash_doc")
        .await
        .expect("create document collection");
    server
        .exec("INSERT INTO qfe_hash_doc { id: 'a', amount: 5 }")
        .await
        .expect("seed row");

    server
        .query_text("SELECT VERIFY_HASH_CHAIN('qfe_hash_doc')")
        .await
        .expect("document collection passes the engine gate");
}
