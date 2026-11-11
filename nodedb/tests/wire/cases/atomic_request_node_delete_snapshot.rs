// SPDX-License-Identifier: BUSL-1.1

//! A CRDT document delete beside a write to a materialized-sum source, in one
//! HTTP request.
//!
//! The delete guards the node's edges. A request with a CRDT document delete
//! keeps the route that runs its edge-maintenance recon. The request must
//! commit, the document and its edge must be gone, and the balance must equal
//! the source sum.

use super::materialized_sum_cross_shard_expanders::{balance, source_sum, sum_fixture};
use super::wire_support::{exec, post};
use crate::harness::TestServer;

const CRDT: &str = "ptv_crdt_nodes";

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {collection}"))
        .await
        .expect("select ids")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect();
    ids.sort();
    ids
}

async fn edges(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text_joined(&format!(
            "MATCH (x)-[:l]->(y) IN '{collection}' RETURN x, y"
        ))
        .await
        .expect("match edges")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crdt_delete_beside_a_sum_source_write_commits_and_keeps_the_sum() {
    let server = sum_fixture().await;
    for sql in [
        format!("CREATE TABLE {CRDT} (id TEXT PRIMARY KEY, name TEXT) WITH (crdt='true')"),
        format!("INSERT INTO {CRDT} (id, name) VALUES ('a', 'A')"),
        format!("GRAPH INSERT EDGE IN '{CRDT}' FROM 'a' TO 'b' TYPE 'l'"),
    ] {
        exec(&server, &sql).await;
    }

    let sql = format!(
        "DELETE FROM {CRDT} WHERE id = 'a'; \
         UPDATE ptv_post SET amount = '1' WHERE id = 'e0'"
    );
    let (status, body) = post(&server, "/v1/query", &sql).await;

    assert!(
        (200..300).contains(&status),
        "a CRDT delete beside a sum source write must commit, got {status}: {body}"
    );
    assert!(ids(&server, CRDT).await.is_empty(), "the document is gone");
    assert!(edges(&server, CRDT).await.is_empty(), "its edge is gone");
    assert_eq!(
        balance(&server, "acc").await,
        source_sum(&server).await.to_string(),
        "the balance equals the source sum"
    );
}
