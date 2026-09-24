// SPDX-License-Identifier: BUSL-1.1

//! In-transaction staging of `GRAPH DELETE EDGE`: it must stage the edge
//! tombstone into the per-transaction overlay, not apply it durably at
//! statement time. An in-transaction `GRAPH NEIGHBORS` must see the edge as
//! removed (read-your-own-writes), ROLLBACK must restore it, and COMMIT
//! must persist the removal.
//!
//! Every edge here is a SELF-LOOP (`_from == _to == node`), keeping both
//! endpoints on one home vShard so the delete is SINGLE-HOME and stages
//! through the single-shard redo-record commit path.

use crate::harness::TestServer;

async fn create_collection(server: &TestServer) {
    server.exec("CREATE COLLECTION g_ed").await.unwrap();
}

/// `GRAPH INSERT EDGE IN 'g_ed' FROM '<node>' TO '<node>' TYPE '<label>'` — a
/// self-loop, committed in autocommit before any transaction begins.
async fn insert_self_loop(server: &TestServer, node: &str, label: &str) {
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN 'g_ed' FROM '{node}' TO '{node}' TYPE '{label}'"
        ))
        .await
        .expect("autocommit GRAPH INSERT EDGE should succeed");
}

/// `GRAPH DELETE EDGE IN 'g_ed' FROM '<node>' TO '<node>' TYPE '<label>'`.
async fn delete_self_loop(server: &TestServer, node: &str, label: &str) -> Result<(), String> {
    server
        .client
        .simple_query(&format!(
            "GRAPH DELETE EDGE IN 'g_ed' FROM '{node}' TO '{node}' TYPE '{label}'"
        ))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Run `GRAPH NEIGHBORS IN 'g_ed' OF '<node>' LABEL '<label>' DIRECTION out` and return
/// the destination node ids from the single-row JSON payload.
async fn neighbors_of(server: &TestServer, node: &str, label: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'g_ed' OF '{node}' LABEL '{label}' DIRECTION out"
        ))
        .await
        .unwrap();
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                out.push(n.to_string());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_edge_delete_is_staged_ryow_and_rollback_restores() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    // Committed edge, before any transaction.
    insert_self_loop(&server, "ryow_node", "knows").await;
    assert!(
        neighbors_of(&server, "ryow_node", "knows")
            .await
            .contains(&"ryow_node".to_string()),
        "the committed self-loop edge must be visible before the transaction"
    );

    server.exec("BEGIN").await.unwrap();

    delete_self_loop(&server, "ryow_node", "knows")
        .await
        .expect("in-tx GRAPH DELETE EDGE should stage at statement time");

    // Read-your-own-writes: the staged delete must hide the edge from an
    // in-transaction read. Pre-fix this still traversed the edge.
    let in_tx = neighbors_of(&server, "ryow_node", "knows").await;
    assert!(
        !in_tx.contains(&"ryow_node".to_string()),
        "in-tx GRAPH NEIGHBORS must NOT observe an edge deleted in the same \
         transaction (read-your-own-writes), got: {in_tx:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    // The delete was staged, never applied — ROLLBACK restores the edge.
    let after_rollback = neighbors_of(&server, "ryow_node", "knows").await;
    assert!(
        after_rollback.contains(&"ryow_node".to_string()),
        "a rolled-back edge delete must leave the edge intact, got: {after_rollback:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_edge_delete_commit_persists_removal() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    insert_self_loop(&server, "commit_node", "knows").await;
    assert!(
        neighbors_of(&server, "commit_node", "knows")
            .await
            .contains(&"commit_node".to_string()),
        "the committed self-loop edge must be visible before the transaction"
    );

    server.exec("BEGIN").await.unwrap();
    delete_self_loop(&server, "commit_node", "knows")
        .await
        .expect("in-tx GRAPH DELETE EDGE should stage at statement time");
    server.exec("COMMIT").await.unwrap();

    // A single-home staged edge delete lands durably at COMMIT through the
    // single-shard redo-record install.
    let after_commit = neighbors_of(&server, "commit_node", "knows").await;
    assert!(
        !after_commit.contains(&"commit_node".to_string()),
        "a committed edge delete must persist, got: {after_commit:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_tx_edge_delete_does_not_affect_unrelated_edge() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    insert_self_loop(&server, "target", "knows").await;
    insert_self_loop(&server, "bystander", "knows").await;

    server.exec("BEGIN").await.unwrap();
    delete_self_loop(&server, "target", "knows")
        .await
        .expect("in-tx GRAPH DELETE EDGE should stage at statement time");

    // The unrelated edge is untouched by the staged delete of a different edge.
    let bystander = neighbors_of(&server, "bystander", "knows").await;
    assert!(
        bystander.contains(&"bystander".to_string()),
        "an unrelated edge must remain visible while another is staged for \
         deletion, got: {bystander:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}
