// SPDX-License-Identifier: BUSL-1.1

//! In-transaction GRAPH single-hop reads (`GRAPH NEIGHBORS`, which dispatches
//! `GraphOp::Neighbors`) observe the transaction's own uncommitted edge
//! writes (read-your-own-writes).
//!
//! A plain `INSERT INTO <document_schemaless collection> { _from, _to,
//! _type }` is mirrored by the Control Plane as an implicit `GraphOp::EdgePut`
//! task appended to the SAME task list as the document write
//! (`append_implicit_edge_tasks`). The two tasks do NOT share a `VShardId`:
//! the document write homes on `VShardId::from_collection`, the
//! edge on `VShardId::from_key(src)` -- for `g_tx` / `staged_rollback` that is
//! 348 vs 797. A self-loop does not change that, and cannot: the two homing
//! functions take different inputs. So the statement classifies as
//! `DispatchClass::MultiShard`.
//!
//! Inside an explicit `BEGIN` a multi-shard statement is NOT dispatched to
//! Calvin as autocommit -- that would apply it durably at statement time and
//! survive ROLLBACK. Every write here is one the staging gate can buffer
//! (`all_writes_bufferable`), so the statement falls through to the ordinary
//! per-task gate (`route_task_in_txn` / `is_stageable_write`), exactly like a
//! KV or Document point write; COMMIT flushes the whole buffer through Calvin.
//! The edge task lands in `GraphTxnOverlay` (`execute_stage_graph`).
//!
//! The SELF-LOOP (document id, `_from`, and `_to` all the SAME string) earns
//! its place for a different reason: `from_key(src) == from_key(dst)`, so the
//! edge's forward and reverse rows share ONE vShard and therefore one overlay.
//! A single in-tx `GRAPH NEIGHBORS` from that node observes the staged edge
//! without depending on dual-home staging. `GRAPH NEIGHBORS` carries the
//! session's active `TxnId` (see `graph_ops::traverse::neighbors`), so the
//! Data Plane's `execute_graph_neighbors` merges that overlay into the durable
//! CSR result before responding.
//!
//! NOTE on DELETE: deleting an implicit edge document routes through the
//! OLLP/Calvin edge-cleanup coordinator regardless of transaction state (see
//! `implicit_edges::append_implicit_edge_delete_tasks`, gated ahead of the
//! ordinary per-task classify/staging loop so the edge delete commits
//! atomically with the predicate delete) -- the same category of gap
//! documented in `sql_transactions_spatial_overlay.rs` for
//! `ColumnarOp::Delete`. The tombstone side of `GraphTxnOverlay` (staging a
//! delete, excluding a tombstoned edge from `Neighbors`, and restoring it on
//! rollback) is covered directly by the `graph_staged` and `graph_txn_merge`
//! unit tests instead, where the overlay is exercised without depending on
//! which SQL surface happens to route a write through the per-task staging
//! gate today.

use crate::harness::TestServer;

async fn create_collection(server: &TestServer) {
    server
        .exec("CREATE COLLECTION g_tx WITH (engine='document_schemaless')")
        .await
        .unwrap();
}

/// Insert a self-loop edge document: `id == _from == _to == node`, labeled
/// `label`. Self-looping keeps every task the implicit edge produces on one
/// `VShardId` (see the module doc comment), independent of the transaction.
async fn insert_self_loop(server: &TestServer, node: &str, label: &str) -> Result<(), String> {
    server
        .client
        .simple_query(&format!(
            "INSERT INTO g_tx {{ id: '{node}', _from: '{node}', _to: '{node}', _type: '{label}' }}"
        ))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Run `GRAPH NEIGHBORS IN 'g_tx' OF '<node>' LABEL '<label>' DIRECTION out` and return
/// the destination node ids from the single-row `[{"label":..,"node":..}, ...]`
/// JSON payload.
async fn neighbors_of(server: &TestServer, node: &str, label: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'g_tx' OF '{node}' LABEL '{label}' DIRECTION out"
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
async fn staged_edge_put_visible_in_tx_read_your_own_writes() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    // Base edge, committed before any transaction starts.
    insert_self_loop(&server, "base_committed", "knows")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // Staged inside the txn: not yet committed.
    insert_self_loop(&server, "staged_commit", "knows")
        .await
        .expect("staged implicit-edge insert should succeed at statement time");

    let staged_in_tx = neighbors_of(&server, "staged_commit", "knows").await;
    assert!(
        staged_in_tx.contains(&"staged_commit".to_string()),
        "in-tx GRAPH NEIGHBORS must observe the transaction's own uncommitted \
         edge write (read-your-own-writes), got: {staged_in_tx:?}"
    );

    let base_in_tx = neighbors_of(&server, "base_committed", "knows").await;
    assert!(
        base_in_tx.contains(&"base_committed".to_string()),
        "a pre-existing committed edge on an unrelated node must remain \
         visible from inside the transaction, got: {base_in_tx:?}"
    );

    // The document write and its implicit edge home on different vShards,
    // so COMMIT flushes the buffer through the single-node Calvin sequencer.
    server
        .exec("COMMIT")
        .await
        .expect("COMMIT of a staged implicit-edge insert must succeed");

    let committed = neighbors_of(&server, "staged_commit", "knows").await;
    assert!(
        committed.contains(&"staged_commit".to_string()),
        "the committed edge must persist after COMMIT, got: {committed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_edge_put_rollback_discards_and_leaves_base_edge_intact() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    insert_self_loop(&server, "base_rollback", "knows")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    insert_self_loop(&server, "staged_rollback", "knows")
        .await
        .unwrap();

    // Visible in-tx before rollback.
    let in_tx = neighbors_of(&server, "staged_rollback", "knows").await;
    assert!(
        in_tx.contains(&"staged_rollback".to_string()),
        "staged edge must be visible in-tx before rollback, got: {in_tx:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after_rollback = neighbors_of(&server, "staged_rollback", "knows").await;
    assert!(
        after_rollback.is_empty(),
        "rolled-back edge insert must not persist, got: {after_rollback:?}"
    );

    // The pre-existing base edge (committed before BEGIN) is unaffected by
    // the rollback of an unrelated staged write.
    let base_after = neighbors_of(&server, "base_rollback", "knows").await;
    assert!(
        base_after.contains(&"base_rollback".to_string()),
        "unrelated base edge must survive the rollback, got: {base_after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_edge_with_non_matching_label_excluded_by_label_filter() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Staged edge under a DIFFERENT label than the one queried below.
    insert_self_loop(&server, "staged_label", "follows")
        .await
        .unwrap();

    let knows_neighbors = neighbors_of(&server, "staged_label", "knows").await;
    assert!(
        !knows_neighbors.contains(&"staged_label".to_string()),
        "a staged edge under a non-matching label must not appear when a \
         label filter is applied, got: {knows_neighbors:?}"
    );

    let follows_neighbors = neighbors_of(&server, "staged_label", "follows").await;
    assert!(
        follows_neighbors.contains(&"staged_label".to_string()),
        "the staged edge must still appear under its OWN label, got: \
         {follows_neighbors:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_edge_on_one_node_does_not_affect_unrelated_node_neighbors() {
    let server = TestServer::start().await;
    create_collection(&server).await;

    server.exec("BEGIN").await.unwrap();

    // Staged edge on `staged_unrelated`, not on `totally_different_node`.
    insert_self_loop(&server, "staged_unrelated", "knows")
        .await
        .unwrap();

    let unrelated = neighbors_of(&server, "totally_different_node", "knows").await;
    assert!(
        unrelated.is_empty(),
        "an unrelated node's neighbors must be unaffected by a staged edge \
         on a different source node, got: {unrelated:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// Run `GRAPH NEIGHBORS IN 'g_tx' OF '<node>' DIRECTION both` and return the
/// `(label, node)` entries, sorted.
async fn neighbors_both(server: &TestServer, node: &str) -> Vec<(String, String)> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'g_tx' OF '{node}' DIRECTION both"
        ))
        .await
        .unwrap();
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            let field = |key: &str| {
                entry
                    .get(key)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            };
            out.push((field("label"), field("node")));
        }
    }
    out.sort();
    out
}

fn pair(label: &str, node: &str) -> (String, String) {
    (label.to_string(), node.to_string())
}

/// A self-loop is one edge. `DIRECTION both` lists a committed self-loop
/// once, and a self-loop staged inside a transaction once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direction_both_lists_a_self_loop_once() {
    let server = TestServer::start().await;
    create_collection(&server).await;
    for sql in [
        "GRAPH INSERT EDGE IN 'g_tx' FROM 'a' TO 'b' TYPE 'l'",
        "GRAPH INSERT EDGE IN 'g_tx' FROM 'a' TO 'a' TYPE 'l'",
        "GRAPH INSERT EDGE IN 'g_tx' FROM 'c' TO 'd' TYPE 'l'",
        "GRAPH INSERT EDGE IN 'g_tx' FROM 'd' TO 'c' TYPE 'l'",
    ] {
        server.exec(sql).await.unwrap();
    }

    assert_eq!(
        neighbors_both(&server, "a").await,
        vec![pair("l", "a"), pair("l", "b")]
    );
    // Two distinct edges between one pair of nodes stay two entries.
    assert_eq!(
        neighbors_both(&server, "c").await,
        vec![pair("l", "d"), pair("l", "d")]
    );

    server.exec("BEGIN").await.unwrap();
    insert_self_loop(&server, "staged_loop", "l").await.unwrap();
    assert_eq!(
        neighbors_both(&server, "staged_loop").await,
        vec![pair("l", "staged_loop")]
    );
    assert_eq!(
        neighbors_both(&server, "a").await,
        vec![pair("l", "a"), pair("l", "b")],
        "a committed self-loop read inside a transaction"
    );
    server.client.simple_query("ROLLBACK").await.unwrap();
}
