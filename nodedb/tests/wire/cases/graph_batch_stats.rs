// SPDX-License-Identifier: BUSL-1.1

//! `SHOW GRAPH STATS` must count a batch write exactly like the equivalent
//! single edge writes: each distinct edge once, each endpoint node once.

use crate::harness::TestServer;

const BATCH_EDGES: &str = "('a','b','l'),('b','c','l'),('c','d','l')";

/// Read `(node_count, edge_count)` for `collection` from compact
/// `SHOW GRAPH STATS`. A collection with no edges may return no row, which
/// reads as `(0, 0)`.
async fn counts(srv: &TestServer, collection: &str) -> (i64, i64) {
    let rows = srv
        .query_rows(&format!("SHOW GRAPH STATS '{collection}'"))
        .await
        .unwrap_or_else(|e| panic!("stats read for {collection} failed: {e:?}"));
    rows.iter()
        .find(|r| r[0] == collection)
        .map(|r| {
            (
                r[1].parse::<i64>().expect("node_count is integer"),
                r[2].parse::<i64>().expect("edge_count is integer"),
            )
        })
        .unwrap_or((0, 0))
}

/// One batch of three distinct edges counts three edges and four nodes. One
/// batch delete of the same three brings the edge count back to zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_insert_counts_each_edge_once() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_bs_insert").await.unwrap();
    srv.exec(&format!(
        "GRAPH INSERT EDGES IN 'g_bs_insert' VALUES {BATCH_EDGES}"
    ))
    .await
    .unwrap();

    let (nodes, edges) = counts(&srv, "g_bs_insert").await;
    assert_eq!(edges, 3, "edge_count after the batch insert");
    assert_eq!(nodes, 4, "node_count after the batch insert");

    srv.exec(&format!(
        "GRAPH DELETE EDGES IN 'g_bs_insert' VALUES {BATCH_EDGES}"
    ))
    .await
    .unwrap();

    let (_, edges) = counts(&srv, "g_bs_insert").await;
    assert_eq!(edges, 0, "edge_count after the batch delete");
}

/// The same triple twice in one batch is one edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeated_edge_in_a_batch_counts_once() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_bs_repeat").await.unwrap();
    srv.exec(
        "GRAPH INSERT EDGES IN 'g_bs_repeat' VALUES \
         ('a','b','l'),('a','b','l'),('b','c','l')",
    )
    .await
    .unwrap();

    let (_, edges) = counts(&srv, "g_bs_repeat").await;
    assert_eq!(edges, 2, "a repeated triple is one edge");
}

/// A self-loop is one edge on one node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_self_loop_in_a_batch_counts_one_node() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_bs_loop").await.unwrap();
    srv.exec("GRAPH INSERT EDGES IN 'g_bs_loop' VALUES ('x','x','l')")
        .await
        .unwrap();

    let (nodes, edges) = counts(&srv, "g_bs_loop").await;
    assert_eq!(edges, 1, "edge_count of a self-loop");
    assert_eq!(nodes, 1, "node_count of a self-loop");
}

/// A batch and three single inserts of the same edges give equal stats.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_matches_single_edge_inserts() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_bs_batch").await.unwrap();
    srv.exec("CREATE COLLECTION g_bs_single").await.unwrap();
    srv.exec(&format!(
        "GRAPH INSERT EDGES IN 'g_bs_batch' VALUES {BATCH_EDGES}"
    ))
    .await
    .unwrap();
    for (from, to) in [("a", "b"), ("b", "c"), ("c", "d")] {
        srv.exec(&format!(
            "GRAPH INSERT EDGE IN 'g_bs_single' FROM '{from}' TO '{to}' TYPE 'l'"
        ))
        .await
        .unwrap();
    }

    let batch = counts(&srv, "g_bs_batch").await;
    let single = counts(&srv, "g_bs_single").await;
    assert_eq!(batch, single, "batch stats must equal single-insert stats");
    assert_eq!(batch, (4, 3), "(node_count, edge_count)");
}
