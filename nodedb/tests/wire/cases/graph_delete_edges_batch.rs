// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH DELETE EDGE` with several edges in one statement, and the
//! per-statement caps both edge-list statements share.
//!
//! A clause an edge already has starts the next edge. The statement binds
//! its endpoints in one exchange and deletes every edge as one unit, so the
//! tests check that each edge leaves both of its homes, that the command tag
//! counts the edges that were live, that a refused statement deletes nothing,
//! and that an oversized statement is refused whole.

use std::collections::BTreeSet;

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// The row count carried by the first `CommandComplete` in `sql`'s response.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    for m in messages {
        if let SimpleQueryMessage::CommandComplete(n) = m {
            return n;
        }
    }
    panic!("statement reported no command tag: {sql}")
}

/// The node ids `GRAPH NEIGHBORS` returns.
async fn neighbors(
    server: &TestServer,
    coll: &str,
    node: &str,
    direction: &str,
) -> BTreeSet<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{coll}' OF '{node}' LABEL 'L' DIRECTION {direction}"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node}: {e}"));
    let mut out = BTreeSet::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(n) = entry.get("node").and_then(|v| v.as_str()) {
                out.insert(n.to_string());
            }
        }
    }
    out
}

/// `n` edges `src<i> -> dst<i>` whose endpoints spread over many vShards.
fn edge_list(n: usize) -> String {
    (0..n)
        .map(|i| format!("FROM 'src{i}' TO 'dst{i}' TYPE 'L'"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn seed(server: &TestServer, coll: &str, n: usize) {
    server
        .exec(&format!("CREATE COLLECTION {coll}"))
        .await
        .unwrap_or_else(|e| panic!("create {coll}: {e}"));
    assert_eq!(
        affected(
            server,
            &format!("GRAPH INSERT EDGE IN '{coll}' {}", edge_list(n))
        )
        .await,
        n as u64,
        "seed {n} edges"
    );
}

async fn assert_edge_present(server: &TestServer, coll: &str, i: usize) {
    let (src, dst) = (format!("src{i}"), format!("dst{i}"));
    assert_eq!(
        neighbors(server, coll, &src, "out").await,
        BTreeSet::from([dst.clone()]),
        "forward row of edge {i}"
    );
    assert_eq!(
        neighbors(server, coll, &dst, "in").await,
        BTreeSet::from([src]),
        "reverse row of edge {i}"
    );
}

async fn assert_edge_absent(server: &TestServer, coll: &str, i: usize) {
    assert!(
        neighbors(server, coll, &format!("src{i}"), "out")
            .await
            .is_empty(),
        "forward row of edge {i} is gone"
    );
    assert!(
        neighbors(server, coll, &format!("dst{i}"), "in")
            .await
            .is_empty(),
        "reverse row of edge {i} is gone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_edge_of_a_statement_leaves_both_homes() {
    let server = TestServer::start().await;
    seed(&server, "de_many", 40).await;

    let count = affected(
        &server,
        &format!("GRAPH DELETE EDGE IN 'de_many' {}", edge_list(40)),
    )
    .await;
    assert_eq!(count, 40, "the tag counts every deleted edge");
    for i in [0, 7, 21, 39] {
        assert_edge_absent(&server, "de_many", i).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tag_counts_only_edges_that_were_live() {
    let server = TestServer::start().await;
    seed(&server, "de_count", 3).await;

    let count = affected(
        &server,
        "GRAPH DELETE EDGE IN 'de_count' FROM 'src0' TO 'dst0' TYPE 'L' \
         FROM 'src1' TO 'dst1' TYPE 'L' FROM 'nobody' TO 'nowhere' TYPE 'L'",
    )
    .await;
    assert_eq!(count, 2, "an edge that was never stored is not counted");
    assert_edge_absent(&server, "de_count", 0).await;
    assert_edge_absent(&server, "de_count", 1).await;
    assert_edge_present(&server, "de_count", 2).await;

    let again = affected(
        &server,
        "GRAPH DELETE EDGE IN 'de_count' FROM 'src0' TO 'dst0' TYPE 'L' \
         FROM 'src1' TO 'dst1' TYPE 'L'",
    )
    .await;
    assert_eq!(again, 0, "deleting deleted edges counts none");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incomplete_edge_refuses_the_whole_statement() {
    let server = TestServer::start().await;
    seed(&server, "de_bad", 2).await;

    for (sql, clause) in [
        (
            "GRAPH DELETE EDGE IN 'de_bad' FROM 'src0' TO 'dst0' TYPE 'L' FROM 'src1' TYPE 'L'",
            "requires TO",
        ),
        (
            "GRAPH DELETE EDGE IN 'de_bad' FROM 'src0' TO 'dst0' TYPE 'L' TO 'dst1' TYPE 'L'",
            "requires FROM",
        ),
        (
            "GRAPH DELETE EDGE IN 'de_bad' FROM 'src0' TO 'dst0' TYPE 'L' FROM 'src1' TO 'dst1'",
            "requires TYPE",
        ),
    ] {
        let error = server
            .exec(sql)
            .await
            .expect_err("an edge missing a clause is refused");
        assert!(error.contains(clause), "{sql}: {error}");
    }
    assert_edge_present(&server, "de_bad", 0).await;
    assert_edge_present(&server, "de_bad", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_stages_every_delete_and_rollback_restores_them() {
    let server = TestServer::start().await;
    seed(&server, "de_txn", 6).await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            &format!("GRAPH DELETE EDGE IN 'de_txn' {}", edge_list(6))
        )
        .await,
        6
    );
    assert!(
        neighbors(&server, "de_txn", "src3", "out").await.is_empty(),
        "a transaction reads its own staged deletes"
    );
    server.exec("ROLLBACK").await.unwrap();
    assert_edge_present(&server, "de_txn", 3).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("GRAPH DELETE EDGE IN 'de_txn' {}", edge_list(6)))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    assert_edge_absent(&server, "de_txn", 3).await;
    assert_edge_absent(&server, "de_txn", 5).await;
}

/// One edge over the cap refuses the statement with SQLSTATE 54000 and a
/// message naming the count, the cap, and the fix. No edge lands or leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_over_the_edge_cap_is_refused_whole() {
    let server = TestServer::start().await;
    seed(&server, "de_cap", 2).await;

    for verb in ["INSERT", "DELETE"] {
        let sql = format!("GRAPH {verb} EDGE IN 'de_cap' {}", edge_list(1001));
        let error = server
            .exec(&sql)
            .await
            .expect_err("1001 edges are over the cap");
        assert!(error.contains("SQLSTATE 54000"), "{verb}: {error}");
        assert!(
            error.contains("1001 edges")
                && error.contains("cap is 1000")
                && error.contains("split the batch"),
            "{verb}: {error}"
        );
    }
    assert_edge_present(&server, "de_cap", 0).await;
    assert_edge_present(&server, "de_cap", 1).await;
    assert!(
        neighbors(&server, "de_cap", "src1000", "out")
            .await
            .is_empty(),
        "the refused insert wrote none of its edges"
    );

    assert_eq!(
        affected(
            &server,
            &format!("GRAPH INSERT EDGE IN 'de_cap' {}", edge_list(1000))
        )
        .await,
        1000,
        "a statement at the cap applies"
    );
}

/// An edge statement whose text passes the size cap is refused before it is
/// parsed, with a message naming the size and the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_over_the_text_cap_is_refused() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION de_text").await.unwrap();

    let padding = " ".repeat(1024 * 1024);
    for verb in ["INSERT", "DELETE"] {
        let sql = format!("GRAPH {verb} EDGE IN 'de_text' {padding}FROM 'a' TO 'b' TYPE 'L'");
        let error = server
            .exec(&sql)
            .await
            .expect_err("a statement over 1 MiB is refused");
        assert!(
            error.contains("cap is 1048576 bytes") && error.contains("split the batch"),
            "{verb}: {error}"
        );
    }
}
