// SPDX-License-Identifier: BUSL-1.1

//! Sorted-index reads inside an explicit transaction.
//!
//! A transaction sees its own DDL and its own writes. `RANK`, `TOPK`,
//! `RANGE` and `SORTED_COUNT` answer inside the transaction that created the
//! index, and over the rows that transaction staged. Nothing of either
//! survives a ROLLBACK.

use crate::harness::TestServer;

async fn board(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, score INT) WITH (engine='kv')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} {{ id: 'p1', score: 10 }}"))
        .await
        .unwrap();
}

/// The keys `TOPK` delivers, in rank order. Rows are `(rank, key)`.
async fn topk_keys(server: &TestServer, index: &str) -> Vec<String> {
    server
        .query_rows(&format!("SELECT * FROM TOPK({index}, 10)"))
        .await
        .unwrap_or_else(|e| panic!("TOPK({index}): {e}"))
        .into_iter()
        .map(|row| row.get(1).cloned().unwrap_or_default())
        .collect()
}

/// The keys `RANGE` delivers for an inclusive score window, sorted.
async fn range_keys(server: &TestServer, index: &str, low: i64, high: i64) -> Vec<String> {
    let mut keys: Vec<String> = server
        .query_rows(&format!("SELECT * FROM RANGE({index}, {low}, {high})"))
        .await
        .unwrap_or_else(|e| panic!("RANGE({index}): {e}"))
        .into_iter()
        .map(|row| row.get(1).cloned().unwrap_or_default())
        .collect();
    keys.sort();
    keys
}

/// The integer a single-cell JSON reply carries under `field`.
fn json_field(text: &str, field: &str) -> Option<i64> {
    let needle = format!("\"{field}\":");
    let rest = text.split_once(&needle)?.1;
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    digits.parse().ok()
}

async fn sorted_count(server: &TestServer, index: &str) -> Option<i64> {
    let rows = server
        .query_text(&format!("SELECT SORTED_COUNT({index})"))
        .await
        .unwrap_or_else(|e| panic!("SORTED_COUNT({index}): {e}"));
    json_field(rows.first()?, "count")
}

async fn rank_of(server: &TestServer, index: &str, id: &str) -> Option<i64> {
    let rows = server
        .query_text(&format!("SELECT RANK({index}, '{id}')"))
        .await
        .unwrap_or_else(|e| panic!("RANK({index}, {id}): {e}"));
    json_field(rows.first()?, "rank")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sorted_index_created_in_a_transaction_answers_every_read_in_it() {
    let server = TestServer::start().await;
    board(&server, "txn_sr_new").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SORTED INDEX txn_sr_new_idx ON txn_sr_new (score DESC) KEY id")
        .await
        .unwrap();
    server
        .exec("INSERT INTO txn_sr_new { id: 'p2', score: 20 }")
        .await
        .unwrap();

    assert_eq!(
        topk_keys(&server, "txn_sr_new_idx").await,
        vec!["p2".to_string(), "p1".to_string()],
        "TOPK ranks the base row and the staged row"
    );
    assert_eq!(sorted_count(&server, "txn_sr_new_idx").await, Some(2));
    assert_eq!(rank_of(&server, "txn_sr_new_idx", "p1").await, Some(2));
    assert_eq!(
        range_keys(&server, "txn_sr_new_idx", 15, 25).await,
        vec!["p2".to_string()]
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        topk_keys(&server, "txn_sr_new_idx").await,
        vec!["p2".to_string(), "p1".to_string()],
        "the committed tree holds what the transaction read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_sorted_index_reads_the_transactions_own_writes() {
    let server = TestServer::start().await;
    board(&server, "txn_sr_own").await;
    server
        .exec("CREATE SORTED INDEX txn_sr_own_idx ON txn_sr_own (score DESC) KEY id")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO txn_sr_own { id: 'p2', score: 20 }")
        .await
        .unwrap();
    server
        .exec("DELETE FROM txn_sr_own WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(
        topk_keys(&server, "txn_sr_own_idx").await,
        vec!["p2".to_string()],
        "the staged insert is ranked and the staged delete is gone"
    );
    assert_eq!(sorted_count(&server, "txn_sr_own_idx").await, Some(1));
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        topk_keys(&server, "txn_sr_own_idx").await,
        vec!["p1".to_string()],
        "a rolled-back write never reaches the tree"
    );
    assert_eq!(sorted_count(&server, "txn_sr_own_idx").await, Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sorted_index_created_then_dropped_in_a_transaction_is_gone_for_it() {
    let server = TestServer::start().await;
    board(&server, "txn_sr_gone").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SORTED INDEX txn_sr_gone_idx ON txn_sr_gone (score DESC) KEY id")
        .await
        .unwrap();
    server
        .exec("DROP SORTED INDEX txn_sr_gone_idx")
        .await
        .unwrap();
    server
        .expect_error("SELECT SORTED_COUNT(txn_sr_gone_idx)", "does not exist")
        .await;
    server.exec("ROLLBACK").await.unwrap();
}
