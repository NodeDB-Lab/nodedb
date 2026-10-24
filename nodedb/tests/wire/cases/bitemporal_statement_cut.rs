// SPDX-License-Identifier: BUSL-1.1

//! One statement reads one system-time cut.
//!
//! The Control Plane pins every `AS OF SYSTEM TIME` of a statement to one
//! ordinal under one read cut before any task leaves it. Both sides of a
//! join, on whatever cores they live, read at that ordinal. A time past the
//! cut reads the state at the statement, and the next statement takes a
//! fresh cut.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// A cut one day past the wall clock.
fn future_ms() -> i64 {
    now_ms() + 86_400_000
}

async fn create_pair(server: &TestServer, prefix: &str) {
    for side in ["a", "b"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {prefix}_{side} \
                 WITH (engine='document_schemaless', bitemporal=true)"
            ))
            .await
            .expect("create collection");
    }
}

/// The `(a.id, b.id)` pairs a full join of the two collections reads at
/// `cut`, an absent side as `""`.
async fn joined(server: &TestServer, prefix: &str, cut: i64) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = server
        .query_rows(&format!(
            "SELECT a.id, b.id FROM {prefix}_a a FULL JOIN {prefix}_b b ON a.id = b.id \
             AS OF SYSTEM TIME {cut}"
        ))
        .await
        .expect("join as of the cut")
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect();
    rows.sort();
    rows
}

/// The writer's key `i`. An even key writes its `a` row first, an odd key
/// its `b` row first.
fn key(i: usize) -> String {
    format!("k{i:05}")
}

fn written_first(key: &str) -> &'static str {
    let index: usize = key[1..].parse().expect("writer key");
    if index.is_multiple_of(2) { "a" } else { "b" }
}

/// Check that `rows` read one cut of the writer's history. The writer
/// finishes each key before it starts the next, so one cut holds at most
/// one half-written key, and that key holds only the row written first.
fn assert_one_cut(rows: &[(String, String)]) {
    let unmatched: Vec<&(String, String)> = rows
        .iter()
        .filter(|(a, b)| a.is_empty() || b.is_empty())
        .collect();
    assert!(
        unmatched.len() <= 1,
        "one cut holds at most one half-written key, read {unmatched:?}"
    );
    for (a, b) in unmatched {
        let (present, side) = if b.is_empty() { (a, "a") } else { (b, "b") };
        assert_eq!(
            written_first(present),
            side,
            "key {present} shows its second row without its first: the two sides read \
             different cuts"
        );
    }
}

/// One join statement `AS OF` a future time, run while a writer commits
/// row pairs, reads both sides at one cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_future_cut_join_reads_both_sides_at_one_cut_under_concurrent_writes() {
    const KEYS: usize = 400;
    let server = TestServer::start_multicores(4).await;
    create_pair(&server, "sc_cc").await;

    let (writer, _writer_task) = server.connect_as("nodedb", "nodedb").await.unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&done);
    let inserter = tokio::spawn(async move {
        for i in 0..KEYS {
            let k = key(i);
            let (first, second) = if i % 2 == 0 { ("a", "b") } else { ("b", "a") };
            for side in [first, second] {
                writer
                    .simple_query(&format!(
                        "INSERT INTO sc_cc_{side} (id, n) VALUES ('{k}', {i})"
                    ))
                    .await
                    .expect("pair insert");
            }
        }
        finished.store(true, Ordering::SeqCst);
    });

    let mut reads = 0usize;
    while !done.load(Ordering::SeqCst) {
        assert_one_cut(&joined(&server, "sc_cc", future_ms()).await);
        reads += 1;
    }
    inserter.await.expect("inserter");
    assert!(reads > 0, "the join ran while the writer wrote");

    let all = joined(&server, "sc_cc", future_ms()).await;
    assert_eq!(
        all.len(),
        KEYS,
        "every pair is under a cut after the writer ends"
    );
    assert_one_cut(&all);
    assert!(all.iter().all(|(a, b)| a == b));
}

/// A future cut reads the state at its statement. The same statement text
/// run again takes a fresh cut and reads a pair written in between: a
/// capped cut never reaches a plan cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_future_cut_statement_takes_a_fresh_cut_each_run() {
    let server = TestServer::start().await;
    create_pair(&server, "sc_fresh").await;
    let cut = future_ms();
    for side in ["a", "b"] {
        server
            .exec(&format!("INSERT INTO sc_fresh_{side} (id) VALUES ('k1')"))
            .await
            .expect("first pair");
    }
    assert_eq!(
        joined(&server, "sc_fresh", cut).await,
        vec![("k1".to_string(), "k1".to_string())]
    );

    for side in ["a", "b"] {
        server
            .exec(&format!("INSERT INTO sc_fresh_{side} (id) VALUES ('k2')"))
            .await
            .expect("second pair");
    }
    assert_eq!(
        joined(&server, "sc_fresh", cut).await,
        vec![
            ("k1".to_string(), "k1".to_string()),
            ("k2".to_string(), "k2".to_string())
        ],
        "the second run of the same statement reads the pair written after the first"
    );
}

/// A past cut reads the same rows on every run, however many writes land
/// after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_past_cut_statement_reads_the_same_rows_every_run() {
    let server = TestServer::start().await;
    create_pair(&server, "sc_past").await;
    for side in ["a", "b"] {
        server
            .exec(&format!("INSERT INTO sc_past_{side} (id) VALUES ('k1')"))
            .await
            .expect("first pair");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let expected = vec![("k1".to_string(), "k1".to_string())];
    assert_eq!(joined(&server, "sc_past", cut).await, expected);

    for side in ["a", "b"] {
        server
            .exec(&format!("INSERT INTO sc_past_{side} (id) VALUES ('k2')"))
            .await
            .expect("second pair");
    }
    assert_eq!(
        joined(&server, "sc_past", cut).await,
        expected,
        "a write after the cut stays out of every run"
    );
}
