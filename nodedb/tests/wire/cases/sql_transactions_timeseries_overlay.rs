// SPDX-License-Identifier: BUSL-1.1

//! Timeseries-engine `INSERT` (ingest) executes at STATEMENT time inside a
//! transaction -- staged into the per-transaction overlay with
//! read-your-own-writes on RAW timeseries scans, a real affected-row count,
//! and `ROLLBACK` discarding the staged rows -- mirroring the columnar staging
//! already in place. COMMIT resolves the staged ingest into the
//! transaction's redo record, which the redo install applies.
//!
//! A timeseries base row has no cross-engine surrogate identity in the scan
//! (it is keyed internally by `series_id`), so the overlay merge is
//! additive-only: RYOW surfaces staged INSERTs, and there is no base-row
//! supersede/tombstone. The merge runs ONLY on the RAW-scan branch; the
//! aggregate / time-bucket branch is committed-only (continuous-aggregate
//! correctness), which the `..._aggregate_is_committed_only` test pins.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a
/// simple-query response (PostgreSQL's `INSERT 0 N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// Collect a single column's text across all returned rows, sorted so the
/// assertion is independent of raw-scan emission order (the overlay merge
/// appends staged rows after the base rows).
fn sorted_col(msgs: &[SimpleQueryMessage], col: &str) -> Vec<String> {
    let mut v: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col).map(str::to_string),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

async fn setup(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION sensors \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')",
        )
        .await
        .unwrap();
    // Three committed base rows at ts 1000/2000/3000.
    for (i, ts, v) in [(1u32, 1000u64, 10u32), (2, 2000, 20), (3, 3000, 30)] {
        server
            .exec(&format!(
                "INSERT INTO sensors (id, ts, v) VALUES ('b{i}', {ts}, {v})"
            ))
            .await
            .unwrap();
    }
}

/// The load-bearing assertion: a staged in-transaction timeseries INSERT is
/// visible to a same-transaction RAW scan. Fails pre-fix — before the
/// staging path, a timeseries ingest inside a transaction was not stageable,
/// so the rows were buffered and only applied at COMMIT (invisible mid-tx).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_visible_in_tx_raw_scan() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();

    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40), ('s5', 5000, 50)")
        .await
        .expect("in-tx timeseries insert should succeed at the statement");
    assert_eq!(
        command_count(&msgs),
        Some(2),
        "in-tx timeseries INSERT must report the real row count at statement time"
    );

    // Read-your-own-writes: both staged rows appear alongside the base rows.
    let all = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&all, "v"),
        vec!["10", "20", "30", "40", "50"],
        "staged timeseries inserts must be visible in the same transaction \
         alongside the committed base rows"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    let committed = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&committed, "v"),
        vec!["10", "20", "30", "40", "50"],
        "committed timeseries inserts must persist"
    );
}

/// A time-range-narrowed in-transaction RAW scan surfaces exactly the staged
/// row whose timestamp falls in the queried window (and no base row does).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_narrow_range_sees_staged_row() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));

    // Window [3500, 4500] contains only the staged ts=4000 row (base rows are
    // at 1000/2000/3000).
    let windowed = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 3500 AND ts <= 4500")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&windowed, "v"),
        vec!["40"],
        "a narrow time-range scan must surface the staged row inside the window"
    );

    // A window with no base or staged row returns nothing.
    let empty = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 6000 AND ts <= 7000")
        .await
        .unwrap();
    assert!(
        sorted_col(&empty, "v").is_empty(),
        "a window matching no staged or base row must return nothing"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
}

/// `ROLLBACK` discards the staged timeseries rows (the overlay is dropped; no
/// undo-log entry and no memtable mutation happened at statement time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_insert_rollback_discards_staged_rows() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s9', 9000, 90)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(1));

    // Visible in-tx before rollback.
    let in_tx = server
        .client
        .simple_query("SELECT v FROM sensors WHERE ts >= 8500 AND ts <= 9500")
        .await
        .unwrap();
    assert_eq!(sorted_col(&in_tx, "v"), vec!["90"]);

    server.client.simple_query("ROLLBACK").await.unwrap();

    let after = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sorted_col(&after, "v"),
        vec!["10", "20", "30"],
        "rolled-back timeseries inserts must not persist; base rows untouched"
    );
}

/// CAGG-exclusion guard: a mid-transaction AGGREGATE over the collection must
/// NOT include the transaction's own staged rows — aggregates (and continuous
/// aggregates) are committed-only. The overlay merge runs on the RAW-scan
/// branch only, never on the aggregate / bucket branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_in_tx_aggregate_is_committed_only() {
    let server = TestServer::start().await;
    setup(&server).await;

    server.exec("BEGIN").await.unwrap();
    let msgs = server
        .client
        .simple_query("INSERT INTO sensors (id, ts, v) VALUES ('s4', 4000, 40), ('s5', 5000, 50)")
        .await
        .unwrap();
    assert_eq!(command_count(&msgs), Some(2));

    // RAW scan sees 5 rows (RYOW), but ...
    let raw = server
        .client
        .simple_query("SELECT v FROM sensors")
        .await
        .unwrap();
    assert_eq!(sorted_col(&raw, "v").len(), 5);

    // ... COUNT(*) must return only the 3 committed base rows.
    let count = server
        .query_rows("SELECT COUNT(*) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        count[0][0], "3",
        "mid-transaction COUNT(*) must exclude staged rows (committed-only)"
    );

    // ... and SUM(v) must be the base sum (10+20+30), not 150.
    let sum = server
        .query_rows("SELECT SUM(v) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        sum[0][0].parse::<f64>().ok(),
        Some(60.0),
        "mid-transaction SUM must exclude staged rows (committed-only)"
    );

    server.client.simple_query("COMMIT").await.unwrap();

    // After COMMIT the staged rows are durable and now counted.
    let count_after = server
        .query_rows("SELECT COUNT(*) FROM sensors")
        .await
        .unwrap();
    assert_eq!(
        count_after[0][0], "5",
        "committed timeseries inserts must be counted after COMMIT"
    );
}
