// SPDX-License-Identifier: BUSL-1.1

//! An autocommit `KV_INCR` survives `kill -9`.
//!
//! `KV_INCR` builds its `KvOp` by hand instead of planning a statement.
//! Outside a transaction block it must take the durable route a planned write
//! takes. The route depends on the node:
//!
//! - The default single-node Calvin stack proposes it through Raft, and the
//!   applying funnel appends its WAL record.
//! - A standalone node has no proposer, and the funnel appends the record on
//!   the local route.
//!
//! The checkpoint interval is pushed beyond the test's runtime, so the values
//! read after the crash come from WAL replay alone.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;

/// A checkpoint between the writes and the kill holds the values independent
/// of the WAL, and a lost record then passes as a survived one.
const NO_CHECKPOINT_SECS: &str = "3600";

/// A second guard against the harness running long enough to reach a
/// checkpoint cycle anyway.
const MAX_TEST_WALL_CLOCK: Duration = Duration::from_secs(120);

#[tokio::test(flavor = "multi_thread")]
async fn an_autocommit_kv_incr_survives_kill_9_on_the_raft_route() {
    survives_kill_9(CrashHarness::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_autocommit_kv_incr_survives_kill_9_on_the_local_route() {
    survives_kill_9(CrashHarness::new().standalone()).await;
}

async fn survives_kill_9(h: CrashHarness) {
    let mut h = h.with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", NO_CHECKPOINT_SECS);
    let spawned_at = Instant::now();
    h.spawn();
    h.wait_ready();

    h.exec("CREATE COLLECTION crash_ctr (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await;
    h.exec("CREATE COLLECTION crash_ctr_raw (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')")
        .await;
    h.exec("CREATE COLLECTION crash_score (key TEXT PRIMARY KEY, score FLOAT) WITH (engine='kv')")
        .await;

    for delta in [5, 2] {
        h.query_col_idx(&format!("SELECT KV_INCR('crash_ctr', 'k', {delta})"), 0)
            .await;
        h.query_col_idx(&format!("SELECT KV_INCR('crash_ctr_raw', 'k', {delta})"), 0)
            .await;
    }
    for delta in ["0.1", "0.2"] {
        h.query_col_idx(
            &format!("SELECT KV_INCR_FLOAT('crash_score', 's', {delta})"),
            0,
        )
        .await;
    }

    let counter = h
        .query_col_idx("SELECT n FROM crash_ctr WHERE key = 'k'", 0)
        .await;
    assert_eq!(counter, vec!["7".to_string()], "live typed counter");
    let raw = counter_value(&h, "SELECT KV_INCR('crash_ctr_raw', 'k', 0)").await;
    assert_eq!(raw, serde_json::json!(7), "live raw counter");
    let score = h
        .query_col_idx("SELECT score FROM crash_score WHERE key = 's'", 0)
        .await;

    assert!(
        spawned_at.elapsed() < MAX_TEST_WALL_CLOCK,
        "the test ran long enough to reach a checkpoint cycle; tighten the test or the bound"
    );
    h.kill_9();
    h.reopen();

    assert_eq!(
        h.query_col_idx("SELECT n FROM crash_ctr WHERE key = 'k'", 0)
            .await,
        counter,
        "an acknowledged autocommit KV_INCR did not survive kill -9 and WAL replay"
    );
    assert_eq!(
        counter_value(&h, "SELECT KV_INCR('crash_ctr_raw', 'k', 0)").await,
        raw,
        "an acknowledged autocommit KV_INCR on a raw counter did not survive kill -9"
    );
    assert_eq!(
        h.query_col_idx("SELECT score FROM crash_score WHERE key = 's'", 0)
            .await,
        score,
        "WAL replay must add the same decimal digits the live KV_INCR_FLOAT added"
    );
}

/// The `value` field of the JSON document a counter function returns.
async fn counter_value(h: &CrashHarness, sql: &str) -> serde_json::Value {
    let rows = h.query_col_idx(sql, 0).await;
    assert_eq!(rows.len(), 1, "{sql} returns one row: {rows:?}");
    let doc: serde_json::Value =
        serde_json::from_str(&rows[0]).unwrap_or_else(|e| panic!("{sql} returns JSON: {e}"));
    doc["value"].clone()
}
