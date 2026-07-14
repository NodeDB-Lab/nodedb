// SPDX-License-Identifier: BUSL-1.1

//! End-to-end Calvin pgwire test: verifies that a multi-shard transaction
//! submitted via `simple_query` as an interactive `BEGIN ... COMMIT` block
//! under `cross_shard_txn = 'strict'` COMMITS through the Calvin sequencer's
//! durable Vote/Verdict barrier.
//!
//! The Calvin multi-shard path is exercised by BEGIN + two point INSERTs into
//! collections on different vShards + COMMIT.  On COMMIT, the neutral commit
//! orchestrator calls `classify_dispatch` on the buffered task set, detects
//! MultiShard, and flushes the whole batch through the leader-routed
//! `dispatch_tasks_to_calvin` — the same routed submit-and-await the autocommit
//! cross-shard path uses.  `admitted_total` advances (the batch reached the
//! sequencer inbox) and both rows are readable after COMMIT.
//!
//! The auto-commit (single-statement) cross-shard write path is covered
//! separately by `single_node_calvin_two_phase::cross_shard_calvin_write_flushes_and_is_visible`.
//!
//! Foldability of individual tasks is also spot-checked: for each buffered task
//! the Calvin response path in `transaction_cmds.rs` does NOT synthesise
//! per-task tags (COMMIT returns a single COMMIT tag), so that path is
//! verified separately by asserting that `is_calvin_foldable` on PointInsert
//! plans returns `true` at the unit-test level in `plan.rs`.
//!
//! File name contains "cluster" so nextest applies the cluster test group:
//! `binary(/cluster/)` → max-threads=1, threads-required=num-test-threads.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use tokio_postgres::SimpleQueryMessage;

/// Fetch the single-row `v` value for `id` in `coll`, or `None` if not visible.
async fn value_of(client: &tokio_postgres::Client, coll: &str, id: &str) -> Option<String> {
    let msgs = client
        .simple_query(&format!("SELECT v FROM {coll} WHERE id = '{id}'"))
        .await
        .expect("SELECT by id");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("v").map(str::to_owned),
        _ => None,
    })
}

/// Find two collection names whose vShard ids differ.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("calvin_e2e_{i}");
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

/// Calvin multi-shard batch via pgwire `simple_query` COMMITS when sent as an
/// interactive `BEGIN ... COMMIT` block.
///
/// Steps:
/// 1. Spin up a single-node cluster (Raft + Calvin sequencer wired by `start_raft`).
/// 2. Wait until `sequencer_metrics` is set (sequencer ready).
/// 3. Create two collections on different vShards.
/// 4. Enable strict cross-shard mode.
/// 5. Via one `simple_query` call, send BEGIN + two point INSERTs + COMMIT.
///    The server splits at semicolons, buffers the INSERTs during the
///    transaction, and on COMMIT the buffered write set spans two vShards, so
///    the neutral commit orchestrator flushes the whole batch through the
///    leader-routed Calvin submit-and-await.
/// 6. Assert the COMMIT succeeds, that `admitted_total` advanced past its
///    baseline (the batch reached the sequencer), and that both rows are
///    readable afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multishard_write_in_explicit_block_commits() {
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node cluster spawn");

    // Allow Raft to elect and the sequencer to start ticking.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Gate on sequencer_metrics being set (wired by start_raft via SequencerService).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if node.shared.sequencer_metrics.get().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("sequencer_metrics not set within 10s — start_raft may not have wired it");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (col_a, col_b) = two_distinct_vshard_collections();

    // Create both collections on this (single) node.
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_a} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_a");

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {col_b} (id STRING PRIMARY KEY, v STRING)"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    // Enable strict Calvin mode so COMMIT's multi-shard path runs.
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    // Baseline before the Calvin batch: the interactive COMMIT must advance
    // `admitted_total` (the batch reaches the sequencer inbox).
    let metrics = node
        .shared
        .sequencer_metrics
        .get()
        .expect("sequencer_metrics must be set");
    let admitted_before = metrics.admitted_total.load(Ordering::Relaxed);

    // Send the full transaction in one `simple_query` call.
    // tokio-postgres sends this as a single wire message; the server's
    // `execute_sql` splits at top-level semicolons and dispatches each
    // statement in order.  The two INSERTs are buffered during the
    // BEGIN block; on COMMIT the buffer spans two vShards → MultiShard, and the
    // whole batch flushes through the leader-routed Calvin submit-and-await.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'hello'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'world'); \
         COMMIT"
    );
    node.client
        .simple_query(&txn_sql)
        .await
        .expect("interactive cross-shard COMMIT must succeed through the Calvin barrier");

    // The batch was submitted to the Calvin sequencer inbox — the admitted
    // counter advanced past its pre-COMMIT baseline.
    let admitted_after = metrics.admitted_total.load(Ordering::Relaxed);
    assert!(
        admitted_after > admitted_before,
        "admitted_total must advance for a committed cross-shard transaction: \
         before={admitted_before} after={admitted_after}"
    );

    // Both rows are readable after the commit applied. The Calvin flush lands
    // asynchronously after the completion ack, so poll for visibility.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let a = value_of(&node.client, &col_a, "k1").await;
        let b = value_of(&node.client, &col_b, "k2").await;
        if a.as_deref() == Some("hello") && b.as_deref() == Some("world") {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("committed cross-shard rows not visible within 10s: col_a={a:?} col_b={b:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    node.shutdown().await;
}
