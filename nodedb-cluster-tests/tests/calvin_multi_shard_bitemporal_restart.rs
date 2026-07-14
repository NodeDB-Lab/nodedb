// SPDX-License-Identifier: BUSL-1.1

//! An interactive `BEGIN; <cross-shard bitemporal INSERTs>; COMMIT` block
//! COMMITS through the Calvin sequencer, and the resulting row versions stay
//! version-stable across a WAL-only restart: replay must reuse the committed
//! system-time stamp and write exactly one version per row, never a
//! wall-clock-divergent duplicate. This is the cross-shard analogue of the
//! single-shard coverage in `bitemporal_document_txn_restart.rs`.
//!
//! 1. Two `document_schemaless` collections, each created `WITH
//!    (bitemporal=true)`, are placed on DIFFERENT vShards
//!    (`distinct_vshard_bitemporal_collections`, same technique as
//!    `calvin_multi_shard_redo_restart.rs`).
//! 2. `BEGIN; INSERT INTO <a>; INSERT INTO <b>; COMMIT` is sent as ONE
//!    `simple_query` call, so both writes are buffered inside the block and,
//!    on COMMIT, `classify_dispatch` sees writes on two vShards → MultiShard
//!    → leader-routed Calvin submit-and-await.
//! 3. Before restart, each inserted row has exactly one system-time version
//!    (no double-stamp from the commit-time resolver).
//! 4. A WAL-only restart (no checkpoint) reopens the same data directory. The
//!    current read must still return both rows, and the audit log (`AS OF
//!    SYSTEM TIME NULL`) must still hold exactly one version per row, at the
//!    SAME `_ts_system` stamp observed before restart — proving replay reused
//!    the committed stamp instead of re-deriving one from the replay-time
//!    clock and appending a second version.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb::types::{DatabaseId, VShardId};
use tokio_postgres::SimpleQueryMessage;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet. Same shape as the sibling `single_node_calvin_*`
/// suite.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// Count of transactions the single-node sequencer has admitted to an epoch,
/// or `0` if the sequencer metrics handle is not installed yet.
fn admitted_total(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// A `(coll_a, coll_b)` pair of bitemporal collection names whose vShard ids
/// differ, so a transaction writing to both is genuinely multi-shard.
/// Deterministic: `VShardId::from_collection_in_database` is a pure function
/// of the database id + collection name bytes. Same technique as
/// `calvin_multi_shard_redo_restart.rs::distinct_vshard_collections`.
fn distinct_vshard_bitemporal_collections() -> (String, String) {
    let a_name = "bt_a".to_string();
    let va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &a_name).as_u32();
    for i in 0u32..512 {
        let b_name = format!("bt_b_{i}");
        if VShardId::from_collection_in_database(DatabaseId::DEFAULT, &b_name).as_u32() != va {
            return (a_name, b_name);
        }
    }
    panic!(
        "could not find a second bitemporal collection name on a distinct vShard \
         from the first in 512 tries"
    );
}

/// Current `value` for `id` in a bitemporal document collection, or `None` if
/// not visible.
async fn current_value(client: &tokio_postgres::Client, coll: &str, id: &str) -> Option<String> {
    let msgs = client
        .simple_query(&format!("SELECT value FROM {coll} WHERE id = '{id}'"))
        .await
        .expect("SELECT current value by id");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("value").map(str::to_owned),
        _ => None,
    })
}

/// Sorted `_ts_system` stamps of every audit-log version (`AS OF SYSTEM TIME
/// NULL`) belonging to `id` in `coll`. Filters by id in Rust rather than in
/// SQL — `AS OF SYSTEM TIME NULL` combined with `WHERE` is not exercised
/// elsewhere in the suite, so this avoids depending on unverified parser
/// support. Exactly one element means exactly one system-time version.
async fn audit_system_stamps(client: &tokio_postgres::Client, coll: &str, id: &str) -> Vec<String> {
    let msgs = client
        .simple_query(&format!(
            "SELECT id, _ts_system FROM {coll} AS OF SYSTEM TIME NULL"
        ))
        .await
        .expect("SELECT audit log (all versions)");
    let mut stamps: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                if r.get("id") == Some(id) {
                    r.get("_ts_system").map(str::to_owned)
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    stamps.sort();
    stamps
}

/// A Calvin cross-shard COMMIT into two `bitemporal=true` document
/// collections on distinct vShards is version-stable across a WAL-only
/// restart: each row keeps exactly one system-time version, stamped
/// identically before and after replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_multi_shard_bitemporal_commit_survives_wal_only_restart() {
    // The node's own data directory (kept alive across both bring-ups).
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_dir_path = data_dir.path().to_path_buf();

    // 4 Data-Plane cores so distinct vShards land on distinct cores — a
    // genuine cross-core, cross-shard write.
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("spawn standalone single-node-calvin server on path");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let (coll_a, coll_b) = distinct_vshard_bitemporal_collections();

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {coll_a} (id STRING PRIMARY KEY, value STRING) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION bitemporal a");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {coll_b} (id STRING PRIMARY KEY, value STRING) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION bitemporal b");
    wait_for(
        "both bitemporal collections visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    // Strict cross-shard mode so COMMIT's multi-shard path routes through
    // Calvin (mirrors `calvin_cluster_pgwire_e2e.rs` /
    // `calvin_multi_shard_redo_restart.rs`).
    node.client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");

    let admitted_before = admitted_total(&node);

    // ONE `simple_query` call carrying the whole transaction: the two INSERTs
    // are buffered during the block, and on COMMIT `classify_dispatch` sees
    // the full task set spanning two vShards → MultiShard → leader-routed
    // Calvin flush.
    let txn_sql = format!(
        "BEGIN; \
         INSERT INTO {coll_a} (id, value) VALUES ('a1', 'va'); \
         INSERT INTO {coll_b} (id, value) VALUES ('b1', 'vb'); \
         COMMIT"
    );
    node.client.simple_query(&txn_sql).await.expect(
        "interactive cross-shard bitemporal COMMIT must succeed through the Calvin barrier",
    );

    // The batch reached the sequencer inbox — admitted advanced past baseline.
    wait_for(
        "calvin admitted the committed cross-shard bitemporal transaction",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || admitted_total(&node) > admitted_before,
    )
    .await;

    // Pre-restart: both rows are visible and each carries exactly one
    // system-time version. The Calvin flush lands asynchronously after the
    // completion ack, so poll.
    let (stamp_a, stamp_b);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let val_a = current_value(&node.client, &coll_a, "a1").await;
        let val_b = current_value(&node.client, &coll_b, "b1").await;
        let stamps_a = audit_system_stamps(&node.client, &coll_a, "a1").await;
        let stamps_b = audit_system_stamps(&node.client, &coll_b, "b1").await;
        if val_a.as_deref() == Some("va")
            && val_b.as_deref() == Some("vb")
            && stamps_a.len() == 1
            && stamps_b.len() == 1
        {
            stamp_a = stamps_a[0].clone();
            stamp_b = stamps_b[0].clone();
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "pre-restart committed bitemporal rows not stable within 10s: \
                 val_a={val_a:?} val_b={val_b:?} stamps_a={stamps_a:?} stamps_b={stamps_b:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // WAL-only restart: shut down cleanly (flushing the WAL, releasing every
    // redb handle) and reopen against the SAME directory — no checkpoint, so
    // both rows must be reconstructed purely from the committed cross-shard
    // transaction's replayed bitemporal stamp.
    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir_path.clone())
        .await
        .expect("reopen standalone single-node-calvin server on the same path");
    wait_for(
        "both bitemporal collections visible after WAL-only restart",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 2,
    )
    .await;

    // Post-restart: current reads return both rows, and each id's audit log
    // holds EXACTLY the SAME single stamp observed pre-restart — proving
    // replay reused the committed system-time stamp rather than re-deriving
    // one from the replay-time clock and appending a second version.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let val_a = current_value(&node.client, &coll_a, "a1").await;
        let val_b = current_value(&node.client, &coll_b, "b1").await;
        let stamps_a = audit_system_stamps(&node.client, &coll_a, "a1").await;
        let stamps_b = audit_system_stamps(&node.client, &coll_b, "b1").await;
        if val_a.as_deref() == Some("va")
            && val_b.as_deref() == Some("vb")
            && stamps_a == [stamp_a.as_str()]
            && stamps_b == [stamp_b.as_str()]
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "post-restart bitemporal version stability check failed within 10s: \
                 val_a={val_a:?} val_b={val_b:?} \
                 stamps_a={stamps_a:?} (expected [{stamp_a}]) \
                 stamps_b={stamps_b:?} (expected [{stamp_b}])"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    node.shutdown().await;
}
