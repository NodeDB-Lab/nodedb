// SPDX-License-Identifier: BUSL-1.1

//! End-to-end proof of the Calvin hot-key read-reservation chain on a
//! single-node Calvin server: a hot key is read inside an interactive
//! cross-shard transaction, which installs a sequenced SHARED reservation; the
//! same transaction then writes that key (self-upgrading the reservation under
//! its owner id) plus a key on a second vShard (forcing the strict Calvin commit
//! path) and commits; the reservation is released on commit and the self-upgraded
//! write is visible.
//!
//! Every async step (sequencer leader election, reservation install after the
//! read, reservation release after commit) is gated by `wait_for`, so the test
//! is deterministic rather than timing-dependent.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::control::cluster::calvin::scheduler::lock::LockKey;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_types::id::{DatabaseId, VShardId};

use common::cluster_harness::{TestClusterNode, wait_for};

/// Observed sequencer-group leader id from a node's local Raft status, or `0`
/// if no leader is known yet.
fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// A collection name that homes to a vShard DISTINCT from `exclude_vshard`, so a
/// write to it unions with the hot key's vShard to size >= 2 (strict Calvin).
fn other_vshard_collection(exclude_vshard: u32) -> String {
    for i in 0u32..4096 {
        let name = format!("hkr_other_{i}");
        if VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32()
            != exclude_vshard
        {
            return name;
        }
    }
    panic!("could not find a distinct-vshard collection in 4096 tries");
}

/// Number of read reservations currently held on `key` in `vshard`'s lock
/// manager, or 0 if that lock manager is absent.
fn reservation_count(node: &TestClusterNode, vshard: u32, key: &LockKey) -> usize {
    let managers = node
        .shared
        .calvin
        .lock_managers
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let Some(lm) = managers.get(&vshard) else {
        return 0;
    };
    let lm = lm.lock().unwrap_or_else(|p| p.into_inner());
    lm.reservation_holder_count(key)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_key_read_reservation_installs_self_upgrades_and_releases() {
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    let hot_coll = "hkr_hot_kv";
    let hot_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, hot_coll).as_u32();
    let other_coll = other_vshard_collection(hot_vshard);

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {hot_coll} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .expect("CREATE hot collection");
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {other_coll} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .expect("CREATE other collection");
    node.client
        .simple_query(&format!(
            "INSERT INTO {hot_coll} (key, n) VALUES ('hotkey', 0)"
        ))
        .await
        .expect("seed hot row");

    // Seed the hot-key detector: three aborts reaches HOT_KEY_ABORT_THRESHOLD
    // within the rolling window. The LockKey mirrors exactly what a KV point read
    // on this collection produces (`LockKey::Kv` with the raw text-literal bytes).
    let lock_key = LockKey::Kv {
        collection: Arc::from(hot_coll),
        key: Arc::from(b"hotkey".as_slice()),
    };
    {
        let mut table = node
            .shared
            .calvin
            .hot_key_table
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        table.record_abort(&lock_key, now);
        table.record_abort(&lock_key, now);
        table.record_abort(&lock_key, now);
    }

    // Interactive cross-shard txn: read the hot key (vShard A), then write it
    // (self-upgrade target) plus a key on vShard B.
    node.client.simple_query("BEGIN").await.expect("BEGIN");
    node.client
        .simple_query(&format!("SELECT n FROM {hot_coll} WHERE key = 'hotkey'"))
        .await
        .expect("hot-key read");

    // Reservation install is async (a Raft round through `submit_reserve_read`).
    wait_for(
        "shared reservation installed on the hot key",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || reservation_count(&node, hot_vshard, &lock_key) > 0,
    )
    .await;

    node.client
        .simple_query(&format!(
            "INSERT INTO {hot_coll} (key, n) VALUES ('hotkey', 99) \
             ON CONFLICT (key) DO UPDATE SET n = EXCLUDED.n"
        ))
        .await
        .expect("self-upgrading write on the reserved key");
    node.client
        .simple_query(&format!(
            "INSERT INTO {other_coll} (key, n) VALUES ('x', 1)"
        ))
        .await
        .expect("cross-shard write on the other vshard");
    node.client
        .simple_query("COMMIT")
        .await
        .expect("cross-shard commit via strict Calvin path must succeed");

    // Release is async (a sequenced ReleaseReservation).
    wait_for(
        "reservation released after commit",
        Duration::from_secs(10),
        Duration::from_millis(25),
        || reservation_count(&node, hot_vshard, &lock_key) == 0,
    )
    .await;

    // Functional proof the self-upgraded write landed.
    let msgs = node
        .client
        .simple_query(&format!("SELECT n FROM {hot_coll} WHERE key = 'hotkey'"))
        .await
        .expect("post-commit read");
    let value = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get("n"),
            _ => None,
        })
        .expect("row present");
    assert_eq!(
        value, "99",
        "self-upgraded write must be visible after commit"
    );

    node.shutdown().await;
}
