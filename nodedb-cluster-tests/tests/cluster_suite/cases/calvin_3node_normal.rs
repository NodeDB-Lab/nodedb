// SPDX-License-Identifier: BUSL-1.1

//! 3-node sequencer normal-path integration test.
//!
//! Verifies the happy path for a Calvin cross-shard transaction:
//!
//! 1. Spin up a 3-node cluster with a wired sequencer Raft group.
//! 2. Wait for the sequencer leader to be elected.
//! 3. Submit a static-set multi-vshard write transaction through the
//!    leader's inbox.
//! 4. Tick the sequencer service once so the epoch is proposed.
//! 5. Wait for the epoch batch to be committed and applied on all 3
//!    replicas (verified via `last_applied_epoch()`).
//! 6. Verify that per-vshard fan-out channels received the transaction
//!    on every participating node.

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_cluster::calvin::{
    sequencer::{SequencerConfig, new_inbox},
    types::{EngineKeySet, ReadWriteSet, SchedulerInput, SortedVec, TxClass, VersionedReadSet},
};
use nodedb_types::{
    TenantId,
    id::{DatabaseId, VShardId},
};
use tokio::sync::mpsc;

use super::cluster_common::{spawn_with_sequencer, try_recv_txn, wait_for_sequencer_leader};

/// Find two collection names that hash to distinct vshards.
fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
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

fn make_multishard_txclass() -> (TxClass, u32, u32) {
    let (col_a, col_b) = two_distinct_collections();
    let va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
    let vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a,
            surrogates: SortedVec::new(vec![1]),
        },
        EngineKeySet::Document {
            collection: col_b,
            surrogates: SortedVec::new(vec![2]),
        },
    ]);
    let tx = TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass");
    (tx, va, vb)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequencer_normal_path_commit_on_all_replicas() {
    let node_ids = vec![1u64, 2, 3];
    let nodes = spawn_with_sequencer(node_ids)
        .await
        .expect("spawn_with_sequencer");

    // Wait for sequencer leader election.
    let leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(10), Duration::from_millis(50)).await;

    // Build the inbox on the leader node.
    let config = SequencerConfig {
        epoch_duration: Duration::from_millis(10),
        ..SequencerConfig::default()
    };
    let (inbox, inbox_receiver) = new_inbox(1024, &config);

    // Wire per-vshard receivers on every node.
    let (tx_a, col_b_name) = two_distinct_collections();
    let va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &tx_a).as_u32();
    let vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b_name).as_u32();

    let mut vshard_rxs_a: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    let mut vshard_rxs_b: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    for node in &nodes {
        let (tx_a_ch, rx_a) = mpsc::channel(64);
        let (tx_b_ch, rx_b) = mpsc::channel(64);
        node.add_vshard_sender(va, tx_a_ch);
        node.add_vshard_sender(vb, tx_b_ch);
        vshard_rxs_a.push(rx_a);
        vshard_rxs_b.push(rx_b);
    }

    // Start the epoch ticker on the leader.
    let (_seq_shutdown, _, _seq_handle) =
        nodes[leader_idx].start_sequencer_service(inbox_receiver, config.clone());

    // Submit a multi-vshard write txn.
    let (tx_class, _va, _vb) = make_multishard_txclass();
    inbox.submit(tx_class).expect("submit");

    // Wait for all 3 nodes to have applied at least one epoch.
    super::cluster_common::wait_for(
        "all 3 nodes apply epoch 0",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || nodes.iter().all(|n| n.last_applied_epoch().is_some()),
    )
    .await;

    // Verify that epoch 0 was applied on all nodes.
    for (i, node) in nodes.iter().enumerate() {
        let epoch = node
            .last_applied_epoch()
            .expect("epoch should be applied on all nodes");
        assert_eq!(
            epoch, 0,
            "node {} should have applied epoch 0, got {}",
            i, epoch
        );
        let applied = node
            .state_machine
            .lock()
            .unwrap()
            .metrics
            .epochs_applied
            .load(Ordering::Relaxed);
        assert!(applied >= 1, "node {} metrics show 0 epochs applied", i);
    }

    // Verify that at least one vshard receiver on each node got the txn.
    for (i, (rx_a, rx_b)) in vshard_rxs_a
        .iter_mut()
        .zip(vshard_rxs_b.iter_mut())
        .enumerate()
    {
        let got_a = try_recv_txn(rx_a).is_some();
        let got_b = try_recv_txn(rx_b).is_some();
        assert!(
            got_a || got_b,
            "node {}: neither vshard receiver got the txn fan-out",
            i
        );
    }

    // Shut down.
    for node in nodes {
        node.shutdown().await;
    }
}
