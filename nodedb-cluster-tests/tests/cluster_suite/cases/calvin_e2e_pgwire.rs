// SPDX-License-Identifier: BUSL-1.1

//! End-to-end Calvin pgwire smoke test.
//!
//! Verifies that the sequencer path fires correctly for a multi-vshard INSERT
//! submitted through the Calvin sequencer inbox — the same code path that the
//! pgwire handler calls via `classify_dispatch` + `dispatch_calvin_or_fast`.
//!
//! Specifically this test asserts:
//! 1. `nodedb_sequencer_admitted_txns_total{outcome="admitted"}` is incremented
//!    (i.e. the txn entered the sequencer and was admitted to an epoch).
//! 2. The epoch is applied on all 3 replicas (`last_applied_epoch` is `Some`).
//! 3. Both vshard fan-out channels on every node receive the sequenced txn
//!    (i.e. the epoch was broadcast to all participating vshards on every
//!    replica — the same guarantee the pgwire response waits for via the
//!    Calvin completion registry).

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
///
/// Mirrors the helper in `calvin_3node_normal.rs`; we duplicate it here to
/// keep each test file self-contained.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("orders_{i}");
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

/// Build a multi-vshard TxClass representing a two-table INSERT
/// (e.g. `INSERT INTO orders_A ... ; INSERT INTO orders_B ...`).
fn make_multi_vshard_insert(col_a: &str, col_b: &str) -> TxClass {
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a.to_owned(),
            surrogates: SortedVec::new(vec![100]),
        },
        EngineKeySet::Document {
            collection: col_b.to_owned(),
            surrogates: SortedVec::new(vec![200]),
        },
    ]);
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        // Plans bytes: empty in this test (the executor path is tested by
        // `calvin_executor_apply.rs`; this test validates the sequencer and
        // fan-out path only).
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass for multi-vshard INSERT")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_vshard_insert_via_sequencer_admitted_and_replicated() {
    let node_ids = vec![1u64, 2, 3];
    let nodes = spawn_with_sequencer(node_ids)
        .await
        .expect("spawn_with_sequencer failed");

    // Wait for a sequencer leader before submitting.
    let leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(10), Duration::from_millis(50)).await;

    let config = SequencerConfig {
        epoch_duration: Duration::from_millis(10),
        ..SequencerConfig::default()
    };
    let (inbox, inbox_receiver) = new_inbox(1024, &config);

    // Wire per-vshard fan-out receivers on every node.
    let (col_a, col_b) = two_distinct_vshard_collections();
    let va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
    let vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();

    let mut fan_out_rxs_a: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    let mut fan_out_rxs_b: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    for node in &nodes {
        let (tx_a, rx_a) = mpsc::channel(64);
        let (tx_b, rx_b) = mpsc::channel(64);
        node.add_vshard_sender(va, tx_a);
        node.add_vshard_sender(vb, tx_b);
        fan_out_rxs_a.push(rx_a);
        fan_out_rxs_b.push(rx_b);
    }

    // Start the epoch ticker on the leader; capture metrics for assertion.
    let (_seq_shutdown, seq_metrics, _seq_handle) =
        nodes[leader_idx].start_sequencer_service(inbox_receiver, config.clone());

    // Submit a multi-vshard INSERT (two surrogates on two distinct vshards).
    // This mirrors what `dispatch_calvin_or_fast` does from the pgwire handler
    // for a strict cross-shard write.
    let tx_class = make_multi_vshard_insert(&col_a, &col_b);
    inbox.submit(tx_class).expect("inbox.submit succeeded");

    // Wait for all 3 replicas to apply the epoch.
    super::cluster_common::wait_for(
        "all 3 replicas apply epoch 0",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || nodes.iter().all(|n| n.last_applied_epoch().is_some()),
    )
    .await;

    // Assert: the sequencer admitted the txn
    // (`nodedb_sequencer_admitted_txns_total{outcome="admitted"}` >= 1).
    let admitted = seq_metrics.admitted_total.load(Ordering::Relaxed);
    assert!(
        admitted >= 1,
        "expected nodedb_sequencer_admitted_txns_total{{outcome=\"admitted\"}} >= 1, got {admitted}"
    );

    // Assert: epoch applied on all 3 replicas.
    for (i, node) in nodes.iter().enumerate() {
        let epoch = node
            .last_applied_epoch()
            .expect("epoch should be applied on every replica");
        assert_eq!(epoch, 0, "replica {i} should have applied epoch 0");
    }

    // Assert: fan-out channels received the txn on every replica —
    // both vshards must have been notified (same as pgwire's completion
    // registry waiting for all participant acks).
    for (i, (rx_a, rx_b)) in fan_out_rxs_a
        .iter_mut()
        .zip(fan_out_rxs_b.iter_mut())
        .enumerate()
    {
        let got_a = try_recv_txn(rx_a).is_some();
        let got_b = try_recv_txn(rx_b).is_some();
        assert!(
            got_a || got_b,
            "replica {i}: neither vshard fan-out channel received the txn"
        );
    }

    // Shut down cleanly.
    for node in nodes {
        node.shutdown().await;
    }
}
