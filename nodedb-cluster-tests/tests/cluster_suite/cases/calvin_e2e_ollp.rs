// SPDX-License-Identifier: BUSL-1.1

//! End-to-end Calvin OLLP sequencer test.
//!
//! Verifies that the sequencer path correctly handles multi-shard Calvin
//! transactions that include an OLLP BulkUpdate plan — the same code path that
//! the pgwire handler triggers via `run_dependent_with_retry` for cross-shard
//! writes that include a value-dependent predicate operation.
//!
//! The OLLP (Optimistic Lock-based Predicate) protocol works as follows:
//!
//! 1. Control Plane runs a pre-execution scan to collect the set of document
//!    surrogates currently matching the predicate ("predicted surrogates").
//! 2. The TxClass is built with the predicted surrogates for the OLLP
//!    collection and static surrogates for any additional static-key writes
//!    in the same transaction (via `build_dependent_tx_class`), then submitted
//!    to the sequencer inbox.
//! 3. The sequencer admits the txn and fans it out to all participant vshards.
//! 4. The Data Plane executor verifies the predicted set against the actual
//!    matching set at admission time; on mismatch it returns `OllpRetryRequired`
//!    without writing (tested separately at the executor layer).
//! 5. On retry, the Control Plane re-scans and re-submits with the corrected
//!    surrogate set.
//!
//! This test validates steps 2–3: that a multi-vshard TxClass whose write set
//! combines OLLP predicted surrogates (for one collection) with static surrogates
//! (for another collection on a different vshard) is admitted and fanned out
//! correctly, and that a re-submission (simulating the retry after
//! OllpRetryRequired) is also admitted. It mirrors the harness in
//! `calvin_e2e_pgwire.rs`.
//!
//! The executor-level verification (step 4) is tested in
//! `nodedb/tests/executor_tests/test_ollp_verification.rs`.

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
/// Mirrors the helper in `calvin_e2e_pgwire.rs`.
fn two_distinct_vshard_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("ollp_col_{i}");
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

/// Build a multi-vshard TxClass that represents:
///
/// - A static-key point write on `col_static` (surrogate `static_surrogate`).
/// - An OLLP BulkUpdate on `col_ollp` with `predicted_surrogates` as the
///   write set (since the exact set is not known statically).
///
/// This mirrors what `build_dependent_tx_class` produces on the Control Plane
/// for a cross-shard transaction that contains a value-dependent predicate write.
fn make_ollp_tx_class(
    col_static: &str,
    static_surrogate: u32,
    col_ollp: &str,
    predicted_surrogates: Vec<u32>,
) -> TxClass {
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_static.to_owned(),
            surrogates: SortedVec::new(vec![static_surrogate]),
        },
        EngineKeySet::Document {
            collection: col_ollp.to_owned(),
            surrogates: SortedVec::new(predicted_surrogates),
        },
    ]);
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        // Empty plan bytes — this test exercises the sequencer admission and
        // fan-out path only. The actual BulkUpdate plan bytes are embedded by
        // `build_dependent_tx_class` in production.
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid multi-vshard OLLP TxClass")
}

/// Assert: one specific vshard channel received at least one sequenced txn.
///
/// The state machine sends into the channel inside `apply`, before it
/// advances the epoch the caller waited for, so the txn is already there.
fn assert_fan_out_received(
    rx: &mut mpsc::Receiver<SchedulerInput>,
    vshard_id: u32,
    replica_idx: usize,
) {
    assert!(
        try_recv_txn(rx).is_some(),
        "replica {replica_idx}: vshard {vshard_id} fan-out channel received no txn"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ollp_bulk_update_txclass_admitted_and_fanned_out() {
    let node_ids = vec![1u64, 2, 3];
    let nodes = spawn_with_sequencer(node_ids)
        .await
        .expect("spawn_with_sequencer failed");

    let leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(10), Duration::from_millis(50)).await;

    let config = SequencerConfig {
        epoch_duration: Duration::from_millis(10),
        ..SequencerConfig::default()
    };
    let (inbox, inbox_receiver) = new_inbox(1024, &config);

    // Find two collections that hash to distinct vshards.
    let (col_static, col_ollp) = two_distinct_vshard_collections();
    let vs_static =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_static).as_u32();
    let vs_ollp = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_ollp).as_u32();

    // Wire per-vshard fan-out receivers on every replica.
    let mut rxs_static: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    let mut rxs_ollp: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    for node in &nodes {
        let (tx_s, rx_s) = mpsc::channel(64);
        let (tx_o, rx_o) = mpsc::channel(64);
        node.add_vshard_sender(vs_static, tx_s);
        node.add_vshard_sender(vs_ollp, tx_o);
        rxs_static.push(rx_s);
        rxs_ollp.push(rx_o);
    }

    let (_seq_shutdown, seq_metrics, _seq_handle) =
        nodes[leader_idx].start_sequencer_service(inbox_receiver, config.clone());

    // Submit a multi-vshard TxClass:
    //   - col_static: surrogate 100 (static point write)
    //   - col_ollp: predicted surrogates [1, 2, 3] (OLLP BulkUpdate pre-exec result)
    let tx_class = make_ollp_tx_class(&col_static, 100, &col_ollp, vec![1, 2, 3]);
    inbox.submit(tx_class).expect("inbox.submit succeeded");

    // Wait for all 3 replicas to apply the epoch.
    super::cluster_common::wait_for(
        "all 3 replicas apply epoch 0 (initial OLLP BulkUpdate)",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || nodes.iter().all(|n| n.last_applied_epoch().is_some()),
    )
    .await;

    // Assert: sequencer admitted the txn.
    let admitted = seq_metrics.admitted_total.load(Ordering::Relaxed);
    assert!(
        admitted >= 1,
        "expected admitted_total >= 1 after initial OLLP BulkUpdate, got {admitted}"
    );

    // Assert: epoch applied on all 3 replicas.
    for (i, node) in nodes.iter().enumerate() {
        let epoch = node
            .last_applied_epoch()
            .expect("epoch should be applied on every replica");
        assert_eq!(epoch, 0, "replica {i} should have applied epoch 0");
    }

    // Assert: both vshards received the txn on every replica.
    for (i, (rx_s, rx_o)) in rxs_static.iter_mut().zip(rxs_ollp.iter_mut()).enumerate() {
        assert_fan_out_received(rx_s, vs_static, i);
        assert_fan_out_received(rx_o, vs_ollp, i);
    }

    // Simulate an OLLP retry: concurrent insert added surrogate 4 to col_ollp.
    // Re-wire fresh fan-out receivers and re-submit with the corrected set.
    let mut retry_rxs_static: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    let mut retry_rxs_ollp: Vec<mpsc::Receiver<SchedulerInput>> = Vec::new();
    for node in &nodes {
        let (tx_s, rx_s) = mpsc::channel(64);
        let (tx_o, rx_o) = mpsc::channel(64);
        node.add_vshard_sender(vs_static, tx_s);
        node.add_vshard_sender(vs_ollp, tx_o);
        retry_rxs_static.push(rx_s);
        retry_rxs_ollp.push(rx_o);
    }

    let retry_tx_class = make_ollp_tx_class(&col_static, 100, &col_ollp, vec![1, 2, 3, 4]);
    inbox
        .submit(retry_tx_class)
        .expect("inbox.submit for retry succeeded");

    // Wait for the retry epoch.
    super::cluster_common::wait_for(
        "all 3 replicas apply epoch 1 (retry OLLP BulkUpdate)",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || {
            nodes
                .iter()
                .all(|n| n.last_applied_epoch().map(|e| e >= 1).unwrap_or(false))
        },
    )
    .await;

    // Assert: retry was also admitted.
    let admitted_after_retry = seq_metrics.admitted_total.load(Ordering::Relaxed);
    assert!(
        admitted_after_retry >= 2,
        "expected admitted_total >= 2 after retry, got {admitted_after_retry}"
    );

    // Assert: both vshards received the retry txn on every replica.
    for (i, (rx_s, rx_o)) in retry_rxs_static
        .iter_mut()
        .zip(retry_rxs_ollp.iter_mut())
        .enumerate()
    {
        assert_fan_out_received(rx_s, vs_static, i);
        assert_fan_out_received(rx_o, vs_ollp, i);
    }

    // Shut down cleanly.
    for node in nodes {
        node.shutdown().await;
    }
}
