// SPDX-License-Identifier: BUSL-1.1

//! A read cut closed through a Calvin cut marker survives a sequencer
//! leader change.
//!
//! A reader closes a cut whose clock part leads every sequencer node's wall
//! clock: here by one hour, as a node with a clock that far ahead closes it.
//! The cut's marker carries that clock part as its floor. The old leader
//! appends the marker, every replica applies it, and the old leader dies.
//! The new leader's wall clock trails the floor. The first epoch it mints
//! must still carry an instant above the floor, or a Calvin version lands
//! under a snapshot a reader already closed and read at.

use std::time::Duration;

use nodedb_cluster::calvin::SequencerEntry;
use nodedb_cluster::calvin::sequencer::{SequencerConfig, new_inbox};
use nodedb_cluster::calvin::types::{
    EngineKeySet, ReadWriteSet, SortedVec, TxClass, VersionedReadSet,
};
use nodedb_types::{TenantId, id::DatabaseId};

use super::cluster_common::{spawn_with_sequencer, wait_for, wait_for_sequencer_leader};

/// Two collections on distinct vShards: a cross-shard class.
fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("cut_floor_{i}");
        let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
            .vshard()
            .as_u32();
        match &first {
            Some((first_name, first_vshard)) if *first_vshard != vshard => {
                return (first_name.clone(), name);
            }
            Some(_) => {}
            None => first = Some((name, vshard)),
        }
    }
    panic!("no two distinct-vShard collections in 512 tries");
}

fn txclass(surrogate: u32) -> TxClass {
    let (col_a, col_b) = two_distinct_collections();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a,
            surrogates: SortedVec::new(vec![surrogate]),
        },
        EngineKeySet::Document {
            collection: col_b,
            surrogates: SortedVec::new(vec![surrogate + 1]),
        },
    ]);
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_sequencer_leader_mints_above_a_closed_cut_floor() {
    let mut nodes = spawn_with_sequencer(vec![1u64, 2, 3])
        .await
        .expect("spawn_with_sequencer");
    let leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(10), Duration::from_millis(50)).await;

    // The cut's clock part: one hour past every node's wall clock.
    let wall_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let floor_ms = wall_ms + 3_600_000;
    let floor = nodedb_types::ms_to_ordinal_upper(floor_ms) - 5;
    let marker = zerompk::to_msgpack_vec(&SequencerEntry::CutMarker {
        hlc: 1,
        restore_point: 0,
        barrier: None,
        floor,
    })
    .expect("encode the marker");
    nodes[leader_idx]
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .propose_to_sequencer(marker, floor)
        .expect("the leader appends the marker");
    wait_for(
        "every replica applied the cut marker",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || {
            nodes.iter().all(|node| {
                node.state_machine
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_epoch_system_ms()
                    == Some(floor_ms)
            })
        },
    )
    .await;

    // The old leader dies with the marker applied.
    let dead_leader = nodes.swap_remove(leader_idx);
    dead_leader.shutdown().await;
    let new_leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(15), Duration::from_millis(50)).await;

    let config = SequencerConfig {
        epoch_duration: Duration::from_millis(10),
        ..SequencerConfig::default()
    };
    let (inbox, inbox_receiver) = new_inbox(64, &config);
    let (_shutdown, _, _handle) =
        nodes[new_leader_idx].start_sequencer_service(inbox_receiver, config);
    inbox.submit(txclass(1)).expect("submit");

    // The new leader's first epoch applies with an instant above the floor.
    // A floor alone leaves the applied instant at `floor_ms` exactly.
    wait_for(
        "the new leader's epoch applied above the cut floor",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || {
            nodes[new_leader_idx]
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_epoch_system_ms()
                .is_some_and(|instant| instant > floor_ms)
        },
    )
    .await;
    assert!(
        nodedb_types::calvin_txn_ordinal(floor_ms + 1, 0) > floor,
        "every version of the epoch lies above the closed cut"
    );

    for node in nodes {
        node.shutdown().await;
    }
}
