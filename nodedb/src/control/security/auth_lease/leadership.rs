// SPDX-License-Identifier: BUSL-1.1

//! Who leads the metadata group, from this node's Raft status.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_cluster::{METADATA_GROUP_ID, RaftRpc};

use crate::control::cluster::warm_peers::register_peers_from_topology;
use crate::control::state::SharedState;

/// The metadata group's leader and term, as this node sees them. A leader
/// id of `0` means none is known.
pub(crate) fn metadata_leader(state: &SharedState) -> Option<(u64, u64)> {
    metadata_status(state).map(|group| (group.leader_id, group.term))
}

/// The status of this node's metadata group replica.
fn metadata_status(state: &SharedState) -> Option<nodedb_cluster::GroupStatus> {
    state.raft_group_status(METADATA_GROUP_ID)
}

/// The term this node leads the metadata group in, if it does.
pub(crate) fn leading_term(state: &SharedState) -> Option<u64> {
    metadata_leader(state)
        .filter(|(leader_id, _)| *leader_id == state.node_id)
        .map(|(_, term)| term)
}

/// The term this node leads the metadata group in, when it is also the
/// group's only voter.
///
/// A single-voter group commits a configuration change inside the propose
/// call and applies it at once. A second voter therefore ends this before it
/// can vote. No other node can lead the group while this returns a term.
pub(crate) fn sole_voter_term(state: &SharedState) -> Option<u64> {
    metadata_status(state)
        .filter(|group| {
            group.role == "Leader" && group.leader_id == state.node_id && group.member_count == 1
        })
        .map(|group| group.term)
}

/// The leader hint to send back with a refusal, and the term this node
/// knows it at: `(leader_hint, term)`.
pub(crate) fn leader_hint(state: &SharedState) -> (Option<u64>, u64) {
    metadata_leader(state).map_or((None, 0), |(leader_id, term)| {
        ((leader_id != 0).then_some(leader_id), term)
    })
}

/// Send `rpc` to the metadata leader `leader_id` and return its answer.
pub(crate) async fn send_to_leader(
    state: &SharedState,
    leader_id: u64,
    rpc: RaftRpc,
    timeout: Duration,
) -> crate::Result<RaftRpc> {
    let Some(transport) = state.cluster_transport.as_ref() else {
        return Err(crate::Error::Internal {
            detail: "authorization lease: no cluster transport on this node".into(),
        });
    };
    let mut targets = BTreeSet::new();
    targets.insert(leader_id);
    register_peers_from_topology(state, transport, &targets);
    transport
        .send_rpc_with_read_timeout(leader_id, rpc, timeout)
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("authorization lease: rpc to metadata leader {leader_id}: {e}"),
        })
}
