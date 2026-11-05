// SPDX-License-Identifier: BUSL-1.1

//! The peers a storage round asks, and the request that asks them.
//!
//! A round asks every peer that can lead a Raft group: active and draining
//! nodes. The requests run in parallel, each under its own timeout. A peer
//! that errors is skipped for the round, and the others still count.

use std::time::{Duration, Instant};

use futures::future::join_all;
use nodedb_cluster::storage_share::{
    StorageShareRequest, StorageShareResponse, decode_response, encode_request,
};
use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};
use nodedb_cluster::{NexarTransport, NodeState, RaftRpc};

use crate::control::state::SharedState;

use super::share_wire::share_from_wire;
use super::shares::StorageShare;

/// Who this node shares the cluster with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Membership {
    /// No cluster routing, or a topology of this node alone.
    Alone,
    /// The peers to ask.
    Peers {
        /// Every peer that can lead a group: active and draining nodes.
        asked: Vec<u64>,
        /// The active peers. A round is complete when each one reported.
        required: Vec<u64>,
    },
    /// Cluster routing with no topology, or a topology without this node:
    /// the peers are not known.
    Unknown,
}

/// The membership of this node now. The topology guard drops on return.
pub fn membership(state: &SharedState) -> Membership {
    if state.cluster_routing.is_none() {
        return Membership::Alone;
    }
    let Some(topology) = state.cluster_topology.as_ref() else {
        return Membership::Unknown;
    };
    let topology = topology.read().unwrap_or_else(|p| p.into_inner());
    if !topology.contains(state.node_id) {
        return Membership::Unknown;
    }
    let mut asked = Vec::new();
    let mut required = Vec::new();
    for node in topology.all_nodes() {
        if node.node_id == state.node_id {
            continue;
        }
        match node.state {
            NodeState::Active => {
                asked.push(node.node_id);
                required.push(node.node_id);
            }
            NodeState::Draining => asked.push(node.node_id),
            NodeState::Joining | NodeState::Learner | NodeState::Decommissioned => {}
        }
    }
    if asked.is_empty() {
        return Membership::Alone;
    }
    asked.sort_unstable();
    required.sort_unstable();
    Membership::Peers { asked, required }
}

/// Ask every node of `peers` for its latest share, in parallel. Each answer
/// is `Ok(None)` when the peer has not sampled yet.
pub async fn fetch_peer_shares(
    state: &SharedState,
    peers: &[u64],
    timeout: Duration,
) -> Vec<(u64, crate::Result<Option<StorageShare>>)> {
    let Some(transport) = state.cluster_transport.as_ref() else {
        return peers
            .iter()
            .map(|peer| {
                let error = crate::Error::Dispatch {
                    detail: format!(
                        "storage share of node {peer}: this node has no cluster transport"
                    ),
                };
                (*peer, Err(error))
            })
            .collect();
    };
    let asks = peers.iter().map(|peer| async move {
        (
            *peer,
            fetch_one(transport, state.node_id, *peer, timeout).await,
        )
    });
    join_all(asks).await
}

async fn fetch_one(
    transport: &NexarTransport,
    node_id: u64,
    peer: u64,
    timeout: Duration,
) -> crate::Result<Option<StorageShare>> {
    let payload = encode_request(&StorageShareRequest {}).map_err(|e| codec(peer, &e))?;
    let envelope = VShardEnvelope::new(
        VShardMessageType::StorageShareRequest,
        node_id,
        peer,
        0,
        payload,
    );
    let rpc = RaftRpc::VShardEnvelope(envelope.to_bytes());
    let answer = tokio::time::timeout(
        timeout,
        transport.send_rpc_with_read_timeout(peer, rpc, timeout),
    )
    .await
    .map_err(|_| crate::Error::Dispatch {
        detail: format!(
            "storage share of node {peer}: no answer within {} ms",
            timeout.as_millis()
        ),
    })?
    .map_err(|e| crate::Error::Dispatch {
        detail: format!("storage share of node {peer}: {e}"),
    })?;
    let received_at = Instant::now();
    let bytes = match answer {
        RaftRpc::VShardEnvelope(bytes) => bytes,
        RaftRpc::VShardRefusal(refusal) => {
            let error = nodedb_cluster::error::ClusterError::from(refusal.error);
            return Err(crate::Error::Dispatch {
                detail: format!("storage share of node {peer}: refused: {error}"),
            });
        }
        other => {
            return Err(crate::Error::Codec {
                detail: format!("storage share of node {peer}: unexpected answer {other:?}"),
            });
        }
    };
    let reply = VShardEnvelope::from_bytes(&bytes).ok_or_else(|| crate::Error::Codec {
        detail: format!("storage share of node {peer}: the answer is not a vShard envelope"),
    })?;
    if reply.msg_type != VShardMessageType::StorageShareResponse {
        return Err(crate::Error::Codec {
            detail: format!(
                "storage share of node {peer}: answer type {:?}, expected StorageShareResponse",
                reply.msg_type
            ),
        });
    }
    let response = decode_response(&reply.payload).map_err(|e| codec(peer, &e))?;
    accept(peer, response, received_at)
}

/// The share of `response`, which node `peer` sent. An answer that names
/// another node is refused, so one node's share never stands for another's.
fn accept(
    peer: u64,
    response: StorageShareResponse,
    received_at: Instant,
) -> crate::Result<Option<StorageShare>> {
    if response.node_id != peer {
        return Err(crate::Error::Codec {
            detail: format!(
                "storage share of node {peer}: the answer names node {}",
                response.node_id
            ),
        });
    }
    Ok(response
        .sample
        .map(|sample| share_from_wire(sample, received_at)))
}

fn codec(peer: u64, error: &nodedb_cluster::error::ClusterError) -> crate::Error {
    crate::Error::Codec {
        detail: format!("storage share of node {peer}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_cluster::storage_share::ShareSample;
    use nodedb_cluster::{ClusterTopology, NodeInfo, RoutingTable};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn fresh_state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("p.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    fn topology_of(nodes: &[(u64, NodeState)]) -> Arc<RwLock<ClusterTopology>> {
        let mut topology = ClusterTopology::new();
        for (i, (node_id, node_state)) in nodes.iter().enumerate() {
            let addr = format!("127.0.0.1:{}", 9000 + i).parse().expect("addr");
            topology.add_node(NodeInfo::new(*node_id, addr, *node_state));
        }
        Arc::new(RwLock::new(topology))
    }

    /// No routing, or a topology of this node alone, is alone. Routing with
    /// no topology, or a topology without this node, is unknown. Active and draining peers are asked, and only
    /// active peers are required.
    #[test]
    fn membership_follows_routing_and_topology() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = fresh_state(dir.path());
        assert_eq!(membership(&state), Membership::Alone);

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.node_id = 1;
        shared.cluster_routing = Some(Arc::new(RwLock::new(RoutingTable::uniform(1, &[1], 1))));
        shared.cluster_topology = None;
        assert_eq!(membership(&state), Membership::Unknown);

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[(1, NodeState::Active)]));
        assert_eq!(membership(&state), Membership::Alone);

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[(2, NodeState::Active)]));
        assert_eq!(membership(&state), Membership::Unknown);

        let shared = Arc::get_mut(&mut state).expect("sole owner of fresh state");
        shared.cluster_topology = Some(topology_of(&[
            (1, NodeState::Active),
            (2, NodeState::Active),
            (3, NodeState::Draining),
            (4, NodeState::Learner),
            (5, NodeState::Decommissioned),
        ]));
        assert_eq!(
            membership(&state),
            Membership::Peers {
                asked: vec![2, 3],
                required: vec![2],
            }
        );
    }

    /// With no transport every peer errors, and the round skips them.
    #[tokio::test]
    async fn no_transport_errors_every_peer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = fresh_state(dir.path());
        let answers = fetch_peer_shares(&state, &[2, 3], Duration::from_millis(10)).await;
        assert_eq!(answers.len(), 2);
        assert!(answers.iter().all(|(_, answer)| answer.is_err()));
    }

    /// An answer that names another node is refused.
    #[test]
    fn an_answer_from_another_node_is_refused() {
        let response = StorageShareResponse {
            node_id: 4,
            sample: Some(ShareSample {
                sample_age_ms: 0,
                groups: Vec::new(),
            }),
        };
        assert!(accept(3, response.clone(), Instant::now()).is_err());
        assert!(
            accept(4, response, Instant::now())
                .expect("own answer")
                .is_some()
        );
    }
}
