// SPDX-License-Identifier: BUSL-1.1

//! Join-leader decision, health Ping, and TopologyUpdate RPC handling.

use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::health;
use crate::rpc_codec::{PingRequest, RaftRpc, TopologyUpdate};

use super::super::loop_core::{CommitApplier, RaftLoop};

/// The Raft group that owns cluster topology / membership.
///
/// Group 0 is the "metadata" group and is the authoritative source of
/// truth for who is in the cluster. Joins must be processed by its
/// leader; this constant is also used by the join orchestration in
/// [`super::super::join`].
pub(in crate::raft_loop) const TOPOLOGY_GROUP_ID: u64 = 0;

/// Outcome of the leader-check phase of the join flow.
///
/// Extracted as a pure enum so the decision logic can be unit-tested
/// without spinning up a real `MultiRaft` just to observe its leader id.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::raft_loop) enum JoinDecision {
    /// This node is the group-0 leader (or the founding seed with no leader
    /// elected yet). Admit the join locally.
    Admit,
    /// Another node is the group-0 leader. The client should retry at
    /// `leader_addr`.
    Redirect { leader_addr: String },
}

/// Pure decision: given the observed group-0 leader, this node's id, and
/// the leader's address (as known to the local topology), should we
/// admit the join or redirect?
///
/// - `group0_leader == 0` means "no elected leader yet". On a freshly
///   bootstrapped single-seed cluster this is normal — the founding node
///   is the only possible leader, so we accept.
/// - `group0_leader == self_node_id` means we are the leader — accept.
/// - Otherwise redirect. If the leader's address is unknown to topology
///   (an operator error that shouldn't happen in practice), we still
///   redirect with an empty string so the client at least sees the
///   `"not leader"` prefix and can decide to try the next seed.
pub(in crate::raft_loop) fn decide_join(
    group0_leader: u64,
    self_node_id: u64,
    leader_addr: Option<String>,
) -> JoinDecision {
    if group0_leader == 0 || group0_leader == self_node_id {
        JoinDecision::Admit
    } else {
        JoinDecision::Redirect {
            leader_addr: leader_addr.unwrap_or_default(),
        }
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    // Health check.
    pub(super) fn handle_ping_rpc(&self, req: PingRequest) -> Result<RaftRpc> {
        let topo_version = {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            topo.version()
        };
        Ok(health::handle_ping(
            self.node_id,
            topo_version,
            &self.snapshot_board,
            &req,
        ))
    }

    // Topology broadcast.
    pub(super) fn handle_topology_update_rpc(&self, update: TopologyUpdate) -> Result<RaftRpc> {
        let (updated, ack) = health::handle_topology_update(self.node_id, &self.topology, &update);
        if updated {
            // Register every member's address with the transport
            // so raft RPCs to newly-learned peers actually have
            // a destination. Without this, a node that joined
            // early and then learned about a later joiner via
            // broadcast would hold a stale peer set in its
            // transport and AppendEntries to the new peer would
            // fail until the circuit breaker opened permanently.
            for node in &update.nodes {
                if node.node_id == self.node_id {
                    continue;
                }
                match node.addr.parse::<std::net::SocketAddr>() {
                    Ok(addr) => self.transport.register_peer(node.node_id, addr),
                    Err(e) => tracing::warn!(
                        node_id = node.node_id,
                        addr = %node.addr,
                        error = %e,
                        "topology update contains unparseable peer address; skipping register_peer"
                    ),
                }
            }
            // Persist the adopted topology so a subsequent
            // restart reads the latest member set from catalog
            // rather than the stale snapshot taken at join
            // time. Persist only when a catalog is attached;
            // failures are logged but never propagate — the
            // next TopologyUpdate will retry.
            if let Some(catalog) = self.catalog.as_ref() {
                let snap = self
                    .topology
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone();
                if let Err(e) = catalog.save_topology(&snap) {
                    tracing::warn!(error = %e, "failed to persist topology update to catalog");
                }
            }
        }
        Ok(ack)
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinDecision, decide_join};

    #[test]
    fn decide_join_self_leader_admits() {
        assert_eq!(
            decide_join(7, 7, Some("10.0.0.7:9400".into())),
            JoinDecision::Admit
        );
    }

    #[test]
    fn decide_join_no_leader_yet_admits() {
        assert_eq!(decide_join(0, 7, None), JoinDecision::Admit);
    }

    #[test]
    fn decide_join_other_leader_redirects() {
        assert_eq!(
            decide_join(1, 7, Some("10.0.0.1:9400".into())),
            JoinDecision::Redirect {
                leader_addr: "10.0.0.1:9400".into()
            }
        );
    }

    #[test]
    fn decide_join_other_leader_unknown_addr_still_redirects() {
        assert_eq!(
            decide_join(1, 7, None),
            JoinDecision::Redirect {
                leader_addr: String::new()
            }
        );
    }
}
