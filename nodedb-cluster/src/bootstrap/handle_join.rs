//! Server-side `JoinRequest` handler: build a `JoinResponse` from current cluster state.
//!
//! Called by [`crate::raft_loop::handle_rpc`] on the group-0 leader when a
//! `JoinRequest` arrives. This function is the single source of truth for how
//! a new node is admitted into the topology wire response; the Raft conf-change
//! that actually replicates membership across groups is driven separately from
//! the RPC arm.
//!
//! Semantics:
//!
//! - **New node**: added to topology as `Active`, full wire response returned.
//! - **Known node, same address**: idempotent — no mutation, full wire response returned.
//! - **Known node, different address**: rejected with `success: false`. This
//!   catches node-id reuse (operator error or a ghost node coming back with a
//!   stale id on a new address).
//! - **Invalid `listen_addr` in the request**: rejected with `success: false`.

use std::net::SocketAddr;

use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinGroupInfo, JoinNodeInfo, JoinRequest, JoinResponse};
use crate::topology::{ClusterTopology, NodeInfo, NodeState};

/// Build a `JoinResponse` for an incoming `JoinRequest`.
///
/// See module docs for semantics. Mutates `topology` only when the node is
/// newly admitted; idempotent for re-joins with the same address.
///
/// `cluster_id` is the id of the cluster this node belongs to — the
/// join flow reads it from the local catalog and threads it through so
/// the joining node can persist it and take the `restart()` path on a
/// subsequent boot. Zero is a valid placeholder when the server's
/// catalog has not yet been populated; rejection responses also carry
/// zero.
pub fn handle_join_request(
    req: &JoinRequest,
    topology: &mut ClusterTopology,
    routing: &RoutingTable,
    cluster_id: u64,
) -> JoinResponse {
    // Validate the listen address early.
    let addr: SocketAddr = match req.listen_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            return reject(format!("invalid listen_addr '{}': {e}", req.listen_addr));
        }
    };

    // Collision / idempotency check — both require reading the existing entry.
    if let Some(existing) = topology.get_node(req.node_id) {
        let existing_addr = existing.addr.clone();
        if existing_addr != req.listen_addr {
            // Same id, different address — reject.
            return reject(format!(
                "node_id {} already registered with different address {} (request: {})",
                req.node_id, existing_addr, req.listen_addr
            ));
        }
        // Same id, same address. If already Active we short-circuit —
        // no topology mutation at all, just rebuild the wire response. If the
        // node was in a non-Active state (Joining/Draining), normalize to
        // Active now because it's clearly back online.
        if existing.state != NodeState::Active
            && let Some(entry) = topology.get_node_mut(req.node_id)
        {
            entry.state = NodeState::Active;
        }
        return build_response(topology, routing, cluster_id);
    }

    // Brand new node — admit as Active. Stamp the joiner's own
    // wire version onto its NodeInfo so every peer that replays
    // this topology sees the correct version in
    // `cluster_version_view` without round-tripping through the
    // old in-memory shadow map.
    topology.add_node(
        NodeInfo::new(req.node_id, addr, NodeState::Active).with_wire_version(req.wire_version),
    );
    build_response(topology, routing, cluster_id)
}

/// Build a successful `JoinResponse` from the current topology and routing.
fn build_response(
    topology: &ClusterTopology,
    routing: &RoutingTable,
    cluster_id: u64,
) -> JoinResponse {
    let nodes: Vec<JoinNodeInfo> = topology
        .all_nodes()
        .map(|n| JoinNodeInfo {
            node_id: n.node_id,
            addr: n.addr.clone(),
            state: n.state.as_u8(),
            raft_groups: n.raft_groups.clone(),
            wire_version: n.wire_version,
        })
        .collect();

    let groups: Vec<JoinGroupInfo> = routing
        .group_members()
        .iter()
        .map(|(&gid, info)| JoinGroupInfo {
            group_id: gid,
            leader: info.leader,
            members: info.members.clone(),
            learners: info.learners.clone(),
        })
        .collect();

    JoinResponse {
        success: true,
        error: String::new(),
        cluster_id,
        nodes,
        vshard_to_group: routing.vshard_to_group().to_vec(),
        groups,
    }
}

/// Build a rejection response with the given error message.
fn reject(error: String) -> JoinResponse {
    JoinResponse {
        success: false,
        error,
        cluster_id: 0,
        nodes: vec![],
        vshard_to_group: vec![],
        groups: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topo_with_one_node() -> ClusterTopology {
        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "10.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topology
    }

    #[test]
    fn handle_join_request_adds_node() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(2, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };

        let resp = handle_join_request(&req, &mut topology, &routing, 42);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2);
        assert_eq!(resp.vshard_to_group.len(), 1024);
        // uniform(2, ...) creates 2 data groups + 1 metadata group = 3 total.
        assert_eq!(resp.groups.len(), 3);

        assert!(topology.contains(2));
        assert_eq!(topology.node_count(), 2);
    }

    #[test]
    fn handle_join_request_idempotent() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };

        let _ = handle_join_request(&req, &mut topology, &routing, 42);
        let resp = handle_join_request(&req, &mut topology, &routing, 42);

        assert!(resp.success);
        assert_eq!(resp.nodes.len(), 2); // Still 2, not 3.
        assert_eq!(topology.node_count(), 2);
    }

    /// A second join with the same id+addr must not mutate topology at all
    /// (no duplicate entries, no state reset). Verify by capturing
    /// `node_count` and the node ordering between calls.
    #[test]
    fn handle_join_request_idempotent_no_mutation() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };

        let resp1 = handle_join_request(&req, &mut topology, &routing, 7);
        let ids_before: Vec<u64> = topology.all_nodes().map(|n| n.node_id).collect();
        let count_before = topology.node_count();

        let resp2 = handle_join_request(&req, &mut topology, &routing, 7);
        assert_eq!(resp1.cluster_id, 7);
        assert_eq!(resp2.cluster_id, 7);
        let ids_after: Vec<u64> = topology.all_nodes().map(|n| n.node_id).collect();

        assert!(resp1.success && resp2.success);
        assert_eq!(count_before, topology.node_count());
        assert_eq!(ids_before, ids_after);
        assert_eq!(resp2.nodes.len(), 2);
        // Node 2 must still be Active.
        let n2 = topology.get_node(2).unwrap();
        assert_eq!(n2.state, NodeState::Active);
    }

    /// Same id, different address → reject.
    #[test]
    fn handle_join_request_rejects_id_collision() {
        let mut topology = topo_with_one_node();
        let routing = RoutingTable::uniform(1, &[1], 1);

        // First join: node 2 at 10.0.0.2:9400.
        let req1 = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.2:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };
        let resp1 = handle_join_request(&req1, &mut topology, &routing, 11);
        assert!(resp1.success);

        // Second join: same id, different address — must be rejected.
        let req2 = JoinRequest {
            node_id: 2,
            listen_addr: "10.0.0.99:9400".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };
        let resp2 = handle_join_request(&req2, &mut topology, &routing, 11);

        assert!(!resp2.success);
        assert!(
            resp2.error.contains("already registered"),
            "error should mention collision: {}",
            resp2.error
        );
        // Topology must not be clobbered.
        assert_eq!(topology.node_count(), 2);
        let n2 = topology.get_node(2).unwrap();
        assert_eq!(n2.addr, "10.0.0.2:9400");
    }

    #[test]
    fn handle_join_invalid_addr() {
        let mut topology = ClusterTopology::new();
        let routing = RoutingTable::uniform(1, &[1], 1);

        let req = JoinRequest {
            node_id: 2,
            listen_addr: "not-a-valid-address".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
        };

        let resp = handle_join_request(&req, &mut topology, &routing, 42);
        assert!(!resp.success);
        assert!(!resp.error.is_empty());
    }
}
