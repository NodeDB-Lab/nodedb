// SPDX-License-Identifier: BUSL-1.1

//! Cluster topology — tracks which nodes exist and their state.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::rpc_codec::JoinNodeInfo;

/// Wire format version carried on every `NodeInfo`. Re-exported from
/// `nodedb_types::wire_version`, which is the single source of truth
/// shared with `nodedb::version` and any other crate that needs to
/// stamp or interpret the value. See
/// `control::rolling_upgrade::view::ClusterVersionView` for the
/// consumer.
pub use nodedb_types::wire_version::WIRE_FORMAT_VERSION as CLUSTER_WIRE_FORMAT_VERSION;

/// Node lifecycle states.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum NodeState {
    /// Node is connecting, receiving initial topology.
    Joining = 0,
    /// Node is fully operational, hosting Raft groups as voting member.
    Active = 1,
    /// Node is being decommissioned, migrating data off.
    Draining = 2,
    /// Node has been removed from the cluster.
    Decommissioned = 3,
    /// Node is catching up as a non-voting learner.
    /// Receives Raft log entries but doesn't vote in elections.
    /// Promoted to Active (voting member) after state catch-up
    /// and checksum validation.
    Learner = 4,
}

impl NodeState {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Joining => 0,
            Self::Active => 1,
            Self::Draining => 2,
            Self::Decommissioned => 3,
            Self::Learner => 4,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Joining),
            1 => Some(Self::Active),
            2 => Some(Self::Draining),
            3 => Some(Self::Decommissioned),
            4 => Some(Self::Learner),
            _ => None,
        }
    }

    /// Whether this node can vote in Raft elections.
    pub fn is_voter(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether this node receives Raft log entries (learner + active).
    pub fn receives_log(self) -> bool {
        matches!(self, Self::Learner | Self::Active)
    }
}

/// Information about a single node in the cluster.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeInfo {
    pub node_id: u64,
    /// Listen address for Raft RPCs (as string for serialization portability).
    pub addr: String,
    pub state: NodeState,
    /// Raft groups hosted on this node.
    pub raft_groups: Vec<u64>,
    /// Wire format version this node is running. Stamped by the
    /// node itself on self-registration (bootstrap / join) and by
    /// `handle_join` when learning about a remote joiner. Read by
    /// `control::rolling_upgrade::view::compute` to derive the
    /// cluster-wide min/max/mixed view on demand from the live
    /// topology.
    pub wire_version: u16,
    /// SPIFFE URI SAN (e.g. `spiffe://cluster.local/node/1`) extracted from
    /// the node's mTLS leaf certificate at join time.  `None` for a node
    /// running in insecure transport mode.
    pub spiffe_id: Option<String>,
    /// SHA-256 digest of the node's SubjectPublicKeyInfo DER blob, pinned at
    /// join time.  Stable across cert renewals that reuse the same key-pair;
    /// changes on key rotation.  `None` until the node has joined and
    /// transmitted its identity fields.
    pub spki_pin: Option<[u8; 32]>,
    /// Bound UDP address of this node's SWIM failure detector. Peers seed
    /// SWIM from it. `None` for a node that runs no SWIM detector.
    pub swim_addr: Option<String>,
}

impl NodeInfo {
    /// Construct a NodeInfo stamped with this build's wire version.
    /// Use [`NodeInfo::with_wire_version`] when stamping a remote
    /// node whose version arrived over the wire.
    pub fn new(node_id: u64, addr: SocketAddr, state: NodeState) -> Self {
        Self {
            node_id,
            addr: addr.to_string(),
            state,
            raft_groups: Vec::new(),
            wire_version: CLUSTER_WIRE_FORMAT_VERSION,
            spiffe_id: None,
            spki_pin: None,
            swim_addr: None,
        }
    }

    /// Override the wire version on an otherwise-fresh NodeInfo.
    /// Builder-style so call sites read as
    /// `NodeInfo::new(id, addr, state).with_wire_version(remote_v)`.
    pub fn with_wire_version(mut self, wire_version: u16) -> Self {
        self.wire_version = wire_version;
        self
    }

    /// Set the SPIFFE URI SAN for this node. Builder-style.
    pub fn with_spiffe_id(mut self, spiffe_id: Option<String>) -> Self {
        self.spiffe_id = spiffe_id;
        self
    }

    /// Set the SPKI fingerprint pin for this node. Builder-style.
    pub fn with_spki_pin(mut self, spki_pin: Option<[u8; 32]>) -> Self {
        self.spki_pin = spki_pin;
        self
    }

    /// Set the SWIM address this node advertises. Builder-style.
    pub fn with_swim_addr(mut self, swim_addr: Option<SocketAddr>) -> Self {
        self.swim_addr = swim_addr.map(|a| a.to_string());
        self
    }

    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.addr.parse().ok()
    }

    /// The advertised SWIM address, if present and parseable.
    pub fn swim_socket_addr(&self) -> Option<SocketAddr> {
        self.swim_addr.as_deref().and_then(|a| a.parse().ok())
    }

    /// Wire form carried in join responses and topology updates.
    pub fn to_wire(&self) -> JoinNodeInfo {
        JoinNodeInfo {
            node_id: self.node_id,
            addr: self.addr.clone(),
            state: self.state.as_u8(),
            raft_groups: self.raft_groups.clone(),
            wire_version: self.wire_version,
            spiffe_id: self.spiffe_id.clone(),
            spki_pin: self.spki_pin.map(|arr| arr.to_vec()),
            swim_addr: self.swim_addr.clone(),
        }
    }

    /// Rebuild a `NodeInfo` from its wire form.
    ///
    /// An unknown state reads as `Active`. An unparseable address keeps the
    /// unspecified address, so the entry stays visible but unroutable. A pin
    /// that is not 32 bytes is dropped.
    pub fn from_wire(node: &JoinNodeInfo) -> Self {
        let state = NodeState::from_u8(node.state).unwrap_or(NodeState::Active);
        let spki_pin: Option<[u8; 32]> = node
            .spki_pin
            .as_deref()
            .and_then(|b| <[u8; 32]>::try_from(b).ok());
        let addr = node
            .addr
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let mut info = NodeInfo::new(node.node_id, addr, state)
            .with_wire_version(node.wire_version)
            .with_spiffe_id(node.spiffe_id.clone())
            .with_spki_pin(spki_pin);
        info.raft_groups = node.raft_groups.clone();
        info.swim_addr = node.swim_addr.clone();
        info
    }
}

/// Cluster topology — authoritative registry of all nodes.
///
/// Updated atomically via Raft (metadata group) in production.
/// Persisted to the cluster catalog (redb).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ClusterTopology {
    nodes: HashMap<u64, NodeInfo>,
    /// Monotonically increasing version for stale detection.
    version: u64,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            version: 0,
        }
    }

    /// Add or update a node. Bumps the version.
    pub fn add_node(&mut self, info: NodeInfo) {
        self.nodes.insert(info.node_id, info);
        self.version += 1;
    }

    /// Remove a node. Bumps the version.
    pub fn remove_node(&mut self, node_id: u64) -> Option<NodeInfo> {
        let removed = self.nodes.remove(&node_id);
        if removed.is_some() {
            self.version += 1;
        }
        removed
    }

    pub fn get_node(&self, node_id: u64) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    pub fn get_node_mut(&mut self, node_id: u64) -> Option<&mut NodeInfo> {
        self.nodes.get_mut(&node_id)
    }

    /// Update a node's state. Bumps the version.
    pub fn set_state(&mut self, node_id: u64, state: NodeState) -> bool {
        if let Some(info) = self.nodes.get_mut(&node_id) {
            info.state = state;
            self.version += 1;
            true
        } else {
            false
        }
    }

    /// All nodes with Active state.
    pub fn active_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| n.state == NodeState::Active)
            .collect()
    }

    /// All nodes (any state).
    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    /// Take the version of a topology adopted wholesale from a peer, so the
    /// next version comparison sees the two as equal.
    pub(crate) fn adopt_version(&mut self, version: u64) {
        self.version = version;
    }

    pub fn contains(&self, node_id: u64) -> bool {
        self.nodes.contains_key(&node_id)
    }

    /// Add a new node as a non-voting learner.
    ///
    /// The node receives Raft log entries and catches up with the cluster
    /// state, but doesn't participate in elections. Once caught up and
    /// validated, call `promote_to_voter()`.
    pub fn join_as_learner(&mut self, info: NodeInfo) -> bool {
        if self.nodes.contains_key(&info.node_id) {
            return false; // Already exists.
        }
        let mut learner = info;
        learner.state = NodeState::Learner;
        self.nodes.insert(learner.node_id, learner);
        self.version += 1;
        true
    }

    /// Promote a learner node to a full voting member.
    ///
    /// Only valid for nodes in `Learner` state. After promotion, the node
    /// participates in Raft elections and counts toward quorum.
    pub fn promote_to_voter(&mut self, node_id: u64) -> bool {
        if let Some(info) = self.nodes.get_mut(&node_id)
            && info.state == NodeState::Learner
        {
            info.state = NodeState::Active;
            self.version += 1;
            return true;
        }
        false
    }

    /// All learner nodes (catching up, not yet voting).
    pub fn learner_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| n.state == NodeState::Learner)
            .collect()
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_lookup() {
        let mut topo = ClusterTopology::new();
        topo.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo.add_node(NodeInfo::new(
            2,
            "127.0.0.1:9401".parse().unwrap(),
            NodeState::Joining,
        ));

        assert_eq!(topo.node_count(), 2);
        assert_eq!(topo.version(), 2);
        assert_eq!(topo.active_nodes().len(), 1);
        assert!(topo.contains(1));
        assert!(topo.contains(2));
    }

    #[test]
    fn remove_node() {
        let mut topo = ClusterTopology::new();
        topo.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        let removed = topo.remove_node(1);
        assert!(removed.is_some());
        assert_eq!(topo.node_count(), 0);
        assert_eq!(topo.version(), 2); // add + remove
    }

    #[test]
    fn set_state() {
        let mut topo = ClusterTopology::new();
        topo.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Joining,
        ));
        assert!(topo.set_state(1, NodeState::Active));
        assert_eq!(topo.get_node(1).unwrap().state, NodeState::Active);
    }

    #[test]
    fn node_state_roundtrip() {
        for state in [
            NodeState::Joining,
            NodeState::Active,
            NodeState::Draining,
            NodeState::Decommissioned,
        ] {
            assert_eq!(NodeState::from_u8(state.as_u8()), Some(state));
        }
        assert_eq!(NodeState::from_u8(255), None);
    }

    #[test]
    fn serde_roundtrip() {
        let mut topo = ClusterTopology::new();
        topo.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        topo.add_node(NodeInfo::new(
            2,
            "127.0.0.1:9401".parse().unwrap(),
            NodeState::Active,
        ));

        let bytes = zerompk::to_msgpack_vec(&topo).unwrap();
        let decoded: ClusterTopology = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.node_count(), 2);
        assert_eq!(decoded.version(), 2);
    }
}
