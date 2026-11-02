// SPDX-License-Identifier: BUSL-1.1

//! Cluster-wide version view derived on demand from the live
//! `ClusterTopology` — no shadow state, no seeded-once field.
//!
//! `SharedState::cluster_version_view()` wraps a call to
//! [`compute_from_topology`] reading `cluster_topology` under a
//! short read guard. Every call is authoritative: adds, removes,
//! and wire-version updates are observed immediately because the
//! topology `RwLock` is the single source of truth.

use nodedb_cluster::{ClusterTopology, NodeInfo};

use crate::version::WIRE_FORMAT_VERSION;

/// Snapshot of the cluster's version distribution at a single
/// instant. Cheap to construct (~one pass over `all_nodes()`),
/// so callers recompute rather than cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterVersionView {
    /// Minimum wire version observed across every known node.
    /// Defaults to the local `WIRE_FORMAT_VERSION` when the
    /// topology is empty — this is the correct behavior for a
    /// fresh single-node bootstrap that has not yet accepted any
    /// peer.
    pub min_version: u16,
    /// Maximum wire version observed across every known node.
    pub max_version: u16,
    /// Number of nodes the view was computed from.
    pub node_count: usize,
    /// Per-node `(node_id, wire_version)` pairs in topology order.
    pub node_versions: Vec<(u64, u16)>,
}

impl ClusterVersionView {
    /// View used when no topology is available — single-node
    /// bootstrap or tests without a cluster handle. Reports the
    /// local build's version as both min and max.
    pub fn single_node() -> Self {
        Self {
            min_version: WIRE_FORMAT_VERSION,
            max_version: WIRE_FORMAT_VERSION,
            node_count: 0,
            node_versions: Vec::new(),
        }
    }

    /// Whether the cluster is in mixed-version mode.
    pub fn is_mixed_version(&self) -> bool {
        self.min_version != self.max_version
    }

    /// Whether every node has caught up to the local build.
    pub fn all_upgraded(&self) -> bool {
        self.min_version == WIRE_FORMAT_VERSION
    }

    /// Whether a feature gated on `required_version` can activate.
    /// Returns `true` only when every node reports at least
    /// `required_version`.
    pub fn can_activate_feature(&self, required_version: u16) -> bool {
        self.min_version >= required_version
    }

    /// Version spread. Must be ≤ 1 for supported operation (N-1
    /// only).
    pub fn version_spread(&self) -> u16 {
        self.max_version.saturating_sub(self.min_version)
    }

    /// Whether the version spread is within supported bounds.
    pub fn is_supported_spread(&self) -> bool {
        self.version_spread() <= 1
    }
}

/// Derive a [`ClusterVersionView`] from a live topology snapshot.
/// Caller holds the `ClusterTopology` read guard; this function
/// never blocks and never allocates beyond the `Vec` in the
/// returned view.
pub fn compute_from_topology(topology: &ClusterTopology) -> ClusterVersionView {
    let mut min_version = u16::MAX;
    let mut max_version = 0u16;
    let mut node_versions = Vec::with_capacity(topology.node_count());
    let mut node_count = 0usize;

    for node in topology.all_nodes() {
        let v = node_wire_version(node);
        node_count += 1;
        if v < min_version {
            min_version = v;
        }
        if v > max_version {
            max_version = v;
        }
        node_versions.push((node.node_id, v));
    }

    if node_count == 0 {
        // Empty topology → treat as a single-node view reporting
        // the local build. `recalc` paths would otherwise leave
        // `min = u16::MAX` and `max = 0`, which would poison every
        // feature gate.
        return ClusterVersionView::single_node();
    }

    ClusterVersionView {
        min_version,
        max_version,
        node_count,
        node_versions,
    }
}

fn node_wire_version(node: &NodeInfo) -> u16 {
    node.wire_version
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::{NodeInfo, NodeState};

    fn node(id: u64, v: u16) -> NodeInfo {
        NodeInfo::new(
            id,
            format!("10.0.0.{id}:9400").parse().unwrap(),
            NodeState::Active,
        )
        .with_wire_version(v)
    }

    #[test]
    fn empty_topology_falls_back_to_local() {
        let topo = ClusterTopology::new();
        let v = compute_from_topology(&topo);
        assert_eq!(v.node_count, 0);
        assert_eq!(v.min_version, WIRE_FORMAT_VERSION);
        assert_eq!(v.max_version, WIRE_FORMAT_VERSION);
        assert!(!v.is_mixed_version());
        assert!(v.all_upgraded());
    }

    #[test]
    fn single_version_cluster() {
        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, WIRE_FORMAT_VERSION));
        topo.add_node(node(2, WIRE_FORMAT_VERSION));
        topo.add_node(node(3, WIRE_FORMAT_VERSION));
        let v = compute_from_topology(&topo);
        assert_eq!(v.node_count, 3);
        assert!(!v.is_mixed_version());
        assert!(v.all_upgraded());
        assert!(v.is_supported_spread());
    }

    #[test]
    fn mixed_version_n_minus_1() {
        if WIRE_FORMAT_VERSION < 2 {
            return;
        }
        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, WIRE_FORMAT_VERSION));
        topo.add_node(node(2, WIRE_FORMAT_VERSION));
        topo.add_node(node(3, WIRE_FORMAT_VERSION - 1));
        let v = compute_from_topology(&topo);
        assert!(v.is_mixed_version());
        assert!(!v.all_upgraded());
        assert!(v.is_supported_spread());
        assert_eq!(v.min_version, WIRE_FORMAT_VERSION - 1);
    }

    #[test]
    fn unsupported_spread_detected() {
        if WIRE_FORMAT_VERSION < 3 {
            return;
        }
        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, WIRE_FORMAT_VERSION));
        topo.add_node(node(2, WIRE_FORMAT_VERSION - 2));
        let v = compute_from_topology(&topo);
        assert_eq!(v.version_spread(), 2);
        assert!(!v.is_supported_spread());
    }

    #[test]
    fn feature_gated_on_min_version() {
        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, 2));
        topo.add_node(node(2, 2));
        topo.add_node(node(3, 1));
        let v = compute_from_topology(&topo);
        assert!(!v.can_activate_feature(2));

        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, 2));
        topo.add_node(node(2, 2));
        topo.add_node(node(3, 2));
        let v = compute_from_topology(&topo);
        assert!(v.can_activate_feature(2));
    }

    #[test]
    fn node_removal_recomputes() {
        let mut topo = ClusterTopology::new();
        topo.add_node(node(1, 2));
        topo.add_node(node(2, 1));
        let v = compute_from_topology(&topo);
        assert!(v.is_mixed_version());

        topo.remove_node(2);
        let v = compute_from_topology(&topo);
        assert!(!v.is_mixed_version());
        assert_eq!(v.min_version, 2);
    }
}
