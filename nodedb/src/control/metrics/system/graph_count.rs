// SPDX-License-Identifier: BUSL-1.1

//! One core's share of the graph node and edge gauges.

/// The graph nodes and edges one Data Plane core reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphCount {
    /// Nodes with at least one live edge whose home vShard the node hosts
    /// and routes to the core.
    pub nodes: u64,
    /// Live edges whose source home vShard the node hosts and routes to the
    /// core.
    pub edges: u64,
}
