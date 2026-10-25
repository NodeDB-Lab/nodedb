// SPDX-License-Identifier: Apache-2.0

//! Boxed payload/result pair for [`super::op::GraphOp::WccSuperstep`] — the
//! distributed-WCC single-round contraction primitive.

use nodedb_graph::AlgoParams;

/// Boxed payload of [`super::op::GraphOp::WccSuperstep`] — the inputs for one shard's
/// single WCC contraction round.
///
/// Unlike PageRank's BSP plan this carries NO round-tripped state: WCC is a
/// single-round contraction, so the coordinator dispatches this once per owner
/// node and stitches the returned [`WccSuperstepResult`]s globally.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WccSuperstepPlan {
    /// Algorithm parameters. Carries the target `collection` (mirroring `Algo`)
    /// plus the optional `edge_label` scoping the subgraph.
    pub params: AlgoParams,
    /// The vShards this shard owns (Control-Plane supplied). A destination node
    /// whose `VShardId::from_key(name)` is not in this set is a ghost
    /// (cross-shard) edge target and the edge is recorded as a boundary edge
    /// rather than unioned locally.
    pub owned_vshards: Vec<u32>,
    /// The watermark of the Calvin cut marker the read cut comes from, `0`
    /// when `system_as_of` already holds it. Every node resolves the same
    /// cut from it, or waits for it under a historical round's pinned
    /// ordinal (see `BspSuperstepPlan::read_cut_marker`), so every node
    /// reads the same graph.
    pub read_cut_marker: u64,
    /// The system-time ordinal every core reads the graph at. The handler
    /// refuses a plan without one.
    pub system_as_of: Option<i64>,
    /// The valid-time instant every edge read must contain (`AS OF VALID
    /// TIME`), or `None` for every valid time.
    pub valid_at_ms: Option<i64>,
}

/// Result of one [`super::op::GraphOp::WccSuperstep`] on a single shard.
///
/// `node_labels` maps every OWNED node name to the lexicographically-minimum
/// owned node name in its local component (the local component root). Combined
/// with `boundary_edges` (owned→ghost edges as `(owned_name, ghost_name)`), the
/// coordinator builds one global union-find over node names: it unions each
/// `(name, local_root)` and each boundary edge, then assigns dense component
/// ids ordered by each component's minimum node name.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WccSuperstepResult {
    /// `(node_name, local_component_root_name)` for every owned node — the
    /// local-component seed unioned into the global union-find by the coordinator.
    pub node_labels: Vec<(String, String)>,
    /// `(owned_name, ghost_name)` for every out-edge whose destination is NOT
    /// owned by this shard — the cross-shard edges the coordinator unions to
    /// stitch components across shard boundaries.
    pub boundary_edges: Vec<(String, String)>,
    /// Number of owned nodes on this shard (== `node_labels.len()`).
    pub vertex_count: usize,
    /// The system-time ordinal the node read the graph at.
    pub system_as_of: Option<i64>,
}
