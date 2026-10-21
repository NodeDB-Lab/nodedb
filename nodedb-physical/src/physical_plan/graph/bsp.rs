// SPDX-License-Identifier: Apache-2.0

//! Boxed payload/result pair for [`super::op::GraphOp::BspSuperstep`] — the
//! distributed-PageRank BSP superstep primitive.

use nodedb_graph::{AlgoParams, GraphAlgorithm};

/// Boxed payload of [`super::op::GraphOp::BspSuperstep`] — all per-superstep inputs.
///
/// Kept out-of-line (the variant holds a `Box`) so the large param + vector
/// fields don't bloat `PhysicalPlan`, which is cloned/moved on every request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BspSuperstepPlan {
    /// Algorithm selector. Only `PageRank` has a BSP form. Every other
    /// variant surfaces a typed `Unsupported` error from the handler.
    pub algorithm: GraphAlgorithm,
    /// Algorithm parameters. Carries the target `collection` (mirroring `Algo`)
    /// plus `damping`.
    pub params: AlgoParams,
    /// Zero-based superstep index.
    ///
    /// - `0` sets the initial rank (`1/global_n`, or the seed share under
    ///   personalization) and scatters it. It applies no contribution.
    /// - `>= 1` takes the rank from `rank_seed`, computes the next rank from
    ///   it and `incoming_contributions`, and scatters the next rank.
    pub superstep: u32,
    /// Total OWNED nodes across all shards (Control-Plane computed). Used as the
    /// PageRank `n` in the teleport / dangling redistribution terms.
    ///
    /// `global_n == 0` is the COUNT-ONLY sentinel: the coordinator dispatches one
    /// superstep with `global_n = 0` (and empty `rank_seed` / `incoming_contributions`)
    /// to every shard BEFORE superstep 0 so it can sum each shard's owned
    /// `vertex_count` into the real `global_n`. On that sentinel the handler
    /// short-circuits after building the owned-node set and runs NO superstep —
    /// it returns only `vertex_count` + `node_names`. Every real superstep
    /// passes `global_n > 0`.
    pub global_n: usize,
    /// The vShards this shard owns (Control-Plane supplied). A destination node
    /// whose `VShardId::from_key(name)` is not in this set is a ghost
    /// (cross-shard) edge target and its contribution is emitted in `outbound`
    /// rather than scattered locally.
    pub owned_vshards: Vec<u32>,
    /// Cross-shard contributions routed to this shard's owned nodes:
    /// `(dst_node_name, contribution)`. Every shard scattered them from the
    /// rank in `rank_seed`, in the previous superstep. Empty on superstep 0.
    /// A contribution to a node this shard does not own is an error.
    pub incoming_contributions: Vec<(String, f64)>,
    /// The current rank as `(node_name, rank)` pairs: the rank the previous
    /// superstep returned. Name-keyed, so the same plan fans across a node's
    /// cores and each core picks its owned nodes by name. Empty on superstep
    /// 0. From superstep 1, an owned node absent from it is an error.
    pub rank_seed: Vec<(String, f64)>,
    /// The dangling-node mass of the rank in `rank_seed`, summed by the
    /// coordinator over every shard's previous `dangling_sum`. It feeds the
    /// redistributed base, so dangling mass spreads over the whole graph.
    /// `0.0` on superstep 0 and the count phase.
    pub global_dangling: f64,
    /// Coordinator-computed GLOBAL `Σ max(w, 0.0)` over the Personalized-PageRank
    /// seed map (`params.personalization_vector`), summed across the WHOLE cluster.
    ///
    /// `0.0` means standard (uniform) PageRank — no personalization is active,
    /// either because no seed map was supplied, the summed weight was ≤ 0, or no
    /// seed name exists anywhere in the cluster graph (matching single-node
    /// `build_personalization` returning `None`). A value `> 0.0` activates
    /// Personalized PageRank on every shard.
    ///
    /// Each shard divides its OWNED nodes' raw seed weights by this GLOBAL sum to
    /// get a globally-normalized seed share `p_i` (`Σ_global p_i == 1.0`). Both the
    /// teleport mass and the dangling mass then redistribute by `p` instead of
    /// uniformly. Normalizing by the cluster-wide sum (never a per-shard sum) is
    /// what preserves the mass-conservation invariant across shards.
    pub personalization_sum: f64,
    /// The watermark of the Calvin cut marker the run's read cut comes
    /// from, `0` when `system_as_of` already holds the cut.
    ///
    /// Every superstep of a run reads the same graph, on every node and
    /// core. The run's first dispatch (the count phase) carries the marker.
    /// Each node proposes it, waits until it applied and every Calvin
    /// scheduler the node runs passed it, and reads at the marker's epoch
    /// instant. Every edge version sequenced before the marker is then
    /// installed and at or below that cut. Every version sequenced after it
    /// is above the cut. A historical run carries its statement's pinned
    /// ordinal in `system_as_of` with the marker: each node waits for the
    /// marker, folds the ordinal into its ordinal clock, and reads at the
    /// ordinal unchanged. The count phase answers the cut in
    /// `BspSuperstepResult::system_as_of`, and every later dispatch carries
    /// it as `system_as_of`.
    pub read_cut_marker: u64,
    /// The system-time ordinal every core reads the graph at. The handler
    /// refuses a plan without one.
    pub system_as_of: Option<i64>,
    /// The valid-time instant every edge read must contain (`AS OF VALID
    /// TIME`), or `None` for every valid time.
    pub valid_at_ms: Option<i64>,
}

/// Result of one [`super::op::GraphOp::BspSuperstep`] on a single shard.
///
/// `rank_vec` and `node_names` are positionally aligned: `rank_vec[i]` is the
/// post-superstep PageRank of the owned node `node_names[i]`. The Control-Plane
/// coordinator zips them into the next superstep's `rank_seed` and uses
/// `node_names` for final assembly. `node_names` is returned on every
/// superstep, which keeps the op stateless.
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
pub struct BspSuperstepResult {
    /// Sum of `|rank_old - rank_new|` over this shard's owned nodes — the
    /// shard's contribution to the global convergence delta. `0.0` on
    /// superstep 0, which changes no rank.
    pub local_delta: f64,
    /// Cross-shard contributions scattered from the returned `rank_vec`, for
    /// the next superstep: `(target_vshard, dst_node_name, contribution)`.
    pub outbound: Vec<(u32, String, f64)>,
    /// Post-superstep rank vector over this shard's owned nodes, aligned with
    /// `node_names`.
    pub rank_vec: Vec<f64>,
    /// Number of owned nodes on this shard (== `rank_vec.len()`).
    pub vertex_count: usize,
    /// Owned-node names, positionally aligned with `rank_vec`.
    pub node_names: Vec<String>,
    /// The dangling-node mass of the returned `rank_vec`: the rank sum of
    /// every owned node with out-degree 0. The coordinator sums these across
    /// shards into the next superstep's `global_dangling`.
    pub dangling_sum: f64,
    /// Number of this shard's OWNED nodes that appear as a positively-weighted key
    /// in the Personalized-PageRank seed map (`params.personalization_vector`),
    /// reported by the COUNT phase (alongside `vertex_count`). The coordinator sums
    /// these across shards: a cluster-wide total of `0` means no seed name exists
    /// anywhere in the graph, so personalization falls back to uniform PageRank
    /// (matching single-node `build_personalization` returning `None`). `0` on
    /// every real superstep (only the count phase populates it).
    pub seed_hits: usize,
    /// The system-time ordinal the node read the graph at.
    pub system_as_of: Option<i64>,
}
