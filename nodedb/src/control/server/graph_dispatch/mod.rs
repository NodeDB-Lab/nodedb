// SPDX-License-Identifier: BUSL-1.1

//! Cross-core BFS and shortest-path orchestration for graph traversal.
//!
//! In single-node mode, BFS is local: the Control Plane broadcasts
//! `GraphNeighbors` to all Data Plane cores hop by hop and collects results.
//!
//! In cluster mode, each hop partitions the incoming frontier by owning
//! vShard (resolved against live Raft leadership) BEFORE expansion: the
//! locally-owned subset expands on local cores, and each remote-owned subset
//! is shipped as a typed `NeighborsMulti` plan to its owning node via the
//! gateway's `dispatch_route` primitive. Local and remote `{src,label,node}`
//! responses decode identically and are merged before the next depth level
//! begins. See `hop::execute_neighbor_hop`.
//!
//! `GRAPH PATH` (`shortest_path`) still uses the post-expansion
//! `control::scatter_gather::coordinate_cross_shard_hop` scatter path; the
//! per-frontier owner-targeted expansion above is specific to the BFS /
//! subgraph traversal read path.

pub mod bfs;
pub mod bsp_pagerank;
pub mod bsp_wcc;
pub(crate) mod cluster_resolve;
pub mod helpers;
pub(crate) mod hop;
pub mod match_broadcast;
pub mod match_scatter;
pub mod shortest_path;
mod subgraph_accumulator;
pub mod traverse_subgraph;

pub use bfs::{CrossCoreBfsParams, cross_core_bfs_with_options};
pub use bsp_pagerank::run_bsp_pagerank;
pub use bsp_wcc::run_bsp_wcc;
pub use match_broadcast::{
    MatchBroadcastOutcome, broadcast_match_to_all_cores, unwrap_match_envelope,
};
pub use match_scatter::{MatchScatterOutcome, scatter_match};
pub use shortest_path::{CrossCoreShortestPathParams, cross_core_shortest_path};
pub use traverse_subgraph::{CrossCoreTraverseSubgraphParams, cross_core_traverse_subgraph};
