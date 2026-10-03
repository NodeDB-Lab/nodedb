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
//! `GRAPH PATH` (`shortest_path`), BFS and subgraph traversal all run each
//! hop through that owner-targeted expansion.

pub mod bfs;
pub mod bsp_pagerank;
pub mod bsp_wcc;
pub(crate) mod cluster_resolve;
pub mod helpers;
pub(crate) mod hop;
pub mod keyed_read;
pub mod match_broadcast;
pub mod match_scatter;
pub(crate) mod neighbor_rows;
pub(crate) mod presence;
pub mod rag_fusion;
pub mod read_groups;
pub(crate) mod run_cut;
pub(crate) mod shard_reads;
pub mod shortest_path;
pub(crate) mod subgraph_edges;
pub mod traverse_subgraph;
pub mod walk_reads;
pub mod whole_graph;

pub use bfs::{CrossCoreBfsParams, cross_core_bfs_with_options};
pub use bsp_pagerank::run_bsp_pagerank;
pub use bsp_wcc::run_bsp_wcc;
pub use keyed_read::{VShardRead, read_on_key_owner, read_on_vshard};
pub use match_broadcast::{
    GraphRead, MatchBroadcastOutcome, broadcast_match_to_all_cores, unwrap_match_envelope,
};
pub use match_scatter::{MatchScatterOutcome, scatter_match};
pub use rag_fusion::serve_rag_plan;
pub use read_groups::{confirm_graph_read, graph_read_groups};
pub use shortest_path::{CrossCoreShortestPathParams, cross_core_shortest_path};
pub use traverse_subgraph::{CrossCoreTraverseSubgraphParams, cross_core_traverse_subgraph};
pub use walk_reads::serve_walk_plan;
pub use whole_graph::{run_graph_algo, scatter_to_graph_owners};
