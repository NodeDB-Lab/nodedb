// SPDX-License-Identifier: BUSL-1.1

//! Graph expansion for cross-engine fusion, in the surrogate domain.
//!
//! Fusion legs meet on identity, and every engine's identity is the global
//! `Surrogate`. The expansion therefore runs surrogate-in / surrogate-out:
//! vector and FTS hits seed it directly, and the walk itself never touches the
//! node-name table. Names are resolved exactly once, at the end, over the
//! reached set — the rows that can actually appear in a response — instead of
//! once per visited node and once per traversed edge.
//!
//! A query that names its own seed node (`... GRAPH SEED 'alice'`) enters
//! through [`GraphSeeds::Names`], which resolves those names to surrogates once
//! and then joins the same path.

use std::collections::HashMap;

use nodedb_graph::{SurrogateBfsParams, SurrogateHops};
use nodedb_types::Surrogate;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::edge_store::Direction;

/// Where an expansion starts.
pub(in crate::data::executor) enum GraphSeeds<'a> {
    /// Seeds that already carry global identity — vector or FTS hits. No
    /// translation happens at all on this path.
    Surrogates(&'a [Surrogate]),
    /// Seeds named by the query itself. Resolved to surrogates once, here.
    Names(&'a [&'a str]),
}

/// Bundled arguments for [`CoreLoop::expand_graph`].
pub(in crate::data::executor) struct GraphExpansionParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub seeds: GraphSeeds<'a>,
    pub label_filter: Option<&'a str>,
    pub direction: Direction,
    pub max_depth: usize,
    pub max_visited: usize,
    pub collection: &'a str,
}

/// Reached nodes, in both currencies.
///
/// `reached` is the surrogate-bound subset with hop distances — the form that
/// fuses with another engine's candidates on the storage key. `names` /
/// `distances` are every reached node resolved by name, for ranking by name
/// and for the response. Both come from a single pass at the end.
pub(in crate::data::executor) struct GraphExpansion {
    pub names: Vec<String>,
    pub distances: HashMap<String, usize>,
    /// `(surrogate, hop distance)` for each reached node that carries a
    /// surrogate. Nodes counted in `unaddressable` are absent here.
    pub reached: Vec<(Surrogate, usize)>,
    pub truncated: bool,
    /// Reached nodes that carry no surrogate, so they are traversed *through*
    /// but can never intersect another engine's candidates. Carried to the
    /// response rather than only logged: a caller comparing a graph-expanded
    /// answer against a vector-only one otherwise sees a smaller graph with no
    /// indication that part of it was unaddressable.
    pub unaddressable: usize,
}

impl CoreLoop {
    /// Expand the graph from `seeds` within one collection.
    ///
    /// Replaces the former name-keyed BFS: that one allocated a `String` per
    /// visited node *and* per traversed edge, on top of translating every
    /// vector hit's surrogate into a name just to look the same node back up.
    pub(in crate::data::executor) fn expand_graph(
        &self,
        params: GraphExpansionParams<'_>,
    ) -> GraphExpansion {
        let GraphExpansionParams {
            database_id,
            tid,
            seeds,
            label_filter,
            direction,
            max_depth,
            max_visited,
            collection,
        } = params;

        let budget_node_limit =
            self.query_tuning.bfs_memory_budget_bytes / self.query_tuning.bfs_bytes_per_node;
        let effective_limit = max_visited.min(budget_node_limit);

        let Some(partition) = self.csr_partition(database_id, tid) else {
            return GraphExpansion {
                names: Vec::new(),
                distances: HashMap::new(),
                reached: Vec::new(),
                truncated: false,
                unaddressable: 0,
            };
        };

        // Both seed kinds resolve to CSR-local ids once, here. A name resolves
        // through the node table, which durable storage rebuilds on every open;
        // a surrogate resolves through the surrogate table, which currently does
        // not survive a rebuild (see `expansion_seeds_are_resolvable`). Routing
        // names via surrogates would inherit that gap for no reason.
        let local_seeds: Vec<u32> = match seeds {
            GraphSeeds::Surrogates(s) => s
                .iter()
                .filter_map(|&s| partition.local_id_for_surrogate(s))
                .collect(),
            GraphSeeds::Names(names) => names
                .iter()
                .filter_map(|n| partition.local_id_for_node(n))
                .collect(),
        };

        let hops = partition.traverse_surrogates_in_collection(SurrogateBfsParams {
            seeds: &local_seeds,
            label_filter,
            direction,
            max_depth,
            max_visited: effective_limit,
            collection,
        });

        if hops.truncated {
            warn!(
                core = self.core_id,
                reached = hops.reached.len(),
                limit = effective_limit,
                budget_limit = budget_node_limit,
                max_visited,
                "graph expansion truncated: memory budget or max_visited reached"
            );
        }
        // A node with no surrogate is traversed *through* but cannot be named
        // in a cross-engine answer. Silence here would look like a smaller
        // graph rather than an unaddressable one.
        if hops.unaddressable > 0 {
            warn!(
                core = self.core_id,
                unaddressable = hops.unaddressable,
                %collection,
                "graph expansion reached nodes with no surrogate; they are \
                 traversed but cannot be fused"
            );
        }

        self.resolve_reached(partition, &hops)
    }

    /// The single name resolution: one lookup per reached node, at the end.
    fn resolve_reached(
        &self,
        partition: &crate::engine::graph::csr::CsrIndex,
        hops: &SurrogateHops,
    ) -> GraphExpansion {
        let mut names = Vec::with_capacity(hops.distances.len());
        let mut distances = HashMap::with_capacity(hops.distances.len());
        let mut reached = Vec::with_capacity(hops.reached.len() as usize);
        for &(local, depth) in &hops.distances {
            // Local ids come from this partition's own walk, so the name lookup
            // is total; skipping rather than unwrapping keeps a torn index from
            // panicking a core.
            if let Some(name) = partition.node_name_checked(local) {
                names.push(name.to_string());
                distances.insert(name.to_string(), depth);
            }
            // `node_surrogate_raw` yields the ZERO sentinel for an unbound node.
            let raw = partition.node_surrogate_raw(local);
            if raw != 0 {
                reached.push((Surrogate::new(raw), depth));
            }
        }
        GraphExpansion {
            names,
            distances,
            reached,
            truncated: hops.truncated,
            unaddressable: hops.unaddressable,
        }
    }
}
