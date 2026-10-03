// SPDX-License-Identifier: Apache-2.0

//! Surrogate-keyed, collection-scoped BFS.
//!
//! The string-keyed traversals materialize a node name for every visited node —
//! and, through `neighbors_in_collection`, one for every *edge* they walk. That
//! is the wrong currency for cross-engine work. Vector, FTS and spatial all
//! produce surrogates, so a graph result keyed on names has to be translated
//! back before it can meet them, which is exactly the per-query translation the
//! global surrogate exists to remove.
//!
//! This walk stays in the surrogate domain from end to end: it seeds from
//! surrogates, expands over the dense `u32` CSR ids, and returns a
//! [`SurrogateBitmap`] plus per-surrogate hop distances. The bitmap intersects
//! directly with any other engine's. Node names are resolved once, by the
//! caller, and only for the rows that survive fusion.

use std::collections::HashSet;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::csr::{CsrIndex, Direction};

/// Inputs to [`CsrIndex::traverse_surrogates_in_collection`].
pub struct SurrogateBfsParams<'a> {
    /// Seed nodes as CSR-local ids. Callers holding surrogates convert with
    /// [`CsrIndex::local_id_for_surrogate`]; callers holding names use
    /// [`CsrIndex::local_id_for_node`]. Taking local ids keeps the walk
    /// independent of which currency the caller happens to have, and keeps a
    /// name-seeded walk working on an index whose surrogate bindings have not
    /// been populated.
    pub seeds: &'a [u32],
    /// Edge labels to expand over. Empty keeps every edge. Otherwise an edge
    /// whose label is any listed label passes.
    pub label_filter: &'a [&'a str],
    pub direction: Direction,
    /// Hops from the seeds. `0` returns just the addressable seeds.
    pub max_depth: usize,
    /// Cap on visited nodes. Reaching it sets [`SurrogateHops::truncated`].
    pub max_visited: usize,
    /// Only edges inserted under this collection are traversed. A collection
    /// with no edges here yields an empty result and never falls back to the
    /// merged, cross-collection view.
    pub collection: &'a str,
}

/// Outcome of a surrogate-keyed BFS.
///
/// The two result fields answer different questions and must not be conflated.
/// `distances` is the *traversal* — every node the walk reached, always
/// complete. `reached` is the *intersectable* subset: only nodes that carry a
/// surrogate can meet another engine's candidate set. Gating `distances` on the
/// surrogate binding would silently shrink a graph answer whenever bindings are
/// missing, which is a storage-state question and has nothing to do with what
/// the graph actually contains.
pub struct SurrogateHops {
    /// Reached nodes that carry a surrogate, ready to intersect with another
    /// engine's candidate set. A subset of `distances`.
    pub reached: SurrogateBitmap,
    /// `(CSR-local node id, hop distance from the nearest seed)` in discovery
    /// order — the complete reached set, so callers that rank by proximity need
    /// no second pass over the graph. Local ids resolve to names through the
    /// same index, which is why this stays complete even with no surrogates
    /// bound at all.
    pub distances: Vec<(u32, usize)>,
    /// `max_visited` cut the walk short — the result is a prefix of the
    /// reachable set, not the whole of it.
    pub truncated: bool,
    /// Reached nodes carrying no surrogate. They are traversed and reported in
    /// `distances` as normal; what they cannot do is appear in `reached`, since
    /// a node with no global identity has nothing to intersect against.
    /// Non-zero means cross-engine fusion sees less than the traversal did, and
    /// callers surface it rather than quietly narrowing the answer.
    pub unaddressable: usize,
}

impl SurrogateHops {
    fn empty() -> Self {
        Self {
            reached: SurrogateBitmap::new(),
            distances: Vec::new(),
            truncated: false,
            unaddressable: 0,
        }
    }

    /// Record a newly reached node in the traversal, and in the intersectable
    /// set when it carries a surrogate.
    fn record(&mut self, csr: &CsrIndex, local: u32, depth: usize) {
        self.distances.push((local, depth));
        let raw = csr.node_surrogate_raw(local);
        if raw == 0 {
            self.unaddressable += 1;
        } else {
            self.reached.insert(Surrogate::new(raw));
        }
    }
}

impl CsrIndex {
    /// BFS from surrogate seeds over one collection's edges, returning
    /// surrogates and hop distances.
    ///
    /// The frontier is the dense `u32` CSR id, exactly as in the durable
    /// string-keyed path; the difference is that nothing is ever converted to a
    /// name. Nodes without a surrogate stay traversable — they just cannot be
    /// reported (see [`SurrogateHops::unaddressable`]).
    ///
    /// The walk admits nodes in `(depth, node name)` order: each level's new
    /// nodes in name order. When `max_visited` cuts the walk, the admitted set
    /// is the same however the edges are stored, which lets a cluster
    /// coordinator walking the same edges across partitions admit the same
    /// nodes.
    pub fn traverse_surrogates_in_collection(
        &self,
        params: SurrogateBfsParams<'_>,
    ) -> SurrogateHops {
        let SurrogateBfsParams {
            seeds,
            label_filter,
            direction,
            max_depth,
            max_visited,
            collection,
        } = params;

        let mut hops = SurrogateHops::empty();
        let Some(collection_id) = self.collection_id(collection) else {
            return hops;
        };
        // A set of unknown labels matches no edge. Falling through to
        // unfiltered expansion would return other labels' neighbourhoods under
        // the caller's labels.
        let labels = self.label_filter(label_filter);
        if labels.keeps_none() {
            self.seed_only(seeds, &mut hops);
            return hops;
        }

        let mut visited: HashSet<u32> = HashSet::with_capacity(max_visited.min(1024));
        let mut frontier: Vec<u32> = Vec::new();
        for &local in seeds {
            if self.is_local_node(local) && visited.insert(local) {
                frontier.push(local);
            }
        }
        self.sort_by_name(&mut frontier);
        for &local in &frontier {
            hops.record(self, local, 0);
        }

        let want_out = matches!(direction, Direction::Out | Direction::Both);
        let want_in = matches!(direction, Direction::In | Direction::Both);

        // Level by level. Each level's new nodes are admitted in node-name
        // order, so under the visit cap the admitted set is the same whatever
        // order the edges are stored in, here or split across partitions.
        for depth in 0..max_depth {
            if frontier.is_empty() {
                break;
            }
            let mut candidates: Vec<u32> = Vec::new();
            let mut offered: HashSet<u32> = HashSet::new();
            for &node in &frontier {
                self.record_access(node);
                let mut neighbors: Vec<(u32, u32)> = Vec::new();
                if want_out {
                    neighbors.extend(self.iter_out_edges_raw_in(node, collection_id));
                }
                if want_in {
                    neighbors.extend(self.iter_in_edges_raw_in(node, collection_id));
                }
                for (lid, other) in neighbors {
                    if !labels.keeps(lid) || visited.contains(&other) || !offered.insert(other) {
                        continue;
                    }
                    candidates.push(other);
                }
            }
            self.sort_by_name(&mut candidates);
            let mut next: Vec<u32> = Vec::with_capacity(candidates.len());
            for other in candidates {
                if visited.len() >= max_visited {
                    hops.truncated = true;
                    return hops;
                }
                visited.insert(other);
                hops.record(self, other, depth + 1);
                self.prefetch_node(other);
                next.push(other);
            }
            frontier = next;
        }

        hops
    }

    /// Order CSR-local ids by node name.
    pub(crate) fn sort_by_name(&self, nodes: &mut [u32]) {
        nodes.sort_by(|a, b| self.node_name_checked(*a).cmp(&self.node_name_checked(*b)));
    }

    /// Record the addressable seeds and nothing else. Used when no requested
    /// edge label exists in this partition, so no expansion is possible
    /// but the seeds themselves are still legitimately reachable at depth 0.
    fn seed_only(&self, seeds: &[u32], hops: &mut SurrogateHops) {
        let mut seen: HashSet<u32> = HashSet::new();
        for &local in seeds {
            if !self.is_local_node(local) {
                continue;
            }
            if seen.insert(local) {
                hops.record(self, local, 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    /// `a -knows-> b -knows-> c` in `people`, plus `a -knows-> x` in `other`.
    /// Every node gets a surrogate except `x`, which deliberately has none.
    fn seeded_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        for (src, dst, coll) in [
            ("a", "b", "people"),
            ("b", "c", "people"),
            ("a", "x", "other"),
        ] {
            csr.add_edge_in_collection(src, "knows", dst, coll)
                .unwrap_or_else(|e| panic!("seed edge {src}->{dst} in {coll}: {e}"));
        }
        for (node, sur) in [("a", 10u32), ("b", 20), ("c", 30)] {
            csr.set_node_surrogate(node, Surrogate::new(sur));
        }
        csr
    }

    /// Seed by name, the way a name-seeded read does.
    fn local(csr: &CsrIndex, node: &str) -> u32 {
        csr.local_id_for_node(node)
            .unwrap_or_else(|| panic!("node {node} not interned"))
    }

    fn params<'a>(seeds: &'a [u32], collection: &'a str) -> SurrogateBfsParams<'a> {
        SurrogateBfsParams {
            seeds,
            label_filter: &[],
            direction: Direction::Out,
            max_depth: 5,
            max_visited: 1000,
            collection,
        }
    }

    #[test]
    fn walk_returns_surrogates_and_hop_distances() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "people"));

        assert!(hops.reached.contains(Surrogate::new(20)));
        assert!(hops.reached.contains(Surrogate::new(30)));
        let depth_of = |node: &str| {
            let id = local(&csr, node);
            hops.distances
                .iter()
                .find(|(l, _)| *l == id)
                .map(|(_, d)| *d)
        };
        assert_eq!(depth_of("a"), Some(0), "seed sits at depth 0");
        assert_eq!(depth_of("b"), Some(1));
        assert_eq!(depth_of("c"), Some(2));
    }

    /// A surrogate-holding caller enters through the same door, without ever
    /// materializing a name.
    #[test]
    fn a_surrogate_seed_resolves_to_the_same_walk() {
        let csr = seeded_csr();
        let seeds = [csr
            .local_id_for_surrogate(Surrogate::new(10))
            .expect("surrogate 10 is bound to `a`")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "people"));

        assert!(hops.reached.contains(Surrogate::new(20)));
        assert!(hops.reached.contains(Surrogate::new(30)));
    }

    /// The whole point of the surrogate domain: the result meets another
    /// engine's candidate set with an intersection and no name lookup.
    #[test]
    fn reached_set_intersects_another_engines_bitmap() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "people"));

        let mut vector_hits = SurrogateBitmap::new();
        vector_hits.insert(Surrogate::new(30));
        vector_hits.insert(Surrogate::new(99));

        let both = hops.reached.intersect(&vector_hits);
        assert_eq!(both.len(), 1);
        assert!(both.contains(Surrogate::new(30)));
    }

    /// Edges of another collection are not walked, even though both live in
    /// one partition under a shared node space.
    #[test]
    fn walk_does_not_cross_a_collection_boundary() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "people"));
        // `x` is only reachable through the `other` collection, and carries no
        // surrogate — so a leak would show up as an unaddressable node.
        assert_eq!(hops.unaddressable, 0, "crossed into another collection");
        assert!(
            !hops.distances.iter().any(|&(l, _)| l == local(&csr, "x")),
            "`x` belongs to the `other` collection"
        );
    }

    /// A node with no surrogate is still traversed and still reported by name —
    /// only its cross-engine addressability is missing, and that is counted.
    #[test]
    fn surrogateless_node_is_traversed_and_counted_not_dropped() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "other"));
        assert_eq!(hops.unaddressable, 1, "`x` has no surrogate to intersect");
        assert!(
            hops.distances.iter().any(|&(l, _)| l == local(&csr, "x")),
            "the traversal itself must stay complete"
        );
        assert!(!hops.reached.contains(Surrogate::new(0)));
    }

    #[test]
    fn seed_that_is_no_node_of_this_partition_is_skipped_not_fatal() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a"), 4242];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "people"));
        assert!(hops.reached.contains(Surrogate::new(10)));
        assert!(
            !hops.distances.iter().any(|&(l, _)| l == 4242),
            "an out-of-range seed must not enter the result"
        );
        assert_eq!(hops.unaddressable, 0, "it is absent, not unaddressable");
    }

    #[test]
    fn unknown_collection_yields_empty_not_the_merged_view() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let hops = csr.traverse_surrogates_in_collection(params(&seeds, "absent"));
        assert!(hops.reached.is_empty());
        assert!(hops.distances.is_empty());
    }

    /// An unknown label must not fall through to unfiltered expansion — that
    /// would answer with another label's neighbourhood.
    #[test]
    fn unknown_label_expands_nothing_but_keeps_the_seed() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.label_filter = &["never_inserted"];
        let hops = csr.traverse_surrogates_in_collection(p);
        assert!(hops.reached.contains(Surrogate::new(10)));
        assert!(!hops.reached.contains(Surrogate::new(20)));
    }

    /// `a -knows-> b -likes-> c -hates-> d` in `people`.
    fn three_label_csr() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        for (src, label, dst) in [
            ("a", "knows", "b"),
            ("b", "likes", "c"),
            ("c", "hates", "d"),
        ] {
            csr.add_edge_in_collection(src, label, dst, "people")
                .unwrap_or_else(|e| panic!("seed edge {src}->{dst}: {e}"));
        }
        csr
    }

    fn reached_names<'a>(csr: &'a CsrIndex, hops: &SurrogateHops) -> Vec<&'a str> {
        let mut names: Vec<&str> = hops
            .distances
            .iter()
            .filter_map(|&(l, _)| csr.node_name_checked(l))
            .collect();
        names.sort_unstable();
        names
    }

    #[test]
    fn a_label_set_expands_over_every_listed_label() {
        let csr = three_label_csr();
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.label_filter = &["knows", "likes"];
        let hops = csr.traverse_surrogates_in_collection(p);
        assert_eq!(reached_names(&csr, &hops), vec!["a", "b", "c"]);
    }

    #[test]
    fn an_unknown_label_in_a_set_adds_nothing() {
        let csr = three_label_csr();
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.label_filter = &["knows", "never_inserted"];
        let hops = csr.traverse_surrogates_in_collection(p);
        assert_eq!(reached_names(&csr, &hops), vec!["a", "b"]);
    }

    #[test]
    fn max_depth_zero_returns_only_the_seeds() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.max_depth = 0;
        let hops = csr.traverse_surrogates_in_collection(p);
        assert_eq!(hops.reached.len(), 1);
        assert!(hops.reached.contains(Surrogate::new(10)));
    }

    /// Under the cap, a level admits its nodes in name order, whatever order
    /// their edges were inserted in.
    #[test]
    fn a_capped_level_admits_nodes_in_name_order() {
        let mut csr = CsrIndex::new(test_memory());
        for dst in ["z", "m", "b"] {
            csr.add_edge_in_collection("a", "knows", dst, "people")
                .unwrap_or_else(|e| panic!("seed edge a->{dst}: {e}"));
        }
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.max_visited = 3;
        let hops = csr.traverse_surrogates_in_collection(p);
        assert!(hops.truncated);
        let admitted: Vec<&str> = hops
            .distances
            .iter()
            .filter_map(|&(l, _)| csr.node_name_checked(l))
            .collect();
        assert_eq!(admitted, vec!["a", "b", "m"]);
    }

    #[test]
    fn hitting_max_visited_reports_truncation() {
        let csr = seeded_csr();
        let seeds = [local(&csr, "a")];
        let mut p = params(&seeds, "people");
        p.max_visited = 2;
        let hops = csr.traverse_surrogates_in_collection(p);
        assert!(hops.truncated, "a cut-short walk must say so");
    }
}
