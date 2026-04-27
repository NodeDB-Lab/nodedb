//! Insertion algorithm for `HnswCodecIndex<C>`.
//!
//! Implements the standard HNSW insert (Malkov & Yashunin 2018, Algorithm 1)
//! using `codec.fast_symmetric_distance` for all neighbor-selection passes.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use nodedb_codec::vector_quant::codec::VectorCodec;

use super::graph::{HnswCodecIndex, NodeC};

/// Ordered pair for priority queues (dist, node_idx in `nodes` vec).
#[derive(Clone, Copy, PartialEq)]
struct Cand {
    dist: f32,
    /// Index into `HnswCodecIndex::nodes`.
    idx: u32,
}

impl Eq for Cand {}

impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(self.idx.cmp(&other.idx))
    }
}

impl<C: VectorCodec> HnswCodecIndex<C> {
    /// Insert a vector with the given caller-supplied `id`.
    ///
    /// Encodes `v` via `codec.encode`, assigns a random layer, and runs the
    /// standard HNSW neighbour-selection algorithm.
    pub fn insert(&mut self, id: u32, v: &[f32]) {
        let quantized = self.codec.encode(v);
        let node_layer = self.random_layer();

        // Allocate the node first so we can use its index in the graph wiring.
        let new_idx = self.nodes.len() as u32;

        // Build empty neighbor lists: one per layer 0..=node_layer.
        let neighbors = vec![Vec::new(); node_layer + 1];

        self.nodes.push(NodeC {
            id,
            deleted: false,
            layer: node_layer,
            quantized,
            neighbors,
        });

        let Some(ep) = self.entry_point else {
            // First node: it becomes the entry point.
            self.entry_point = Some(new_idx);
            self.max_layer = node_layer;
            return;
        };

        // Phase 1: greedy descent from max_layer down to node_layer + 1.
        // Carry a single nearest candidate per layer (ef = 1).
        let mut cur_ep = ep;
        for layer in (node_layer + 1..=self.max_layer).rev() {
            cur_ep = self.greedy_nearest(new_idx, cur_ep, layer);
        }

        // Phase 2: ef_construction search from node_layer down to 0.
        let ef = self.ef_construction;
        for layer in (0..=node_layer.min(self.max_layer)).rev() {
            let candidates = self.search_layer_build(new_idx, cur_ep, ef, layer);

            // Choose the m (or m0 at layer 0) nearest as neighbours.
            let max_nb = self.max_neighbors(layer);
            let chosen: Vec<u32> = candidates
                .iter()
                .filter(|c| c.idx != new_idx)
                .take(max_nb)
                .map(|c| c.idx)
                .collect();

            // Set new node's neighbours at this layer.
            self.nodes[new_idx as usize].neighbors[layer] = chosen.clone();

            // Update chosen neighbours reciprocally.
            for &nb_idx in &chosen {
                let new_dist = {
                    let nb_q = &self.nodes[nb_idx as usize].quantized as *const C::Quantized;
                    let new_q = &self.nodes[new_idx as usize].quantized as *const C::Quantized;
                    // SAFETY: we hold exclusive access to `self`; the two
                    // borrows are to distinct nodes.
                    unsafe { self.codec.fast_symmetric_distance(&*nb_q, &*new_q) }
                };

                if layer < self.nodes[nb_idx as usize].neighbors.len() {
                    let nb_layer = &mut self.nodes[nb_idx as usize].neighbors[layer];
                    if !nb_layer.contains(&new_idx) {
                        nb_layer.push(new_idx);
                    }
                    // Prune if over capacity.
                    if nb_layer.len() > max_nb {
                        self.prune_neighbors(nb_idx, layer, max_nb, new_dist, new_idx);
                    }
                }
            }

            // Update entry point for the next lower layer.
            if let Some(best) = candidates.first() {
                cur_ep = best.idx;
            }
        }

        // If the new node's layer exceeds the current max, promote it.
        if node_layer > self.max_layer {
            self.entry_point = Some(new_idx);
            self.max_layer = node_layer;
        }
    }

    /// Greedy descent: starting at `ep_idx`, find the single nearest node to
    /// `query_idx` at the given `layer`.
    fn greedy_nearest(&self, query_idx: u32, ep_idx: u32, layer: usize) -> u32 {
        let mut best_idx = ep_idx;
        let mut best_dist = self.sym_dist(query_idx, ep_idx);

        loop {
            let mut improved = false;
            for &nb in self.neighbors_at(best_idx, layer) {
                if self.nodes[nb as usize].deleted {
                    continue;
                }
                let d = self.sym_dist(query_idx, nb);
                if d < best_dist {
                    best_dist = d;
                    best_idx = nb;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }

        best_idx
    }

    /// Beam search over `layer` with the given `ef`, returning candidates
    /// sorted by ascending distance from `query_idx` (excluding deleted nodes).
    fn search_layer_build(
        &self,
        query_idx: u32,
        ep_idx: u32,
        ef: usize,
        layer: usize,
    ) -> Vec<Cand> {
        let mut visited: HashSet<u32> = HashSet::new();
        visited.insert(ep_idx);

        let ep_dist = self.sym_dist(query_idx, ep_idx);
        let ep_cand = Cand {
            dist: ep_dist,
            idx: ep_idx,
        };

        let mut candidates: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        candidates.push(Reverse(ep_cand));

        let mut results: BinaryHeap<Cand> = BinaryHeap::new();
        if !self.nodes[ep_idx as usize].deleted {
            results.push(ep_cand);
        }

        while let Some(Reverse(cur)) = candidates.pop() {
            let worst = results.peek().map_or(f32::INFINITY, |w| w.dist);
            if cur.dist > worst && results.len() >= ef {
                break;
            }

            for &nb in self.neighbors_at(cur.idx, layer) {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.sym_dist(query_idx, nb);
                let worst_now = results.peek().map_or(f32::INFINITY, |w| w.dist);
                if d < worst_now || results.len() < ef {
                    candidates.push(Reverse(Cand { dist: d, idx: nb }));
                }
                if !self.nodes[nb as usize].deleted {
                    results.push(Cand { dist: d, idx: nb });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        let mut out: Vec<Cand> = results.into_vec();
        out.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
        out
    }

    /// Prune the neighbor list of `nb_idx` at `layer` to `max_nb` entries,
    /// removing the farthest neighbours (simple distance-based strategy).
    fn prune_neighbors(
        &mut self,
        nb_idx: u32,
        layer: usize,
        max_nb: usize,
        _hint_dist: f32,
        _hint_id: u32,
    ) {
        // Collect (dist_to_nb, idx) for every current neighbour.
        let nb_list = self.nodes[nb_idx as usize].neighbors[layer].clone();
        let mut scored: Vec<(f32, u32)> = nb_list
            .iter()
            .map(|&cand_idx| {
                let d = {
                    let a = &self.nodes[nb_idx as usize].quantized as *const C::Quantized;
                    let b = &self.nodes[cand_idx as usize].quantized as *const C::Quantized;
                    // SAFETY: a and b point to distinct nodes in `self.nodes`.
                    unsafe { self.codec.fast_symmetric_distance(&*a, &*b) }
                };
                (d, cand_idx)
            })
            .collect();

        // Keep the `max_nb` nearest.
        scored.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        scored.truncate(max_nb);

        self.nodes[nb_idx as usize].neighbors[layer] =
            scored.into_iter().map(|(_, idx)| idx).collect();
    }

    /// Symmetric distance between two nodes identified by their dense indices.
    #[inline]
    pub(crate) fn sym_dist(&self, a_idx: u32, b_idx: u32) -> f32 {
        let a = &self.nodes[a_idx as usize].quantized;
        let b = &self.nodes[b_idx as usize].quantized;
        // Both borrows are read-only; safe even through raw pointers if
        // called for distinct indices, but here we just borrow directly.
        self.codec.fast_symmetric_distance(a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantize::Sq8Codec;

    fn make_sq8(dim: usize, n: usize) -> Sq8Codec {
        let vecs: Vec<Vec<f32>> = (0..n)
            .map(|i| (0..dim).map(|d| (i * dim + d) as f32 * 0.1).collect())
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        Sq8Codec::calibrate(&refs, dim)
    }

    #[test]
    fn insert_sets_entry_point() {
        let codec = make_sq8(4, 10);
        let mut idx: HnswCodecIndex<Sq8Codec> = HnswCodecIndex::new(4, 8, 50, codec, 1);
        idx.insert(0, &[0.1, 0.2, 0.3, 0.4]);
        assert!(idx.entry_point.is_some());
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn insert_multiple_grows_nodes() {
        let codec = make_sq8(4, 30);
        let mut idx: HnswCodecIndex<Sq8Codec> = HnswCodecIndex::new(4, 8, 50, codec, 42);
        for i in 0..20u32 {
            let v: Vec<f32> = (0..4).map(|d| (i as usize * 4 + d) as f32).collect();
            idx.insert(i, &v);
        }
        assert_eq!(idx.len(), 20);
        assert!(idx.entry_point.is_some());
    }
}
