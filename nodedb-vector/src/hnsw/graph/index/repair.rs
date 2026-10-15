// SPDX-License-Identifier: Apache-2.0

//! Reachability repair for compaction.
//!
//! Search finds a node only along edges from the entry point. Deleted nodes
//! keep their edges, so search walks through them. Compaction removes them,
//! which can cut every path to a live node. Two passes repair this:
//! - `bridge_deleted_neighbors` replaces edges into deleted nodes with edges
//!   to their live neighbors, before the deleted nodes go.
//! - `connect_unreachable` links each node the entry point cannot reach,
//!   after the deleted nodes go.

use std::collections::VecDeque;

use super::HnswIndex;
use crate::hnsw::build::select_neighbors_heuristic;
use crate::hnsw::graph::Candidate;
use crate::hnsw::search::search_layer;

/// Parent value of a node the spanning tree does not reach.
const UNREACHED: u32 = u32::MAX;

impl HnswIndex {
    /// Re-selects the list of each live node that points at a deleted node.
    ///
    /// The pool holds the live neighbors plus the live neighbors of each
    /// deleted neighbor. The diversity heuristic picks up to the layer cap.
    pub(super) fn bridge_deleted_neighbors(&mut self) {
        let metric = self.params.metric;
        let mut rewrites: Vec<(usize, usize, Vec<u32>)> = Vec::new();
        for (id, node) in self.nodes.iter().enumerate() {
            if node.deleted {
                continue;
            }
            for (layer, list) in node.neighbors.iter().enumerate() {
                if !list.iter().any(|&n| self.nodes[n as usize].deleted) {
                    continue;
                }
                let mut pool: Vec<u32> = Vec::with_capacity(list.len() * 2);
                for &n in list {
                    if !self.nodes[n as usize].deleted {
                        pool.push(n);
                        continue;
                    }
                    pool.extend(
                        self.neighbors_at(n, layer)
                            .iter()
                            .copied()
                            .filter(|&nn| nn as usize != id && !self.nodes[nn as usize].deleted),
                    );
                }
                pool.sort_unstable();
                pool.dedup();
                let probe = self.node_probe(id as u32);
                let mut candidates: Vec<Candidate> = pool
                    .into_iter()
                    .map(|nid| Candidate {
                        id: nid,
                        dist: probe
                            .as_ref()
                            .and_then(|p| self.dist_to_node(p, nid, metric))
                            .unwrap_or(f32::INFINITY),
                    })
                    .collect();
                candidates.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));
                let cap = self.max_neighbors(layer);
                let selected = select_neighbors_heuristic(self, &candidates, cap)
                    .iter()
                    .map(|c| c.id)
                    .collect();
                rewrites.push((id, layer, selected));
            }
        }
        for (id, layer, list) in rewrites {
            self.nodes[id].neighbors[layer] = list;
        }
    }

    /// Links every node the entry point cannot reach, top layer first.
    ///
    /// A spanning tree from the entry point tracks reachability. A link never
    /// removes a tree edge, so the tree only grows and one pass per layer
    /// covers every node.
    pub(super) fn connect_unreachable(&mut self) {
        let Some(ep) = self.entry_point else {
            return;
        };
        for layer in (0..=self.max_layer).rev() {
            let mut parent = vec![UNREACHED; self.nodes.len()];
            parent[ep as usize] = ep;
            self.grow_tree(ep, layer, &mut parent);
            for id in 0..self.nodes.len() as u32 {
                if parent[id as usize] != UNREACHED
                    || self.nodes[id as usize].neighbors.len() <= layer
                {
                    continue;
                }
                parent[id as usize] = self.link_orphan(ep, id, layer, &parent);
                self.grow_tree(id, layer, &mut parent);
            }
        }
    }

    /// Breadth-first growth of the spanning tree from `start` at `layer`.
    fn grow_tree(&self, start: u32, layer: usize, parent: &mut [u32]) {
        let mut queue = VecDeque::from([start]);
        while let Some(cur) = queue.pop_front() {
            for &next in self.neighbors_at(cur, layer) {
                if parent[next as usize] == UNREACHED {
                    parent[next as usize] = cur;
                    queue.push_back(next);
                }
            }
        }
    }

    /// Adds an edge into `orphan` from a reachable host and returns the host.
    ///
    /// Hosts nearest to `orphan` come first. The tree over the reached set R
    /// holds |R|-1 edges. When every reached node is full, R holds at least
    /// |R| edges, so a non-tree edge exists and the scan over every reached
    /// node always finds a host.
    fn link_orphan(&mut self, ep: u32, orphan: u32, layer: usize, parent: &[u32]) -> u32 {
        let near = self.reachable_candidates(ep, orphan, layer, parent);
        if let Some(host) = self.attach(orphan, layer, &near, parent) {
            return host;
        }
        let anywhere: Vec<u32> = (0..self.nodes.len() as u32)
            .filter(|&id| parent[id as usize] != UNREACHED)
            .collect();
        self.attach(orphan, layer, &anywhere, parent).unwrap_or(ep)
    }

    /// Links `orphan` from the first host in `pool` with a free slot. If none
    /// has one, the first host with a non-tree edge gives that edge up.
    fn attach(&mut self, orphan: u32, layer: usize, pool: &[u32], parent: &[u32]) -> Option<u32> {
        let cap = self.max_neighbors(layer);
        if let Some(host) = pool
            .iter()
            .copied()
            .find(|&h| self.neighbors_at(h, layer).len() < cap)
        {
            self.nodes[host as usize].neighbors[layer].push(orphan);
            return Some(host);
        }
        let (host, slot) = pool.iter().copied().find_map(|h| {
            self.neighbors_at(h, layer)
                .iter()
                .rposition(|&t| parent[t as usize] != h)
                .map(|slot| (h, slot))
        })?;
        self.nodes[host as usize].neighbors[layer][slot] = orphan;
        Some(host)
    }

    /// Reachable nodes nearest to `orphan` at `layer`, nearest first.
    fn reachable_candidates(&self, ep: u32, orphan: u32, layer: usize, parent: &[u32]) -> Vec<u32> {
        let Some(probe) = self.node_probe(orphan) else {
            return vec![ep];
        };
        let mut start = ep;
        for upper in (layer + 1..=self.max_layer).rev() {
            if let Some(best) = search_layer(self, &probe, start, 1, upper, None, 0).first() {
                start = best.id;
            }
        }
        let ef = self.params.ef_construction;
        search_layer(self, &probe, start, ef, layer, None, 0)
            .into_iter()
            .map(|c| c.id)
            .filter(|&id| parent[id as usize] != UNREACHED)
            .collect()
    }
}
