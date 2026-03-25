//! Distributed WCC — cross-shard component merging via label propagation.
//!
//! Each shard computes local WCC via union-find, then iteratively exchanges
//! component labels across shard boundaries. For each cross-shard edge,
//! the target shard adopts the lexicographically smaller label. Converges
//! when no shard changes any labels in a round.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Cross-shard component merge request: shard → target shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentMergeRequest {
    pub round: u32,
    pub source_shard: u16,
    /// (target_vertex_name, source_component_label).
    pub merges: Vec<(String, String)>,
}

/// WCC round acknowledgement: shard → coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WccRoundAck {
    pub shard_id: u16,
    pub round: u32,
    pub labels_changed: usize,
    pub merges_sent: usize,
}

/// Per-shard WCC execution state.
#[derive(Debug)]
pub struct ShardWccState {
    pub vertex_count: usize,
    parent: Vec<usize>,
    rank: Vec<u8>,
    pub global_labels: Vec<String>,
    pub shard_id: u16,
    pub boundary_edges: HashMap<u32, Vec<(String, u16)>>,
    node_names: Vec<String>,
}

impl ShardWccState {
    /// Initialize WCC state for a local CSR partition.
    pub fn init(
        vertex_count: usize,
        shard_id: u16,
        node_names: Vec<String>,
        local_edges: &dyn Fn(u32) -> Vec<u32>,
        ghost_edges: &dyn Fn(u32) -> Vec<(String, u16)>,
    ) -> Self {
        let parent: Vec<usize> = (0..vertex_count).collect();
        let rank = vec![0u8; vertex_count];

        let mut state = Self {
            vertex_count,
            parent,
            rank,
            global_labels: Vec::new(),
            shard_id,
            boundary_edges: HashMap::new(),
            node_names,
        };

        // Local union-find pass.
        for u in 0..vertex_count {
            for v in local_edges(u as u32) {
                state.union(u, v as usize);
            }
        }

        // Build boundary edge map.
        for u in 0..vertex_count {
            let ghosts = ghost_edges(u as u32);
            if !ghosts.is_empty() {
                state.boundary_edges.insert(u as u32, ghosts);
            }
        }

        // Initialize global labels from local roots.
        state.global_labels = (0..vertex_count)
            .map(|i| {
                let root = state.find(i);
                format!("{}:{}", shard_id, state.node_names[root])
            })
            .collect();

        state
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }

    /// Produce outbound merge requests for boundary edges.
    pub fn round(&self) -> (HashMap<u16, Vec<(String, String)>>, usize) {
        let mut outbound: HashMap<u16, Vec<(String, String)>> = HashMap::new();

        for (&local_id, ghost_list) in &self.boundary_edges {
            let root = find_static(&self.parent, local_id as usize);
            let label = self.global_labels[root].clone();
            for (remote_name, target_shard) in ghost_list {
                outbound
                    .entry(*target_shard)
                    .or_default()
                    .push((remote_name.clone(), label.clone()));
            }
        }

        let sent: usize = outbound.values().map(|v| v.len()).sum();
        (outbound, sent)
    }

    /// Apply incoming merges. Returns number of labels changed.
    pub fn apply_merges(&mut self, merges: &[(String, String)]) -> usize {
        let mut changed = 0;

        let name_to_id: HashMap<&str, usize> = self
            .node_names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), i))
            .collect();

        for (vertex_name, remote_label) in merges {
            let Some(&local_id) = name_to_id.get(vertex_name.as_str()) else {
                continue;
            };

            let root = find_static(&self.parent, local_id);
            let local_label = &self.global_labels[root];

            if local_label != remote_label && *remote_label < *local_label {
                self.global_labels[root] = remote_label.clone();
                changed += 1;
            }
        }

        // Propagate updated labels to all nodes.
        for i in 0..self.vertex_count {
            let root = find_static(&self.parent, i);
            if i != root {
                self.global_labels[i] = self.global_labels[root].clone();
            }
        }

        changed
    }

    /// Get current component assignment: (vertex_name, global_label).
    pub fn component_labels(&self) -> Vec<(String, String)> {
        (0..self.vertex_count)
            .map(|i| {
                let root = find_static(&self.parent, i);
                (self.node_names[i].clone(), self.global_labels[root].clone())
            })
            .collect()
    }
}

/// Non-mutating find (no path compression). Borrow-safe.
fn find_static(parent: &[usize], mut x: usize) -> usize {
    while parent[x] != x {
        x = parent[x];
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_merge_request_serde() {
        let req = ComponentMergeRequest {
            round: 2,
            source_shard: 1,
            merges: vec![("alice".into(), "0:root_a".into())],
        };
        let bytes = rmp_serde::to_vec_named(&req).unwrap();
        let decoded: ComponentMergeRequest = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.round, 2);
    }

    #[test]
    fn wcc_round_ack_serde() {
        let ack = WccRoundAck {
            shard_id: 3,
            round: 1,
            labels_changed: 5,
            merges_sent: 12,
        };
        let bytes = rmp_serde::to_vec_named(&ack).unwrap();
        let decoded: WccRoundAck = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.labels_changed, 5);
    }

    #[test]
    fn wcc_shard_local_only() {
        let state = ShardWccState::init(
            3,
            0,
            vec!["a".into(), "b".into(), "c".into()],
            &|node| match node {
                0 => vec![1],
                1 => vec![2],
                _ => Vec::new(),
            },
            &|_| Vec::new(),
        );
        let labels = state.component_labels();
        assert_eq!(labels[0].1, labels[1].1);
        assert_eq!(labels[1].1, labels[2].1);
    }

    #[test]
    fn wcc_shard_with_boundary_edges() {
        let state = ShardWccState::init(
            2,
            0,
            vec!["a".into(), "b".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 1 {
                    vec![("c".into(), 1)]
                } else {
                    Vec::new()
                }
            },
        );
        assert_eq!(state.boundary_edges.len(), 1);
        let (outbound, sent) = state.round();
        assert!(outbound.contains_key(&1));
        assert_eq!(sent, 1);
    }

    #[test]
    fn wcc_apply_merges_adopts_smaller_label() {
        let mut state = ShardWccState::init(
            2,
            1,
            vec!["c".into(), "d".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|_| Vec::new(),
        );
        let changed = state.apply_merges(&[("c".into(), "0:a".into())]);
        assert!(changed > 0);
        let labels = state.component_labels();
        assert_eq!(labels[0].1, "0:a");
        assert_eq!(labels[1].1, "0:a");
    }

    #[test]
    fn wcc_apply_merges_keeps_smaller_label() {
        let mut state =
            ShardWccState::init(1, 0, vec!["a".into()], &|_| Vec::new(), &|_| Vec::new());
        let changed = state.apply_merges(&[("a".into(), "1:z".into())]);
        assert_eq!(changed, 0);
        assert_eq!(state.component_labels()[0].1, "0:a");
    }

    #[test]
    fn wcc_multi_round_convergence() {
        let mut shard0 = ShardWccState::init(
            2,
            0,
            vec!["a".into(), "b".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 1 {
                    vec![("c".into(), 1)]
                } else {
                    Vec::new()
                }
            },
        );

        let mut shard1 = ShardWccState::init(
            2,
            1,
            vec!["c".into(), "d".into()],
            &|node| match node {
                0 => vec![1],
                _ => Vec::new(),
            },
            &|node| {
                if node == 0 {
                    vec![("b".into(), 0)]
                } else {
                    Vec::new()
                }
            },
        );

        // Round 1.
        let (out0, _) = shard0.round();
        let (out1, _) = shard1.round();
        let c0 = out1.get(&0).map_or(0, |m| shard0.apply_merges(m));
        let c1 = out0.get(&1).map_or(0, |m| shard1.apply_merges(m));
        assert!(c0 + c1 > 0);

        // Round 2.
        let (out0_r2, _) = shard0.round();
        let (out1_r2, _) = shard1.round();
        let c0_r2 = out1_r2.get(&0).map_or(0, |m| shard0.apply_merges(m));
        let c1_r2 = out0_r2.get(&1).map_or(0, |m| shard1.apply_merges(m));
        assert_eq!(c0_r2 + c1_r2, 0, "should converge");

        // All 4 nodes should share one global label.
        let l0 = shard0.component_labels();
        let l1 = shard1.component_labels();
        assert_eq!(l0[0].1, l1[0].1, "cross-shard merge");
    }
}
