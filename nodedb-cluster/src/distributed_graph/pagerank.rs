//! Per-shard PageRank execution state for distributed BSP.

use std::collections::HashMap;

/// Per-shard PageRank state maintained across supersteps.
#[derive(Debug)]
pub struct ShardPageRankState {
    pub vertex_count: usize,
    pub rank: Vec<f64>,
    pub next_rank: Vec<f64>,
    pub out_degrees: Vec<usize>,
    pub is_dangling: Vec<bool>,
    pub boundary_edges: HashMap<u32, Vec<(String, u16)>>,
    pub incoming_contributions: HashMap<String, f64>,
}

impl ShardPageRankState {
    /// Initialize from local CSR partition.
    pub fn init<F>(
        vertex_count: usize,
        out_degrees: Vec<usize>,
        _ghost_lookup: F,
        csr_out_edges: &dyn Fn(u32) -> Vec<(String, bool, u16)>,
    ) -> Self
    where
        F: Fn(&str) -> Option<u16>,
    {
        let init_rank = if vertex_count > 0 {
            1.0 / vertex_count as f64
        } else {
            0.0
        };

        let rank = vec![init_rank; vertex_count];
        let next_rank = vec![0.0; vertex_count];
        let is_dangling: Vec<bool> = out_degrees.iter().map(|&d| d == 0).collect();

        let mut boundary_edges: HashMap<u32, Vec<(String, u16)>> = HashMap::new();
        for node in 0..vertex_count {
            for (dst_name, is_ghost, target_shard) in csr_out_edges(node as u32) {
                if is_ghost {
                    boundary_edges
                        .entry(node as u32)
                        .or_default()
                        .push((dst_name, target_shard));
                }
            }
        }

        Self {
            vertex_count,
            rank,
            next_rank,
            out_degrees,
            is_dangling,
            boundary_edges,
            incoming_contributions: HashMap::new(),
        }
    }

    /// Execute one superstep. Returns (local_delta, outbound_contributions).
    pub fn superstep(
        &mut self,
        damping: f64,
        global_n: usize,
        local_edge_iter: &dyn Fn(u32) -> Vec<u32>,
    ) -> (f64, HashMap<u16, Vec<(String, f64)>>) {
        let n = global_n as f64;
        let teleport = (1.0 - damping) / n;

        let dangling_sum: f64 = self
            .rank
            .iter()
            .enumerate()
            .filter(|(i, _)| self.is_dangling[*i])
            .map(|(_, r)| r)
            .sum();

        let base = teleport + damping * dangling_sum / n;

        for r in self.next_rank.iter_mut() {
            *r = base;
        }

        let mut outbound: HashMap<u16, Vec<(String, f64)>> = HashMap::new();
        for u in 0..self.vertex_count {
            let deg = self.out_degrees[u];
            if deg == 0 {
                continue;
            }
            let contrib = damping * self.rank[u] / deg as f64;

            // Scatter to local edges.
            for dst in local_edge_iter(u as u32) {
                self.next_rank[dst as usize] += contrib;
            }

            // Scatter to boundary edges (cross-shard).
            if let Some(boundary) = self.boundary_edges.get(&(u as u32)) {
                for (dst_name, target_shard) in boundary {
                    outbound
                        .entry(*target_shard)
                        .or_default()
                        .push((dst_name.clone(), contrib));
                }
            }
        }

        let delta: f64 = self
            .rank
            .iter()
            .zip(self.next_rank.iter())
            .map(|(old, new)| (old - new).abs())
            .sum();

        std::mem::swap(&mut self.rank, &mut self.next_rank);
        self.incoming_contributions.clear();

        (delta, outbound)
    }

    pub fn apply_incoming_contributions(&mut self, node_id_to_local: &dyn Fn(&str) -> Option<u32>) {
        for (vertex_name, contrib) in &self.incoming_contributions {
            if let Some(local_id) = node_id_to_local(vertex_name) {
                self.next_rank[local_id as usize] += contrib;
            }
        }
    }

    pub fn add_remote_contribution(&mut self, vertex_name: String, value: f64) {
        *self
            .incoming_contributions
            .entry(vertex_name)
            .or_insert(0.0) += value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_state_init() {
        let state = ShardPageRankState::init(3, vec![2, 1, 0], |_| None, &|_node| Vec::new());
        assert_eq!(state.vertex_count, 3);
        assert!(!state.is_dangling[0]);
        assert!(state.is_dangling[2]);
    }

    #[test]
    fn shard_state_with_ghost_edges() {
        let state = ShardPageRankState::init(
            2,
            vec![2, 1],
            |node| if node == "remote" { Some(5) } else { None },
            &|node| {
                if node == 0 {
                    vec![("remote".into(), true, 5)]
                } else {
                    Vec::new()
                }
            },
        );
        assert_eq!(state.boundary_edges.len(), 1);
        assert_eq!(state.boundary_edges[&0][0].1, 5);
    }

    #[test]
    fn shard_superstep_local_only() {
        let mut state = ShardPageRankState::init(3, vec![1, 1, 1], |_| None, &|_| Vec::new());
        let (delta, outbound) = state.superstep(0.85, 3, &|node| match node {
            0 => vec![1],
            1 => vec![2],
            2 => vec![0],
            _ => Vec::new(),
        });
        assert!(outbound.is_empty());
        assert!(delta >= 0.0);
        let sum: f64 = state.rank.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6);
    }

    #[test]
    fn remote_contribution_accumulation() {
        let mut state = ShardPageRankState::init(2, vec![1, 0], |_| None, &|_| Vec::new());
        state.add_remote_contribution("n0".into(), 0.1);
        state.add_remote_contribution("n0".into(), 0.2);
        state.add_remote_contribution("n1".into(), 0.3);
        assert!((state.incoming_contributions["n0"] - 0.3).abs() < 1e-10);
        assert!((state.incoming_contributions["n1"] - 0.3).abs() < 1e-10);
    }
}
