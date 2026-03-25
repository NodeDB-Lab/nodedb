//! BSP coordinator for distributed graph algorithms.
//!
//! Runs on the Control Plane. Tracks which shards have completed each
//! superstep and aggregates convergence metrics.

use std::collections::HashMap;

use super::types::{AlgoComplete, SuperstepAck, SuperstepBarrier};

#[derive(Debug)]
pub struct BspCoordinator {
    pub algorithm: String,
    pub iteration: u32,
    pub max_iterations: u32,
    pub tolerance: f64,
    pub shard_ids: Vec<u16>,
    pub acks: HashMap<u16, SuperstepAck>,
    pub completed: bool,
}

impl BspCoordinator {
    pub fn new(
        algorithm: String,
        max_iterations: u32,
        tolerance: f64,
        shard_ids: Vec<u16>,
    ) -> Self {
        Self {
            algorithm,
            iteration: 0,
            max_iterations,
            tolerance,
            shard_ids,
            acks: HashMap::new(),
            completed: false,
        }
    }

    pub fn record_ack(&mut self, ack: SuperstepAck) {
        self.acks.insert(ack.shard_id, ack);
    }

    pub fn all_acked(&self) -> bool {
        self.shard_ids.iter().all(|id| self.acks.contains_key(id))
    }

    /// Sum of per-shard convergence deltas. Only meaningful when `all_acked()`.
    pub fn global_delta(&self) -> f64 {
        debug_assert!(
            self.all_acked(),
            "global_delta called before all shards ACKed"
        );
        self.acks.values().map(|ack| ack.local_delta).sum()
    }

    /// Total vertex count across all shards. Only meaningful when `all_acked()`.
    pub fn total_vertices(&self) -> usize {
        debug_assert!(
            self.all_acked(),
            "total_vertices called before all shards ACKed"
        );
        self.acks.values().map(|ack| ack.vertex_count).sum()
    }

    /// Advance to next superstep. Returns `true` if should continue.
    pub fn advance(&mut self) -> bool {
        let delta = self.global_delta();
        self.iteration += 1;
        self.acks.clear();

        if delta < self.tolerance || self.iteration >= self.max_iterations {
            self.completed = true;
            return false;
        }
        true
    }

    pub fn barrier_message(&self) -> SuperstepBarrier {
        SuperstepBarrier {
            algorithm: self.algorithm.clone(),
            iteration: self.iteration + 1,
            max_iterations: self.max_iterations,
            params: String::new(),
        }
    }

    pub fn completion_message(&self) -> AlgoComplete {
        AlgoComplete {
            iterations: self.iteration,
            converged: self.global_delta() < self.tolerance,
            final_delta: self.global_delta(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_convergence() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1, 2]);

        for id in 0..3u16 {
            coord.record_ack(SuperstepAck {
                shard_id: id,
                iteration: 1,
                local_delta: 0.3,
                vertex_count: 100,
                contributions_sent: 10,
            });
        }
        assert!(coord.all_acked());
        assert!((coord.global_delta() - 0.9).abs() < 1e-10);
        assert!(coord.advance());

        for id in 0..3u16 {
            coord.record_ack(SuperstepAck {
                shard_id: id,
                iteration: 2,
                local_delta: 1e-8,
                vertex_count: 100,
                contributions_sent: 10,
            });
        }
        assert!(!coord.advance());
        assert!(coord.completed);
    }

    #[test]
    fn coordinator_max_iterations() {
        let mut coord = BspCoordinator::new("pagerank".into(), 2, 1e-10, vec![0]);

        coord.record_ack(SuperstepAck {
            shard_id: 0,
            iteration: 1,
            local_delta: 1.0,
            vertex_count: 10,
            contributions_sent: 0,
        });
        assert!(coord.advance());

        coord.record_ack(SuperstepAck {
            shard_id: 0,
            iteration: 2,
            local_delta: 0.5,
            vertex_count: 10,
            contributions_sent: 0,
        });
        assert!(!coord.advance());
        assert!(coord.completed);
    }
}
