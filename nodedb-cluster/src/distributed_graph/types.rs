//! Shared BSP message types for distributed graph algorithms.

use serde::{Deserialize, Serialize};

/// Superstep barrier message: coordinator → all shards.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SuperstepBarrier {
    pub algorithm: String,
    pub iteration: u32,
    pub max_iterations: u32,
    pub params: String,
    /// Bitemporal system-time ordinal for the algorithm run. `None` means
    /// "current state"; shards building their local `CsrSnapshot` from an
    /// `EdgeStore` thread this through to `scan_all_edges_decoded` so every
    /// shard sees the same historical topology.
    #[serde(default)]
    pub system_as_of: Option<i64>,
}

/// Boundary vertex contributions: shard → target shard (scatter phase).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct BoundaryContributions {
    pub iteration: u32,
    pub source_shard: u16,
    pub contributions: Vec<(String, f64)>,
}

/// Superstep acknowledgement: shard → coordinator (gather phase).
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct SuperstepAck {
    pub shard_id: u16,
    pub iteration: u32,
    pub local_delta: f64,
    pub vertex_count: usize,
    pub contributions_sent: usize,
}

/// Algorithm completion signal: coordinator → all shards.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct AlgoComplete {
    pub iterations: u32,
    pub converged: bool,
    pub final_delta: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superstep_barrier_serde() {
        let barrier = SuperstepBarrier {
            algorithm: "pagerank".into(),
            iteration: 3,
            max_iterations: 20,
            params: r#"{"damping":0.85}"#.into(),
            system_as_of: None,
        };
        let bytes = zerompk::to_msgpack_vec(&barrier).unwrap();
        let decoded: SuperstepBarrier = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.iteration, 3);
        assert!(decoded.system_as_of.is_none());
    }

    #[test]
    fn superstep_barrier_carries_system_as_of() {
        let barrier = SuperstepBarrier {
            algorithm: "pagerank".into(),
            iteration: 1,
            max_iterations: 10,
            params: String::new(),
            system_as_of: Some(1_700_000_000_000_000_000),
        };
        let bytes = zerompk::to_msgpack_vec(&barrier).unwrap();
        let decoded: SuperstepBarrier = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.system_as_of, Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn boundary_contributions_serde() {
        let contrib = BoundaryContributions {
            iteration: 1,
            source_shard: 5,
            contributions: vec![("alice".into(), 0.042), ("bob".into(), 0.031)],
        };
        let bytes = zerompk::to_msgpack_vec(&contrib).unwrap();
        let decoded: BoundaryContributions = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.contributions.len(), 2);
    }

    #[test]
    fn superstep_ack_serde() {
        let ack = SuperstepAck {
            shard_id: 3,
            iteration: 2,
            local_delta: 0.001,
            vertex_count: 1000,
            contributions_sent: 50,
        };
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: SuperstepAck = zerompk::from_msgpack(&bytes).unwrap();
        assert!((decoded.local_delta - 0.001).abs() < 1e-10);
    }

    #[test]
    fn algo_complete_serde() {
        let msg = AlgoComplete {
            iterations: 15,
            converged: true,
            final_delta: 1e-8,
        };
        let bytes = zerompk::to_msgpack_vec(&msg).unwrap();
        let decoded: AlgoComplete = zerompk::from_msgpack(&bytes).unwrap();
        assert!(decoded.converged);
    }
}
