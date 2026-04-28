//! Cluster configuration and post-start state.

use std::net::SocketAddr;
use std::time::Duration;

use std::sync::{Arc, Mutex, RwLock};

use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

/// Tunable retry policy for the join loop.
///
/// The schedule is computed by halving from the configured ceiling:
/// for `max_attempts = 8` and `max_backoff_secs = 32`, the per-attempt
/// delays are `0.25 s, 0.5 s, 1 s, 2 s, 4 s, 8 s, 16 s, 32 s` — i.e.
/// each delay is `max_backoff_secs >> (max_attempts - attempt)`. This
/// keeps the formula obvious from a single number while preserving
/// exponential growth.
///
/// Defaults match the production schedule. Tests construct their own
/// policy with a much smaller `max_backoff_secs` so the integration
/// suite doesn't pay a ~minute backoff on every join failure path.
#[derive(Debug, Clone, Copy)]
pub struct JoinRetryPolicy {
    /// Number of join attempts before the loop gives up.
    pub max_attempts: u32,
    /// Cap on the per-attempt backoff delay, in seconds. The schedule
    /// is derived from this ceiling — see the struct doc comment.
    pub max_backoff_secs: u64,
}

impl Default for JoinRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            max_backoff_secs: 32,
        }
    }
}

impl JoinRetryPolicy {
    /// Backoff delay before `attempt` (1-indexed). Attempt 0 is the
    /// initial try and never sleeps. Returns `Duration::ZERO` for
    /// out-of-range attempts.
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if attempt == 0 || attempt > self.max_attempts {
            return Duration::ZERO;
        }
        // Schedule grows exponentially toward `max_backoff_secs`. We
        // compute in millis so small `max_backoff_secs` values (test
        // configs) still produce non-zero delays for the early
        // attempts instead of being floored to zero seconds.
        let exp = self.max_attempts - attempt;
        let max_ms = self.max_backoff_secs.saturating_mul(1_000);
        let ms = max_ms >> exp;
        Duration::from_millis(ms.max(1))
    }
}

/// Configuration for cluster formation.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's unique ID.
    pub node_id: u64,
    /// Address to listen on for Raft RPCs.
    pub listen_addr: SocketAddr,
    /// Seed node addresses for bootstrap/join.
    pub seed_nodes: Vec<SocketAddr>,
    /// Number of Raft groups to create on bootstrap.
    pub num_groups: u64,
    /// Replication factor (number of replicas per group).
    pub replication_factor: usize,
    /// Data directory for persistent Raft log storage.
    pub data_dir: std::path::PathBuf,
    /// Operator escape hatch: bypass the probe phase and bootstrap this
    /// node unconditionally even if it is not the lexicographically
    /// smallest seed.
    ///
    /// Set this only on disaster recovery when the designated
    /// bootstrapper is permanently unreachable. Requires `listen_addr`
    /// to be present in `seed_nodes` (enforced at the caller's config
    /// validation layer).
    pub force_bootstrap: bool,
    /// Retry policy for the join loop. Defaults to production values
    /// (`8` attempts, `32 s` ceiling). Tests override this with a
    /// faster policy.
    pub join_retry: JoinRetryPolicy,
    /// Optional UDP bind address for the SWIM failure detector. `None`
    /// disables SWIM entirely — cluster startup then relies solely on
    /// the existing raft transport for membership observations. When
    /// `Some`, the operator is expected to spawn SWIM separately via
    /// [`crate::spawn_swim`] after the cluster is up and feed the
    /// seed list from `seed_nodes`.
    pub swim_udp_addr: Option<SocketAddr>,
    /// Raft election timeout range. Controls how long a follower waits
    /// before starting an election after losing contact with the leader.
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
}

/// Result of cluster startup — everything needed to run the Raft loop.
///
/// All mutable fields are wrapped in `Arc<RwLock<T>>` or `Arc<Mutex<T>>`
/// so subsystems started during `start_cluster` can hold live references
/// to the same shared state without copying it out of the initial
/// bootstrap result. The `RunningCluster` produced by the subsystem
/// registry holds `Arc` clones that keep the data alive alongside the
/// caller's `ClusterHandle`.
pub struct ClusterState {
    pub topology: Arc<RwLock<ClusterTopology>>,
    pub routing: Arc<RwLock<RoutingTable>>,
    pub multi_raft: Arc<Mutex<MultiRaft>>,
}
