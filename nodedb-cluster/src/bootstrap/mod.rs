//! Cluster bootstrap and join protocol.
//!
//! Three startup paths:
//!
//! 1. **Bootstrap**: First seed node — creates topology, routing table, Raft groups,
//!    persists to catalog. The cluster is born.
//!
//! 2. **Join**: New node contacts a seed, receives full cluster state via
//!    `JoinResponse`, persists, and registers peers.
//!
//! 3. **Restart**: Node loads topology + routing from catalog, reconnects to
//!    known peers.

pub mod bootstrap_fn;
pub mod config;
pub mod handle_join;
pub mod join;
pub mod probe;
pub mod restart;
pub mod start;

pub use config::{ClusterConfig, ClusterState, JoinRetryPolicy};
pub use handle_join::handle_join_request;
pub use start::{start_cluster, start_cluster_subsystems};
