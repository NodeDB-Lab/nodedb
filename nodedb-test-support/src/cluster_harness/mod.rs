// SPDX-License-Identifier: BUSL-1.1

//! Multi-node in-process cluster harness for nodedb-crate integration tests.
//!
//! One `TestClusterNode` owns a full NodeDB server stack: temp data dir,
//! WAL, `SharedState` (credentials + catalog + metadata_cache), Data
//! Plane core, response poller, Event Plane, cluster transport (QUIC),
//! metadata Raft group applier, pgwire listener, and a connected
//! `tokio_postgres::Client`. A `TestCluster` spawns N of them —
//! node 1 bootstraps a fresh raft group 0, nodes 2..N join over QUIC.
//!
//! The harness is the nodedb-crate analogue of
//! `nodedb-cluster/tests/common/mod.rs::TestNode`. It differs because
//! nodedb-crate tests must exercise the full pgwire path — end to
//! end, leader election, DDL replication, follower redb writeback,
//! Data Plane register on apply — not just raft plumbing.

pub mod cluster;
pub mod node;
pub mod pitr;
pub mod retriable;
pub mod shared_steps;
pub mod wait;

pub use cluster::{GroupLeader, StoppedCluster, StoppedNodeInfo, TestCluster};
pub use node::TestClusterNode;
pub use pitr::PitrStorage;
pub use retriable::{is_no_serving_leader, read_once_a_leader_exists};
pub use wait::{wait_for, wait_for_async, wait_for_report};
