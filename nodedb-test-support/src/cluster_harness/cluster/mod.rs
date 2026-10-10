// SPDX-License-Identifier: BUSL-1.1

//! Multi-node cluster orchestration.
//!
//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions live in
//! [`types`]; spawn-variant convenience wrappers in [`spawn_variants`];
//! the bringup body in [`bringup`]; the convergence barriers in [`ready`];
//! the in-place restart in [`restart`]; post-spawn membership + DDL helpers
//! in [`membership`]; data-group leadership moves and waits in
//! [`leadership`].

mod bringup;
mod leadership;
mod membership;
mod ready;
mod restart;
mod spawn_variants;
mod types;

pub use leadership::GroupLeader;
pub use restart::{StoppedCluster, StoppedMember, StoppedNodeInfo};
pub use types::TestCluster;
pub(crate) use types::{ClusterSpawnConfig, DEFAULT_NUM_GROUPS};
