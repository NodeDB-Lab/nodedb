// SPDX-License-Identifier: BUSL-1.1

//! Multi-node cluster orchestration.
//!
//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions live in
//! [`types`]; spawn-variant convenience wrappers in [`spawn_variants`];
//! the bringup body in [`bringup`]; the convergence barriers in [`ready`];
//! the in-place restart in [`restart`]; post-spawn membership + DDL helpers
//! in [`membership`].

mod bringup;
mod membership;
mod ready;
mod restart;
mod spawn_variants;
mod types;

pub(crate) use types::ClusterSpawnConfig;
pub use types::TestCluster;
