// SPDX-License-Identifier: BUSL-1.1

//! Per-core install of a data-group Raft snapshot.

pub mod barrier;
pub mod clear;
pub mod dispatch;
pub mod error;
pub mod split;

pub use barrier::{append_install_barrier, append_install_marker};
pub use clear::{GroupCollection, clear_targets_per_core, group_collections};
pub use dispatch::install_on_every_core;
pub use error::{SettleStep, SnapshotInstallError};
pub use split::{CoreMap, CoreShares, split_by_core};
