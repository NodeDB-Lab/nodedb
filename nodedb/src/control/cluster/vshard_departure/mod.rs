// SPDX-License-Identifier: BUSL-1.1

//! Purge the data of vShards that left this node, once their new owners
//! serve them.

pub mod fence;
pub mod ledger;
pub mod pass;
pub mod purge;
pub mod resident;
pub mod task;

pub use fence::{DepartureFence, InstallGuard, PurgeGuard};
pub use pass::DeparturePass;
pub use resident::ResidencyView;
pub use task::spawn_departure_purge;
