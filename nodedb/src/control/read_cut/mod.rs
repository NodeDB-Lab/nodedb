// SPDX-License-Identifier: BUSL-1.1

//! The read cut of a historical statement or a distributed run: the cap it
//! pins, the cut marker that fixes it, the wait every serving node does
//! before it reads, the ordinal reservations that close a cut, and the
//! transaction snapshots open on this node.

pub mod close;
pub mod reservations;
pub mod run_marker;
pub mod snapshot_age;
pub mod snapshots;
pub mod state;
pub mod statement_cap;
pub mod wait;

pub use reservations::{OrdinalReservation, OrdinalReservations};
pub(crate) use run_marker::{agreed_cut, new_cut_marker};
pub use snapshot_age::{check_snapshot_age, snapshot_max_hold};
pub use snapshots::{ActiveSnapshots, SnapshotGuard};
pub use state::ReadCutState;
pub use statement_cap::{StatementCap, open_txn_snapshot};
pub(crate) use statement_cap::{pin_historical_cut, statement_cap};
pub(crate) use wait::{await_read_cut, cut_read_groups};
