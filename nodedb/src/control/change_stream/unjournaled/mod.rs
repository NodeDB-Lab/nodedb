// SPDX-License-Identifier: BUSL-1.1

//! The applied data-group entries whose change events the change journal
//! does not hold yet.
//!
//! - [`store`]: the entries, with the WAL floor each one holds.
//! - [`recover`]: boot's read of the WAL for the entries a restart delivers
//!   again.

pub mod recover;
pub mod store;

pub use store::{RecoveredChanges, UnjournaledEntries};
