// SPDX-License-Identifier: BUSL-1.1

//! Document operation handlers — module root.
//! Submodules: read (scan), write (batch insert, register),
//! index_maintenance (backfill, drop index), sort (external sort, sort
//! helpers).

pub mod apply_balance_delta;
pub mod history_charge;
pub mod index_backfill_versions;
pub mod index_fetch;
pub mod index_maintenance;
pub mod read;
pub mod resolve;
pub mod sort;
pub mod write;
