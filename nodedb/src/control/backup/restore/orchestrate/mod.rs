// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator logic.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then re-issues every section as durable, replicated
//! writes.
//!
//! Durable re-issue of columnar/timeseries/vector rows lives in [`reissue`];
//! post-install surrogate rebinding and tombstone warnings live in
//! [`rebind`].

mod rebind;
mod reissue;
mod restore;
mod stats;

pub use restore::restore_tenant;
pub use stats::RestoreStats;
