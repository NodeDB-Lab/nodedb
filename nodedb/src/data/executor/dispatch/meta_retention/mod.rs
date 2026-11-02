// SPDX-License-Identifier: BUSL-1.1

//! Retention / temporal-purge / last-value Meta dispatch.
//!
//! Split off from `dispatch/other.rs` to respect the per-file size budget.
//! Handlers are grouped by concern:
//!
//! - `handlers` — continuous-agg + last-value + edge-store /
//!   document-strict / timeseries-columnar temporal-purge handlers and the
//!   dispatch entry point [`CoreLoop::dispatch_meta_retention`].
//! - `columnar_plain` — plain columnar temporal-purge (segment-scanning,
//!   delete-bitmap marking).
//! - `ts_retention` — timeseries partition retention.
//! - `last_value` — last-value cache reads, current and at a cut.
//! - `version_purge` / `version_purge_units` — the history purge below the
//!   cleanup horizon.

pub mod columnar_plain;
pub mod handlers;
pub mod last_value;
pub mod ts_retention;
pub mod version_purge;
pub mod version_purge_units;
