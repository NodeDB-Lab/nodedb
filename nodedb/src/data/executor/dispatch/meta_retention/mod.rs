//! Retention / temporal-purge / last-value Meta dispatch.
//!
//! Split off from `dispatch/other.rs` to respect the per-file size budget.
//! Handlers are grouped by concern:
//!
//! - `handlers` — retention + continuous-agg + last-value + edge-store /
//!   document-strict / timeseries-columnar temporal-purge handlers and the
//!   dispatch entry point [`CoreLoop::dispatch_meta_retention`].
//! - `columnar_plain` — plain columnar temporal-purge (segment-scanning,
//!   delete-bitmap marking).

pub mod columnar_plain;
pub mod handlers;
