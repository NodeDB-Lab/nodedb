// SPDX-License-Identifier: BUSL-1.1

//! The background maintenance state a core holds.

use std::sync::Arc;

use crate::data::executor::handlers::control::reindex::{PendingReindex, ReindexWaiter};
use crate::data::executor::handlers::timeseries::cagg_cut::CaggRefreshWaiter;
use crate::data::executor::handlers::timeseries::read_aggregate::CaggReadWaiter;

/// Compaction pacing, the maintenance CPU budget, and in-flight index
/// rebuilds.
pub(in crate::data::executor) struct MaintenanceState {
    /// Last time periodic maintenance (compaction, edge sweep) ran.
    pub(in crate::data::executor) last_maintenance: Option<std::time::Instant>,

    /// How often `maybe_run_maintenance` triggers.
    pub(in crate::data::executor) compaction_interval: std::time::Duration,

    /// Tombstone ratio threshold for auto-compaction (0.0–1.0).
    pub(in crate::data::executor) compaction_tombstone_threshold: f64,

    /// L1 segment compaction config for the storage layer.
    pub(in crate::data::executor) segment_compaction_config:
        crate::storage::compaction::CompactionConfig,

    /// Shared per-database maintenance CPU budget tracker. Every maintenance
    /// site gates per-database background work against the quota's
    /// `maintenance_cpu_pct`. `set_maintenance_budget` sets it after spawn.
    pub(in crate::data::executor) maintenance_budget:
        Option<Arc<crate::control::maintenance::MaintenanceBudgetTracker>>,

    /// In-flight full-text and CSR rebuilds, polled each tick.
    ///
    /// Each entry yields its rebuilt index once its OS thread finishes the
    /// build; the poll cuts it over on this core. One collection runs one
    /// concurrent REINDEX at a time; `execute_rebuild_index` refuses a
    /// second one with `ObjectNotInPrerequisiteState`.
    pub(in crate::data::executor) pending_reindex: Vec<PendingReindex>,

    /// Plain REINDEX requests waiting for their rebuilds to cut over. The
    /// tick answers each one when they have, or at its deadline.
    pub(in crate::data::executor) reindex_waiters: Vec<ReindexWaiter>,

    /// `REFRESH CONTINUOUS AGGREGATE` requests waiting for their cut to
    /// fold. The tick answers each one when it has, or at its deadline.
    pub(in crate::data::executor) cagg_refresh_waiters: Vec<CaggRefreshWaiter>,

    /// Reads of continuous aggregates whose root still owes folding. The
    /// tick answers each one once the fold pass caught up, or at its
    /// deadline.
    pub(in crate::data::executor) cagg_read_waiters: Vec<CaggReadWaiter>,

    /// The history purge below the cleanup horizon, and the commit snapshot
    /// window every replica checks.
    pub(in crate::data::executor) version_purge: super::version_purge::VersionPurgeState,
}

impl MaintenanceState {
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            last_maintenance: None,
            compaction_interval: std::time::Duration::from_secs(600),
            compaction_tombstone_threshold: 0.2,
            segment_compaction_config: crate::storage::compaction::CompactionConfig::default(),
            maintenance_budget: None,
            pending_reindex: Vec::new(),
            reindex_waiters: Vec::new(),
            cagg_refresh_waiters: Vec::new(),
            cagg_read_waiters: Vec::new(),
            version_purge: super::version_purge::VersionPurgeState::new(),
        }
    }
}
