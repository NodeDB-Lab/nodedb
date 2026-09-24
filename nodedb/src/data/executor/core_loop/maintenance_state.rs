// SPDX-License-Identifier: BUSL-1.1

//! The background maintenance state a core holds.

use std::sync::Arc;

use crate::data::executor::handlers::control::reindex::PendingReindex;

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

    /// In-flight concurrent index rebuilds, polled each tick.
    ///
    /// Each entry yields a `RebuildResult` once its background OS thread
    /// finishes the shadow build. Only one rebuild per collection runs at a
    /// time. `execute_rebuild_index` returns `ErrorCode::Conflict` for a
    /// second one.
    pub(in crate::data::executor) pending_reindex: Vec<PendingReindex>,
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
        }
    }
}
