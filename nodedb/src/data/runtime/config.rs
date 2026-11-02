// SPDX-License-Identifier: BUSL-1.1

//! Compaction / maintenance tuning handed to each Data Plane core at spawn.

/// Compaction configuration passed to each Data Plane core.
#[derive(Debug, Clone)]
pub struct CoreCompactionConfig {
    /// How often to run automatic compaction.
    pub interval: std::time::Duration,
    /// Tombstone ratio threshold for auto-compaction.
    pub tombstone_threshold: f64,
    /// Query execution tuning parameters.
    pub query: nodedb_types::config::tuning::QueryTuning,
    /// Graph engine tuning (traversal limits + variable-length expansion caps).
    pub graph: nodedb_types::config::tuning::GraphTuning,
    /// Timeseries engine tuning (memtable soft/hard budgets, tag cardinality
    /// ceiling). Drives the record-boundary admission gate on the ingest path.
    pub timeseries: nodedb_types::config::tuning::TimeseriesToning,
    /// Vector engine tuning: seal threshold and PQ / IVF defaults.
    pub vector: nodedb_types::config::tuning::VectorTuning,
    /// How often this core's event loop flushes vector + sparse-vector
    /// indexes to disk (the per-core backstop checkpoint, distinct from but
    /// sourced from the same `[checkpoint].interval_secs` as the Control
    /// Plane's coordinated `checkpoint_manager` cycle).
    pub checkpoint_interval: std::time::Duration,
    /// The longest a committing transaction's snapshot can trail its commit
    /// ordinal (`NetworkTuning::snapshot_max_hold`). A commit past it is
    /// refused on every replica, so a replica's purge below the cleanup
    /// horizon never changes a commit verdict.
    pub commit_snapshot_window: std::time::Duration,
}

impl Default for CoreCompactionConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(600),
            tombstone_threshold: 0.2,
            query: nodedb_types::config::tuning::QueryTuning::default(),
            graph: nodedb_types::config::tuning::GraphTuning::default(),
            timeseries: nodedb_types::config::tuning::TimeseriesToning::default(),
            vector: nodedb_types::config::tuning::VectorTuning::default(),
            checkpoint_interval: std::time::Duration::from_secs(300),
            commit_snapshot_window: nodedb_types::config::tuning::NetworkTuning::default()
                .snapshot_max_hold(),
        }
    }
}
