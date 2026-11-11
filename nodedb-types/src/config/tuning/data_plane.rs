// SPDX-License-Identifier: Apache-2.0

//! Data Plane core runtime and query execution tuning.

use serde::{Deserialize, Serialize};

/// Data Plane core runtime tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlaneTuning {
    #[serde(default = "default_idle_poll_timeout_ms")]
    pub idle_poll_timeout_ms: i32,
    #[serde(default = "default_max_consecutive_panics")]
    pub max_consecutive_panics: u32,
    #[serde(default = "default_panic_window_secs")]
    pub panic_window_secs: u64,
    #[serde(default = "default_degraded_cooldown_secs")]
    pub degraded_cooldown_secs: u64,
}

impl Default for DataPlaneTuning {
    fn default() -> Self {
        Self {
            idle_poll_timeout_ms: default_idle_poll_timeout_ms(),
            max_consecutive_panics: default_max_consecutive_panics(),
            panic_window_secs: default_panic_window_secs(),
            degraded_cooldown_secs: default_degraded_cooldown_secs(),
        }
    }
}

fn default_idle_poll_timeout_ms() -> i32 {
    100
}
fn default_max_consecutive_panics() -> u32 {
    3
}
fn default_panic_window_secs() -> u64 {
    60
}
fn default_degraded_cooldown_secs() -> u64 {
    30
}

/// Query execution tuning for the Data Plane executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTuning {
    #[serde(default = "default_sort_run_size")]
    pub sort_run_size: usize,
    #[serde(default = "default_stream_chunk_size")]
    pub stream_chunk_size: usize,
    #[serde(default = "default_arrow_batch_max_rows")]
    pub arrow_batch_max_rows: usize,
    #[serde(default = "default_arrow_batch_max_bytes")]
    pub arrow_batch_max_bytes: usize,
    #[serde(default = "default_bitmap_over_fetch_factor")]
    pub bitmap_over_fetch_factor: usize,
    #[serde(default = "default_bfs_memory_budget_bytes")]
    pub bfs_memory_budget_bytes: usize,
    #[serde(default = "default_bfs_bytes_per_node")]
    pub bfs_bytes_per_node: usize,
    /// Per-core LRU document cache size (number of entries).
    /// See `DocCache::new` in `core_loop.rs`.
    #[serde(default = "default_doc_cache_entries")]
    pub doc_cache_entries: usize,
    /// Columnar memtable flush threshold in rows.
    /// See `nodedb_columnar::memtable::DEFAULT_FLUSH_THRESHOLD`.
    #[serde(default = "default_columnar_flush_threshold")]
    pub columnar_flush_threshold: usize,
    /// Target segment size in bytes after compaction.
    /// See `nodedb::storage::compaction::CompactionConfig`.
    #[serde(default = "default_compaction_target_bytes")]
    pub compaction_target_bytes: usize,
    /// Maximum number of distinct group keys held in memory during GROUP BY
    /// before the spiller triggers a spill run to disk.  When exceeded, the
    /// current in-memory accumulator map is serialized to a temp file and the
    /// map is cleared.  All spill runs are k-way merged at finalize time.
    /// Default: 1_000_000.
    #[serde(default = "default_groupby_max_groups_in_mem")]
    pub groupby_max_groups_in_mem: usize,
    /// Number of documents the streaming aggregate processes per chunk before
    /// checking whether a spill run should be triggered.  A smaller value
    /// reduces peak memory at the cost of more frequent spill checks; a larger
    /// value amortises check overhead at the cost of holding more documents in
    /// flight between checks.  The LIMIT clause is applied after all chunks are
    /// accumulated, so this knob does not affect query correctness.
    /// Default: 10_000.
    #[serde(default = "default_aggregate_chunk_size")]
    pub aggregate_chunk_size: usize,
    /// Maximum total bytes a single unbounded (no-LIMIT) document scan may
    /// materialize on a Data Plane core before the handler aborts with a
    /// deterministic `ResourcesExhausted` error.  A `SELECT * FROM <coll>`
    /// without a LIMIT is no longer silently truncated; instead it returns
    /// every row that fits this budget and surfaces a typed error (suggesting
    /// the client add a LIMIT) if the result would exceed it.  This is a
    /// per-core, per-query bound that sits below the Control-Plane aggregate
    /// `network.max_query_result_bytes` ceiling.
    /// Default: 512 MiB.
    #[serde(default = "default_max_scan_result_bytes")]
    pub max_scan_result_bytes: usize,
    /// Per-core upper bound on the staged bytes one transaction's overlay
    /// holds. A staged write past it fails with `program_limit_exceeded`
    /// (SQLSTATE 54000) and changes nothing more.
    /// Default: 256 MiB.
    #[serde(default = "default_max_txn_overlay_bytes")]
    pub max_txn_overlay_bytes: usize,
}

impl Default for QueryTuning {
    fn default() -> Self {
        Self {
            sort_run_size: default_sort_run_size(),
            stream_chunk_size: default_stream_chunk_size(),
            arrow_batch_max_rows: default_arrow_batch_max_rows(),
            arrow_batch_max_bytes: default_arrow_batch_max_bytes(),
            bitmap_over_fetch_factor: default_bitmap_over_fetch_factor(),
            bfs_memory_budget_bytes: default_bfs_memory_budget_bytes(),
            bfs_bytes_per_node: default_bfs_bytes_per_node(),
            doc_cache_entries: default_doc_cache_entries(),
            columnar_flush_threshold: default_columnar_flush_threshold(),
            compaction_target_bytes: default_compaction_target_bytes(),
            groupby_max_groups_in_mem: default_groupby_max_groups_in_mem(),
            aggregate_chunk_size: default_aggregate_chunk_size(),
            max_scan_result_bytes: default_max_scan_result_bytes(),
            max_txn_overlay_bytes: default_max_txn_overlay_bytes(),
        }
    }
}

fn default_sort_run_size() -> usize {
    100_000
}
fn default_stream_chunk_size() -> usize {
    1_000
}
fn default_arrow_batch_max_rows() -> usize {
    65_536
}
fn default_arrow_batch_max_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_bitmap_over_fetch_factor() -> usize {
    3
}
fn default_bfs_memory_budget_bytes() -> usize {
    256 * 1024
}
fn default_bfs_bytes_per_node() -> usize {
    192
}
fn default_doc_cache_entries() -> usize {
    4096
}
fn default_columnar_flush_threshold() -> usize {
    65_536
}
fn default_compaction_target_bytes() -> usize {
    256 * 1024 * 1024
}
fn default_groupby_max_groups_in_mem() -> usize {
    1_000_000
}
fn default_aggregate_chunk_size() -> usize {
    10_000
}
fn default_max_scan_result_bytes() -> usize {
    512 * 1024 * 1024
}
fn default_max_txn_overlay_bytes() -> usize {
    256 * 1024 * 1024
}
