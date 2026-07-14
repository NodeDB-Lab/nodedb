// SPDX-License-Identifier: BUSL-1.1

//! `SystemMetrics` struct definition — all atomic fields.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use super::super::histogram::{AtomicHistogram, WAL_FSYNC_BUCKETS_US};
use super::super::purge::PurgeMetrics;
use crate::data::io::IoMetrics;

/// Core metrics collected across the system.
#[derive(Debug, Default)]
pub struct SystemMetrics {
    // ── WAL ──
    pub wal_fsync_seconds: AtomicHistogram,
    pub wal_fsync_count: AtomicU64,
    pub wal_segment_count: AtomicU64,
    pub wal_segment_bytes: AtomicU64,

    // ── Raft / replication ──
    pub raft_apply_lag: AtomicU64,
    pub raft_commit_index: AtomicU64,
    pub raft_applied_index: AtomicU64,
    pub raft_leader_term: AtomicU64,
    pub raft_snapshot_count: AtomicU64,
    pub vshard_migrations_active: AtomicU64,

    // ── Bridge ──
    pub bridge_utilization: AtomicU64,

    // ── Compaction ──
    pub compaction_debt: AtomicU64,
    pub compaction_cycles: AtomicU64,
    pub compaction_bytes_total: AtomicU64,

    // ── Auth ──
    pub auth_failures: AtomicU64,
    pub auth_successes: AtomicU64,

    // ── Connections ──
    pub active_connections: AtomicU64,
    pub pgwire_connections: AtomicU64,
    pub http_connections: AtomicU64,
    pub native_connections: AtomicU64,
    pub websocket_connections: AtomicU64,
    pub ilp_connections: AtomicU64,

    // ── Queries ──
    pub queries_total: AtomicU64,
    pub query_errors: AtomicU64,
    pub slow_queries_total: AtomicU64,
    pub query_planning_seconds: AtomicHistogram,
    pub query_execution_seconds: AtomicHistogram,
    pub query_latency: AtomicHistogram,

    // ── Per-engine operations ──
    pub vector_searches: AtomicU64,
    pub vector_collections: AtomicU64,
    pub vector_vectors_stored: AtomicU64,
    pub vector_query_seconds: AtomicHistogram,

    pub graph_traversals: AtomicU64,
    pub graph_nodes: AtomicU64,
    pub graph_edges: AtomicU64,

    pub document_inserts: AtomicU64,
    pub document_reads: AtomicU64,
    pub document_collections: AtomicU64,
    /// Count of `DocumentOp::BackfillIndex` handler invocations on this
    /// node's Data Plane. Per-node: every node increments its own
    /// counter exactly when its local core runs the backfill primitive.
    /// Tests assert per-node fan-out for distributed CREATE INDEX by
    /// reading this counter on every node after DDL completion.
    pub document_index_backfills: AtomicU64,

    pub columnar_segments: AtomicU64,
    pub columnar_compaction_queue: AtomicU64,
    pub columnar_compression_ratio: AtomicU64, // stored as ratio × 100

    pub fts_searches: AtomicU64,
    pub fts_indexes: AtomicU64,
    pub fts_query_seconds: AtomicHistogram,

    // ── KV engine ──
    pub kv_gets_total: AtomicU64,
    pub kv_puts_total: AtomicU64,
    pub kv_deletes_total: AtomicU64,
    pub kv_scans_total: AtomicU64,
    pub kv_expiries_total: AtomicU64,
    pub kv_memory_bytes: AtomicU64,
    pub kv_total_keys: AtomicU64,

    // ── Per-engine query counts ──
    pub queries_vector: AtomicU64,
    pub queries_graph: AtomicU64,
    pub queries_document: AtomicU64,
    pub queries_columnar: AtomicU64,
    pub queries_kv: AtomicU64,
    pub queries_fts: AtomicU64,

    // ── Data Plane (aggregate across all cores) ──
    pub io_uring_submissions: AtomicU64,
    pub io_uring_completions: AtomicU64,
    pub tpc_utilization_ratio: AtomicU64,
    pub arena_memory_bytes: AtomicU64,
    /// Current number of live per-transaction staging overlays across all
    /// Data-Plane cores (txn_overlays + graph_txn_overlays entries). A gauge:
    /// rises on first staged write of a transaction, falls when the overlay is
    /// dropped on COMMIT/ROLLBACK/teardown. A persistently non-zero idle value
    /// indicates leaked abandoned-transaction overlays.
    pub active_txn_overlays: AtomicU64,

    // ── Contention ──
    pub mmap_major_faults: AtomicU64,
    pub nvme_queue_depth: AtomicU64,
    pub throttle_activations: AtomicU64,
    pub cache_contention_events: AtomicU64,

    // ── Storage tiers ──
    pub storage_l0_bytes: AtomicU64,
    pub storage_l1_bytes: AtomicU64,
    pub storage_l2_bytes: AtomicU64,
    pub mmap_rss_bytes: AtomicU64,

    // ── Subscriptions ──
    pub active_subscriptions: AtomicU64,
    pub active_listen_channels: AtomicU64,
    pub change_events_delivered: AtomicU64,
    /// Global CDC drop counter (sum across all streams). Kept for backward
    /// compatibility with existing dashboards that query this name without labels.
    pub change_events_dropped: AtomicU64,
    /// Per-stream CDC drop counters. Key: `(tenant_id, stream_name)`.
    /// Rendered as `nodedb_cdc_events_dropped_total{tenant="<id>",stream="<name>"}`.
    pub cdc_events_dropped_by_stream: RwLock<HashMap<(u64, String), u64>>,

    // ── Backpressure ──
    /// Per-engine Critical-pressure fire count.
    /// Key: engine name string (e.g. "vector", "columnar").
    /// Rendered as `nodedb_backpressure_critical_total{engine="..."}`.
    pub backpressure_critical_by_engine: RwLock<HashMap<String, u64>>,
    /// Per-engine Emergency-pressure fire count.
    /// Key: engine name string.
    /// Rendered as `nodedb_backpressure_emergency_total{engine="..."}`.
    pub backpressure_emergency_by_engine: RwLock<HashMap<String, u64>>,

    // ── Checkpoints ──
    pub checkpoints: AtomicU64,

    // ── Catalog sanity check ──
    /// Labeled counter: (registry, outcome) → total.
    /// `outcome` is one of "ok", "warning", "error".
    pub catalog_sanity_check_totals: RwLock<HashMap<(String, String), u64>>,

    // ── Collection hard-delete (purge) ──
    pub purge: PurgeMetrics,

    // ── Shutdown ──
    /// Gauge: phase name → last observed drain duration in milliseconds.
    /// Updated once per phase transition during graceful shutdown.
    pub shutdown_phase_durations_ms: RwLock<HashMap<String, u64>>,

    // ── Per-database metrics ──
    /// Per-database query counter.
    /// Key: database name string.
    /// Rendered as `nodedb_database_queries_total{database="..."}`.
    pub database_queries_by_name: RwLock<HashMap<String, u64>>,
    /// Per-database error counter.
    /// Key: database name string.
    /// Rendered as `nodedb_database_errors_total{database="..."}`.
    pub database_errors_by_name: RwLock<HashMap<String, u64>>,
    /// Per-database collection count gauge.
    /// Key: database name string.
    /// Rendered as `nodedb_database_collections_total{database="..."}`.
    pub database_collections_by_name: RwLock<HashMap<String, u64>>,
    /// Per-database tenant count gauge.
    /// Key: database name string.
    /// Rendered as `nodedb_database_tenants_total{database="..."}`.
    /// Incremented at tenant-in-database registration; stub-zero pending
    /// per-database tenant tracking work.
    pub database_tenants_by_name: RwLock<HashMap<String, u64>>,
    /// Per-database memory usage gauge (bytes).
    /// Key: database name string.
    /// Rendered as `nodedb_database_memory_bytes{database="..."}`.
    /// Updated by the memory governor; stub-zero pending per-database
    /// memory accounting work.
    pub database_memory_bytes_by_name: RwLock<HashMap<String, u64>>,
    /// Per-database storage usage gauge (bytes).
    /// Key: database name string.
    /// Rendered as `nodedb_database_storage_bytes{database="..."}`.
    /// Updated by compaction; stub-zero pending per-database storage
    /// accounting work.
    pub database_storage_bytes_by_name: RwLock<HashMap<String, u64>>,

    // ── IO priority scheduler ──
    /// Per-priority IO queue-depth and wait-latency metrics.
    ///
    /// Shared `Arc` is cloned into each `CoreLoop` at startup so the Data
    /// Plane can update counters without crossing the plane boundary.
    /// The Prometheus handler reads from here.
    pub io_metrics: Arc<IoMetrics>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        // WAL fsync latency uses sub-millisecond buckets (100µs–1s range).
        Self {
            wal_fsync_seconds: AtomicHistogram::with_buckets(WAL_FSYNC_BUCKETS_US),
            ..Self::default()
        }
    }
}
