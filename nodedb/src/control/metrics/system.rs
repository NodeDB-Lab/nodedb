//! Core system metrics collected across all NodeDB subsystems.
//!
//! All fields are atomic — safe for concurrent reads/writes from
//! Control Plane, Data Plane handlers, and the HTTP metrics endpoint.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use super::histogram::AtomicHistogram;

/// Core metrics collected across the system.
#[derive(Debug, Default)]
pub struct SystemMetrics {
    // ── WAL ──
    pub wal_fsync_latency_us: AtomicU64,
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
    pub compaction_throughput_bytes_sec: AtomicU64,

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
    pub query_planning_us: AtomicU64,
    pub query_execution_us: AtomicU64,
    pub query_latency: AtomicHistogram,

    // ── Per-engine operations ──
    pub vector_searches: AtomicU64,
    pub vector_collections: AtomicU64,
    pub vector_vectors_stored: AtomicU64,
    pub vector_avg_latency_us: AtomicU64,

    pub graph_traversals: AtomicU64,
    pub graph_nodes: AtomicU64,
    pub graph_edges: AtomicU64,

    pub document_inserts: AtomicU64,
    pub document_reads: AtomicU64,
    pub document_collections: AtomicU64,

    pub columnar_segments: AtomicU64,
    pub columnar_compaction_queue: AtomicU64,
    pub columnar_compression_ratio: AtomicU64, // stored as ratio × 100

    pub text_searches: AtomicU64,
    pub text_indexes: AtomicU64,
    pub text_avg_latency_us: AtomicU64,

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
    pub tpc_utilization_pct: AtomicU64,
    pub arena_memory_bytes: AtomicU64,

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
    pub change_events_dropped: AtomicU64,

    // ── Checkpoints ──
    pub checkpoints: AtomicU64,

    // ── Catalog sanity check ──
    /// Labeled counter: (registry, outcome) → total.
    /// `outcome` is one of "ok", "warning", "error".
    pub catalog_sanity_check_totals: RwLock<HashMap<(String, String), u64>>,

    // ── Shutdown ──
    /// Gauge: phase name → last observed drain duration in milliseconds.
    /// Updated once per phase transition during graceful shutdown.
    pub shutdown_phase_durations_ms: RwLock<HashMap<String, u64>>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    // ── WAL ──

    pub fn record_wal_fsync(&self, duration_us: u64) {
        self.wal_fsync_latency_us
            .store(duration_us, Ordering::Relaxed);
        self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_wal_segments(&self, count: u64, bytes: u64) {
        self.wal_segment_count.store(count, Ordering::Relaxed);
        self.wal_segment_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Replication ──

    pub fn record_raft_lag(&self, lag: u64) {
        self.raft_apply_lag.store(lag, Ordering::Relaxed);
    }

    pub fn update_raft_state(&self, commit_idx: u64, applied_idx: u64, term: u64) {
        self.raft_commit_index.store(commit_idx, Ordering::Relaxed);
        self.raft_applied_index
            .store(applied_idx, Ordering::Relaxed);
        self.raft_leader_term.store(term, Ordering::Relaxed);
    }

    pub fn record_raft_snapshot(&self) {
        self.raft_snapshot_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_vshard_migrations(&self, active: u64) {
        self.vshard_migrations_active
            .store(active, Ordering::Relaxed);
    }

    // ── Bridge ──

    pub fn record_bridge_utilization(&self, pct: u64) {
        self.bridge_utilization.store(pct, Ordering::Relaxed);
    }

    // ── Compaction ──

    pub fn update_compaction(&self, debt: u64, throughput_bps: u64) {
        self.compaction_debt.store(debt, Ordering::Relaxed);
        self.compaction_throughput_bytes_sec
            .store(throughput_bps, Ordering::Relaxed);
    }

    pub fn record_compaction_cycle(&self) {
        self.compaction_cycles.fetch_add(1, Ordering::Relaxed);
    }

    // ── Auth ──

    pub fn record_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_auth_success(&self) {
        self.auth_successes.fetch_add(1, Ordering::Relaxed);
    }

    // ── Connections ──

    pub fn update_connections(
        &self,
        pgwire: u64,
        http: u64,
        native: u64,
        websocket: u64,
        ilp: u64,
    ) {
        self.pgwire_connections.store(pgwire, Ordering::Relaxed);
        self.http_connections.store(http, Ordering::Relaxed);
        self.native_connections.store(native, Ordering::Relaxed);
        self.websocket_connections
            .store(websocket, Ordering::Relaxed);
        self.ilp_connections.store(ilp, Ordering::Relaxed);
        self.active_connections
            .store(pgwire + http + native + websocket + ilp, Ordering::Relaxed);
    }

    pub fn inc_pgwire_connections(&self) {
        self.pgwire_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_pgwire_connections(&self) {
        self.pgwire_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_http_connections(&self) {
        self.http_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_http_connections(&self) {
        self.http_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_websocket_connections(&self) {
        self.websocket_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_websocket_connections(&self) {
        self.websocket_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_ilp_connections(&self) {
        self.ilp_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ilp_connections(&self) {
        self.ilp_connections.fetch_sub(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    // ── Queries ──

    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_error(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_latency(&self, latency_us: u64) {
        self.query_latency.observe(latency_us);
        if latency_us > 100_000 {
            self.slow_queries_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_query_timing(&self, planning_us: u64, execution_us: u64) {
        self.query_planning_us.store(planning_us, Ordering::Relaxed);
        self.query_execution_us
            .store(execution_us, Ordering::Relaxed);
    }

    pub fn record_query_by_engine(&self, engine: &str) {
        match engine {
            "vector" => self.queries_vector.fetch_add(1, Ordering::Relaxed),
            "graph" => self.queries_graph.fetch_add(1, Ordering::Relaxed),
            "document" => self.queries_document.fetch_add(1, Ordering::Relaxed),
            "columnar" => self.queries_columnar.fetch_add(1, Ordering::Relaxed),
            "kv" => self.queries_kv.fetch_add(1, Ordering::Relaxed),
            "fts" => self.queries_fts.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    // ── Vector engine ──

    pub fn record_vector_search(&self, latency_us: u64) {
        self.vector_searches.fetch_add(1, Ordering::Relaxed);
        self.vector_avg_latency_us
            .store(latency_us, Ordering::Relaxed);
    }

    pub fn update_vector_stats(&self, collections: u64, vectors: u64) {
        self.vector_collections
            .store(collections, Ordering::Relaxed);
        self.vector_vectors_stored.store(vectors, Ordering::Relaxed);
    }

    // ── Graph engine ──

    pub fn record_graph_traversal(&self) {
        self.graph_traversals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_graph_stats(&self, nodes: u64, edges: u64) {
        self.graph_nodes.store(nodes, Ordering::Relaxed);
        self.graph_edges.store(edges, Ordering::Relaxed);
    }

    // ── Document engine ──

    pub fn record_document_insert(&self) {
        self.document_inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_document_read(&self) {
        self.document_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_document_collections(&self, count: u64) {
        self.document_collections.store(count, Ordering::Relaxed);
    }

    // ── Columnar engine ──

    pub fn update_columnar_stats(&self, segments: u64, compaction_queue: u64, ratio_x100: u64) {
        self.columnar_segments.store(segments, Ordering::Relaxed);
        self.columnar_compaction_queue
            .store(compaction_queue, Ordering::Relaxed);
        self.columnar_compression_ratio
            .store(ratio_x100, Ordering::Relaxed);
    }

    // ── FTS engine ──

    pub fn record_text_search(&self, latency_us: u64) {
        self.text_searches.fetch_add(1, Ordering::Relaxed);
        self.text_avg_latency_us
            .store(latency_us, Ordering::Relaxed);
    }

    pub fn update_text_indexes(&self, count: u64) {
        self.text_indexes.store(count, Ordering::Relaxed);
    }

    // ── KV engine ──

    pub fn record_kv_get(&self) {
        self.kv_gets_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_put(&self) {
        self.kv_puts_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_delete(&self) {
        self.kv_deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_scan(&self) {
        self.kv_scans_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_kv_expiries(&self, count: u64) {
        self.kv_expiries_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn update_kv_memory(&self, bytes: u64) {
        self.kv_memory_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn update_kv_keys(&self, count: u64) {
        self.kv_total_keys.store(count, Ordering::Relaxed);
    }

    // ── Data Plane ──

    pub fn record_io_uring_submission(&self) {
        self.io_uring_submissions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_io_uring_completion(&self) {
        self.io_uring_completions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_tpc_utilization(&self, pct: u64) {
        self.tpc_utilization_pct.store(pct, Ordering::Relaxed);
    }

    pub fn update_arena_memory(&self, bytes: u64) {
        self.arena_memory_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Contention ──

    pub fn record_mmap_fault(&self) {
        self.mmap_major_faults.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_throttle(&self) {
        self.throttle_activations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_contention(&self) {
        self.cache_contention_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_nvme_queue_depth(&self, depth: u64) {
        self.nvme_queue_depth.store(depth, Ordering::Relaxed);
    }

    // ── Storage tiers ──

    pub fn update_storage_tiers(&self, l0: u64, l1: u64, l2: u64) {
        self.storage_l0_bytes.store(l0, Ordering::Relaxed);
        self.storage_l1_bytes.store(l1, Ordering::Relaxed);
        self.storage_l2_bytes.store(l2, Ordering::Relaxed);
    }

    pub fn update_mmap_rss(&self, bytes: u64) {
        self.mmap_rss_bytes.store(bytes, Ordering::Relaxed);
    }

    // ── Catalog sanity check ──

    /// Record the outcome of one registry's catalog sanity check.
    ///
    /// `outcome` must be `"ok"`, `"warning"`, or `"error"`.
    pub fn record_catalog_sanity_check(&self, registry: &str, outcome: &str) {
        let mut m = self
            .catalog_sanity_check_totals
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry((registry.to_string(), outcome.to_string()))
            .or_insert(0) += 1;
    }

    /// Record the duration of a single shutdown phase.
    ///
    /// Called by `ShutdownBus::initiate()` after each phase drains.
    /// The value is overwritten on each shutdown so `/metrics` always
    /// shows the most recent run.
    pub fn record_shutdown_phase_duration(&self, phase: &str, duration_ms: u64) {
        let mut m = self
            .shutdown_phase_durations_ms
            .write()
            .unwrap_or_else(|p| p.into_inner());
        m.insert(phase.to_string(), duration_ms);
    }

    /// Serialize all metrics as Prometheus text format 0.0.4.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(8192);
        self.prometheus_core(&mut out);
        self.prometheus_engines(&mut out);
        self.prometheus_catalog_sanity(&mut out);
        self.prometheus_shutdown_phases(&mut out);
        out
    }

    /// Emit `shutdown_last_duration_ms{phase}` gauges.
    fn prometheus_shutdown_phases(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .shutdown_phase_durations_ms
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP shutdown_last_duration_ms Duration of each shutdown phase in the last graceful shutdown\n\
             # TYPE shutdown_last_duration_ms gauge\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (phase, ms) in pairs {
            let _ = writeln!(out, r#"shutdown_last_duration_ms{{phase="{phase}"}} {ms}"#);
        }
    }

    /// Emit `catalog_sanity_check_total{registry,outcome}` labeled counters.
    fn prometheus_catalog_sanity(&self, out: &mut String) {
        use std::fmt::Write as _;
        let m = self
            .catalog_sanity_check_totals
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if m.is_empty() {
            return;
        }
        let _ = out.write_str(
            "# HELP catalog_sanity_check_total Catalog sanity check outcomes per registry\n\
             # TYPE catalog_sanity_check_total counter\n",
        );
        let mut pairs: Vec<_> = m.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for ((registry, outcome), count) in pairs {
            let _ = writeln!(
                out,
                r#"catalog_sanity_check_total{{registry="{registry}",outcome="{outcome}"}} {count}"#
            );
        }
    }
}
