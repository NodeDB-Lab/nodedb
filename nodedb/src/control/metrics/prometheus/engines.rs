// SPDX-License-Identifier: BUSL-1.1

//! Prometheus serialization: per-engine metrics, data plane, storage, subscriptions.

use std::sync::atomic::Ordering;

use super::core::{counter, gauge, gauge_f64};
use crate::control::metrics::system::SystemMetrics;

impl SystemMetrics {
    /// Serialize engine-specific and infrastructure metrics as Prometheus text format.
    ///
    /// Called by [`to_prometheus()`](Self::to_prometheus) — not directly by handlers.
    pub(in crate::control::metrics) fn prometheus_engines(&self, out: &mut String) {
        // ── Vector engine ──
        counter(
            out,
            "nodedb_vector_searches_total",
            "Vector search operations",
            self.vector_searches.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_vector_collections",
            "Vector collections",
            self.vector_collections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_vector_vectors_stored",
            "Vectors stored",
            self.vector_vectors_stored.load(Ordering::Relaxed),
        );
        self.vector_query_seconds.write_prometheus(
            out,
            "nodedb_vector_query_seconds",
            "Vector search latency distribution in seconds",
        );

        // ── Graph engine ──
        counter(
            out,
            "nodedb_graph_traversals_total",
            "Graph traversal operations",
            self.graph_traversals.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_graph_nodes",
            "Graph nodes stored",
            self.graph_nodes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_graph_edges",
            "Graph edges stored",
            self.graph_edges.load(Ordering::Relaxed),
        );

        // ── Document engine ──
        counter(
            out,
            "nodedb_document_inserts_total",
            "Document insert operations",
            self.document_inserts.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_document_reads_total",
            "Document read operations",
            self.document_reads.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_document_collections",
            "Document collections",
            self.document_collections.load(Ordering::Relaxed),
        );

        // ── Columnar engine ──
        gauge(
            out,
            "nodedb_columnar_segments",
            "Columnar segments",
            self.columnar_segments.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_columnar_compaction_queue",
            "Columnar compaction queue depth",
            self.columnar_compaction_queue.load(Ordering::Relaxed),
        );
        gauge_f64(
            out,
            "nodedb_columnar_compression_ratio",
            "Columnar compression ratio",
            self.columnar_compression_ratio.load(Ordering::Relaxed) as f64 / 100.0,
        );

        // ── FTS engine ──
        counter(
            out,
            "nodedb_fts_searches_total",
            "Full-text search operations",
            self.fts_searches.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_fts_indexes",
            "Full-text search indexes",
            self.fts_indexes.load(Ordering::Relaxed),
        );
        self.fts_query_seconds.write_prometheus(
            out,
            "nodedb_fts_query_seconds",
            "Full-text search latency distribution in seconds",
        );

        // ── KV engine ──
        counter(
            out,
            "nodedb_kv_gets_total",
            "KV GET operations",
            self.kv_gets_total.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_kv_puts_total",
            "KV PUT operations",
            self.kv_puts_total.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_kv_deletes_total",
            "KV DELETE operations",
            self.kv_deletes_total.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_kv_scans_total",
            "KV SCAN operations",
            self.kv_scans_total.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_kv_expiries_total",
            "KV keys expired",
            self.kv_expiries_total.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_kv_memory_bytes",
            "KV engine memory usage",
            self.kv_memory_bytes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_kv_total_keys",
            "Total KV keys",
            self.kv_total_keys.load(Ordering::Relaxed),
        );

        // ── Data Plane ──
        counter(
            out,
            "nodedb_io_uring_submissions_total",
            "io_uring submissions",
            self.io_uring_submissions.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_io_uring_completions_total",
            "io_uring completions",
            self.io_uring_completions.load(Ordering::Relaxed),
        );
        gauge_f64(
            out,
            "nodedb_tpc_utilization_ratio",
            "TPC event loop utilization as a fraction (0.0–1.0)",
            self.tpc_utilization_ratio.load(Ordering::Relaxed) as f64 / 100.0,
        );
        gauge(
            out,
            "nodedb_arena_memory_bytes",
            "Per-core arena memory bytes",
            self.arena_memory_bytes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_active_txn_overlays",
            "Live per-transaction staging overlays across all Data-Plane cores",
            self.active_txn_overlays.load(Ordering::Relaxed),
        );

        // ── Contention ──
        counter(
            out,
            "nodedb_mmap_major_faults_total",
            "mmap major page faults",
            self.mmap_major_faults.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_nvme_queue_depth",
            "NVMe io_uring queue depth",
            self.nvme_queue_depth.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_throttle_activations_total",
            "Backpressure throttle activations",
            self.throttle_activations.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_cache_contention_events_total",
            "Cache contention events",
            self.cache_contention_events.load(Ordering::Relaxed),
        );

        // ── Storage tiers ──
        gauge(
            out,
            "nodedb_storage_l0_bytes",
            "L0 (hot/RAM) storage bytes",
            self.storage_l0_bytes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_storage_l1_bytes",
            "L1 (warm/NVMe) storage bytes",
            self.storage_l1_bytes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_storage_l2_bytes",
            "L2 (cold/S3) storage bytes",
            self.storage_l2_bytes.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_mmap_rss_bytes",
            "mmap resident set size bytes",
            self.mmap_rss_bytes.load(Ordering::Relaxed),
        );

        // ── Subscriptions ──
        gauge(
            out,
            "nodedb_active_subscriptions",
            "Active WebSocket/LISTEN subscriptions",
            self.active_subscriptions.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_active_listen_channels",
            "Active LISTEN channels",
            self.active_listen_channels.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_change_events_delivered_total",
            "Change events delivered",
            self.change_events_delivered.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_change_events_dropped_total",
            "Change events dropped",
            self.change_events_dropped.load(Ordering::Relaxed),
        );

        // ── Checkpoints ──
        counter(
            out,
            "nodedb_checkpoints_total",
            "Checkpoints completed",
            self.checkpoints.load(Ordering::Relaxed),
        );
    }
}
