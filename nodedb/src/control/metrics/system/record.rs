// SPDX-License-Identifier: BUSL-1.1

//! `SystemMetrics` record/update methods.

use std::sync::atomic::{AtomicU64, Ordering};

use super::fields::SystemMetrics;
use super::graph_count::GraphCount;

/// Move a cross-core gauge by one core's change from `before` to `after`.
fn move_gauge(gauge: &AtomicU64, before: u64, after: u64) {
    if after > before {
        gauge.fetch_add(after - before, Ordering::Relaxed);
    } else if before > after {
        gauge.fetch_sub(before - after, Ordering::Relaxed);
    }
}

impl SystemMetrics {
    // ── WAL ──

    pub fn record_wal_fsync(&self, duration_us: u64) {
        self.wal_fsync_seconds.observe(duration_us);
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

    pub fn update_compaction(&self, debt: u64, bytes_written: u64) {
        self.compaction_debt.store(debt, Ordering::Relaxed);
        self.compaction_bytes_total
            .fetch_add(bytes_written, Ordering::Relaxed);
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
        self.query_planning_seconds.observe(planning_us);
        self.query_execution_seconds.observe(execution_us);
    }

    pub fn record_query_by_engine(&self, engine: &str) {
        match engine {
            "vector" => self.queries_vector.fetch_add(1, Ordering::Relaxed),
            "graph" => self.queries_graph.fetch_add(1, Ordering::Relaxed),
            "document_schemaless" | "document_strict" => {
                self.queries_document.fetch_add(1, Ordering::Relaxed)
            }
            "columnar" => self.queries_columnar.fetch_add(1, Ordering::Relaxed),
            "kv" => self.queries_kv.fetch_add(1, Ordering::Relaxed),
            "fts" => self.queries_fts.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    // ── Vector engine ──

    pub fn record_vector_search(&self, latency_us: u64) {
        self.vector_searches.fetch_add(1, Ordering::Relaxed);
        self.vector_query_seconds.observe(latency_us);
    }

    pub fn record_vector_build_started(&self) {
        self.vector_builds_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_vector_build_completed(&self) {
        self.vector_builds_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_vector_build_failed(&self) {
        self.vector_builds_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_vector_build_deferred(&self) {
        self.vector_builds_deferred.fetch_add(1, Ordering::Relaxed);
    }

    /// Move the cross-core pending-build gauge from one core's previous
    /// count `before` to its current count `after`.
    pub fn move_vector_build_pending(&self, before: u64, after: u64) {
        move_gauge(&self.vector_build_pending, before, after);
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

    /// Move the cross-core node and edge gauges from one core's previous
    /// count `before` to its current count `after`. Each core reports only
    /// its own share, so the gauge moves by the change and never takes a
    /// stored value.
    pub fn update_graph_stats(&self, before: GraphCount, after: GraphCount) {
        move_gauge(&self.graph_nodes, before.nodes, after.nodes);
        move_gauge(&self.graph_edges, before.edges, after.edges);
    }

    pub fn record_graph_edges_written(&self, count: u64) {
        self.graph_edges_written.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_graph_edges_deleted(&self, count: u64) {
        self.graph_edges_deleted.fetch_add(count, Ordering::Relaxed);
    }

    // ── Document engine ──

    pub fn record_document_insert(&self) {
        self.document_inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_document_read(&self) {
        self.document_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_document_index_backfill(&self) {
        self.document_index_backfills
            .fetch_add(1, Ordering::Relaxed);
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

    pub fn record_fts_search(&self, latency_us: u64) {
        self.fts_searches.fetch_add(1, Ordering::Relaxed);
        self.fts_query_seconds.observe(latency_us);
    }

    pub fn update_fts_indexes(&self, count: u64) {
        self.fts_indexes.store(count, Ordering::Relaxed);
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
        self.tpc_utilization_ratio.store(pct, Ordering::Relaxed);
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

    // ── CDC per-stream drops ──

    /// Record `count` events evicted from a specific CDC stream buffer.
    ///
    /// Increments both the global `change_events_dropped` counter (backward
    /// compatibility) and the per-stream labelled map used for Prometheus
    /// `nodedb_cdc_events_dropped_total{tenant, stream}`.
    pub fn record_cdc_stream_drop(&self, tenant_id: u64, stream_name: &str, count: u64) {
        self.change_events_dropped
            .fetch_add(count, Ordering::Relaxed);
        let mut m = self
            .cdc_events_dropped_by_stream
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry((tenant_id, stream_name.to_string())).or_insert(0) += count;
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

    /// Increment the Critical-pressure counter for the given engine.
    ///
    /// Called by the Data Plane pressure check on every Critical-branch fire.
    pub fn record_backpressure_critical(&self, engine: &str) {
        let mut m = self
            .backpressure_critical_by_engine
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry(engine.to_string()).or_insert(0) += 1;
    }

    /// Increment the Emergency-pressure counter for the given engine.
    ///
    /// Called by the Data Plane pressure check on every Emergency-branch fire.
    pub fn record_backpressure_emergency(&self, engine: &str) {
        let mut m = self
            .backpressure_emergency_by_engine
            .write()
            .unwrap_or_else(|p| p.into_inner());
        *m.entry(engine.to_string()).or_insert(0) += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric_line(out: &str, name: &str) -> Option<u64> {
        out.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(' '))
                .and_then(|value| value.trim().parse().ok())
        })
    }

    #[test]
    fn edge_write_counters_add_their_counts_and_render_as_counters() {
        let metrics = SystemMetrics::new();
        metrics.record_graph_edges_written(3);
        metrics.record_graph_edges_written(1);
        metrics.record_graph_edges_deleted(2);

        let out = metrics.to_prometheus();
        assert_eq!(
            metric_line(&out, "nodedb_graph_edges_written_total"),
            Some(4)
        );
        assert_eq!(
            metric_line(&out, "nodedb_graph_edges_deleted_total"),
            Some(2)
        );
        assert!(out.contains("# TYPE nodedb_graph_edges_written_total counter"));
        assert!(out.contains("# TYPE nodedb_graph_edges_deleted_total counter"));
    }

    /// Each core moves the gauges by its own change, so two cores' shares
    /// add up and a shrinking share subtracts only its own difference.
    #[test]
    fn graph_stats_gauges_sum_every_cores_share() {
        let metrics = SystemMetrics::new();
        let zero = GraphCount::default();
        let core_a = GraphCount { nodes: 3, edges: 5 };
        let core_b = GraphCount { nodes: 2, edges: 1 };
        metrics.update_graph_stats(zero, core_a);
        metrics.update_graph_stats(zero, core_b);
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 6);

        let core_a_after = GraphCount { nodes: 1, edges: 2 };
        metrics.update_graph_stats(core_a, core_a_after);
        assert_eq!(metrics.graph_nodes.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.graph_edges.load(Ordering::Relaxed), 3);

        let out = metrics.to_prometheus();
        assert_eq!(metric_line(&out, "nodedb_graph_nodes"), Some(3));
        assert_eq!(metric_line(&out, "nodedb_graph_edges"), Some(3));
    }
}
