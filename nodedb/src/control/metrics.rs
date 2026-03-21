//! Required metrics baseline for operational monitoring.
//!
//! Defines the core metrics that MUST be collected for production:
//! - WAL fsync latency
//! - Raft apply lag
//! - SPSC queue utilization
//! - Compaction debt (pending segments)
//! - Per-tenant quota usage
//! - Failed auth count
//!
//! Metrics are collected from TelemetryRing buffers (Data Plane)
//! and Control Plane state. Served via HTTP endpoint.

use std::sync::atomic::{AtomicU64, Ordering};

/// Core metrics collected across the system.
#[derive(Debug, Default)]
pub struct SystemMetrics {
    /// WAL fsync latency — last observed duration in microseconds.
    pub wal_fsync_latency_us: AtomicU64,
    /// WAL fsync count.
    pub wal_fsync_count: AtomicU64,
    /// Raft apply lag — entries behind leader.
    pub raft_apply_lag: AtomicU64,
    /// SPSC bridge utilization — percentage (0-100).
    pub bridge_utilization: AtomicU64,
    /// Compaction debt — number of L1 segments pending compaction.
    pub compaction_debt: AtomicU64,
    /// Compaction cycles completed.
    pub compaction_cycles: AtomicU64,
    /// Failed authentication attempts.
    pub auth_failures: AtomicU64,
    /// Successful authentication attempts.
    pub auth_successes: AtomicU64,
    /// Active connections count.
    pub active_connections: AtomicU64,
    /// Total queries executed.
    pub queries_total: AtomicU64,
    /// Query errors.
    pub query_errors: AtomicU64,
    /// Vector search count.
    pub vector_searches: AtomicU64,
    /// Graph traversal count.
    pub graph_traversals: AtomicU64,
    /// Text search count.
    pub text_searches: AtomicU64,
    /// Checkpoint count.
    pub checkpoints: AtomicU64,

    // ── Contention metrics ──
    /// mmap major page faults observed across all cores.
    pub mmap_major_faults: AtomicU64,
    /// NVMe queue depth (pending io_uring submissions).
    pub nvme_queue_depth: AtomicU64,
    /// Throttle activation count (backpressure engaged).
    pub throttle_activations: AtomicU64,
    /// Cache contention events (L3 miss proxy — estimated from
    /// SPSC bridge latency spikes above p95 threshold).
    pub cache_contention_events: AtomicU64,

    // ── Subscription metrics ──
    /// Active WebSocket subscriptions.
    pub active_subscriptions: AtomicU64,
    /// Active LISTEN channels across all pgwire connections.
    pub active_listen_channels: AtomicU64,
    /// Change events delivered to subscribers.
    pub change_events_delivered: AtomicU64,
    /// Change events dropped (slow consumer backpressure).
    pub change_events_dropped: AtomicU64,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a WAL fsync latency observation.
    pub fn record_wal_fsync(&self, duration_us: u64) {
        self.wal_fsync_latency_us
            .store(duration_us, Ordering::Relaxed);
        self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a Raft apply lag observation.
    pub fn record_raft_lag(&self, lag: u64) {
        self.raft_apply_lag.store(lag, Ordering::Relaxed);
    }

    /// Record bridge utilization.
    pub fn record_bridge_utilization(&self, pct: u64) {
        self.bridge_utilization.store(pct, Ordering::Relaxed);
    }

    /// Record an authentication failure.
    pub fn record_auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authentication success.
    pub fn record_auth_success(&self) {
        self.auth_successes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a query execution.
    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a query error.
    pub fn record_query_error(&self) {
        self.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a mmap major page fault.
    pub fn record_mmap_fault(&self) {
        self.mmap_major_faults.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a throttle activation (backpressure engaged).
    pub fn record_throttle(&self) {
        self.throttle_activations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache contention event.
    pub fn record_cache_contention(&self) {
        self.cache_contention_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Update NVMe queue depth.
    pub fn update_nvme_queue_depth(&self, depth: u64) {
        self.nvme_queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Serialize all metrics as a Prometheus-compatible text format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        out.push_str(&format!(
            "# HELP nodedb_wal_fsync_latency_us WAL fsync latency in microseconds\n\
             # TYPE nodedb_wal_fsync_latency_us gauge\n\
             nodedb_wal_fsync_latency_us {}\n",
            self.wal_fsync_latency_us.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_wal_fsync_total WAL fsync count\n\
             # TYPE nodedb_wal_fsync_total counter\n\
             nodedb_wal_fsync_total {}\n",
            self.wal_fsync_count.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_raft_apply_lag Raft apply lag entries\n\
             # TYPE nodedb_raft_apply_lag gauge\n\
             nodedb_raft_apply_lag {}\n",
            self.raft_apply_lag.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_bridge_utilization SPSC bridge utilization percent\n\
             # TYPE nodedb_bridge_utilization gauge\n\
             nodedb_bridge_utilization {}\n",
            self.bridge_utilization.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_compaction_debt Pending L1 segments for compaction\n\
             # TYPE nodedb_compaction_debt gauge\n\
             nodedb_compaction_debt {}\n",
            self.compaction_debt.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_auth_failures_total Failed auth attempts\n\
             # TYPE nodedb_auth_failures_total counter\n\
             nodedb_auth_failures_total {}\n",
            self.auth_failures.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_queries_total Total queries executed\n\
             # TYPE nodedb_queries_total counter\n\
             nodedb_queries_total {}\n",
            self.queries_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_active_connections Active client connections\n\
             # TYPE nodedb_active_connections gauge\n\
             nodedb_active_connections {}\n",
            self.active_connections.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_mmap_major_faults mmap major page faults\n\
             # TYPE nodedb_mmap_major_faults counter\n\
             nodedb_mmap_major_faults {}\n",
            self.mmap_major_faults.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_nvme_queue_depth NVMe io_uring queue depth\n\
             # TYPE nodedb_nvme_queue_depth gauge\n\
             nodedb_nvme_queue_depth {}\n",
            self.nvme_queue_depth.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_throttle_activations Backpressure throttle activations\n\
             # TYPE nodedb_throttle_activations counter\n\
             nodedb_throttle_activations {}\n",
            self.throttle_activations.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_cache_contention_events Cache contention (L3 miss proxy)\n\
             # TYPE nodedb_cache_contention_events counter\n\
             nodedb_cache_contention_events {}\n",
            self.cache_contention_events.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_active_subscriptions Active WebSocket/LISTEN subscriptions\n\
             # TYPE nodedb_active_subscriptions gauge\n\
             nodedb_active_subscriptions {}\n",
            self.active_subscriptions.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# HELP nodedb_change_events_delivered Change events delivered\n\
             # TYPE nodedb_change_events_delivered counter\n\
             nodedb_change_events_delivered {}\n",
            self.change_events_delivered.load(Ordering::Relaxed)
        ));
        out
    }
}

/// Per-tenant quota usage tracking.
#[derive(Debug)]
pub struct TenantQuotaMetrics {
    pub tenant_id: u32,
    pub memory_bytes_used: u64,
    pub memory_bytes_limit: u64,
    pub storage_bytes_used: u64,
    pub storage_bytes_limit: u64,
    pub qps_current: u64,
    pub qps_limit: u64,
    pub connections_active: u64,
    pub connections_limit: u64,
}

impl TenantQuotaMetrics {
    /// Whether any quota is exceeded.
    pub fn is_over_quota(&self) -> bool {
        (self.memory_bytes_limit > 0 && self.memory_bytes_used > self.memory_bytes_limit)
            || (self.storage_bytes_limit > 0 && self.storage_bytes_used > self.storage_bytes_limit)
            || (self.qps_limit > 0 && self.qps_current > self.qps_limit)
            || (self.connections_limit > 0 && self.connections_active > self.connections_limit)
    }

    /// Utilization as a percentage (0-100) of the most constrained resource.
    pub fn max_utilization_pct(&self) -> u8 {
        let mut max = 0.0f64;
        if self.memory_bytes_limit > 0 {
            max = max.max(self.memory_bytes_used as f64 / self.memory_bytes_limit as f64);
        }
        if self.storage_bytes_limit > 0 {
            max = max.max(self.storage_bytes_used as f64 / self.storage_bytes_limit as f64);
        }
        if self.qps_limit > 0 {
            max = max.max(self.qps_current as f64 / self.qps_limit as f64);
        }
        (max * 100.0).min(100.0) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_recording() {
        let m = SystemMetrics::new();
        m.record_wal_fsync(150);
        m.record_auth_failure();
        m.record_auth_failure();
        m.record_query();
        m.record_query();
        m.record_query();

        assert_eq!(m.wal_fsync_latency_us.load(Ordering::Relaxed), 150);
        assert_eq!(m.auth_failures.load(Ordering::Relaxed), 2);
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn prometheus_format() {
        let m = SystemMetrics::new();
        m.record_wal_fsync(42);
        let prom = m.to_prometheus();
        assert!(prom.contains("nodedb_wal_fsync_latency_us 42"));
        assert!(prom.contains("# TYPE nodedb_wal_fsync_total counter"));
    }

    #[test]
    fn tenant_quota_check() {
        let q = TenantQuotaMetrics {
            tenant_id: 1,
            memory_bytes_used: 800,
            memory_bytes_limit: 1000,
            storage_bytes_used: 500,
            storage_bytes_limit: 1000,
            qps_current: 50,
            qps_limit: 100,
            connections_active: 5,
            connections_limit: 10,
        };
        assert!(!q.is_over_quota());
        assert_eq!(q.max_utilization_pct(), 80); // memory is 80%.

        let over = TenantQuotaMetrics {
            memory_bytes_used: 1100,
            ..q
        };
        assert!(over.is_over_quota());
    }
}
