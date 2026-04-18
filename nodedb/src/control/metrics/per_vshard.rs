//! Per-vShard QPS + latency histograms.
//!
//! Every dispatched request crosses [`dispatch_to_data_plane_with_source`]
//! in `server/dispatch_utils.rs` with its `vshard_id` already bound. That
//! site records the observed wall-time into the vshard's
//! [`PerVShardMetrics`] entry so the operator (`SHOW RANGES`), the
//! Prometheus scrape, and the rebalancer all see the same numbers.
//!
//! QPS is a lazy 1-second rolling window: every observation checks
//! whether the window has expired and, if so, CAS-closes it to compute
//! the rate. No background sampler is needed.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::histogram::AtomicHistogram;

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Standardized per-vShard observations.
#[derive(Debug)]
pub struct PerVShardMetrics {
    vshard_id: u16,
    requests_total: AtomicU64,
    latency: AtomicHistogram,
    /// Requests observed in the currently-open window.
    window_count: AtomicU64,
    /// Unix ms when the current window began.
    window_start_ms: AtomicU64,
    /// Most recently computed QPS, scaled by 100 (fixed-point).
    /// Readers divide by 100.0 to get the rate.
    recent_qps_centihz: AtomicU64,
}

/// A read-only snapshot of per-vShard metrics at a single moment.
#[derive(Debug, Clone)]
pub struct VShardStatsSnapshot {
    pub vshard_id: u16,
    pub requests_total: u64,
    pub qps: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub latency_count: u64,
    pub latency_sum_us: u64,
}

impl PerVShardMetrics {
    pub fn new(vshard_id: u16) -> Arc<Self> {
        Arc::new(Self {
            vshard_id,
            requests_total: AtomicU64::new(0),
            latency: AtomicHistogram::new(),
            window_count: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(unix_now_ms()),
            recent_qps_centihz: AtomicU64::new(0),
        })
    }

    pub fn vshard_id(&self) -> u16 {
        self.vshard_id
    }

    /// Record one completed request. Called from the Control-Plane
    /// dispatch site with the measured wall-time; safe for concurrent
    /// callers across tokio tasks (all fields are atomic).
    pub fn observe(&self, latency_us: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.latency.observe(latency_us);
        let now = unix_now_ms();
        let start = self.window_start_ms.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(start);
        if elapsed >= 1_000 {
            // CAS-close the window. If another observer won the race
            // we simply fall through and increment window_count;
            // nothing is lost because they captured our increment too.
            if self
                .window_start_ms
                .compare_exchange(start, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                let captured = self.window_count.swap(1, Ordering::Relaxed);
                let qps = captured as f64 * 1_000.0 / elapsed.max(1) as f64;
                self.recent_qps_centihz
                    .store((qps * 100.0) as u64, Ordering::Relaxed);
                return;
            }
        }
        self.window_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    /// Most recently computed QPS. Zero until the first window closes
    /// (~1s of live traffic).
    pub fn qps(&self) -> f64 {
        self.recent_qps_centihz.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn snapshot(&self) -> VShardStatsSnapshot {
        VShardStatsSnapshot {
            vshard_id: self.vshard_id,
            requests_total: self.requests_total(),
            qps: self.qps(),
            p50_us: self.latency.percentile(0.50),
            p95_us: self.latency.percentile(0.95),
            p99_us: self.latency.percentile(0.99),
            latency_count: self.latency.count(),
            latency_sum_us: self.latency.sum_us(),
        }
    }
}

/// Registry of [`PerVShardMetrics`] keyed by vshard id.
///
/// Entries are created lazily on first observation. A registry
/// instance lives for the life of the process inside `SharedState`.
#[derive(Debug, Default)]
pub struct PerVShardMetricsRegistry {
    map: RwLock<HashMap<u16, Arc<PerVShardMetrics>>>,
}

impl PerVShardMetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Return (creating if needed) the metrics entry for `vshard_id`.
    pub fn get_or_create(&self, vshard_id: u16) -> Arc<PerVShardMetrics> {
        // Fast path: read lock only.
        if let Ok(g) = self.map.read()
            && let Some(m) = g.get(&vshard_id)
        {
            return Arc::clone(m);
        }
        let mut g = self.map.write().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            g.entry(vshard_id)
                .or_insert_with(|| PerVShardMetrics::new(vshard_id)),
        )
    }

    /// Record one observation. Convenience helper used by the dispatch
    /// site so call sites don't have to hold the `Arc` themselves.
    pub fn observe(&self, vshard_id: u16, latency_us: u64) {
        self.get_or_create(vshard_id).observe(latency_us);
    }

    pub fn snapshot(&self, vshard_id: u16) -> Option<VShardStatsSnapshot> {
        let g = self.map.read().unwrap_or_else(|p| p.into_inner());
        g.get(&vshard_id).map(|m| m.snapshot())
    }

    /// Snapshot every registered vshard, sorted by id for deterministic
    /// output (used by `SHOW RANGES` and by Prometheus rendering).
    pub fn all_snapshots(&self) -> Vec<VShardStatsSnapshot> {
        let g = self.map.read().unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<VShardStatsSnapshot> = g.values().map(|m| m.snapshot()).collect();
        out.sort_by_key(|s| s.vshard_id);
        out
    }

    /// Render Prometheus exposition for every tracked vshard. Emits
    /// `nodedb_vshard_requests_total{vshard_id}` counter +
    /// `nodedb_vshard_qps{vshard_id}` gauge + a per-vshard histogram.
    pub fn render_prom(&self, out: &mut String) {
        use std::fmt::Write as _;
        let snaps = self.all_snapshots();
        if snaps.is_empty() {
            return;
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_vshard_requests_total Total requests dispatched to each vshard."
        );
        let _ = writeln!(out, "# TYPE nodedb_vshard_requests_total counter");
        for s in &snaps {
            let _ = writeln!(
                out,
                "nodedb_vshard_requests_total{{vshard_id=\"{}\"}} {}",
                s.vshard_id, s.requests_total
            );
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_vshard_qps Current queries-per-second rate for each vshard (1s window)."
        );
        let _ = writeln!(out, "# TYPE nodedb_vshard_qps gauge");
        for s in &snaps {
            let _ = writeln!(
                out,
                "nodedb_vshard_qps{{vshard_id=\"{}\"}} {}",
                s.vshard_id, s.qps
            );
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_vshard_latency_p99_seconds p99 dispatch latency per vshard."
        );
        let _ = writeln!(out, "# TYPE nodedb_vshard_latency_p99_seconds gauge");
        for s in &snaps {
            let _ = writeln!(
                out,
                "nodedb_vshard_latency_p99_seconds{{vshard_id=\"{}\"}} {}",
                s.vshard_id,
                s.p99_us as f64 / 1_000_000.0
            );
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_vshard_latency_p95_seconds p95 dispatch latency per vshard."
        );
        let _ = writeln!(out, "# TYPE nodedb_vshard_latency_p95_seconds gauge");
        for s in &snaps {
            let _ = writeln!(
                out,
                "nodedb_vshard_latency_p95_seconds{{vshard_id=\"{}\"}} {}",
                s.vshard_id,
                s.p95_us as f64 / 1_000_000.0
            );
        }
        out.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn observe_increments_total_and_latency() {
        let m = PerVShardMetrics::new(7);
        assert_eq!(m.requests_total(), 0);
        m.observe(100);
        m.observe(500);
        assert_eq!(m.requests_total(), 2);
        let s = m.snapshot();
        assert_eq!(s.vshard_id, 7);
        assert_eq!(s.latency_count, 2);
        assert_eq!(s.latency_sum_us, 600);
    }

    #[test]
    fn qps_computes_after_window_elapses() {
        let m = PerVShardMetrics::new(0);
        // Burst observations in the initial window.
        for _ in 0..50 {
            m.observe(10);
        }
        // Before the 1-second window closes, recent qps is still 0.
        assert_eq!(m.qps(), 0.0);
        thread::sleep(Duration::from_millis(1_100));
        // Next observation closes the previous window.
        m.observe(10);
        let qps = m.qps();
        assert!(qps > 0.0, "qps should have been computed, got {qps}");
    }

    #[test]
    fn registry_is_lazy_and_deterministic() {
        let reg = PerVShardMetricsRegistry::new();
        reg.observe(3, 100);
        reg.observe(1, 200);
        reg.observe(3, 300);
        let all = reg.all_snapshots();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].vshard_id, 1);
        assert_eq!(all[1].vshard_id, 3);
        assert_eq!(all[1].requests_total, 2);
    }

    #[test]
    fn render_prom_emits_every_vshard_for_every_metric() {
        let reg = PerVShardMetricsRegistry::new();
        reg.observe(2, 500);
        reg.observe(4, 1_500);
        let mut out = String::new();
        reg.render_prom(&mut out);
        assert!(out.contains("nodedb_vshard_requests_total{vshard_id=\"2\"} 1"));
        assert!(out.contains("nodedb_vshard_requests_total{vshard_id=\"4\"} 1"));
        assert!(out.contains("nodedb_vshard_qps{vshard_id=\"2\"}"));
        assert!(out.contains("nodedb_vshard_latency_p99_seconds{vshard_id=\"4\"}"));
    }

    #[test]
    fn render_prom_empty_registry_writes_nothing() {
        let reg = PerVShardMetricsRegistry::new();
        let mut out = String::new();
        reg.render_prom(&mut out);
        assert!(out.is_empty());
    }
}
