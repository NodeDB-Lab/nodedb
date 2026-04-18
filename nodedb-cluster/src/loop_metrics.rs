//! Standardized control-loop metrics.
//!
//! Every periodic loop in the cluster (Principle 2.4) exposes the
//! same four observations:
//!
//! - `{loop_name}_iterations_total` — counter, incremented at the end
//!   of every tick (success or failure).
//! - `{loop_name}_last_iteration_duration_seconds` — gauge, wall-time
//!   of the most recent tick.
//! - `{loop_name}_errors_total{kind}` — counter keyed by error kind.
//! - `{loop_name}_up` — gauge (0/1), set by the loop's lifecycle
//!   owner when the driver task spawns/exits.
//!
//! Loop-specific gauges (`raft_tick_loop_pending_groups`,
//! `health_loop_suspect_peers{peer_id}`, etc.) are rendered by the
//! Prometheus route directly from the owning subsystem — they are
//! not part of this primitive because their sources are not
//! uniform.
//!
//! # Usage
//!
//! A driver owns an `Arc<LoopMetrics>` and registers it with a
//! cluster-scoped [`LoopMetricsRegistry`] on spawn. Inside the tick
//! body:
//!
//! ```ignore
//! let t = Instant::now();
//! match self.sweep().await {
//!     Ok(()) => {}
//!     Err(e) => self.metrics.record_error(e.kind_label()),
//! }
//! self.metrics.observe(t.elapsed());
//! ```
//!
//! On spawn: `metrics.set_up(true)`. On graceful shutdown:
//! `metrics.set_up(false)`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Standardized per-loop observations.
#[derive(Debug)]
pub struct LoopMetrics {
    name: &'static str,
    iterations_total: AtomicU64,
    last_iteration_duration_ns: AtomicU64,
    up: AtomicBool,
    /// Error counts keyed by short kind label. Labels are caller-
    /// supplied `&'static str` so the set stays bounded and cardinality
    /// never explodes — do not pass a formatted error string here.
    errors: Mutex<BTreeMap<&'static str, u64>>,
}

impl LoopMetrics {
    /// Construct a metrics handle for a loop named `name` (e.g.
    /// `"rebalancer_loop"`). The name appears as the prefix of every
    /// emitted metric; use `snake_case` and include the `_loop`
    /// suffix to match the checklist vocabulary.
    pub fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            iterations_total: AtomicU64::new(0),
            last_iteration_duration_ns: AtomicU64::new(0),
            up: AtomicBool::new(false),
            errors: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Record a completed tick. Increments `iterations_total` and
    /// stores the duration for the `last_iteration_duration_seconds`
    /// gauge. Call regardless of success/failure — errors are tracked
    /// separately via [`record_error`](Self::record_error).
    pub fn observe(&self, duration: Duration) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.last_iteration_duration_ns.store(ns, Ordering::Relaxed);
        self.iterations_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter for an error kind. `kind` must be a short
    /// bounded label — never a formatted error string.
    pub fn record_error(&self, kind: &'static str) {
        let mut g = self.errors.lock().unwrap_or_else(|p| p.into_inner());
        *g.entry(kind).or_insert(0) += 1;
    }

    /// Mark the loop up (driver task is running) or down (driver exited
    /// or not yet spawned).
    pub fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::Relaxed);
    }

    pub fn iterations_total(&self) -> u64 {
        self.iterations_total.load(Ordering::Relaxed)
    }

    pub fn last_iteration_duration(&self) -> Duration {
        Duration::from_nanos(self.last_iteration_duration_ns.load(Ordering::Relaxed))
    }

    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::Relaxed)
    }

    pub fn errors_snapshot(&self) -> BTreeMap<&'static str, u64> {
        self.errors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Append Prometheus-format exposition for this loop to `out`.
    /// Emits HELP/TYPE headers plus one sample per metric.
    pub fn render_prom(&self, out: &mut String) {
        let n = self.name;

        let _ = writeln!(
            out,
            "# HELP {n}_iterations_total Total completed iterations of the {n} driver."
        );
        let _ = writeln!(out, "# TYPE {n}_iterations_total counter");
        let _ = writeln!(out, "{n}_iterations_total {}", self.iterations_total());

        let _ = writeln!(
            out,
            "# HELP {n}_last_iteration_duration_seconds Wall-time of the most recent {n} iteration."
        );
        let _ = writeln!(out, "# TYPE {n}_last_iteration_duration_seconds gauge");
        let _ = writeln!(
            out,
            "{n}_last_iteration_duration_seconds {}",
            self.last_iteration_duration().as_secs_f64()
        );

        let _ = writeln!(
            out,
            "# HELP {n}_up 1 if the {n} driver task is currently running, 0 otherwise."
        );
        let _ = writeln!(out, "# TYPE {n}_up gauge");
        let _ = writeln!(out, "{n}_up {}", if self.is_up() { 1 } else { 0 });

        let _ = writeln!(
            out,
            "# HELP {n}_errors_total Errors observed by {n}, by kind."
        );
        let _ = writeln!(out, "# TYPE {n}_errors_total counter");
        let snap = self.errors_snapshot();
        if snap.is_empty() {
            // Emit a zero sample with `kind="none"` so scrapes never
            // see the series disappear between ticks.
            let _ = writeln!(out, "{n}_errors_total{{kind=\"none\"}} 0");
        } else {
            for (kind, count) in snap {
                let _ = writeln!(out, "{n}_errors_total{{kind=\"{kind}\"}} {count}");
            }
        }
        out.push('\n');
    }
}

/// Collection of [`LoopMetrics`] handles so a single Prometheus render
/// pass can iterate every registered loop.
///
/// Owners (the lifecycle code that spawns drivers) call
/// [`register`](Self::register) once per driver with the driver's
/// shared metrics handle. The Prometheus route iterates via
/// [`render_prom`](Self::render_prom).
#[derive(Debug, Default)]
pub struct LoopMetricsRegistry {
    loops: Mutex<Vec<Arc<LoopMetrics>>>,
}

impl LoopMetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, metrics: Arc<LoopMetrics>) {
        let mut g = self.loops.lock().unwrap_or_else(|p| p.into_inner());
        // Idempotent: a driver that respawns shouldn't duplicate its
        // entry. Match on the interned name — there is exactly one
        // `LoopMetrics` per loop in a process.
        if g.iter().any(|m| m.name() == metrics.name()) {
            return;
        }
        g.push(metrics);
    }

    pub fn render_prom(&self, out: &mut String) {
        let loops: Vec<Arc<LoopMetrics>> = {
            let g = self.loops.lock().unwrap_or_else(|p| p.into_inner());
            g.iter().cloned().collect()
        };
        for m in loops {
            m.render_prom(out);
        }
    }

    /// Return every registered loop handle. Used by tests and by the
    /// `/cluster/debug/transport` style inspectors.
    pub fn loops(&self) -> Vec<Arc<LoopMetrics>> {
        self.loops
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_increments_count_and_sets_duration() {
        let m = LoopMetrics::new("test_loop");
        assert_eq!(m.iterations_total(), 0);
        m.observe(Duration::from_millis(10));
        assert_eq!(m.iterations_total(), 1);
        assert!(m.last_iteration_duration() >= Duration::from_millis(10));
        m.observe(Duration::from_millis(20));
        assert_eq!(m.iterations_total(), 2);
    }

    #[test]
    fn record_error_counts_by_kind() {
        let m = LoopMetrics::new("test_loop");
        m.record_error("transport");
        m.record_error("transport");
        m.record_error("plan");
        let snap = m.errors_snapshot();
        assert_eq!(snap.get("transport").copied(), Some(2));
        assert_eq!(snap.get("plan").copied(), Some(1));
    }

    #[test]
    fn up_flag_toggles() {
        let m = LoopMetrics::new("test_loop");
        assert!(!m.is_up());
        m.set_up(true);
        assert!(m.is_up());
        m.set_up(false);
        assert!(!m.is_up());
    }

    #[test]
    fn render_prom_emits_all_four_metrics() {
        let m = LoopMetrics::new("rebalancer_loop");
        m.observe(Duration::from_millis(5));
        m.record_error("plan");
        m.set_up(true);
        let mut out = String::new();
        m.render_prom(&mut out);
        assert!(out.contains("rebalancer_loop_iterations_total 1"));
        assert!(out.contains("rebalancer_loop_last_iteration_duration_seconds"));
        assert!(out.contains("rebalancer_loop_up 1"));
        assert!(out.contains("rebalancer_loop_errors_total{kind=\"plan\"} 1"));
    }

    #[test]
    fn render_prom_emits_none_sample_when_no_errors() {
        let m = LoopMetrics::new("quiet_loop");
        let mut out = String::new();
        m.render_prom(&mut out);
        assert!(out.contains("quiet_loop_errors_total{kind=\"none\"} 0"));
    }

    #[test]
    fn registry_renders_every_loop_once() {
        let reg = LoopMetricsRegistry::new();
        let a = LoopMetrics::new("a_loop");
        let b = LoopMetrics::new("b_loop");
        a.observe(Duration::from_millis(1));
        b.observe(Duration::from_millis(2));
        reg.register(Arc::clone(&a));
        reg.register(Arc::clone(&b));
        let mut out = String::new();
        reg.render_prom(&mut out);
        assert!(out.contains("a_loop_iterations_total 1"));
        assert!(out.contains("b_loop_iterations_total 1"));
    }

    #[test]
    fn registry_register_is_idempotent_by_name() {
        let reg = LoopMetricsRegistry::new();
        let m = LoopMetrics::new("dup_loop");
        reg.register(Arc::clone(&m));
        reg.register(Arc::clone(&m));
        assert_eq!(reg.loops().len(), 1);
    }
}
