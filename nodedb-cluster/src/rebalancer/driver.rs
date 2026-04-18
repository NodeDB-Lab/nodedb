//! Rebalancer driver loop.
//!
//! [`RebalancerLoop`] is the active half of the load-based rebalancer.
//! Every `interval` it walks this sequence:
//!
//! 1. Ask the injected `ElectionGate` whether any raft group is
//!    currently mid-election. If so, skip this tick entirely —
//!    moves during an election race with the new leader's log and
//!    are almost guaranteed to be wasted work.
//! 2. Ask the injected [`LoadMetricsProvider`] for a snapshot of
//!    every node's current load metrics.
//! 3. Call [`compute_load_based_plan`] against the live routing +
//!    topology with the configured plan config. If the plan is
//!    empty (cluster within threshold, or no cold candidates), do
//!    nothing.
//! 4. Dispatch each planned move through the injected
//!    [`MigrationDispatcher`], fire-and-forget. The dispatcher is
//!    where the bridge to the production `MigrationExecutor` lives
//!    — tests use a mock that records the calls.
//!
//! The loop holds no state of its own; the dispatcher tracks
//! in-flight work and the breaker/scheduler state is on the
//! underlying subsystems. This keeps the driver trivially
//! restartable: crash mid-tick, respawn, resume.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::{Notify, watch};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::loop_metrics::LoopMetrics;
use crate::rebalance::PlannedMove;
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

use super::metrics::LoadMetricsProvider;
use super::plan::{RebalancerPlanConfig, compute_load_based_plan};

/// Injection seam: tells the driver whether it's safe to dispatch
/// moves. Production wraps a `MultiRaft` status probe; tests return
/// a constant boolean.
#[async_trait]
pub trait ElectionGate: Send + Sync {
    /// Return `true` if **any** raft group is currently holding an
    /// election (no stable leader). The driver skips its tick when
    /// this is `true`.
    async fn any_group_electing(&self) -> bool;
}

/// Permissive gate that never blocks the driver. Useful in tests
/// and in single-node clusters where elections are instantaneous.
pub struct AlwaysReadyGate;

#[async_trait]
impl ElectionGate for AlwaysReadyGate {
    async fn any_group_electing(&self) -> bool {
        false
    }
}

/// Injection seam: executes a single planned move. Production
/// wraps `MigrationExecutor::execute` and reports success/failure
/// via logging + the tracker; tests record the move.
#[async_trait]
pub trait MigrationDispatcher: Send + Sync {
    async fn dispatch(&self, mv: PlannedMove) -> Result<()>;
}

/// Configuration for [`RebalancerLoop`].
#[derive(Debug, Clone)]
pub struct RebalancerLoopConfig {
    /// Period between rebalance sweeps. Defaults to 30 s.
    pub interval: Duration,
    /// Plan computation config propagated to
    /// [`compute_load_based_plan`] on every tick.
    pub plan: RebalancerPlanConfig,
    /// CPU utilization threshold (0.0–1.0) above which the
    /// rebalancer pauses to avoid amplifying load. If ANY node in
    /// the metrics snapshot exceeds this value, the sweep is skipped
    /// and a STATUS event is logged. Default 0.80 (80%).
    pub backpressure_cpu_threshold: f64,
}

impl Default for RebalancerLoopConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            plan: RebalancerPlanConfig::default(),
            backpressure_cpu_threshold: 0.80,
        }
    }
}

/// The driver itself.
pub struct RebalancerLoop {
    cfg: RebalancerLoopConfig,
    metrics: Arc<dyn LoadMetricsProvider>,
    dispatcher: Arc<dyn MigrationDispatcher>,
    gate: Arc<dyn ElectionGate>,
    routing: Arc<RwLock<RoutingTable>>,
    topology: Arc<RwLock<ClusterTopology>>,
    /// Standardized loop observations (iterations, last-iteration
    /// duration, errors by kind, up flag). Register this handle with
    /// the cluster's [`LoopMetricsRegistry`](crate::LoopMetricsRegistry)
    /// so scrapes include its samples.
    loop_metrics: Arc<LoopMetrics>,
    /// Membership-change notification. When any caller (a SWIM
    /// subscriber, a manual admin trigger, etc.) calls
    /// [`notify`](Notify::notify_one) on this handle, the run loop
    /// wakes up immediately and runs an extra sweep instead of
    /// waiting for the next 30 s tick.
    kick: Arc<Notify>,
}

impl RebalancerLoop {
    pub fn new(
        cfg: RebalancerLoopConfig,
        metrics: Arc<dyn LoadMetricsProvider>,
        dispatcher: Arc<dyn MigrationDispatcher>,
        gate: Arc<dyn ElectionGate>,
        routing: Arc<RwLock<RoutingTable>>,
        topology: Arc<RwLock<ClusterTopology>>,
    ) -> Self {
        Self {
            cfg,
            metrics,
            dispatcher,
            gate,
            routing,
            topology,
            loop_metrics: LoopMetrics::new("rebalancer_loop"),
            kick: Arc::new(Notify::new()),
        }
    }

    /// Return a handle that callers can use to trigger an immediate
    /// sweep. Cloning the `Arc<Notify>` is cheap; every clone
    /// shares the same waker.
    pub fn kick_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.kick)
    }

    /// Shared handle to this loop's standardized metrics. Lifecycle
    /// owners register this with the cluster registry on spawn.
    pub fn loop_metrics(&self) -> Arc<LoopMetrics> {
        Arc::clone(&self.loop_metrics)
    }

    /// Run the driver until `shutdown` flips to `true`.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = interval(self.cfg.interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Consume the immediate first tick so the first sweep fires
        // a full interval after start. Prevents start-up stampedes
        // when many nodes restart together.
        tick.tick().await;
        self.loop_metrics.set_up(true);
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
                _ = tick.tick() => {
                    self.sweep_once().await;
                }
                _ = self.kick.notified() => {
                    debug!("rebalancer: membership-change kick received");
                    self.sweep_once().await;
                }
            }
        }
        self.loop_metrics.set_up(false);
        debug!("rebalancer loop shutting down");
    }

    /// Run a single sweep. Exposed for tests that drive the loop
    /// manually rather than through `run`.
    pub async fn sweep_once(&self) {
        let started = Instant::now();
        if self.gate.any_group_electing().await {
            debug!("rebalancer: raft election in progress, skipping tick");
            self.loop_metrics.observe(started.elapsed());
            return;
        }
        let metrics = match self.metrics.snapshot().await {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "rebalancer: failed to collect metrics");
                self.loop_metrics.record_error("metrics_snapshot");
                self.loop_metrics.observe(started.elapsed());
                return;
            }
        };
        if let Some(hot) = metrics
            .iter()
            .find(|m| m.cpu_utilization > self.cfg.backpressure_cpu_threshold)
        {
            info!(
                node_id = hot.node_id,
                cpu = format!("{:.0}%", hot.cpu_utilization * 100.0),
                threshold = format!("{:.0}%", self.cfg.backpressure_cpu_threshold * 100.0),
                "rebalancer: back-pressure — cluster under load, skipping sweep"
            );
            self.loop_metrics.record_error("backpressure");
            self.loop_metrics.observe(started.elapsed());
            return;
        }
        let plan = {
            let routing = self.routing.read().unwrap_or_else(|p| p.into_inner());
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            compute_load_based_plan(&metrics, &routing, &topo, &self.cfg.plan)
        };
        if plan.is_empty() {
            debug!("rebalancer: no moves needed this tick");
            self.loop_metrics.observe(started.elapsed());
            return;
        }
        info!(
            move_count = plan.len(),
            "rebalancer: dispatching planned moves"
        );
        let dispatcher = Arc::clone(&self.dispatcher);
        let err_metrics = Arc::clone(&self.loop_metrics);
        for mv in plan {
            let dispatcher = Arc::clone(&dispatcher);
            let err_metrics = Arc::clone(&err_metrics);
            tokio::spawn(async move {
                if let Err(e) = dispatcher.dispatch(mv).await {
                    warn!(error = %e, "rebalancer: dispatch failed");
                    err_metrics.record_error("dispatch");
                }
            });
        }
        self.loop_metrics.observe(started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rebalancer::metrics::LoadMetrics;
    use crate::topology::{NodeInfo, NodeState};
    use std::net::SocketAddr;
    use std::sync::Mutex;

    struct StaticMetrics(Vec<LoadMetrics>);

    #[async_trait]
    impl LoadMetricsProvider for StaticMetrics {
        async fn snapshot(&self) -> Result<Vec<LoadMetrics>> {
            Ok(self.0.clone())
        }
    }

    struct RecordingDispatcher {
        calls: Mutex<Vec<PlannedMove>>,
    }

    impl RecordingDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }
        fn take(&self) -> Vec<PlannedMove> {
            let mut g = self.calls.lock().unwrap();
            let out = g.clone();
            g.clear();
            out
        }
    }

    #[async_trait]
    impl MigrationDispatcher for RecordingDispatcher {
        async fn dispatch(&self, mv: PlannedMove) -> Result<()> {
            self.calls.lock().unwrap().push(mv);
            Ok(())
        }
    }

    struct BlockingGate(bool);

    #[async_trait]
    impl ElectionGate for BlockingGate {
        async fn any_group_electing(&self) -> bool {
            self.0
        }
    }

    fn topo(nodes: &[u64]) -> Arc<RwLock<ClusterTopology>> {
        let mut t = ClusterTopology::new();
        for (i, id) in nodes.iter().enumerate() {
            let a: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, a, NodeState::Active));
        }
        Arc::new(RwLock::new(t))
    }

    fn routing_hot_on(node: u64) -> Arc<RwLock<RoutingTable>> {
        let mut r = RoutingTable::uniform(6, &[1, 2, 3], 1);
        for gid in 0..6 {
            r.set_leader(gid, node);
        }
        Arc::new(RwLock::new(r))
    }

    fn lm(id: u64, v: u32, bytes_mib: u64, w: f64, r: f64) -> LoadMetrics {
        LoadMetrics {
            node_id: id,
            vshards_led: v,
            bytes_stored: bytes_mib * 1_048_576,
            writes_per_sec: w,
            reads_per_sec: r,
            qps_recent: 0.0,
            p95_latency_us: 0,
            cpu_utilization: 0.0,
        }
    }

    fn hot_cluster_loop(
        gate: Arc<dyn ElectionGate>,
    ) -> (Arc<RebalancerLoop>, Arc<RecordingDispatcher>) {
        let metrics: Arc<dyn LoadMetricsProvider> = Arc::new(StaticMetrics(vec![
            lm(1, 500, 5000, 200.0, 200.0),
            lm(2, 5, 5, 5.0, 5.0),
            lm(3, 5, 5, 5.0, 5.0),
        ]));
        let dispatcher = RecordingDispatcher::new();
        let disp_dyn: Arc<dyn MigrationDispatcher> = dispatcher.clone();
        let rloop = Arc::new(RebalancerLoop::new(
            RebalancerLoopConfig {
                interval: Duration::from_millis(50),
                ..Default::default()
            },
            metrics,
            disp_dyn,
            gate,
            routing_hot_on(1),
            topo(&[1, 2, 3]),
        ));
        (rloop, dispatcher)
    }

    #[tokio::test]
    async fn sweep_dispatches_moves_when_imbalanced() {
        let (rloop, dispatcher) = hot_cluster_loop(Arc::new(AlwaysReadyGate));
        rloop.sweep_once().await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let calls = dispatcher.take();
        assert!(!calls.is_empty());
        for c in &calls {
            assert_eq!(c.source_node, 1);
        }
    }

    #[tokio::test]
    async fn sweep_skipped_during_election() {
        let (rloop, dispatcher) = hot_cluster_loop(Arc::new(BlockingGate(true)));
        rloop.sweep_once().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(dispatcher.take().is_empty());
    }

    #[tokio::test]
    async fn sweep_noop_on_balanced_cluster() {
        let metrics: Arc<dyn LoadMetricsProvider> = Arc::new(StaticMetrics(vec![
            lm(1, 50, 500, 100.0, 100.0),
            lm(2, 50, 500, 100.0, 100.0),
            lm(3, 50, 500, 100.0, 100.0),
        ]));
        let dispatcher = RecordingDispatcher::new();
        let rloop = Arc::new(RebalancerLoop::new(
            RebalancerLoopConfig::default(),
            metrics,
            dispatcher.clone() as Arc<dyn MigrationDispatcher>,
            Arc::new(AlwaysReadyGate),
            routing_hot_on(1),
            topo(&[1, 2, 3]),
        ));
        rloop.sweep_once().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(dispatcher.take().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_fires_sweeps_and_shuts_down() {
        let (rloop, dispatcher) = hot_cluster_loop(Arc::new(AlwaysReadyGate));
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn({
            let d = Arc::clone(&rloop);
            async move { d.run(rx).await }
        });
        // First tick consumed immediately by run(); advance past a
        // couple of real intervals with interleaved yields so the
        // run-loop's select + spawned dispatch tasks all get to poll.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(80)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        }
        assert!(!dispatcher.take().is_empty());

        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;
    }

    #[tokio::test]
    async fn sweep_skipped_under_cpu_backpressure() {
        let metrics: Arc<dyn LoadMetricsProvider> = Arc::new(StaticMetrics(vec![
            LoadMetrics {
                node_id: 1,
                vshards_led: 500,
                bytes_stored: 5000 * 1_048_576,
                writes_per_sec: 200.0,
                reads_per_sec: 200.0,
                qps_recent: 0.0,
                p95_latency_us: 0,
                cpu_utilization: 0.95, // above 80% threshold
            },
            lm(2, 5, 5, 5.0, 5.0),
            lm(3, 5, 5, 5.0, 5.0),
        ]));
        let dispatcher = RecordingDispatcher::new();
        let rloop = Arc::new(RebalancerLoop::new(
            RebalancerLoopConfig {
                interval: Duration::from_millis(50),
                ..Default::default()
            },
            metrics,
            dispatcher.clone() as Arc<dyn MigrationDispatcher>,
            Arc::new(AlwaysReadyGate),
            routing_hot_on(1),
            topo(&[1, 2, 3]),
        ));
        rloop.sweep_once().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            dispatcher.take().is_empty(),
            "dispatcher should not fire when cluster is under CPU backpressure"
        );
    }
}
