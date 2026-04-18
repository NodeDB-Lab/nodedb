//! [`ReachabilityDriver`] — periodic open-breaker probe loop.
//!
//! Every `interval`, the driver asks the shared [`CircuitBreaker`]
//! for its currently-Open peer set and fires a probe at each via the
//! injected [`ReachabilityProber`]. Probes run in parallel via
//! `tokio::spawn` so a slow peer never blocks the next one. Probe
//! results are intentionally ignored: the production `TransportProber`
//! routes through `NexarTransport::send_rpc`, which already walks the
//! circuit breaker's `check → record_success|record_failure` path, so
//! the driver does not need to bookkeep anything itself.
//!
//! Shutdown is cooperative via `tokio::sync::watch`. On `true` the
//! run loop breaks at the next tick or immediately if it is waiting.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, trace};

use crate::circuit_breaker::CircuitBreaker;
use crate::loop_metrics::LoopMetrics;

use super::prober::ReachabilityProber;

/// Configuration for the reachability driver.
#[derive(Debug, Clone)]
pub struct ReachabilityDriverConfig {
    /// Period between open-peer sweeps. Defaults to 30 s in
    /// production; tests override to milliseconds.
    pub interval: Duration,
}

impl Default for ReachabilityDriverConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// Drives periodic reachability probes against every Open-state peer.
pub struct ReachabilityDriver {
    breaker: Arc<CircuitBreaker>,
    prober: Arc<dyn ReachabilityProber>,
    cfg: ReachabilityDriverConfig,
    loop_metrics: Arc<LoopMetrics>,
}

impl ReachabilityDriver {
    pub fn new(
        breaker: Arc<CircuitBreaker>,
        prober: Arc<dyn ReachabilityProber>,
        cfg: ReachabilityDriverConfig,
    ) -> Self {
        Self {
            breaker,
            prober,
            cfg,
            loop_metrics: LoopMetrics::new("reachability_loop"),
        }
    }

    /// Shared handle to this loop's standardized metrics.
    pub fn loop_metrics(&self) -> Arc<LoopMetrics> {
        Arc::clone(&self.loop_metrics)
    }

    /// Run the driver until `shutdown` flips to `true`.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = interval(self.cfg.interval);
        // Skip the immediate first tick so the first probe fires one
        // full interval after start. Otherwise every process restart
        // would stampede every open breaker at once.
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
            }
        }
        self.loop_metrics.set_up(false);
        debug!("reachability driver shutting down");
    }

    /// Single sweep — exposed for tests that drive the loop manually.
    pub async fn sweep_once(&self) {
        let started = Instant::now();
        let open = self.breaker.open_peers();
        if open.is_empty() {
            self.loop_metrics.observe(started.elapsed());
            return;
        }
        trace!(count = open.len(), "reachability sweep: probing open peers");
        for peer in open {
            let prober = Arc::clone(&self.prober);
            let err_metrics = Arc::clone(&self.loop_metrics);
            tokio::spawn(async move {
                if let Err(e) = prober.probe(peer).await {
                    tracing::debug!(peer, error = %e, "reachability probe failed");
                    err_metrics.record_error("probe");
                }
            });
        }
        self.loop_metrics.observe(started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CircuitBreakerConfig;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct RecordingProber {
        calls: Mutex<Vec<u64>>,
    }

    impl RecordingProber {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }
        fn take(&self) -> Vec<u64> {
            let mut g = self.calls.lock().unwrap();
            let out = g.clone();
            g.clear();
            out
        }
    }

    #[async_trait]
    impl ReachabilityProber for RecordingProber {
        async fn probe(&self, peer: u64) -> Result<(), crate::error::ClusterError> {
            self.calls.lock().unwrap().push(peer);
            Ok(())
        }
    }

    fn open_breaker() -> Arc<CircuitBreaker> {
        Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            cooldown: Duration::from_secs(60),
        }))
    }

    #[tokio::test]
    async fn sweep_probes_every_open_peer() {
        let breaker = open_breaker();
        breaker.record_failure(1);
        breaker.record_failure(2);
        breaker.record_failure(3);

        let prober = RecordingProber::new();
        let driver = Arc::new(ReachabilityDriver::new(
            Arc::clone(&breaker),
            prober.clone() as Arc<dyn ReachabilityProber>,
            ReachabilityDriverConfig {
                interval: Duration::from_millis(50),
            },
        ));
        driver.sweep_once().await;
        // Let spawned probe tasks run.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let mut calls = prober.take();
        calls.sort_unstable();
        assert_eq!(calls, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn sweep_skips_closed_peers() {
        let breaker = open_breaker();
        breaker.record_success(1); // Registers 1 as Closed.
        breaker.record_failure(2); // Opens 2.
        let prober = RecordingProber::new();
        let driver = Arc::new(ReachabilityDriver::new(
            Arc::clone(&breaker),
            prober.clone() as Arc<dyn ReachabilityProber>,
            ReachabilityDriverConfig::default(),
        ));
        driver.sweep_once().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(prober.take(), vec![2]);
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_fires_sweeps_on_interval_and_shuts_down() {
        let breaker = open_breaker();
        breaker.record_failure(7);
        let prober = RecordingProber::new();
        let driver = Arc::new(ReachabilityDriver::new(
            Arc::clone(&breaker),
            prober.clone() as Arc<dyn ReachabilityProber>,
            ReachabilityDriverConfig {
                interval: Duration::from_millis(100),
            },
        ));
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn({
            let d = Arc::clone(&driver);
            async move { d.run(rx).await }
        });

        // First tick is skipped, second delivers a sweep.
        tokio::time::advance(Duration::from_millis(120)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(120)).await;
        tokio::task::yield_now().await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert!(
            !prober.take().is_empty(),
            "driver never probed in run-loop mode"
        );

        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;
    }
}
