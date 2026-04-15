//! Suspicion timer — the state that tracks which peers are in
//! [`MemberState::Suspect`] and when they should be promoted to
//! [`MemberState::Dead`].
//!
//! Per Lifeguard §3.1, the suspicion timeout is
//! `max(min_suspicion, suspicion_mult * log2(n).max(1) * probe_interval)`,
//! where `n` is the cluster size at the moment the timer is armed. This
//! file owns the timeout math and the `(NodeId, deadline)` table; the
//! detector runner polls [`SuspicionTimer::drain_expired`] on every
//! probe tick and promotes entries whose deadline has passed.

use std::collections::HashMap;
use std::time::Duration;

use nodedb_types::NodeId;
use tokio::time::Instant;

use crate::swim::config::SwimConfig;

/// Tracks pending `Suspect → Dead` transitions.
#[derive(Debug, Default)]
pub struct SuspicionTimer {
    pending: HashMap<NodeId, Instant>,
}

impl SuspicionTimer {
    /// Fresh, empty timer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Compute the suspicion timeout for a cluster of size `cluster_size`.
    /// Matches Lifeguard's formula.
    pub fn compute_timeout(cfg: &SwimConfig, cluster_size: usize) -> Duration {
        let n = cluster_size.max(2);
        let log_n = (n as f64).log2().max(1.0);
        let scaled = cfg
            .probe_interval
            .mul_f64(log_n * cfg.suspicion_mult as f64);
        scaled.max(cfg.min_suspicion)
    }

    /// Arm (or refresh) the suspicion timer for `node`. The deadline is
    /// computed from `now + compute_timeout(...)`.
    pub fn arm(&mut self, node: NodeId, now: Instant, cfg: &SwimConfig, cluster_size: usize) {
        let deadline = now + Self::compute_timeout(cfg, cluster_size);
        self.pending.insert(node, deadline);
    }

    /// Cancel the timer for `node` (e.g. after receiving a refuting
    /// `Alive` rumour). No-op if no timer was armed.
    pub fn cancel(&mut self, node: &NodeId) {
        self.pending.remove(node);
    }

    /// Number of peers currently on a suspicion timer.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// True if no peers are under suspicion.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Return and remove every entry whose deadline has passed. Caller
    /// then promotes each one to [`MemberState::Dead`] via the
    /// membership list.
    pub fn drain_expired(&mut self, now: Instant) -> Vec<NodeId> {
        let expired: Vec<NodeId> = self
            .pending
            .iter()
            .filter_map(|(k, &v)| if v <= now { Some(k.clone()) } else { None })
            .collect();
        for k in &expired {
            self.pending.remove(k);
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::config::SwimConfig;

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: crate::swim::incarnation::Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    #[test]
    fn compute_timeout_respects_min() {
        // 2-node cluster: log2(2)=1, mult=4, interval=100ms → 400ms,
        // which is below the 500ms floor, so the floor wins.
        assert_eq!(
            SuspicionTimer::compute_timeout(&cfg(), 2),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn compute_timeout_scales_with_cluster() {
        // 64-node cluster: log2(64)=6, mult=4, interval=100ms → 2400ms.
        assert_eq!(
            SuspicionTimer::compute_timeout(&cfg(), 64),
            Duration::from_millis(2400)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn arm_and_expire() {
        let mut timer = SuspicionTimer::new();
        let now = Instant::now();
        timer.arm(NodeId::new("n1"), now, &cfg(), 2);
        assert_eq!(timer.len(), 1);
        // Not expired yet.
        assert!(timer.drain_expired(now).is_empty());
        // Advance past the 500ms floor.
        tokio::time::advance(Duration::from_millis(600)).await;
        let later = Instant::now();
        let expired = timer.drain_expired(later);
        assert_eq!(expired, vec![NodeId::new("n1")]);
        assert!(timer.is_empty());
    }

    #[test]
    fn cancel_removes_entry() {
        let mut timer = SuspicionTimer::new();
        let now = Instant::now();
        timer.arm(NodeId::new("n1"), now, &cfg(), 2);
        timer.cancel(&NodeId::new("n1"));
        assert!(timer.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_entries_expire_independently() {
        let mut timer = SuspicionTimer::new();
        let t0 = Instant::now();
        timer.arm(NodeId::new("a"), t0, &cfg(), 2);
        tokio::time::advance(Duration::from_millis(200)).await;
        let t1 = Instant::now();
        timer.arm(NodeId::new("b"), t1, &cfg(), 2);
        // Advance so `a` expires (>=500ms from t0) but `b` does not (<500ms from t1).
        tokio::time::advance(Duration::from_millis(350)).await;
        let now = Instant::now();
        let expired = timer.drain_expired(now);
        assert_eq!(expired, vec![NodeId::new("a")]);
        assert_eq!(timer.len(), 1);
    }
}
