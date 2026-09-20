// SPDX-License-Identifier: BUSL-1.1

//! Retryable capacity-busy accounting and the bounded re-drive backoff.

use std::time::{Duration, Instant};

use super::scheduler::Scheduler;

impl Scheduler {
    /// Note a retryable capacity-busy dispatch outcome: count it, arm the
    /// drain backoff, and return true. Shared by every dispatch site so none of
    /// them logs ERROR or drives the epoch unpaced.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn note_dispatch_busy(
        &mut self,
        e: &crate::Error,
        epoch: u64,
    ) -> bool {
        if !matches!(e, crate::Error::DispatchCapacityBusy { .. }) {
            return false;
        }
        self.metrics.record_dispatch_busy();
        let attempts = self.dispatch_busy_attempts.saturating_add(1);
        self.dispatch_busy_attempts = attempts;
        self.dispatch_busy_until =
            // no-determinism: the backoff deadline is scheduler observability, not Calvin WAL data
            Some(Instant::now() + Self::busy_backoff(attempts, self.vshard_id, epoch));
        true
    }

    /// Count a capacity-busy dispatch from a `&self` context (no backoff
    /// arming: the drain gates on the `&mut self` sites).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn note_dispatch_busy_shared(
        &self,
        e: &crate::Error,
    ) -> bool {
        if !matches!(e, crate::Error::DispatchCapacityBusy { .. }) {
            return false;
        }
        self.metrics.record_dispatch_busy();
        true
    }

    /// Bounded, deterministic backoff for a capacity-busy dispatch.
    ///
    /// 5 ms doubling on consecutive busy outcomes, capped at 250 ms, plus a
    /// small deterministic jitter derived from the vShard and epoch so peers do
    /// not re-drive in lockstep. No RNG: the value is reproducible in tests.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn busy_backoff(
        attempts: u32,
        vshard_id: u32,
        epoch: u64,
    ) -> Duration {
        let base = 5u64.saturating_mul(1u64 << attempts.min(6));
        let capped = base.min(250);
        let jitter = (u64::from(vshard_id).wrapping_add(epoch)) % 7;
        Duration::from_millis(capped + jitter)
    }
}
