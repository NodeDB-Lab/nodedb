//! Sync wait primitive used to block on a metadata-group raft commit.
//!
//! When a node proposes a `MetadataEntry`, the raft layer returns the
//! assigned log index. To know that the entry has been *applied locally*
//! (not merely committed on the leader), the proposer waits until the
//! [`crate::control::cluster::metadata_applier::MetadataCommitApplier`]
//! has advanced the applied watermark to at least the proposed index.
//!
//! This is the stepping stone for CRDB-style "DDL is done when every
//! node has applied the entry". Today we wait for **this node**'s
//! applied watermark; follower-acknowledgement waits layer on top via
//! the descriptor lease path.
//!
//! The watcher uses `std::sync::Condvar` so it is safe to call from
//! synchronous pgwire handler code without entering a tokio reactor.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Tracks the highest metadata-group log index applied on this node.
#[derive(Debug, Default)]
pub struct AppliedIndexWatcher {
    state: Mutex<u64>,
    cv: Condvar,
}

impl AppliedIndexWatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the watermark. Called by
    /// [`super::metadata_applier::MetadataCommitApplier`] after each
    /// apply batch. Idempotent: smaller indices are ignored.
    pub fn bump(&self, applied_index: u64) {
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if applied_index > *guard {
            *guard = applied_index;
            self.cv.notify_all();
        }
    }

    /// Read the current watermark without blocking.
    pub fn current(&self) -> u64 {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Block until the watermark reaches `target` or the timeout elapses.
    /// Returns `true` on success, `false` on timeout.
    pub fn wait_for(&self, target: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while *guard < target {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(r) if !r.is_zero() => r,
                _ => return *guard >= target,
            };
            let wait_result = self
                .cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|p| p.into_inner());
            guard = wait_result.0;
            if wait_result.1.timed_out() {
                return *guard >= target;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn bump_notifies_waiter() {
        let w = Arc::new(AppliedIndexWatcher::new());
        let w2 = w.clone();
        let handle = thread::spawn(move || w2.wait_for(5, Duration::from_secs(2)));
        thread::sleep(Duration::from_millis(20));
        w.bump(3);
        thread::sleep(Duration::from_millis(20));
        w.bump(5);
        assert!(handle.join().unwrap());
    }

    #[test]
    fn times_out_if_never_bumped() {
        let w = AppliedIndexWatcher::new();
        assert!(!w.wait_for(1, Duration::from_millis(30)));
    }

    #[test]
    fn already_past_target_returns_immediately() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(10)));
    }
}
