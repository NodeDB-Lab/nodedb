// SPDX-License-Identifier: BUSL-1.1

//! The backup cut markers each Calvin scheduler on this node passed.
//!
//! A scheduler passes a marker once every transaction delivered to it before
//! the marker finished. A backup's cut proposes a marker carrying its
//! watermark and waits here until every scheduler this node runs passed it.

use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::Notify;

/// Highest marker watermark each local scheduler passed, by vShard.
#[derive(Debug, Default)]
pub struct CalvinCuts {
    passed: Mutex<BTreeMap<u32, u64>>,
    changed: Notify,
}

impl CalvinCuts {
    /// Register the scheduler of `vshard_id`. A cut waits on it from now on.
    pub fn register(&self, vshard_id: u32) {
        self.passed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(vshard_id)
            .or_insert(0);
    }

    /// Record that the scheduler of `vshard_id` passed the marker `hlc`.
    pub fn note_passed(&self, vshard_id: u32, hlc: u64) {
        {
            let mut passed = self.passed.lock().unwrap_or_else(|p| p.into_inner());
            let highest = passed.entry(vshard_id).or_insert(0);
            *highest = (*highest).max(hlc);
        }
        self.changed.notify_waiters();
    }

    /// Whether any scheduler runs on this node.
    pub fn is_empty(&self) -> bool {
        self.passed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// The vShards whose scheduler has not passed the marker `hlc`.
    pub fn lagging(&self, hlc: u64) -> Vec<u32> {
        self.passed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(_, passed)| **passed < hlc)
            .map(|(vshard_id, _)| *vshard_id)
            .collect()
    }

    /// Wait until every scheduler passed the marker `hlc`, or `deadline`.
    /// Returns the vShards still lagging, empty once every one passed.
    pub async fn await_passed(&self, hlc: u64, deadline: tokio::time::Instant) -> Vec<u32> {
        loop {
            // Registered before the check, so a pass between the check and
            // the wait still wakes it.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let lagging = self.lagging(hlc);
            if lagging.is_empty() {
                return lagging;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.lagging(hlc);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_cut_waits_for_every_registered_scheduler() {
        let cuts = CalvinCuts::default();
        cuts.register(1);
        cuts.register(2);
        cuts.note_passed(1, 50);
        let soon = tokio::time::Instant::now() + std::time::Duration::from_millis(20);
        assert_eq!(cuts.await_passed(50, soon).await, vec![2]);

        cuts.note_passed(2, 60);
        let later = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert!(cuts.await_passed(50, later).await.is_empty());
        assert_eq!(cuts.lagging(55), vec![1]);
    }
}
