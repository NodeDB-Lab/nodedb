// SPDX-License-Identifier: BUSL-1.1

//! Per-group bound on committed entries between hand-off and settle.
//!
//! The apply loop enqueues a group's entries in log order and collects each
//! outcome independently, so a parked write holds its own position and no
//! other. The window bounds how many of a group's entries the loop holds at
//! once. The applier refuses a batch that would pass the bound, and Raft
//! delivers it again on a later tick. Only the saturated group waits: every
//! other group keeps its own window.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::bridge::dispatch::DATA_PLANE_QUEUE_CAPACITY;

/// Entries of one group the apply loop may hold between hand-off and settle.
///
/// One core's request queue. A group's writes spread over the cores that own
/// its vShards, so a window of one queue keeps every one of those cores busy.
/// A larger window only adds writes parked behind a full queue.
pub const APPLY_WINDOW_PER_GROUP: usize = DATA_PLANE_QUEUE_CAPACITY;

/// Outstanding entry counts per group.
#[derive(Debug)]
pub struct ApplyWindow {
    limit: usize,
    outstanding: Mutex<HashMap<u64, usize>>,
}

impl Default for ApplyWindow {
    fn default() -> Self {
        Self::new(APPLY_WINDOW_PER_GROUP)
    }
}

impl ApplyWindow {
    /// A window of `limit` entries per group.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            outstanding: Mutex::new(HashMap::new()),
        }
    }

    /// Take `count` entries of `group_id` into the window. Refuses when the
    /// group holds entries and `count` more would pass the limit. A group that
    /// holds none always takes the batch, so a batch longer than the limit
    /// still applies.
    pub fn try_admit(&self, group_id: u64, count: usize) -> bool {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|p| p.into_inner());
        let held = outstanding.entry(group_id).or_insert(0);
        if *held > 0 && *held + count > self.limit {
            return false;
        }
        *held += count;
        true
    }

    /// Release `count` entries of `group_id`: settled by the loop, or never
    /// handed to it.
    pub fn release(&self, group_id: u64, count: usize) {
        let mut outstanding = self.outstanding.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(held) = outstanding.get_mut(&group_id) {
            *held = held.saturating_sub(count);
        }
    }

    /// Entries of `group_id` the window holds.
    pub fn outstanding(&self, group_id: u64) -> usize {
        self.outstanding
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_group_refuses_while_another_group_admits() {
        let window = ApplyWindow::new(4);
        assert!(window.try_admit(1, 3));
        assert!(!window.try_admit(1, 2), "group 1 would pass its bound");
        assert!(window.try_admit(2, 4), "group 2 keeps its own window");
        window.release(1, 1);
        assert!(window.try_admit(1, 2));
        assert_eq!(window.outstanding(1), 4);
    }

    #[test]
    fn an_empty_group_takes_a_batch_longer_than_the_limit() {
        let window = ApplyWindow::new(2);
        assert!(window.try_admit(7, 5));
        assert!(!window.try_admit(7, 1));
    }
}
