// SPDX-License-Identifier: BUSL-1.1

//! Whether each running Calvin scheduler on this node reached its rebuild
//! target.
//!
//! A scheduler starts with a rebuild target: the highest epoch its recovery
//! found applied for the vShard. It is caught up once its fully-applied
//! watermark reaches that target. A scheduler with no target is caught up
//! from the start. Startup holds the client gateway until every running
//! scheduler is caught up.
//!
//! Each scheduler holds a [`CaughtUpHandle`] for its vShard. The handle
//! removes its entry when the scheduler is dropped, so the registry lists
//! only running schedulers. A restarted scheduler replaces its predecessor's
//! entry, and the predecessor's drop leaves the new entry in place.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

type Entries = Arc<Mutex<BTreeMap<u32, Arc<AtomicBool>>>>;

/// Caught-up state of every running scheduler on this node, by vShard.
#[derive(Debug, Default)]
pub struct CaughtUpRegistry {
    entries: Entries,
}

impl CaughtUpRegistry {
    /// Register the scheduler starting for `vshard_id`, not caught up yet.
    pub fn register(&self, vshard_id: u32) -> CaughtUpHandle {
        let flag = Arc::new(AtomicBool::new(false));
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&flag));
        CaughtUpHandle {
            vshard_id,
            flag,
            entries: Arc::clone(&self.entries),
        }
    }

    /// The vShards whose running scheduler is not caught up, in order.
    pub fn lagging(&self) -> Vec<u32> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(_, flag)| !flag.load(Ordering::Acquire))
            .map(|(vshard_id, _)| *vshard_id)
            .collect()
    }
}

/// One scheduler's entry in the [`CaughtUpRegistry`].
#[derive(Debug)]
pub struct CaughtUpHandle {
    vshard_id: u32,
    flag: Arc<AtomicBool>,
    entries: Entries,
}

impl CaughtUpHandle {
    /// Whether this scheduler reported caught up.
    pub fn is_caught_up(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Report this scheduler caught up. The state never returns to lagging:
    /// the fully-applied watermark only rises.
    pub fn mark_caught_up(&self) {
        self.flag.store(true, Ordering::Release);
    }
}

impl Drop for CaughtUpHandle {
    fn drop(&mut self) {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries
            .get(&self.vshard_id)
            .is_some_and(|flag| Arc::ptr_eq(flag, &self.flag))
        {
            entries.remove(&self.vshard_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_scheduler_lags_until_it_reports() {
        let registry = CaughtUpRegistry::default();
        let first = registry.register(3);
        let second = registry.register(1);
        assert_eq!(registry.lagging(), vec![1, 3]);

        second.mark_caught_up();
        assert!(second.is_caught_up());
        assert_eq!(registry.lagging(), vec![3]);

        first.mark_caught_up();
        assert!(registry.lagging().is_empty());
    }

    #[test]
    fn a_dropped_scheduler_leaves_the_registry() {
        let registry = CaughtUpRegistry::default();
        let handle = registry.register(5);
        assert_eq!(registry.lagging(), vec![5]);
        drop(handle);
        assert!(registry.lagging().is_empty());
    }

    #[test]
    fn a_predecessor_drop_keeps_its_successor_entry() {
        let registry = CaughtUpRegistry::default();
        let predecessor = registry.register(7);
        let successor = registry.register(7);
        drop(predecessor);
        assert_eq!(registry.lagging(), vec![7], "the successor still lags");
        successor.mark_caught_up();
        assert!(registry.lagging().is_empty());
    }
}
