// SPDX-License-Identifier: BUSL-1.1

//! A shared view of which Calvin positions this node's scheduler for one
//! vShard applied.
//!
//! The scheduler owns its [`super::AppliedGate`] and mutates it on its own
//! task. Other Control-Plane code needs one question answered: did this
//! node's replica of the vShard apply `(epoch, position)`? The scheduler
//! mirrors each applied position and each watermark advance here, so the
//! answer needs no message to the scheduler task.
//!
//! The mirror keeps the same shape as the gate: a fully-applied watermark and
//! the applied positions above it. The tail is pruned as the watermark
//! advances, so it stays as small as the gate's.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use super::recovery::NOT_YET_APPLIED_EPOCH;
use crate::control::security::catalog::calvin_applied::StoredCalvinApplied;

#[derive(Debug)]
struct MirrorState {
    /// Every position of every epoch at or below this is applied.
    /// [`NOT_YET_APPLIED_EPOCH`] means none is.
    fully_applied_epoch: u64,
    /// Applied positions of epochs above the watermark.
    tail: BTreeSet<(u64, u32)>,
}

/// Applied positions of one vShard's scheduler on this node.
#[derive(Debug)]
pub struct AppliedMirror {
    state: Mutex<MirrorState>,
}

impl AppliedMirror {
    /// A mirror seeded from the scheduler's recovery scan.
    pub fn new(fully_applied_epoch: u64, tail: BTreeSet<(u64, u32)>) -> Self {
        Self {
            state: Mutex::new(MirrorState {
                fully_applied_epoch,
                tail,
            }),
        }
    }

    /// Record that `(epoch, position)` applied.
    pub fn mark(&self, epoch: u64, position: u32) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.fully_applied_epoch != NOT_YET_APPLIED_EPOCH && epoch <= state.fully_applied_epoch
        {
            return;
        }
        state.tail.insert((epoch, position));
    }

    /// Record that every position of every epoch at or below `watermark`
    /// applied.
    pub fn fold(&self, watermark: u64) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.fully_applied_epoch != NOT_YET_APPLIED_EPOCH
            && watermark <= state.fully_applied_epoch
        {
            return;
        }
        state.fully_applied_epoch = watermark;
        state.tail = state.tail.split_off(&(watermark.saturating_add(1), 0));
    }

    /// The mirror's state: the fully-applied watermark and the applied
    /// positions above it.
    pub fn snapshot(&self) -> (u64, BTreeSet<(u64, u32)>) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (state.fully_applied_epoch, state.tail.clone())
    }

    /// Whether this node's replica applied `(epoch, position)`.
    pub fn is_applied(&self, epoch: u64, position: u32) -> bool {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (state.fully_applied_epoch != NOT_YET_APPLIED_EPOCH && epoch <= state.fully_applied_epoch)
            || state.tail.contains(&(epoch, position))
    }
}

/// The applied mirror of every vShard scheduler on this node.
#[derive(Debug, Default)]
pub struct AppliedMirrors {
    by_vshard: Mutex<HashMap<u32, Arc<AppliedMirror>>>,
}

impl AppliedMirrors {
    /// Register the mirror of a scheduler starting for `vshard_id`. A
    /// restarted scheduler replaces its predecessor's mirror.
    pub fn register(
        &self,
        vshard_id: u32,
        fully_applied_epoch: u64,
        tail: &BTreeSet<(u64, u32)>,
    ) -> Arc<AppliedMirror> {
        let mirror = Arc::new(AppliedMirror::new(fully_applied_epoch, tail.clone()));
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&mirror));
        mirror
    }

    /// Every registered mirror's state, in the shape the catalog stores.
    pub fn snapshot_all(&self) -> Vec<StoredCalvinApplied> {
        let mirrors: Vec<(u32, Arc<AppliedMirror>)> = self
            .by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(vshard_id, mirror)| (*vshard_id, Arc::clone(mirror)))
            .collect();
        mirrors
            .into_iter()
            .map(|(vshard_id, mirror)| {
                let (fully_applied_epoch, tail) = mirror.snapshot();
                StoredCalvinApplied {
                    vshard_id,
                    fully_applied_epoch,
                    tail,
                }
            })
            .collect()
    }

    /// The mirror of `vshard_id`, when this node runs its scheduler.
    pub fn get(&self, vshard_id: u32) -> Option<Arc<AppliedMirror>> {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&vshard_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mirror_answers_like_the_gate() {
        let mirror = AppliedMirror::new(NOT_YET_APPLIED_EPOCH, BTreeSet::new());
        assert!(!mirror.is_applied(0, 0));
        mirror.mark(3, 1);
        assert!(mirror.is_applied(3, 1));
        assert!(!mirror.is_applied(3, 0));
        mirror.fold(3);
        assert!(mirror.is_applied(3, 0));
        assert!(mirror.is_applied(2, 9));
        assert!(!mirror.is_applied(4, 0));
        // A mark at or below the watermark changes nothing.
        mirror.mark(1, 0);
        assert!(mirror.is_applied(1, 0));
        // A lower watermark never moves it back.
        mirror.fold(1);
        assert!(mirror.is_applied(3, 0));
    }

    #[test]
    fn a_restarted_scheduler_replaces_its_mirror() {
        let mirrors = AppliedMirrors::default();
        let first = mirrors.register(7, NOT_YET_APPLIED_EPOCH, &BTreeSet::new());
        first.mark(1, 0);
        let second = mirrors.register(7, 4, &BTreeSet::new());
        let current = mirrors.get(7).expect("mirror");
        assert!(Arc::ptr_eq(&current, &second));
        assert!(current.is_applied(4, 0));
        assert!(mirrors.get(8).is_none());
    }
}
