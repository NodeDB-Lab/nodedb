// SPDX-License-Identifier: BUSL-1.1

//! The lock keys replicated writes hold on their data-group leader.
//!
//! The leader's write gate takes a write's keys before its propose. The keys
//! stay held until the leader's apply loop starts the entry: its enqueue
//! returned, or it concluded without one. The apply loop enqueues a group's
//! writes on their cores in log order, and a core runs its queue in order. A
//! Calvin transaction granted a key after the release therefore stages on
//! the core after the write, and reads the write's effects.
//!
//! The keys are never held across a Data-Plane apply. A write that holds keys
//! waits only for the apply loop to start its entry, which never waits for a
//! lock. So no held key waits for a Calvin transaction to finish.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use crate::types::VShardId;

use super::gate::WriteAdmissionGuard;

/// One write's keys, held for their `Drop`.
struct Held {
    vshard: VShardId,
    _guard: WriteAdmissionGuard,
}

#[derive(Default)]
struct Inner {
    /// Held keys by the `(group_id, log_index)` of the write's entry.
    holds: BTreeMap<(u64, u64), Vec<Held>>,
    /// Per group, the highest log index whose apply started on this node.
    started: HashMap<u64, u64>,
}

/// Every replicated write's held keys on this node, by entry.
#[derive(Default)]
pub struct AdmissionHolds {
    inner: Mutex<Inner>,
}

impl AdmissionHolds {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `guard` until the apply of entry `log_index` of `group_id`
    /// starts on this node. An entry whose apply started already releases
    /// it at once.
    pub fn hold(
        &self,
        group_id: u64,
        log_index: u64,
        vshard: VShardId,
        guard: WriteAdmissionGuard,
    ) {
        let held = Held {
            vshard,
            _guard: guard,
        };
        // Dropped after the table's mutex: a guard's drop takes the lock
        // table's mutex.
        let _released = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if inner
                .started
                .get(&group_id)
                .is_some_and(|started| *started >= log_index)
            {
                Some(held)
            } else {
                inner
                    .holds
                    .entry((group_id, log_index))
                    .or_default()
                    .push(held);
                None
            }
        };
    }

    /// The apply of entry `log_index` of `group_id` started on this node,
    /// after every entry of the group below it. Release the keys every write
    /// through it holds.
    pub fn release_through(&self, group_id: u64, log_index: u64) {
        let _released: Vec<Vec<Held>> = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let started = inner.started.entry(group_id).or_default();
            *started = (*started).max(log_index);
            let through: Vec<(u64, u64)> = inner
                .holds
                .range((group_id, 0)..=(group_id, log_index))
                .map(|(key, _)| *key)
                .collect();
            through
                .into_iter()
                .filter_map(|key| inner.holds.remove(&key))
                .collect()
        };
    }

    /// The scheduler of `vshard` stopped on this node, and its lock table
    /// with it. Drop every hold on that table.
    pub fn release_vshard(&self, vshard: VShardId) {
        let _released: Vec<Held> = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let mut released = Vec::new();
            inner.holds.retain(|_, held| {
                let (gone, kept): (Vec<Held>, Vec<Held>) = std::mem::take(held)
                    .into_iter()
                    .partition(|h| h.vshard == vshard);
                released.extend(gone);
                *held = kept;
                !held.is_empty()
            });
            released
        };
    }

    /// The number of entries whose writes hold keys.
    #[cfg(test)]
    fn held_entries(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .holds
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::control::cluster::calvin::scheduler::lock_manager::{
        LockKey, LockManager, LockMode, TxnId,
    };

    fn guard(lock_manager: &Arc<Mutex<LockManager>>, position: u32) -> WriteAdmissionGuard {
        let txn = TxnId::new(TxnId::AUTOCOMMIT_EPOCH, position);
        let key = LockKey::Surrogate {
            collection: Arc::from("c"),
            surrogate: position,
        };
        assert!(
            lock_manager
                .lock()
                .expect("table")
                .try_acquire(txn, BTreeMap::from([(key, LockMode::Exclusive)]))
        );
        WriteAdmissionGuard::new(Arc::clone(lock_manager), txn, None)
    }

    fn held_keys(lock_manager: &Arc<Mutex<LockManager>>) -> usize {
        lock_manager.lock().expect("table").lock_count()
    }

    /// A write's keys stay held until its entry starts, and no longer.
    #[test]
    fn keys_release_when_their_entry_starts() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holds = AdmissionHolds::new();
        holds.hold(4, 10, VShardId::new(1), guard(&lock_manager, 1));
        holds.hold(4, 11, VShardId::new(1), guard(&lock_manager, 2));
        assert_eq!(held_keys(&lock_manager), 2);
        holds.release_through(4, 10);
        assert_eq!(held_keys(&lock_manager), 1);
        assert_eq!(holds.held_entries(), 1);
        holds.release_through(4, 11);
        assert_eq!(held_keys(&lock_manager), 0);
    }

    /// An entry that started before its proposer registered the hold
    /// releases the keys at once.
    #[test]
    fn a_hold_on_a_started_entry_releases_at_once() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holds = AdmissionHolds::new();
        holds.release_through(4, 12);
        holds.hold(4, 12, VShardId::new(1), guard(&lock_manager, 1));
        assert_eq!(held_keys(&lock_manager), 0);
        assert_eq!(holds.held_entries(), 0);
    }

    /// Another group's start releases nothing.
    #[test]
    fn groups_release_apart() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holds = AdmissionHolds::new();
        holds.hold(4, 10, VShardId::new(1), guard(&lock_manager, 1));
        holds.release_through(5, 20);
        assert_eq!(held_keys(&lock_manager), 1);
    }

    /// A stopped scheduler drops the holds on its table.
    #[test]
    fn a_stopped_vshard_drops_its_holds() {
        let lock_manager = Arc::new(Mutex::new(LockManager::new()));
        let holds = AdmissionHolds::new();
        holds.hold(4, 10, VShardId::new(1), guard(&lock_manager, 1));
        holds.hold(4, 10, VShardId::new(2), guard(&lock_manager, 2));
        holds.release_vshard(VShardId::new(1));
        assert_eq!(held_keys(&lock_manager), 1);
        assert_eq!(holds.held_entries(), 1);
    }
}
