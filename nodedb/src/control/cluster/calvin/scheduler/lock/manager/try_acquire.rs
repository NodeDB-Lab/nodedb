// SPDX-License-Identifier: BUSL-1.1

//! Non-blocking acquire fast path, and the probe that files a
//! write-admission watch on a contended request.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::LockMode;
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::classify::KeyState;
use super::types::LockManager;

/// The outcome of [`LockManager::try_acquire_or_watch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateProbe {
    /// The txn holds every key of its request.
    Acquired,
    /// The txn holds nothing. Its signal waits on this key, the first one
    /// that blocks the request.
    Blocked(LockKey),
}

impl LockManager {
    /// Non-blocking acquire: take all `keys` for `txn` iff none conflicts,
    /// returning `true`; otherwise return `false` WITHOUT enqueuing a waiter or
    /// recording any pending state.
    ///
    /// A key is taken when it is free, already held by `txn`, or held in a
    /// compatible mode with nobody waiting (see [`LockMode::compatible`]).
    ///
    /// This is the fast path's probe. Unlike [`acquire`](Self::acquire), the
    /// contended (`false`) path touches NOTHING — no holder, no `pending_keys`,
    /// no waiter `VecDeque` — so a caller that does not intend to block (an
    /// autocommit point write that will instead route to the scheduler) never
    /// leaves an orphaned waiter that a later `release` would promote to an
    /// unowned holder. It also never perturbs the FIFO ordering that Calvin
    /// transactions depend on.
    pub fn try_acquire(&mut self, txn: TxnId, keys: BTreeMap<LockKey, LockMode>) -> bool {
        if self.first_conflict(txn, &keys).is_some() {
            return false;
        }
        for (key, mode) in &keys {
            self.grant(txn, key, *mode);
        }
        self.finish_ready(txn, keys);
        true
    }

    /// Take all `keys` for `txn` like [`Self::try_acquire`], or file `signal`
    /// under the first key that blocks the request.
    ///
    /// The blocked path takes no key and queues no waiter. The next release
    /// of the returned key wakes `signal` once and drops the watch. A watch
    /// never affects a grant.
    pub fn try_acquire_or_watch(
        &mut self,
        txn: TxnId,
        keys: &BTreeMap<LockKey, LockMode>,
        signal: &Arc<Notify>,
    ) -> GateProbe {
        if let Some(blocker) = self.first_conflict(txn, keys) {
            let blocker = blocker.clone();
            self.watchers
                .entry(blocker.clone())
                .or_default()
                .push(Arc::clone(signal));
            #[cfg(test)]
            {
                self.watch_registrations += 1;
            }
            return GateProbe::Blocked(blocker);
        }
        for (key, mode) in keys {
            self.grant(txn, key, *mode);
        }
        self.held_locks
            .entry(txn)
            .or_default()
            .extend(keys.keys().cloned());
        GateProbe::Acquired
    }

    /// Drop the watch `signal` filed under `key`, if it is still filed.
    pub fn unwatch(&mut self, key: &LockKey, signal: &Arc<Notify>) {
        let Some(signals) = self.watchers.get_mut(key) else {
            return;
        };
        signals.retain(|filed| !Arc::ptr_eq(filed, signal));
        if signals.is_empty() {
            self.watchers.remove(key);
        }
    }

    /// The first key of `keys`, in key order, that blocks `txn`.
    fn first_conflict<'k>(
        &self,
        txn: TxnId,
        keys: &'k BTreeMap<LockKey, LockMode>,
    ) -> Option<&'k LockKey> {
        keys.iter()
            .find(|(key, mode)| self.classify(txn, key, **mode) == KeyState::Conflict)
            .map(|(key, _)| key)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn coll() -> LockKey {
        LockKey::Collection {
            collection: Arc::from("c"),
        }
    }

    fn row(surrogate: u32) -> LockKey {
        LockKey::Surrogate {
            collection: Arc::from("other"),
            surrogate,
        }
    }

    #[test]
    fn try_acquire_joins_compatible_holders_and_refuses_conflicts() {
        let mut lm = LockManager::new();
        let (a, b, c) = (TxnId::new(1, 0), TxnId::new(1, 1), TxnId::new(1, 2));
        assert!(lm.try_acquire(a, BTreeMap::from([(coll(), LockMode::Intent)])));
        assert!(lm.try_acquire(b, BTreeMap::from([(coll(), LockMode::Intent)])));
        assert!(!lm.try_acquire(c, BTreeMap::from([(coll(), LockMode::Exclusive)])));
        let entry = lm.table.get(&coll()).unwrap();
        assert_eq!(entry.holder_ids(), vec![a, b]);
        assert!(entry.waiters.is_empty(), "a refused probe leaves no waiter");
    }

    /// Releases of other keys leave a watch asleep and filed. The release of
    /// its blocking key wakes it once and unfiles it.
    #[tokio::test]
    async fn a_watch_wakes_only_on_a_release_of_its_blocking_key() {
        let mut lm = LockManager::new();
        let holder = TxnId::new(1, 0);
        let watcher = TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 0);
        let request = BTreeMap::from([(coll(), LockMode::Intent)]);
        let signal = Arc::new(Notify::new());
        assert!(lm.try_acquire(holder, BTreeMap::from([(coll(), LockMode::Exclusive)])));
        assert_eq!(
            lm.try_acquire_or_watch(watcher, &request, &signal),
            GateProbe::Blocked(coll())
        );
        assert_eq!(lm.watcher_count(), 1);

        for n in 0..100 {
            let other = TxnId::new(2, n);
            assert!(lm.try_acquire(other, BTreeMap::from([(row(n), LockMode::Exclusive)])));
            lm.release(other);
        }
        assert_eq!(lm.watcher_count(), 1, "other keys leave the watch filed");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), signal.notified())
                .await
                .is_err(),
            "other keys never wake the watch"
        );

        lm.release(holder);
        assert_eq!(lm.watcher_count(), 0, "the release unfiles the watch");
        tokio::time::timeout(Duration::from_secs(5), signal.notified())
            .await
            .expect("the stored permit wakes the watch");
        assert_eq!(
            lm.try_acquire_or_watch(watcher, &request, &signal),
            GateProbe::Acquired
        );
        assert_eq!(lm.watch_registrations(), 1);
        assert!(lm.release(watcher).is_empty());
        assert_eq!(lm.lock_count(), 0);
    }
}
