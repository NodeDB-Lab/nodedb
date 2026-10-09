// SPDX-License-Identifier: BUSL-1.1

//! Non-blocking acquire fast path.

use std::collections::BTreeMap;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::LockMode;
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::classify::KeyState;
use super::types::LockManager;

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
        let contended = keys
            .iter()
            .any(|(key, mode)| self.classify(txn, key, *mode) == KeyState::Conflict);
        if contended {
            return false;
        }
        for (key, mode) in &keys {
            self.grant(txn, key, *mode);
        }
        self.finish_ready(txn, keys);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn coll() -> LockKey {
        LockKey::Collection {
            collection: Arc::from("c"),
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
        assert_eq!(entry.holders.as_slice(), &[a, b]);
        assert!(entry.waiters.is_empty(), "a refused probe leaves no waiter");
    }
}
