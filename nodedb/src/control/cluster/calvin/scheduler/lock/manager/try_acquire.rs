// SPDX-License-Identifier: BUSL-1.1

//! Non-blocking exclusive acquire fast path.

use std::collections::BTreeSet;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::AcquireOutcome;
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::LockManager;

impl LockManager {
    /// Non-blocking exclusive acquire: take all `keys` for `txn` iff every one is
    /// free (or already held by `txn`), returning `true`; otherwise return
    /// `false` WITHOUT enqueuing a waiter or recording any pending state.
    ///
    /// This is the fast path's probe. Unlike [`acquire`](Self::acquire), the
    /// contended (`false`) path touches NOTHING — no holder, no `pending_keys`,
    /// no waiter `VecDeque` — so a caller that does not intend to block (an
    /// autocommit point write that will instead route to the scheduler) never
    /// leaves an orphaned waiter that a later `release` would promote to an
    /// unowned holder. It also never perturbs the FIFO ordering that Calvin
    /// transactions depend on.
    pub fn try_acquire(&mut self, txn: TxnId, keys: BTreeSet<LockKey>) -> bool {
        if !self.is_ready(txn, &keys) {
            // Contended: leave the table, waiter queues, and pending_keys
            // completely untouched.
            return false;
        }
        // Every key is free or already held by `txn`, so `acquire` takes its
        // all-available path — it inserts the holder and never enqueues.
        let outcome = self.acquire(txn, keys);
        debug_assert_eq!(
            outcome,
            AcquireOutcome::Ready,
            "try_acquire: is_ready was true but acquire returned Blocked"
        );
        true
    }
}
