// SPDX-License-Identifier: BUSL-1.1

//! Per-key classification of a lock request, and the grant that installs a
//! requester on a key it can take now.

use std::collections::VecDeque;

use smallvec::smallvec;

use crate::control::cluster::calvin::scheduler::lock::lock_entry::{LockEntry, LockMode};
use crate::control::cluster::calvin::scheduler::lock::lock_key::{LockKey, TxnId};

use super::types::LockManager;

/// Where one requested key stands for one requester.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyState {
    /// No entry: the requester can take the key.
    Free,
    /// The requester already holds the key in a mode that covers the request,
    /// or holds it alone and can raise its mode in place.
    Held,
    /// Other holders hold the key in a compatible mode and nobody waits: the
    /// requester can join them.
    Join,
    /// The requester must wait: an incompatible holder, an earlier waiter, or
    /// its own earlier wait on this key.
    Conflict,
}

impl LockManager {
    /// Classify `key` requested in `mode` by `txn`.
    pub(super) fn classify(&self, txn: TxnId, key: &LockKey, mode: LockMode) -> KeyState {
        let Some(entry) = self.table.get(key) else {
            return KeyState::Free;
        };
        if entry.holders.contains(&txn) {
            if entry.mode.merge(mode) == entry.mode || entry.held_solely_by(txn) {
                return KeyState::Held;
            }
            return KeyState::Conflict;
        }
        if entry.has_waiter(txn) {
            return KeyState::Conflict;
        }
        if entry.mode.compatible(mode) && entry.waiters.is_empty() {
            return KeyState::Join;
        }
        KeyState::Conflict
    }

    /// Install `txn` on `key` in `mode`. The caller classified the key as
    /// [`KeyState::Free`], [`KeyState::Held`], or [`KeyState::Join`].
    pub(super) fn grant(&mut self, txn: TxnId, key: &LockKey, mode: LockMode) {
        match self.table.get_mut(key) {
            None => {
                self.table.insert(
                    key.clone(),
                    LockEntry {
                        mode,
                        holders: smallvec![txn],
                        waiters: VecDeque::new(),
                    },
                );
            }
            Some(entry) => {
                if entry.holders.contains(&txn) {
                    // A sole holder raises its mode in place. A co-holder only
                    // reaches here when its mode already covers the request.
                    if entry.held_solely_by(txn) {
                        entry.mode = entry.mode.merge(mode);
                    }
                } else {
                    entry.holders.push(txn);
                }
            }
        }
    }

    /// Enqueue `txn` as a waiter on a [`KeyState::Conflict`] key.
    ///
    /// A txn that holds the key and needs a stronger mode drops its own hold
    /// and waits for the merged mode. Its read degrades to plain OCC until the
    /// grant, and the key can drain so the grant can fire.
    pub(super) fn enqueue(&mut self, txn: TxnId, key: &LockKey, mode: LockMode) {
        let Some(entry) = self.table.get_mut(key) else {
            return;
        };
        if entry.has_waiter(txn) {
            return;
        }
        let wanted = if entry.holders.contains(&txn) {
            entry.holders.retain(|h| *h != txn);
            entry.mode.merge(mode)
        } else {
            mode
        };
        entry.waiters.push_back((txn, wanted));
    }
}
