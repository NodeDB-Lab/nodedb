// SPDX-License-Identifier: BUSL-1.1

//! Eviction of completion entries no waiter will collect.
//!
//! Every sequencer replica applies every `CompletionAck`, so every replica
//! builds a completion entry for every transaction, and the entry holds each
//! participant's apply result. Only the replica on the coordinator's node
//! gains a waiter. A terminal entry keeps its outcome so a waiter that
//! registers after the last ack still receives it. A coordinator registers
//! within its statement deadline, so a terminal entry that gained no waiter
//! within the eviction window never will, and is removed with its results.
//!
//! An entry becomes terminal when its verdict is stored and every expected
//! participant acked, or when the transaction lost its parts. An entry whose
//! outcome fired stays too, for the participants that still probe its
//! verdict. A terminal entry with no waiter joins a queue stamped with the
//! instant it did. Each later registry change sweeps the queue front, so the
//! queue holds at most the entries of one window. A non-terminal sequenced
//! entry is never evicted: the scheduler that holds its txn reports the
//! stall.
//!
//! # Orphans
//!
//! The `EpochBatch` apply seeds a txn's entry on every replica, and marks it
//! sequenced, before it fans the txn out. Every vote, verdict, and ack of the
//! txn follows its `EpochBatch` in the sequencer log. A signal therefore
//! creates an unsequenced entry only in these cases:
//!
//! - It applies after the waiterless sweep evicted the terminal entry.
//! - Its `EpochBatch` applied before this replica's registry existed, in a
//!   snapshot or an earlier process.
//! - A coordinator registered, or the leader assigned, before the
//!   `EpochBatch` applied here, or for a batch that never commits.
//!
//! No local participant holds such a txn: a scheduler receives a txn only
//! from this replica's `EpochBatch` apply, or from a replay of an applied
//! entry. Only a coordinator can wait on an orphan. Each orphan joins a
//! second queue at creation. The sweep evicts it once the window passed
//! since its creation and no live waiter listens. A waiter that listens
//! re-queues it.
//!
//! The rule never evicts an entry a live coordinator waits on:
//!
//! - A registered waiter whose receiver still listens re-queues the entry.
//! - A coordinator registers within its statement deadline of its submit. A
//!   signal of the txn exists only after the submit, so registration lands
//!   inside the window that starts at the orphan's creation. The host sets
//!   the window longer than the deadline.
//!
//! A late ack or verdict for an evicted txn creates an orphan with no
//! waiter. The sweep evicts it one window later, so it never accumulates.

use std::collections::btree_map::Entry;
use std::time::{Duration, Instant};

use super::completion::{Inner, TxnId};
use super::completion_entry::PendingCompletion;

/// The eviction window when the host sets none. Longer than any statement
/// deadline, so a coordinator's waiter always registers inside it.
pub const DEFAULT_WAITERLESS_TTL: Duration = Duration::from_secs(120);

impl Inner {
    /// `txn`'s entry. A missing one is created unsequenced and joins the
    /// orphan queue.
    pub(crate) fn entry_mut(&mut self, txn: TxnId) -> &mut PendingCompletion {
        match self.completions.entry(txn) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(slot) => {
                // no-determinism: node-local eviction clock; never in the log.
                let now = Instant::now();
                self.orphans.push_back((now, txn));
                slot.insert(PendingCompletion::new(now))
            }
        }
    }

    /// `txn`'s entry, marked sequenced. A missing one is created without an
    /// orphan slot.
    pub(crate) fn sequenced_entry_mut(&mut self, txn: TxnId) -> &mut PendingCompletion {
        let entry = self
            .completions
            .entry(txn)
            // no-determinism: node-local eviction clock; never in the log.
            .or_insert_with(|| PendingCompletion::new(Instant::now()));
        entry.sequenced = true;
        entry
    }

    /// Queue `txn`'s entry when it just became a waiterless terminal entry,
    /// then evict every queued entry whose window passed.
    pub(crate) fn settle_waiterless(&mut self, txn: TxnId) {
        // no-determinism: node-local eviction of finished entries; never in the log.
        let now = Instant::now();
        self.park_waiterless(txn, now);
        self.sweep_waiterless(now);
        self.sweep_orphans(now);
    }

    /// The eviction window.
    fn window(&self) -> Duration {
        self.waiterless_ttl.unwrap_or(DEFAULT_WAITERLESS_TTL)
    }

    /// Evict every queued orphan that outlived the window with no live
    /// waiter. An orphan whose waiter still listens is queued again.
    ///
    /// A queue slot is skipped when its entry is sequenced, parked as
    /// terminal, or younger than the window. A younger entry is a later
    /// incarnation of the txn, and it holds its own slot.
    pub(crate) fn sweep_orphans(&mut self, now: Instant) {
        let window = self.window();
        let mut waited_on = Vec::new();
        while let Some(&(since, txn)) = self.orphans.front() {
            if now.saturating_duration_since(since) < window {
                break;
            }
            self.orphans.pop_front();
            let Some(entry) = self.completions.get(&txn) else {
                continue;
            };
            if entry.sequenced
                || entry.parked
                || now.saturating_duration_since(entry.created) < window
            {
                continue;
            }
            if entry.has_live_waiter() {
                waited_on.push((now, txn));
                continue;
            }
            self.completions.remove(&txn);
        }
        self.orphans.extend(waited_on);
    }

    /// Queue `txn`'s entry for eviction when it is terminal, has no waiter,
    /// and is not queued yet.
    pub(crate) fn park_waiterless(&mut self, txn: TxnId, now: Instant) {
        let Some(entry) = self.completions.get_mut(&txn) else {
            return;
        };
        if entry.parked || entry.has_waiter() || !entry.is_terminal() {
            return;
        }
        entry.parked = true;
        self.waiterless.push_back((now, txn));
    }

    /// Remove every queued entry that stayed waiterless for the whole
    /// window.
    pub(crate) fn sweep_waiterless(&mut self, now: Instant) {
        let ttl = self.window();
        while let Some(&(since, txn)) = self.waiterless.front() {
            if now.saturating_duration_since(since) < ttl {
                break;
            }
            self.waiterless.pop_front();
            if self
                .completions
                .get(&txn)
                .is_some_and(|entry| !entry.has_waiter())
            {
                self.completions.remove(&txn);
            }
        }
    }
}
