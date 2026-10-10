// SPDX-License-Identifier: BUSL-1.1

//! Dependent-read barrier events that reached a vShard before its scheduler
//! took them.
//!
//! The data-group apply loop folds every `CalvinReadResult` and
//! `CalvinReadTimeout` entry into the txn's stored barrier row (see
//! [`super::barrier_store`]), then into the vShard's [`VShardReadResults`].
//! The entry can apply before this node's scheduler granted the txn, or
//! before a scheduler for the vShard runs here at all. The events then wait
//! here. The scheduler takes a txn's events once it holds the txn: at the
//! grant, and on each push for a txn it holds.
//!
//! A txn's events wait in one of two places:
//!
//! - in memory, as a folded [`BarrierLog`];
//! - in its stored row alone. Such a txn is "stored", and the scheduler reads
//!   the row when it takes the txn.
//!
//! Memory holds at most `capacity` events per vShard. An event past it moves
//! its txn to stored: the row holds every saved event of the txn, so nothing
//! is dropped. Boot and a snapshot install mark the txns of the rows they
//! find as stored.
//!
//! An event whose row write failed stays in memory, past the bound. Its txn
//! becomes "unsaved": the row then holds a prefix of the txn's events, so
//! every later event of the txn stays in memory too and skips the row. The
//! row stays a prefix, so a restart that delivers the failed entry again
//! folds the events in log order.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::barrier_log::{BarrierEvent, BarrierLog};
use super::lock::TxnId;

/// The waiting events of one txn, as [`VShardReadResults::take`] hands
/// them out.
#[derive(Debug, Default, PartialEq)]
pub struct BufferedEvents {
    /// The events memory held, folded.
    pub memory: BarrierLog,
    /// Whether the txn's stored row holds events memory does not. Its
    /// events are the row's, extended with `memory`.
    pub stored: bool,
}

#[derive(Debug, Default)]
struct Buffered {
    logs: BTreeMap<TxnId, BarrierLog>,
    /// Sum of `event_count` over `logs`.
    events: usize,
    /// Txns whose stored row holds events memory does not.
    stored: BTreeSet<TxnId>,
    /// Txns with an event the stored row lacks.
    unsaved: BTreeSet<TxnId>,
}

impl Buffered {
    fn remove_log(&mut self, txn: TxnId) -> Option<BarrierLog> {
        let log = self.logs.remove(&txn)?;
        self.events = self.events.saturating_sub(log.event_count());
        Some(log)
    }

    /// Fold `log`, events after every event memory holds for `txn`.
    fn extend_log(&mut self, txn: TxnId, later: BarrierLog) {
        let mut log = self.remove_log(txn).unwrap_or_default();
        log.extend(later);
        self.events += log.event_count();
        self.logs.insert(txn, log);
    }

    /// Take `txn`'s waiting events, when it has any.
    fn take(&mut self, txn: TxnId) -> Option<BufferedEvents> {
        let memory = self.remove_log(txn);
        let stored = self.stored.remove(&txn);
        if memory.is_none() && !stored {
            return None;
        }
        Some(BufferedEvents {
            memory: memory.unwrap_or_default(),
            stored,
        })
    }

    /// Whether `txn` has waiting events.
    fn waits(&self, txn: TxnId) -> bool {
        self.logs.contains_key(&txn) || self.stored.contains(&txn)
    }
}

/// One vShard's waiting barrier events.
#[derive(Debug)]
pub struct VShardReadResults {
    vshard_id: u32,
    state: Mutex<Buffered>,
    /// Wakes the scheduler: an event arrived.
    pushed: Notify,
    /// Read-result entries the apply loop applied for the vShard, a finished
    /// txn's included.
    reads_applied: AtomicU64,
    /// The `(group, log index)` of the latest read-result entries applied,
    /// oldest first, at most [`READ_ENTRY_HISTORY`].
    read_entries: Mutex<VecDeque<(u64, u64)>>,
}

/// How many applied read-result entries a vShard's buffer names.
pub const READ_ENTRY_HISTORY: usize = 64;

impl VShardReadResults {
    fn new(vshard_id: u32) -> Self {
        Self {
            vshard_id,
            state: Mutex::new(Buffered::default()),
            pushed: Notify::new(),
            reads_applied: AtomicU64::new(0),
            read_entries: Mutex::new(VecDeque::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Buffered> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The vShard this buffer serves.
    pub fn vshard_id(&self) -> u32 {
        self.vshard_id
    }

    /// Count one read-result entry the apply loop applied, at `log_index`
    /// of `group_id`.
    pub fn note_read_applied(&self, group_id: u64, log_index: u64) {
        self.reads_applied.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.read_entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries.len() == READ_ENTRY_HISTORY {
            entries.pop_front();
        }
        entries.push_back((group_id, log_index));
    }

    /// The `(group, log index)` of the latest read-result entries applied
    /// for the vShard since this buffer was created, oldest first.
    pub fn read_entries(&self) -> Vec<(u64, u64)> {
        self.read_entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .copied()
            .collect()
    }

    /// How many read-result entries the apply loop applied for the vShard
    /// since this buffer was created.
    pub fn reads_applied(&self) -> u64 {
        self.reads_applied.load(Ordering::Relaxed)
    }

    /// Whether an event of `txn` is missing from its stored row. Every later
    /// event of `txn` then skips the row.
    pub fn is_unsaved(&self, txn: TxnId) -> bool {
        self.lock().unsaved.contains(&txn)
    }

    /// Whether some txn of the vShard has an event its stored row lacks.
    pub fn has_unsaved(&self) -> bool {
        !self.lock().unsaved.is_empty()
    }

    /// Fold `event` of `txn`. `saved` says the event's row write
    /// succeeded.
    ///
    /// A saved event of a stored txn stays in its row alone. A saved event
    /// past `capacity` moves its txn to stored. An unsaved event stays in
    /// memory whatever the bound, and marks its txn unsaved.
    pub fn push(&self, txn: TxnId, event: BarrierEvent, capacity: usize, saved: bool) {
        {
            let mut state = self.lock();
            let in_row_alone = saved && !state.unsaved.contains(&txn);
            if !in_row_alone {
                state.unsaved.insert(txn);
            }
            let skip = if !in_row_alone {
                false
            } else if state.stored.contains(&txn) {
                true
            } else {
                let grows = state.logs.get(&txn).is_none_or(|log| log.accepts(&event));
                if grows && state.events >= capacity {
                    state.remove_log(txn);
                    state.stored.insert(txn);
                    true
                } else {
                    false
                }
            };
            if !skip {
                let mut later = BarrierLog::default();
                later.note(event);
                state.extend_log(txn, later);
            }
        }
        self.pushed.notify_one();
    }

    /// Take `txn`'s waiting events.
    pub fn take(&self, txn: TxnId) -> Option<BufferedEvents> {
        self.lock().take(txn)
    }

    /// Take the waiting events of every txn `held` names, in txn order.
    pub fn take_held(&self, held: impl Fn(TxnId) -> bool) -> Vec<(TxnId, BufferedEvents)> {
        let mut state = self.lock();
        let txns: BTreeSet<TxnId> = state
            .logs
            .keys()
            .chain(state.stored.iter())
            .copied()
            .filter(|txn| held(*txn))
            .collect();
        txns.into_iter()
            .filter_map(|txn| state.take(txn).map(|events| (txn, events)))
            .collect()
    }

    /// Hand back `earlier`, the events of `txn` a scheduler took and did
    /// not finish. They come before any event buffered since. `stored`
    /// says the txn's row holds events `earlier` lacks. A scheduler holds
    /// at most its in-flight backlog of txns, so the hand back takes no
    /// capacity check, and the next take returns them. It wakes no
    /// scheduler: the events are ones a scheduler took already.
    pub fn restore(&self, txn: TxnId, earlier: BufferedEvents) {
        let mut state = self.lock();
        let later = state.remove_log(txn);
        let mut log = earlier.memory;
        if let Some(later) = later {
            log.extend(later);
        }
        state.extend_log(txn, log);
        if earlier.stored {
            state.stored.insert(txn);
        }
    }

    /// Mark `txns` stored: their rows hold their events.
    pub fn mark_stored(&self, txns: impl IntoIterator<Item = TxnId>) {
        self.lock().stored.extend(txns);
        self.pushed.notify_one();
    }

    /// Replace every waiting event with the rows of `stored`. A snapshot
    /// install replaced the vShard's rows with the builder's.
    pub fn reset_to_stored(&self, stored: BTreeSet<TxnId>) {
        {
            let mut state = self.lock();
            *state = Buffered {
                stored,
                ..Buffered::default()
            };
        }
        self.pushed.notify_one();
    }

    /// Drop every waiting event of `txn`, which finished. Returns whether
    /// it had any.
    pub fn forget_txn(&self, txn: TxnId) -> bool {
        let mut state = self.lock();
        let unsaved = state.unsaved.remove(&txn);
        state.take(txn).is_some() || unsaved
    }

    /// Drop the events of every txn `finished` names: its position is
    /// applied, so no barrier of it opens again.
    pub fn discard_finished(&self, finished: impl Fn(TxnId) -> bool) {
        let mut state = self.lock();
        let done: Vec<TxnId> = state
            .logs
            .keys()
            .chain(state.stored.iter())
            .chain(state.unsaved.iter())
            .copied()
            .filter(|txn| finished(*txn))
            .collect();
        for txn in done {
            state.take(txn);
            state.unsaved.remove(&txn);
        }
    }

    /// Resolves once an event was pushed since the last wake.
    pub async fn pushed(&self) {
        self.pushed.notified().await;
    }

    /// How many txns have waiting events, in memory or in their row.
    pub fn waiting_txns(&self) -> usize {
        let state = self.lock();
        state
            .logs
            .keys()
            .filter(|txn| !state.stored.contains(txn))
            .count()
            + state.stored.len()
    }

    /// Whether `txn` has waiting events.
    pub fn waits(&self, txn: TxnId) -> bool {
        self.lock().waits(txn)
    }
}

/// The waiting barrier events of every vShard of this node.
#[derive(Debug, Default)]
pub struct CalvinReadResults {
    by_vshard: Mutex<HashMap<u32, Arc<VShardReadResults>>>,
}

impl CalvinReadResults {
    /// The buffer of `vshard_id`, created empty on first use.
    pub fn vshard(&self, vshard_id: u32) -> Arc<VShardReadResults> {
        let mut by_vshard = self.by_vshard.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            by_vshard
                .entry(vshard_id)
                .or_insert_with(|| Arc::new(VShardReadResults::new(vshard_id))),
        )
    }

    /// How many txns of `vshard_id` have waiting events.
    pub fn waiting_txns(&self, vshard_id: u32) -> usize {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&vshard_id)
            .map_or(0, |buffer| buffer.waiting_txns())
    }

    /// Drop the buffer of `vshard_id`, which left this node.
    pub fn forget(&self, vshard_id: u32) {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard_id);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
    use nodedb_types::{QualifiedCollection, Value};

    use super::*;

    fn read(passive_vshard: u32) -> BarrierEvent {
        BarrierEvent::Read {
            passive_vshard,
            values: vec![(
                PassiveReadKeyId::surrogate(QualifiedCollection::from_stored("c".to_owned()), 1),
                Value::Null,
            )],
        }
    }

    /// Events of one txn fold into one log, in push order.
    #[test]
    fn events_of_a_txn_fold_in_push_order() {
        let buffer = VShardReadResults::new(4);
        let txn = TxnId::new(3, 1);
        buffer.push(txn, read(1), 8, true);
        buffer.push(txn, BarrierEvent::Timeout, 8, true);
        buffer.push(txn, read(2), 8, true);
        assert_eq!(buffer.waiting_txns(), 1);
        let events = buffer.take(txn).expect("buffered log");
        assert!(!events.stored);
        assert_eq!(
            events.memory.missing(&[1, 2].into_iter().collect()),
            vec![2]
        );
        assert_eq!(buffer.waiting_txns(), 0);
    }

    /// A saved event past the bound moves its txn to its row, and every
    /// later saved event of it stays in the row alone.
    #[test]
    fn a_saved_event_past_the_bound_moves_its_txn_to_its_row() {
        let buffer = VShardReadResults::new(4);
        let first = TxnId::new(1, 0);
        let second = TxnId::new(1, 1);
        buffer.push(first, read(1), 1, true);
        buffer.push(second, read(1), 1, true);
        buffer.push(second, BarrierEvent::Timeout, 1, true);
        assert_eq!(buffer.waiting_txns(), 2);
        // A duplicate of a held event takes no room and moves nothing.
        buffer.push(first, read(1), 1, true);
        let first_events = buffer.take(first).expect("first txn");
        assert!(!first_events.stored);
        assert_eq!(first_events.memory.event_count(), 1);
        let second_events = buffer.take(second).expect("second txn");
        assert_eq!(
            second_events,
            BufferedEvents {
                memory: BarrierLog::default(),
                stored: true,
            }
        );
    }

    /// An unsaved event stays in memory past the bound, and every later
    /// event of its txn stays in memory too.
    #[test]
    fn an_unsaved_event_stays_in_memory_with_every_later_one() {
        let buffer = VShardReadResults::new(4);
        let full = TxnId::new(1, 0);
        let txn = TxnId::new(2, 0);
        buffer.push(full, read(1), 1, true);
        buffer.push(txn, read(1), 1, false);
        assert!(buffer.is_unsaved(txn));
        assert!(buffer.has_unsaved());
        buffer.push(txn, BarrierEvent::Timeout, 1, true);
        let events = buffer.take(txn).expect("unsaved txn");
        assert!(!events.stored);
        assert_eq!(events.memory.event_count(), 2);
        assert!(buffer.is_unsaved(txn), "the row still lacks the event");
        assert!(buffer.forget_txn(txn));
        assert!(!buffer.has_unsaved());
    }

    /// Only the txns the scheduler holds leave the buffer, stored ones too.
    #[test]
    fn take_held_takes_only_held_txns() {
        let buffer = VShardReadResults::new(4);
        buffer.push(TxnId::new(1, 0), read(1), 8, true);
        buffer.push(TxnId::new(2, 0), read(1), 8, true);
        buffer.mark_stored([TxnId::new(3, 0)]);
        let taken = buffer.take_held(|txn| txn.epoch >= 2);
        let txns: Vec<TxnId> = taken.iter().map(|(txn, _)| *txn).collect();
        assert_eq!(txns, vec![TxnId::new(2, 0), TxnId::new(3, 0)]);
        assert!(taken[1].1.stored);
        assert_eq!(buffer.waiting_txns(), 1);
    }

    /// Events a stopping scheduler hands back come before the ones buffered
    /// since, and a finished txn's events leave the buffer.
    #[test]
    fn restore_keeps_log_order_and_discard_drops_finished() {
        let buffer = VShardReadResults::new(4);
        let txn = TxnId::new(5, 0);
        buffer.push(txn, BarrierEvent::Timeout, 8, true);
        let mut earlier = BarrierLog::default();
        earlier.note(read(1));
        buffer.restore(
            txn,
            BufferedEvents {
                memory: earlier,
                stored: true,
            },
        );
        let events = buffer.take(txn).expect("restored log");
        assert!(events.stored);
        assert!(events.memory.missing(&[1].into_iter().collect()).is_empty());

        buffer.push(TxnId::new(6, 0), read(1), 8, false);
        buffer.mark_stored([TxnId::new(7, 0)]);
        buffer.discard_finished(|txn| txn.epoch >= 6);
        assert_eq!(buffer.waiting_txns(), 0);
        assert!(!buffer.has_unsaved());
    }

    /// A snapshot install replaces every waiting event with its rows.
    #[test]
    fn a_reset_keeps_only_the_installed_rows() {
        let buffer = VShardReadResults::new(4);
        buffer.push(TxnId::new(1, 0), read(1), 8, false);
        buffer.reset_to_stored(BTreeSet::from([TxnId::new(2, 0)]));
        assert!(!buffer.has_unsaved());
        assert!(!buffer.waits(TxnId::new(1, 0)));
        assert!(buffer.waits(TxnId::new(2, 0)));
    }

    /// The buffer names the latest applied read-result entries, oldest
    /// first, and drops the oldest past the history bound.
    #[test]
    fn applied_read_entries_keep_the_latest() {
        let buffer = VShardReadResults::new(4);
        for index in 0..(READ_ENTRY_HISTORY as u64 + 2) {
            buffer.note_read_applied(3, index);
        }
        let entries = buffer.read_entries();
        assert_eq!(entries.len(), READ_ENTRY_HISTORY);
        assert_eq!(entries.first(), Some(&(3, 2)));
        assert_eq!(buffer.reads_applied(), READ_ENTRY_HISTORY as u64 + 2);
    }

    /// A push wakes a waiting scheduler.
    #[tokio::test]
    async fn a_push_wakes_the_scheduler() {
        let buffer = Arc::new(VShardReadResults::new(4));
        let waiter = {
            let buffer = Arc::clone(&buffer);
            tokio::spawn(async move { buffer.pushed().await })
        };
        buffer.push(TxnId::new(1, 0), read(1), 8, true);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("the push wakes the waiter")
            .expect("waiter task");
    }
}
