// SPDX-License-Identifier: BUSL-1.1

//! The decoded committed entries the catch-up replay reuses.
//!
//! Every armed vShard replays the same committed log range. Without a cache,
//! each one decodes every entry of the range again, though most entries
//! carry nothing for it. The cache holds each entry's [`ReplayEntry`] once per
//! node:
//!
//! - The live apply keeps a batch it decoded while any vShard is armed.
//! - The replay decodes only the entries the cache lacks, and keeps them.
//! - Clearing or dropping a catch-up forgets every entry below the lowest
//!   armed index, and every entry once nothing is armed.
//!
//! A byte budget bounds the cache. Past it, the lowest indexes go first. An
//! evicted entry is read and decoded again from the log when a replay needs
//! it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::calvin::types::{EpochBatch, SchedulerInput};

use super::core::SequencerStateMachine;
use super::replay_entry::ReplayEntry;

/// The byte budget of the cache.
pub(super) const REPLAY_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// The fixed charge each entry adds to its raw length.
pub(super) const ENTRY_CHARGE: usize = 256;

/// The decoded entries, by sequencer log index, with their charged bytes.
#[derive(Debug, Default)]
pub(super) struct ReplayCache {
    entries: BTreeMap<u64, (usize, Arc<ReplayEntry>)>,
    bytes: usize,
}

impl ReplayCache {
    /// The highest index `h` in `from ..= to` such that every index of
    /// `from ..= h` is held, or `None` when `from` is not held.
    pub(super) fn kept_through(&self, from: u64, to: u64) -> Option<u64> {
        if from > to {
            return None;
        }
        let mut through = None;
        let mut next = from;
        for index in self.entries.range(from..=to).map(|(&index, _)| index) {
            if index != next {
                break;
            }
            through = Some(index);
            match next.checked_add(1) {
                Some(after) => next = after,
                None => break,
            }
        }
        through
    }

    /// The held entries of `from ..= to`, in index order.
    pub(super) fn held(&self, from: u64, to: u64) -> Vec<(u64, Arc<ReplayEntry>)> {
        if from > to {
            return Vec::new();
        }
        self.entries
            .range(from..=to)
            .map(|(&index, (_, entry))| (index, Arc::clone(entry)))
            .collect()
    }

    /// Hold `entry`, decoded from `raw_len` bytes at `index`, then evict the
    /// lowest indexes while the cache is past its budget.
    pub(super) fn keep(&mut self, index: u64, raw_len: usize, entry: Arc<ReplayEntry>) {
        let charge = raw_len.saturating_add(ENTRY_CHARGE);
        if let Some((old, _)) = self.entries.insert(index, (charge, entry)) {
            self.bytes = self.bytes.saturating_sub(old);
        }
        self.bytes = self.bytes.saturating_add(charge);
        while self.bytes > REPLAY_CACHE_MAX_BYTES {
            let Some((_, (charge, _))) = self.entries.pop_first() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(charge);
        }
    }

    /// Forget every entry below `floor`, or every entry when `floor` is
    /// `None`.
    pub(super) fn forget_below(&mut self, floor: Option<u64>) {
        match floor {
            Some(floor) => {
                let kept = self.entries.split_off(&floor);
                let forgotten = std::mem::replace(&mut self.entries, kept);
                let freed: usize = forgotten.values().map(|(charge, _)| *charge).sum();
                self.bytes = self.bytes.saturating_sub(freed);
            }
            None => {
                self.entries.clear();
                self.bytes = 0;
            }
        }
    }

    /// How many entries the cache holds.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

impl SequencerStateMachine {
    /// The highest index `h` in `from ..= to` such that the replay holds
    /// every entry of `from ..= h` decoded, or `None` when it lacks `from`.
    ///
    /// The catch-up drain reads the log only from `h + 1`.
    pub fn replay_kept_through(&self, from: u64, to: u64) -> Option<u64> {
        self.replay_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .kept_through(from, to)
    }

    /// The inputs for `vshard` of the committed entries `from ..= to`, each
    /// with its Raft index, in log order.
    ///
    /// `read` holds the entries read from the log. An entry the cache lacks
    /// is decoded once from `read`, and kept. Returns `None` when an index
    /// of the range is neither held nor read: the caller reads again.
    pub fn replay_window_for_vshard(
        &self,
        read: &[nodedb_raft::LogEntry],
        vshard: u32,
        from: u64,
        to: u64,
    ) -> Option<Vec<(u64, SchedulerInput)>> {
        if from > to {
            return Some(Vec::new());
        }
        let held = self
            .replay_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .held(from, to);
        let mut window: BTreeMap<u64, Arc<ReplayEntry>> = held.into_iter().collect();
        let missing: Vec<&nodedb_raft::LogEntry> = read
            .iter()
            .filter(|entry| entry.index >= from && entry.index <= to)
            .filter(|entry| !window.contains_key(&entry.index))
            .collect();
        let span = (to - from).checked_add(1)?;
        let present = (window.len() as u64).saturating_add(missing.len() as u64);
        if present < span {
            return None;
        }

        // Decode outside the cache lock, then keep the results.
        let fresh: Vec<(u64, usize, Arc<ReplayEntry>)> = missing
            .into_iter()
            .map(|entry| {
                self.metrics.replay_decodes.fetch_add(1, Ordering::Relaxed);
                (
                    entry.index,
                    entry.data.len(),
                    Arc::new(ReplayEntry::decode(entry)),
                )
            })
            .collect();
        {
            let mut cache = self.replay_cache.lock().unwrap_or_else(|p| p.into_inner());
            for (index, raw_len, entry) in &fresh {
                cache.keep(*index, *raw_len, Arc::clone(entry));
            }
        }
        for (index, _, entry) in fresh {
            window.insert(index, entry);
        }

        let mut out = Vec::new();
        let mut inputs = Vec::new();
        for (index, entry) in window {
            entry.project(vshard, 0, u64::MAX, &mut inputs);
            out.extend(inputs.drain(..).map(|input| (index, input)));
        }
        Some(out)
    }

    /// Keep `batch`, the restored batch apply decoded from `raw_len` bytes at
    /// `index`, when an armed vShard's replay covers `index`.
    pub(super) fn keep_for_replay(&self, index: u64, raw_len: usize, batch: EpochBatch) {
        if self.replay_covers(index) {
            self.keep_replay_entry(index, raw_len, ReplayEntry::restored_batch(batch));
        }
    }

    /// Keep an entry with no input at `index`, decoded from `raw_len` bytes,
    /// when an armed vShard's replay covers `index`. Apply skipped it.
    pub(super) fn keep_skipped_for_replay(&self, index: u64, raw_len: usize) {
        if self.replay_covers(index) {
            self.keep_replay_entry(index, raw_len, ReplayEntry::Nothing);
        }
    }

    /// Whether the lowest armed catch-up index is at or below `index`.
    fn replay_covers(&self, index: u64) -> bool {
        self.min_catch_up_from().is_some_and(|from| from <= index)
    }

    /// Hold `entry` at `index` in the cache.
    fn keep_replay_entry(&self, index: u64, raw_len: usize, entry: ReplayEntry) {
        self.replay_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keep(index, raw_len, Arc::new(entry));
    }

    /// Forget every held entry below the lowest armed catch-up index, or
    /// every entry when nothing is armed.
    pub(super) fn prune_replay_cache(&self) {
        // Read the floor first: the catch-up lock is never held with the cache
        // lock.
        let floor = self.min_catch_up_from();
        self.replay_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .forget_below(floor);
    }

    /// How many decoded entries the replay holds.
    #[cfg(test)]
    pub(super) fn replay_cache_len(&self) -> usize {
        self.replay_cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::sequencer::entry::SequencerEntry;
    use crate::calvin::types::{
        EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass, VersionedReadSet,
    };
    use nodedb_types::TenantId;
    use nodedb_types::id::{CollectionKey, DatabaseId};

    const VSHARDS: usize = 64;

    /// Collection names homed on `count` distinct vShards, with the vShards.
    fn distinct_collections(count: usize) -> Vec<(String, u32)> {
        let mut seen = std::collections::HashSet::new();
        let mut found = Vec::new();
        for i in 0u32..100_000 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            if seen.insert(vshard) {
                found.push((name, vshard));
                if found.len() == count {
                    return found;
                }
            }
        }
        panic!("found {} distinct vShards, wanted {count}", found.len());
    }

    /// One epoch-0 batch whose one txn writes every collection of `homes`.
    fn header(homes: &[(String, u32)]) -> EpochBatch {
        let write_set = ReadWriteSet::new(
            homes
                .iter()
                .enumerate()
                .map(|(i, (collection, _))| EngineKeySet::Document {
                    collection: collection.clone(),
                    surrogates: SortedVec::new(vec![i as u32 + 1]),
                })
                .collect(),
        );
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class: Arc::new(tx_class),
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 0,
                lock_owner: None,
                local_plans: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        }
    }

    fn encode(batch: &EpochBatch) -> Vec<u8> {
        zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        })
        .expect("encode")
    }

    /// A state machine hosting every vShard of `homes`, each armed at 0, with
    /// the receivers kept open.
    fn armed_machine(
        homes: &[(String, u32)],
    ) -> (SequencerStateMachine, Vec<mpsc::Receiver<SchedulerInput>>) {
        let mut senders = HashMap::new();
        let mut receivers = Vec::new();
        for (_, vshard) in homes {
            let (tx, rx) = mpsc::channel(4);
            senders.insert(*vshard, tx);
            receivers.push(rx);
        }
        let sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        for (_, vshard) in homes {
            sm.arm_catch_up_from(*vshard, 0);
        }
        (sm, receivers)
    }

    fn replay_decodes(sm: &SequencerStateMachine) -> u64 {
        sm.metrics.replay_decodes.load(Ordering::Relaxed)
    }

    #[test]
    fn a_header_applied_while_armed_is_never_decoded_by_replay() {
        let homes = distinct_collections(VSHARDS);
        let (mut sm, _receivers) = armed_machine(&homes);
        sm.apply(0, &encode(&header(&homes)));

        assert_eq!(sm.replay_kept_through(0, 0), Some(0));
        for (_, vshard) in &homes {
            let inputs = sm
                .replay_window_for_vshard(&[], *vshard, 0, 0)
                .expect("the cache holds index 0");
            assert_eq!(inputs.len(), 1, "one txn for vShard {vshard}");
            match &inputs[0] {
                (0, SchedulerInput::Txn(txn)) => {
                    assert_eq!(txn.epoch_vshard_txn_count, 1);
                    assert_eq!(txn.epoch_system_ms, 1_700_000_000_000);
                }
                other => panic!("expected the txn at index 0, got {other:?}"),
            }
        }
        assert_eq!(replay_decodes(&sm), 0);
    }

    #[test]
    fn an_uncached_entry_decodes_once_for_every_armed_vshard() {
        let homes = distinct_collections(VSHARDS);
        let (sm, _receivers) = armed_machine(&homes);
        let batch = header(&homes);
        let read = vec![nodedb_raft::LogEntry {
            term: 1,
            index: 0,
            data: encode(&batch),
        }];

        assert_eq!(sm.replay_kept_through(0, 0), None);
        for (_, vshard) in &homes {
            let inputs = sm
                .replay_window_for_vshard(&read, *vshard, 0, 0)
                .expect("the read covers index 0");
            assert_eq!(inputs.len(), 1);
        }
        assert_eq!(replay_decodes(&sm), 1);

        // An index neither held nor read leaves the window to a later read.
        assert!(sm.replay_window_for_vshard(&[], homes[0].1, 0, 1).is_none());
        assert_eq!(replay_decodes(&sm), 1);
    }

    #[test]
    fn clearing_every_catch_up_empties_the_cache() {
        let homes = distinct_collections(4);
        let (mut sm, _receivers) = armed_machine(&homes);
        sm.apply(0, &encode(&header(&homes)));
        assert_eq!(sm.replay_cache_len(), 1);

        for (_, vshard) in &homes[..3] {
            sm.clear_catch_up_up_to(*vshard, 0);
        }
        assert_eq!(sm.replay_cache_len(), 1, "one vShard still owes index 0");
        sm.clear_catch_up_up_to(homes[3].1, 0);
        assert_eq!(sm.replay_cache_len(), 0);
    }

    #[test]
    fn eviction_past_the_budget_drops_the_lowest_indexes() {
        let mut cache = ReplayCache::default();
        let raw_len = REPLAY_CACHE_MAX_BYTES / 4;
        for index in 0..4 {
            cache.keep(index, raw_len, Arc::new(ReplayEntry::Nothing));
        }
        assert_eq!(cache.len(), 3, "the fourth charge passes the budget");
        assert_eq!(cache.kept_through(0, 3), None, "index 0 went first");
        assert_eq!(cache.kept_through(1, 3), Some(3));
        assert!(cache.bytes <= REPLAY_CACHE_MAX_BYTES);

        cache.forget_below(Some(3));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes, raw_len + ENTRY_CHARGE);
        cache.forget_below(None);
        assert_eq!((cache.len(), cache.bytes), (0, 0));
    }
}
