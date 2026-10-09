// SPDX-License-Identifier: BUSL-1.1

use crate::error::{RaftError, Result};
use crate::message::LogEntry;
use crate::storage::LogStorage;

/// Most entry data one AppendEntries request carries. Half the 64 MiB RPC
/// frame limit, so a batch and its envelope fit one frame. A single entry
/// larger than this ships alone, and the host bounds an entry's size.
pub const MAX_APPEND_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// In-memory Raft log backed by a pluggable `LogStorage`.
///
/// The log is 1-indexed. Index 0 is a sentinel (term 0, empty data).
/// Compacted entries are replaced by a snapshot; only entries after
/// `snapshot_index` are retained in memory.
pub struct RaftLog<S: LogStorage> {
    /// In-memory buffer of log entries (post-snapshot).
    entries: Vec<LogEntry>,
    /// Index of the last entry included in the snapshot.
    snapshot_index: u64,
    /// Term of the last entry included in the snapshot.
    snapshot_term: u64,
    /// Persistent storage backend.
    storage: S,
}

impl<S: LogStorage> RaftLog<S> {
    pub fn new(storage: S) -> Self {
        Self {
            entries: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            storage,
        }
    }

    /// Restore from storage on startup.
    pub fn restore(&mut self) -> Result<()> {
        let (snap_index, snap_term) = self.storage.snapshot_metadata();
        self.snapshot_index = snap_index;
        self.snapshot_term = snap_term;
        self.entries = self.storage.load_entries_after(snap_index)?;
        Ok(())
    }

    /// Last log index (snapshot_index if log is empty).
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    /// Last log term.
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Get the term at a given index.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index {
            return None; // Compacted.
        }
        self.entry_at(index).map(|e| e.term)
    }

    /// Get entry at a given index.
    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_index || index > self.last_index() {
            return None;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Entries from `lo` up to `hi` inclusive whose data sums to at most
    /// `max_bytes`. The first entry is always included, so an entry larger
    /// than `max_bytes` still ships, alone.
    pub fn entries_within(&self, lo: u64, hi: u64, max_bytes: usize) -> Result<&[LogEntry]> {
        let range = self.entries_range(lo, hi)?;
        let mut total = 0usize;
        let mut end = 0usize;
        for entry in range {
            total = total.saturating_add(entry.data.len());
            if end > 0 && total > max_bytes {
                break;
            }
            end += 1;
        }
        Ok(&range[..end])
    }

    /// Get entries in range [lo, hi] inclusive.
    pub fn entries_range(&self, lo: u64, hi: u64) -> Result<&[LogEntry]> {
        if lo <= self.snapshot_index {
            return Err(RaftError::LogCompacted {
                requested: lo,
                first_available: self.snapshot_index + 1,
            });
        }
        if lo > hi || lo > self.last_index() {
            return Ok(&[]);
        }
        let start = (lo - self.snapshot_index - 1) as usize;
        let end = ((hi.min(self.last_index()) - self.snapshot_index - 1) + 1) as usize;
        Ok(&self.entries[start..end])
    }

    /// Append new entries from a leader's AppendEntries RPC. Returns whether
    /// storage took a write.
    ///
    /// Handles conflict detection per Raft paper §5.3:
    /// - If an existing entry conflicts with a new one (same index, different
    ///   terms), delete the existing entry and all that follow it.
    /// - Append any new entries not already in the log.
    ///
    /// An entry this log already holds at the same term is not written again.
    /// A leader resends a range until it is acknowledged, and a rewrite of a
    /// held range only delays the reply.
    ///
    /// Persistence happens BEFORE the in-memory log is mutated. A success
    /// reply claims the matched entries as durable on this node. A storage
    /// write that fails after memory moved leaves a claim on entries storage
    /// never took, and a restart loses them. A failed persist therefore
    /// leaves memory covering exactly what storage took.
    /// Storage that stages writes accepts them at once. When this returns
    /// `true`, the caller makes them durable before the reply leaves.
    pub fn append_entries(&mut self, _prev_index: u64, entries: &[LogEntry]) -> Result<bool> {
        // The first entry this log does not hold at the same term. Detection
        // mutates nothing: the durable writes below can still fail.
        let Some(first_new) = entries.iter().position(|e| {
            e.index > self.snapshot_index
                && self
                    .entry_at(e.index)
                    .is_none_or(|existing| existing.term != e.term)
        }) else {
            return Ok(false);
        };
        let new = &entries[first_new..];

        let conflict = new
            .first()
            .filter(|e| self.entry_at(e.index).is_some())
            .map(|e| e.index);
        if let Some(index) = conflict {
            self.truncate_from(index)?;
        }
        self.storage.append(new)?;
        self.entries.extend_from_slice(new);
        Ok(true)
    }

    /// Append a single entry proposed by the leader.
    pub fn append(&mut self, entry: LogEntry) -> Result<()> {
        self.storage.append(std::slice::from_ref(&entry))?;
        self.entries.push(entry);
        Ok(())
    }

    /// Truncate entries from `index` onward (inclusive).
    ///
    /// The storage truncation is applied first and its failure is propagated.
    /// Dropping the suffix in memory while storage still holds it would make
    /// this node ack the overwriting entries while a restart resurrects the
    /// stale suffix underneath them.
    fn truncate_from(&mut self, index: u64) -> Result<()> {
        if index <= self.snapshot_index {
            return Ok(());
        }
        self.storage.truncate(index)?;
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.truncate(offset);
        Ok(())
    }

    /// Apply a snapshot: discard all entries up to `last_included_index`.
    ///
    /// Storage compacts first. A storage error leaves the in-memory log
    /// untouched, so memory never claims a boundary storage does not hold.
    pub fn apply_snapshot(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        if last_included_index <= self.snapshot_index {
            return Ok(());
        }
        self.storage
            .compact(last_included_index, last_included_term)?;
        let new_start = last_included_index + 1;
        self.entries.retain(|e| e.index >= new_start);
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
        Ok(())
    }

    /// The last index of this log that storage holds durably.
    ///
    /// Storage whose writes are durable on return holds the whole log. Storage
    /// that stages writes reports its last durable entry `(index, term)`. That
    /// entry counts only when this log holds the same entry: Raft logs that
    /// agree on one entry agree on every entry before it. A durable entry this
    /// log no longer holds, or holds at another term, means a truncation is
    /// still on its way to disk. The durable prefix is then not known past the
    /// snapshot boundary, and the smaller of the two answers.
    pub fn stable_index(&self) -> u64 {
        let Some((index, term)) = self.storage.stable_through() else {
            return self.last_index();
        };
        if index <= self.last_index() && self.term_at(index) == Some(term) {
            return index;
        }
        index.min(self.snapshot_index)
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    fn make_entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![],
        }
    }

    #[test]
    fn empty_log() {
        let log = RaftLog::new(MemStorage::new());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0));
    }

    #[test]
    fn append_and_retrieve() {
        let mut log = RaftLog::new(MemStorage::new());
        log.append(make_entry(1, 1)).unwrap();
        log.append(make_entry(1, 2)).unwrap();
        log.append(make_entry(2, 3)).unwrap();

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert!(log.entry_at(4).is_none());
    }

    #[test]
    fn entries_range() {
        let mut log = RaftLog::new(MemStorage::new());
        for i in 1..=5 {
            log.append(make_entry(1, i)).unwrap();
        }
        let range = log.entries_range(2, 4).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[2].index, 4);
    }

    #[test]
    fn entries_within_stops_at_the_byte_budget_and_keeps_one_entry() {
        let mut log = RaftLog::new(MemStorage::new());
        for i in 1..=4 {
            log.append(LogEntry {
                term: 1,
                index: i,
                data: vec![0; 10],
            })
            .unwrap();
        }
        assert_eq!(log.entries_within(1, 4, 25).unwrap().len(), 2);
        assert_eq!(log.entries_within(1, 4, 40).unwrap().len(), 4);
        // An entry past the budget ships alone.
        assert_eq!(log.entries_within(2, 4, 5).unwrap().len(), 1);
        assert!(log.entries_within(5, 4, 5).unwrap().is_empty());
    }

    #[test]
    fn conflict_detection() {
        let mut log = RaftLog::new(MemStorage::new());
        log.append(make_entry(1, 1)).unwrap();
        log.append(make_entry(1, 2)).unwrap();
        log.append(make_entry(1, 3)).unwrap();

        // Leader sends entries starting at index 2 with different term.
        let new_entries = vec![make_entry(2, 2), make_entry(2, 3), make_entry(2, 4)];
        log.append_entries(1, &new_entries).unwrap();

        assert_eq!(log.last_index(), 4);
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), Some(2));
    }

    #[test]
    fn snapshot_compaction() {
        let mut log = RaftLog::new(MemStorage::new());
        for i in 1..=10 {
            log.append(make_entry(1, i)).unwrap();
        }

        log.apply_snapshot(5, 1).unwrap();
        assert_eq!(log.snapshot_index(), 5);
        assert_eq!(log.last_index(), 10);
        // Compacted entries are gone.
        assert!(log.entry_at(3).is_none());
        assert!(log.entry_at(5).is_none()); // snapshot boundary
        assert!(log.entry_at(6).is_some());

        // Range query into compacted region fails.
        assert!(log.entries_range(3, 8).is_err());
    }

    /// A `LogStorage` whose `append` / `truncate` can be armed to fail, so the
    /// in-memory log's reaction to a durability failure is observable.
    #[derive(Default)]
    struct FlakyStorage {
        inner: MemStorage,
        fail_append: bool,
        fail_truncate: bool,
    }

    impl LogStorage for FlakyStorage {
        fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
            if self.fail_append {
                return Err(RaftError::Storage {
                    detail: "injected append failure".into(),
                });
            }
            self.inner.append(entries)
        }

        fn truncate(&mut self, index: u64) -> Result<()> {
            if self.fail_truncate {
                return Err(RaftError::Storage {
                    detail: "injected truncate failure".into(),
                });
            }
            self.inner.truncate(index)
        }

        fn load_entries_after(&self, snapshot_index: u64) -> Result<Vec<LogEntry>> {
            self.inner.load_entries_after(snapshot_index)
        }

        fn compact(&mut self, index: u64, term: u64) -> Result<()> {
            self.inner.compact(index, term)
        }

        fn snapshot_metadata(&self) -> (u64, u64) {
            self.inner.snapshot_metadata()
        }

        fn save_hard_state(&mut self, state: &crate::state::HardState) -> Result<()> {
            self.inner.save_hard_state(state)
        }

        fn load_hard_state(&self) -> Result<crate::state::HardState> {
            self.inner.load_hard_state()
        }

        fn save_applied_index(&mut self, index: u64) -> Result<()> {
            self.inner.save_applied_index(index)
        }

        fn load_applied_index(&self) -> Result<u64> {
            self.inner.load_applied_index()
        }
    }

    /// A failed persist must leave `last_index()` covering only what storage
    /// holds — otherwise the AppendEntries response advertises entries that
    /// vanish on restart.
    #[test]
    fn failed_persist_does_not_advance_last_index() {
        let mut log = RaftLog::new(FlakyStorage::default());
        log.append(make_entry(1, 1)).expect("first append");
        assert_eq!(log.last_index(), 1);

        log.storage_mut().fail_append = true;
        let err = log.append_entries(1, &[make_entry(1, 2), make_entry(1, 3)]);
        assert!(err.is_err(), "storage failure must propagate");
        assert_eq!(
            log.last_index(),
            1,
            "in-memory log must not cover unpersisted entries"
        );
        assert!(log.entry_at(2).is_none());

        // Storage recovered: the leader's resend now lands for real.
        log.storage_mut().fail_append = false;
        log.append_entries(1, &[make_entry(1, 2), make_entry(1, 3)])
            .expect("resend after recovery");
        assert_eq!(log.last_index(), 3);
    }

    /// A resent range this log already holds takes no storage write. Only the
    /// entries past it are written.
    #[test]
    fn a_resent_held_range_writes_nothing() {
        let mut log = RaftLog::new(FlakyStorage::default());
        let held = [make_entry(1, 1), make_entry(1, 2)];
        assert!(log.append_entries(0, &held).expect("first delivery"));

        log.storage_mut().fail_append = true;
        let wrote = log
            .append_entries(0, &held)
            .expect("a held range needs no write");
        assert!(!wrote);

        log.storage_mut().fail_append = false;
        let wrote = log
            .append_entries(0, &[make_entry(1, 1), make_entry(1, 2), make_entry(1, 3)])
            .expect("extend past the held range");
        assert!(wrote);
        assert_eq!(log.last_index(), 3);
        let persisted = log.storage().load_entries_after(0).expect("load");
        assert_eq!(persisted.len(), 3);
    }

    /// A failed truncate must leave the conflicting suffix in memory, matching
    /// what storage still holds, and must not append the overwriting entries.
    #[test]
    fn failed_truncate_keeps_memory_and_storage_in_sync() {
        let mut log = RaftLog::new(FlakyStorage::default());
        for i in 1..=3 {
            log.append(make_entry(1, i)).expect("seed append");
        }

        log.storage_mut().fail_truncate = true;
        let err = log.append_entries(1, &[make_entry(2, 2), make_entry(2, 3)]);
        assert!(err.is_err(), "truncate failure must propagate");
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(2), Some(1), "old suffix must survive");
        assert_eq!(log.term_at(3), Some(1));

        let persisted = log.storage().load_entries_after(0).expect("load");
        assert_eq!(persisted.len(), 3);
        assert!(persisted.iter().all(|e| e.term == 1));
    }

    /// The successful conflict path still overwrites in both places.
    #[test]
    fn successful_truncate_persists_the_overwrite() {
        let mut log = RaftLog::new(FlakyStorage::default());
        for i in 1..=3 {
            log.append(make_entry(1, i)).expect("seed append");
        }

        log.append_entries(1, &[make_entry(2, 2), make_entry(2, 3), make_entry(2, 4)])
            .expect("overwrite");
        assert_eq!(log.last_index(), 4);

        let persisted = log.storage().load_entries_after(0).expect("load");
        assert_eq!(persisted.len(), 4);
        assert_eq!(persisted[1].term, 2);
        assert_eq!(persisted[3].index, 4);
    }
}
