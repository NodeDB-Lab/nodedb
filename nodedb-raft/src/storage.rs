// SPDX-License-Identifier: BUSL-1.1

use crate::error::Result;
use crate::message::LogEntry;
use crate::state::HardState;

/// Trait for persistent Raft log storage.
///
/// Implementors handle durability. The `nodedb-cluster` crate provides
/// a production implementation backed by `nodedb-wal`.
pub trait LogStorage: Send {
    /// Persist log entries: durable before returning, or staged and reported
    /// through [`Self::stable_through`] once durable.
    fn append(&mut self, entries: &[LogEntry]) -> Result<()>;

    /// Truncate log entries from `index` onward (inclusive).
    fn truncate(&mut self, index: u64) -> Result<()>;

    /// Load all entries after `snapshot_index` on startup.
    fn load_entries_after(&self, snapshot_index: u64) -> Result<Vec<LogEntry>>;

    /// Compact: discard entries up to `index`, save snapshot metadata.
    fn compact(&mut self, index: u64, term: u64) -> Result<()>;

    /// Return (last_included_index, last_included_term) of the latest snapshot.
    fn snapshot_metadata(&self) -> (u64, u64);

    /// Persist hard state (current_term, voted_for).
    fn save_hard_state(&mut self, state: &HardState) -> Result<()>;

    /// Load hard state on startup.
    fn load_hard_state(&self) -> Result<HardState>;

    /// Persist the durable applied index (must be durable before returning).
    ///
    /// `index` names an entry whose state-machine effects are ALREADY durable.
    /// The next boot resumes delivery at `index + 1`, so saving ahead of
    /// durability drops the entries in between; saving behind it re-delivers
    /// them.
    fn save_applied_index(&mut self, index: u64) -> Result<()>;

    /// Load the durable applied index on startup.
    ///
    /// Returns 0 when no index has ever been saved, which replays the whole
    /// retained log — the safe direction for storage written before this index
    /// existed.
    fn load_applied_index(&self) -> Result<u64>;

    /// The last log entry this storage holds durably, as `(index, term)`.
    ///
    /// `None`, the default, for storage whose every write is durable before
    /// it returns. Storage that stages writes and makes them durable later
    /// reports how far its disk has come. The node then counts only durable
    /// entries toward its own acknowledgement of a commit (see
    /// [`crate::RaftLog::stable_index`]). A caller of such storage makes a
    /// write durable before it sends a reply or a vote request that depends
    /// on it.
    fn stable_through(&self) -> Option<(u64, u64)> {
        None
    }
}

/// In-memory storage for testing.
#[derive(Debug, Default)]
pub struct MemStorage {
    entries: Vec<LogEntry>,
    hard_state: HardState,
    snapshot_index: u64,
    snapshot_term: u64,
    applied_index: u64,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStorage for MemStorage {
    fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        for entry in entries {
            // Overwrite or append.
            if let Some(pos) = self.entries.iter().position(|e| e.index == entry.index) {
                self.entries[pos] = entry.clone();
            } else {
                self.entries.push(entry.clone());
            }
        }
        Ok(())
    }

    fn truncate(&mut self, index: u64) -> Result<()> {
        self.entries.retain(|e| e.index < index);
        Ok(())
    }

    fn load_entries_after(&self, snapshot_index: u64) -> Result<Vec<LogEntry>> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index > snapshot_index)
            .cloned()
            .collect())
    }

    fn compact(&mut self, index: u64, term: u64) -> Result<()> {
        self.entries.retain(|e| e.index > index);
        self.snapshot_index = index;
        self.snapshot_term = term;
        Ok(())
    }

    fn snapshot_metadata(&self) -> (u64, u64) {
        (self.snapshot_index, self.snapshot_term)
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<()> {
        self.hard_state = state.clone();
        Ok(())
    }

    fn load_hard_state(&self) -> Result<HardState> {
        Ok(self.hard_state.clone())
    }

    fn save_applied_index(&mut self, index: u64) -> Result<()> {
        self.applied_index = index;
        Ok(())
    }

    fn load_applied_index(&self) -> Result<u64> {
        Ok(self.applied_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_storage_append_and_load() {
        let mut s = MemStorage::new();
        let entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                data: b"a".to_vec(),
            },
            LogEntry {
                term: 1,
                index: 2,
                data: b"b".to_vec(),
            },
        ];
        s.append(&entries).unwrap();

        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn mem_storage_truncate() {
        let mut s = MemStorage::new();
        for i in 1..=5 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.truncate(3).unwrap();
        let loaded = s.load_entries_after(0).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.last().unwrap().index, 2);
    }

    #[test]
    fn mem_storage_compact() {
        let mut s = MemStorage::new();
        for i in 1..=10 {
            s.append(&[LogEntry {
                term: 1,
                index: i,
                data: vec![],
            }])
            .unwrap();
        }
        s.compact(5, 1).unwrap();
        assert_eq!(s.snapshot_metadata(), (5, 1));
        let loaded = s.load_entries_after(5).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].index, 6);
    }

    #[test]
    fn mem_storage_hard_state() {
        let mut s = MemStorage::new();
        let hs = HardState {
            current_term: 5,
            voted_for: 2,
            ordinal_floor: 77,
        };
        s.save_hard_state(&hs).unwrap();
        let loaded = s.load_hard_state().unwrap();
        assert_eq!(loaded, hs);
    }

    #[test]
    fn mem_storage_applied_index() {
        let mut s = MemStorage::new();
        // Never saved: replay the whole retained log.
        assert_eq!(s.load_applied_index().unwrap(), 0);

        s.save_applied_index(42).unwrap();
        assert_eq!(s.load_applied_index().unwrap(), 42);
    }
}
