// SPDX-License-Identifier: BUSL-1.1

//! The Raft log storage a mounted group runs on: every write is staged on
//! the group's disk and returns at once.
//!
//! The Raft node keeps its whole log in memory, so it reads nothing back
//! after its restore. A write returns before it is durable, so the node
//! learns how far the disk has come from [`LogStorage::stable_through`], and
//! every caller that sends a reply or a vote request depending on a write
//! first awaits a [`super::DurabilityTicket`].

use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use nodedb_raft::storage::LogStorage;
use tokio::sync::Notify;
use tracing::error;

use super::ticket::DurabilityTicket;
use super::writer::{GroupDisk, StorageOp};

/// A group's staged log storage and the owner of its writer thread.
pub struct StagedLogStorage {
    disk: Arc<GroupDisk>,
    /// The snapshot boundary as this node's log holds it.
    snapshot: (u64, u64),
    writer: Option<JoinHandle<()>>,
}

impl StagedLogStorage {
    /// Open `group_id`'s log at `path` and start its writer thread. `wake` is
    /// notified each time the writer makes a batch durable. Blocks on disk:
    /// call it off the async threads.
    pub fn open(group_id: u64, path: &Path, wake: Arc<Notify>) -> crate::Result<Self> {
        let disk = Arc::new(GroupDisk::open(group_id, path, wake)?);
        let snapshot = disk.storage().snapshot_metadata();
        disk.set_restored_stable(snapshot);
        let writer = {
            let disk = Arc::clone(&disk);
            std::thread::Builder::new()
                .name(format!("raft-disk-{group_id}"))
                .spawn(move || disk.run_writer())
                .map_err(|e| crate::ClusterError::Storage {
                    detail: format!("start the disk writer of raft group {group_id}: {e}"),
                })?
        };
        Ok(Self {
            disk,
            snapshot,
            writer: Some(writer),
        })
    }

    /// The group's disk, shared with its tickets.
    pub fn disk(&self) -> &Arc<GroupDisk> {
        &self.disk
    }

    /// A ticket for every write staged so far, or `None` when all of them
    /// are durable.
    pub fn ticket(&self) -> Option<DurabilityTicket> {
        DurabilityTicket::for_staged(&self.disk)
    }

    /// The sequence number of the last staged write. A caller reads it
    /// before a Raft call and passes it to [`Self::reply_ticket`].
    pub fn staged_through(&self) -> u64 {
        self.disk.staged_through()
    }

    /// A ticket for the writes a reply built after `mark` depends on, or
    /// `None` when they are durable. See [`DurabilityTicket::for_reply`].
    pub fn reply_ticket(&self, mark: u64) -> Option<DurabilityTicket> {
        DurabilityTicket::for_reply(&self.disk, mark)
    }
}

impl Drop for StagedLogStorage {
    /// Close the disk and wait for its writer to make the staged writes
    /// durable. The log file is closed when this returns, so the group can be
    /// opened again. Callers drop a replica off the async threads.
    fn drop(&mut self) {
        self.disk.close();
        if let Some(writer) = self.writer.take()
            && writer.join().is_err()
        {
            error!(
                group_id = self.disk.group_id(),
                "raft disk: the writer thread panicked"
            );
        }
    }
}

impl LogStorage for StagedLogStorage {
    fn append(&mut self, entries: &[LogEntry]) -> nodedb_raft::error::Result<()> {
        if !entries.is_empty() {
            self.disk.stage(StorageOp::Append(entries.to_vec()));
        }
        Ok(())
    }

    fn truncate(&mut self, index: u64) -> nodedb_raft::error::Result<()> {
        self.disk.stage(StorageOp::Truncate(index));
        Ok(())
    }

    fn load_entries_after(&self, snapshot_index: u64) -> nodedb_raft::error::Result<Vec<LogEntry>> {
        let entries = self.disk.storage().load_entries_after(snapshot_index)?;
        if let Some(last) = entries.last() {
            self.disk.set_restored_stable((last.index, last.term));
        }
        Ok(entries)
    }

    fn compact(&mut self, index: u64, term: u64) -> nodedb_raft::error::Result<()> {
        if index > self.snapshot.0 {
            self.snapshot = (index, term);
        }
        self.disk.stage(StorageOp::Compact { index, term });
        Ok(())
    }

    fn snapshot_metadata(&self) -> (u64, u64) {
        self.snapshot
    }

    fn save_hard_state(&mut self, state: &HardState) -> nodedb_raft::error::Result<()> {
        self.disk.stage(StorageOp::HardState(state.clone()));
        Ok(())
    }

    fn load_hard_state(&self) -> nodedb_raft::error::Result<HardState> {
        self.disk.storage().load_hard_state()
    }

    fn save_applied_index(&mut self, index: u64) -> nodedb_raft::error::Result<()> {
        self.disk.stage(StorageOp::AppliedIndex(index));
        Ok(())
    }

    fn load_applied_index(&self) -> nodedb_raft::error::Result<u64> {
        self.disk.storage().load_applied_index()
    }

    fn stable_through(&self) -> Option<(u64, u64)> {
        Some(self.disk.progress().stable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![index as u8],
        }
    }

    /// Staged writes become durable in order, and a reopen reads them back.
    #[test]
    fn staged_writes_are_durable_after_their_ticket_and_survive_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-7.redb");
        {
            let mut storage = StagedLogStorage::open(7, &path, Arc::default()).expect("open");
            storage
                .append(&[entry(1, 1), entry(2, 1), entry(3, 1)])
                .expect("stage");
            storage.truncate(3).expect("stage");
            storage.append(&[entry(3, 2)]).expect("stage");
            storage
                .save_hard_state(&HardState {
                    current_term: 2,
                    voted_for: 5,
                })
                .expect("stage");
            let ticket = storage.ticket().expect("writes are staged");
            assert!(ticket.wait_blocking());
            assert_eq!(storage.stable_through(), Some((3, 2)));
            assert!(storage.ticket().is_none(), "nothing is left to write");
        }
        let storage = StagedLogStorage::open(7, &path, Arc::default()).expect("reopen");
        let entries = storage.load_entries_after(0).expect("load");
        let terms: Vec<(u64, u64)> = entries.iter().map(|e| (e.index, e.term)).collect();
        assert_eq!(terms, vec![(1, 1), (2, 1), (3, 2)]);
        assert_eq!(storage.load_hard_state().expect("load").voted_for, 5);
        assert_eq!(storage.stable_through(), Some((3, 2)));
    }

    /// Dropping the storage makes every staged write durable first.
    #[test]
    fn a_drop_flushes_the_staged_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-8.redb");
        {
            let mut storage = StagedLogStorage::open(8, &path, Arc::default()).expect("open");
            storage.append(&[entry(1, 1)]).expect("stage");
            storage.save_applied_index(1).expect("stage");
        }
        let storage = StagedLogStorage::open(8, &path, Arc::default()).expect("reopen");
        assert_eq!(storage.load_entries_after(0).expect("load").len(), 1);
        assert_eq!(storage.load_applied_index().expect("load"), 1);
    }
}
