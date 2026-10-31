// SPDX-License-Identifier: BUSL-1.1

//! One Raft group's disk: its redb log storage and the writer thread that
//! makes staged writes durable, in the order they were staged.
//!
//! A group's Raft node stages every storage write here and returns at once,
//! so no disk write runs under the `MultiRaft` lock or on an async thread.
//! The writer thread drains the queue and applies each write to redb, one
//! fsync per batch of contiguous appends. It then publishes:
//! - the highest staged sequence number that is durable, for callers that
//!   hold a [`super::DurabilityTicket`];
//! - the last log entry the disk holds durably, which the node's leader
//!   counts toward its own commit acknowledgement.
//!
//! A write that fails is retried after [`RETRY_DELAY`] and never skipped:
//! every later write depends on it. Once the disk is closed, a failing write
//! is abandoned after one attempt, and the writer ends.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use nodedb_raft::storage::LogStorage;
use tokio::sync::{Notify, watch};
use tracing::{error, warn};

use crate::raft_storage::RedbLogStorage;

/// The wait before a failed write is tried again.
const RETRY_DELAY: Duration = Duration::from_millis(50);

/// One storage write, staged by the group's Raft node.
#[derive(Debug, Clone)]
pub(super) enum StorageOp {
    Append(Vec<LogEntry>),
    Truncate(u64),
    Compact { index: u64, term: u64 },
    HardState(HardState),
    AppliedIndex(u64),
}

/// How far the disk has come.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskProgress {
    /// Every write staged at or below this sequence number is durable.
    pub durable_seq: u64,
    /// The last log entry the disk holds durably, as `(index, term)`. A term
    /// of 0 above index 0 means the term is not known here.
    pub stable: (u64, u64),
    /// The disk is closed. A write not yet durable never becomes durable.
    pub closed: bool,
}

/// The staged writes and the next sequence number.
#[derive(Debug, Default)]
struct Queue {
    ops: VecDeque<(u64, StorageOp)>,
    /// The sequence number of the last staged write.
    staged_through: u64,
    /// The sequence number of the last staged hard state, or 0.
    hard_state_seq: u64,
    closed: bool,
}

/// One group's disk, shared by its staged storage, its writer thread and the
/// holders of its durability tickets.
pub struct GroupDisk {
    group_id: u64,
    storage: Mutex<RedbLogStorage>,
    queue: Mutex<Queue>,
    work: Condvar,
    progress: Mutex<DiskProgress>,
    progressed: Condvar,
    progress_tx: watch::Sender<DiskProgress>,
    /// Woken after each batch the writer makes durable, so the Raft loop
    /// counts the new durable entries at once instead of at its next tick.
    wake: Arc<Notify>,
}

impl GroupDisk {
    /// Open `group_id`'s redb log at `path`. `wake` is notified after each
    /// batch the writer makes durable. Blocks on disk: call it off the async
    /// threads.
    pub(super) fn open(group_id: u64, path: &Path, wake: Arc<Notify>) -> crate::Result<Self> {
        let storage = RedbLogStorage::open(path)?;
        Ok(Self {
            group_id,
            storage: Mutex::new(storage),
            queue: Mutex::new(Queue::default()),
            work: Condvar::new(),
            progress: Mutex::new(DiskProgress::default()),
            progressed: Condvar::new(),
            progress_tx: watch::Sender::new(DiskProgress::default()),
            wake,
        })
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// The storage itself, for the reads a restore runs before the writer
    /// takes any write.
    pub(super) fn storage(&self) -> std::sync::MutexGuard<'_, RedbLogStorage> {
        self.storage.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record the last entry the disk holds, read at restore.
    pub(super) fn set_restored_stable(&self, stable: (u64, u64)) {
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.stable = stable;
        self.progress_tx.send_replace(*progress);
    }

    /// Queue `op` behind every earlier write. Never waits on disk.
    pub(super) fn stage(&self, op: StorageOp) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.staged_through += 1;
        let seq = queue.staged_through;
        if matches!(op, StorageOp::HardState(_)) {
            queue.hard_state_seq = seq;
        }
        queue.ops.push_back((seq, op));
        self.work.notify_one();
    }

    /// The sequence number of the last staged write.
    pub fn staged_through(&self) -> u64 {
        self.queue
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .staged_through
    }

    /// The sequence numbers of the last staged write and of the last staged
    /// hard state, read together.
    pub(super) fn staged_marks(&self) -> (u64, u64) {
        let queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        (queue.staged_through, queue.hard_state_seq)
    }

    /// How far the disk has come.
    pub fn progress(&self) -> DiskProgress {
        *self.progress.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A receiver of every progress update, for async waiters.
    pub(super) fn subscribe(&self) -> watch::Receiver<DiskProgress> {
        self.progress_tx.subscribe()
    }

    /// Block until `seq` is durable or the disk closed. Returns whether it is
    /// durable. For callers off the async threads.
    pub(super) fn wait_blocking(&self, seq: u64) -> bool {
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if progress.durable_seq >= seq {
                return true;
            }
            if progress.closed {
                return false;
            }
            progress = self
                .progressed
                .wait(progress)
                .unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Stop taking writes. The writer makes every write staged so far durable,
    /// then ends.
    pub(super) fn close(&self) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.closed = true;
        self.work.notify_one();
    }

    /// The writer thread's body: drain and apply the queue until it is
    /// closed and empty.
    pub(super) fn run_writer(&self) {
        loop {
            let (batch, closed) = {
                let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                while queue.ops.is_empty() && !queue.closed {
                    queue = self.work.wait(queue).unwrap_or_else(|p| p.into_inner());
                }
                (queue.ops.drain(..).collect::<Vec<_>>(), queue.closed)
            };
            if batch.is_empty() {
                break;
            }
            self.apply_batch(batch, closed);
        }
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.closed = true;
        self.progress_tx.send_replace(*progress);
        self.progressed.notify_all();
    }

    /// Apply `batch` in order, then publish the progress it made.
    fn apply_batch(&self, batch: Vec<(u64, StorageOp)>, closed: bool) {
        let Some(through) = batch.last().map(|(seq, _)| *seq) else {
            return;
        };
        let mut stable = self.progress().stable;
        for op in merge_appends(batch) {
            stable = next_stable(stable, &op);
            let mut attempt = 0u32;
            while let Err(e) = self.apply(&op) {
                attempt += 1;
                if closed {
                    error!(
                        group_id = self.group_id,
                        error = %e,
                        "raft disk: a write failed while the disk closes; it is abandoned"
                    );
                    break;
                }
                warn!(
                    group_id = self.group_id,
                    attempt,
                    error = %e,
                    "raft disk: a write failed; it is retried"
                );
                std::thread::sleep(RETRY_DELAY);
            }
        }
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.durable_seq = progress.durable_seq.max(through);
        progress.stable = stable;
        self.progress_tx.send_replace(*progress);
        self.progressed.notify_all();
        self.wake.notify_one();
    }

    fn apply(&self, op: &StorageOp) -> nodedb_raft::error::Result<()> {
        let mut storage = self.storage();
        match op {
            StorageOp::Append(entries) => storage.append(entries),
            StorageOp::Truncate(index) => storage.truncate(*index),
            StorageOp::Compact { index, term } => storage.compact(*index, *term),
            StorageOp::HardState(state) => storage.save_hard_state(state),
            StorageOp::AppliedIndex(index) => storage.save_applied_index(*index),
        }
    }
}

/// `batch`'s writes in order, with each run of consecutive appends joined
/// into one append: one redb commit, one fsync.
fn merge_appends(batch: Vec<(u64, StorageOp)>) -> Vec<StorageOp> {
    let mut merged: Vec<StorageOp> = Vec::with_capacity(batch.len());
    for (_, op) in batch {
        match (merged.last_mut(), op) {
            (Some(StorageOp::Append(run)), StorageOp::Append(more)) => run.extend(more),
            (_, op) => merged.push(op),
        }
    }
    merged
}

/// The last durable entry once `op` is durable, from `stable` before it.
fn next_stable(stable: (u64, u64), op: &StorageOp) -> (u64, u64) {
    match op {
        StorageOp::Append(entries) => entries
            .last()
            .map_or(stable, |entry| (entry.index, entry.term)),
        // The entry before the cut is still on disk, at a term not known
        // here. It is recorded at term 0: no Raft entry above index 0 has
        // term 0, so it never matches the log, and index 0 has term 0.
        StorageOp::Truncate(index) if stable.0 >= *index => (index.saturating_sub(1), 0),
        StorageOp::Compact { index, term } if *index > stable.0 => (*index, *term),
        _ => stable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: Vec::new(),
        }
    }

    #[test]
    fn consecutive_appends_join_and_other_writes_keep_their_place() {
        let merged = merge_appends(vec![
            (1, StorageOp::Append(vec![entry(1, 1)])),
            (2, StorageOp::Append(vec![entry(2, 1)])),
            (3, StorageOp::HardState(HardState::new())),
            (4, StorageOp::Append(vec![entry(3, 1)])),
        ]);
        assert_eq!(merged.len(), 3);
        assert!(matches!(&merged[0], StorageOp::Append(run) if run.len() == 2));
        assert!(matches!(merged[1], StorageOp::HardState(_)));
        assert!(matches!(&merged[2], StorageOp::Append(run) if run.len() == 1));
    }

    #[test]
    fn the_stable_entry_follows_appends_truncations_and_compactions() {
        let stable = next_stable((0, 0), &StorageOp::Append(vec![entry(1, 1), entry(2, 1)]));
        assert_eq!(stable, (2, 1));
        // A cut at 2 leaves entry 1, at a term not known here.
        let stable = next_stable(stable, &StorageOp::Truncate(2));
        assert_eq!(stable, (1, 0));
        let stable = next_stable(stable, &StorageOp::Append(vec![entry(2, 2)]));
        assert_eq!(stable, (2, 2));
        // A cut above the stable entry leaves it.
        assert_eq!(next_stable(stable, &StorageOp::Truncate(5)), (2, 2));
        // A snapshot past the log moves the boundary; one below leaves it.
        assert_eq!(
            next_stable(stable, &StorageOp::Compact { index: 9, term: 3 }),
            (9, 3)
        );
        assert_eq!(
            next_stable(stable, &StorageOp::Compact { index: 1, term: 1 }),
            (2, 2)
        );
    }
}
