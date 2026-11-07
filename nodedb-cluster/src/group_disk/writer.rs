// SPDX-License-Identifier: BUSL-1.1

//! One Raft group's disk: its redb log storage and the writer thread that
//! makes staged writes durable, in the order they were staged.
//!
//! A group's Raft node stages every storage write here and returns at once,
//! so no disk write runs under the `MultiRaft` lock or on an async thread.
//! The writer thread drains the queue and applies the whole drained run to
//! redb in one transaction, so one commit makes it durable. It then
//! publishes:
//! - the highest staged sequence number that is durable, for callers that
//!   hold a [`super::DurabilityTicket`];
//! - the last log entry the disk holds durably, which the node's leader
//!   counts toward its own commit acknowledgement.
//!
//! A write is never skipped: every later write depends on it. One that fails
//! ends one of three ways:
//! - An I/O error is retried. The wait doubles from [`RETRY_DELAY_MIN`] to
//!   [`RETRY_DELAY_MAX`], and each retry opens the log again, because redb
//!   refuses every call on a handle whose write failed. The retries last
//!   until the disk recovers or closes, so a full disk resumes once freed.
//! - An error no retry clears fail-stops the disk (see [`super::signals`]).
//! - A failing write on a closed disk is abandoned after one attempt.
//!
//! The last two end the writer with the write not durable. Its progress
//! reports it never, so every waiter on it gets an error.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use nodedb_raft::error::RaftError;
use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;
use tokio::sync::watch;
use tracing::{debug, error, warn};

use crate::raft_storage::RedbLogStorage;
use crate::raft_storage_ops::{LogOp, LogWriteError};

use super::signals::DiskSignals;

/// The first wait before a failed write is tried again.
const RETRY_DELAY_MIN: Duration = Duration::from_millis(50);

/// The longest wait before a failed write is tried again.
const RETRY_DELAY_MAX: Duration = Duration::from_secs(5);

/// How one drained run of writes ended.
#[derive(Debug, PartialEq, Eq)]
enum CommitEnd {
    Durable,
    /// The disk closed and the run's write failed.
    Abandoned,
    /// The run's write failed with an error no retry clears.
    FailStopped,
}

/// One storage write, staged by the group's Raft node.
#[derive(Debug, Clone)]
pub(super) enum StorageOp {
    Append(Vec<LogEntry>),
    Truncate(u64),
    Compact { index: u64, term: u64 },
    HardState(HardState),
    AppliedIndex(u64),
}

impl StorageOp {
    fn kind(&self) -> &'static str {
        match self {
            Self::Append(_) => "append",
            Self::Truncate(_) => "truncate",
            Self::Compact { .. } => "compact",
            Self::HardState(_) => "hard_state",
            Self::AppliedIndex(_) => "applied_index",
        }
    }

    fn as_log_op(&self) -> LogOp<'_> {
        match self {
            Self::Append(entries) => LogOp::Append(entries),
            Self::Truncate(index) => LogOp::Truncate(*index),
            Self::Compact { index, term } => LogOp::Compact {
                index: *index,
                term: *term,
            },
            Self::HardState(state) => LogOp::HardState(state),
            Self::AppliedIndex(index) => LogOp::AppliedIndex(*index),
        }
    }
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
    /// The disk fail-stopped on a write error. It is closed too.
    pub failed: bool,
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
    /// The writer ended on a write it could not make durable. A write staged
    /// now takes a sequence number that never becomes durable.
    stopped: bool,
}

/// One group's disk, shared by its staged storage, its writer thread and the
/// holders of its durability tickets.
pub struct GroupDisk {
    group_id: u64,
    path: PathBuf,
    /// The open log. `None` between a failed write and the reopen that the
    /// retry runs.
    storage: Mutex<Option<RedbLogStorage>>,
    queue: Mutex<Queue>,
    work: Condvar,
    progress: Mutex<DiskProgress>,
    progressed: Condvar,
    progress_tx: watch::Sender<DiskProgress>,
    signals: DiskSignals,
    /// Errors the next commits fail with, before they reach redb.
    #[cfg(test)]
    injected: Mutex<VecDeque<LogWriteError>>,
}

impl GroupDisk {
    /// Open `group_id`'s redb log at `path`. Blocks on disk: call it off the
    /// async threads.
    pub(super) fn open(group_id: u64, path: &Path, signals: DiskSignals) -> crate::Result<Self> {
        let storage = RedbLogStorage::open(path)?;
        Ok(Self {
            group_id,
            path: path.to_path_buf(),
            storage: Mutex::new(Some(storage)),
            queue: Mutex::new(Queue::default()),
            work: Condvar::new(),
            progress: Mutex::new(DiskProgress::default()),
            progressed: Condvar::new(),
            progress_tx: watch::Sender::new(DiskProgress::default()),
            signals,
            #[cfg(test)]
            injected: Mutex::new(VecDeque::new()),
        })
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// Run `read` on the storage, for the reads a restore runs before the
    /// writer takes any write. Fails while a write error holds the log closed.
    pub(super) fn read_storage<T>(
        &self,
        read: impl FnOnce(&RedbLogStorage) -> nodedb_raft::error::Result<T>,
    ) -> nodedb_raft::error::Result<T> {
        let slot = self.storage.lock().unwrap_or_else(|p| p.into_inner());
        match slot.as_ref() {
            Some(storage) => read(storage),
            None => Err(RaftError::Storage {
                detail: format!(
                    "raft group {}: read the log at {}: a write error holds it closed. \
                     Retry after the disk recovers.",
                    self.group_id,
                    self.path.display()
                ),
            }),
        }
    }

    /// Whether the disk fail-stopped.
    pub fn is_failed(&self) -> bool {
        self.progress().failed
    }

    /// Refuse a write this group cannot make durable. A log write refused
    /// here leaves the in-memory log as the disk holds it.
    pub(super) fn check_running(&self) -> nodedb_raft::error::Result<()> {
        if self.queue.lock().unwrap_or_else(|p| p.into_inner()).stopped {
            return Err(RaftError::Storage {
                detail: format!(
                    "raft group {}: stage a write: the disk stopped after a write error and \
                     takes no write. Fix the disk and restart the node.",
                    self.group_id
                ),
            });
        }
        Ok(())
    }

    /// Record the last entry the disk holds, read at restore.
    pub(super) fn set_restored_stable(&self, stable: (u64, u64)) {
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.stable = stable;
        self.progress_tx.send_replace(*progress);
    }

    /// Queue `op` behind every earlier write. Never waits on disk. On a
    /// stopped disk `op` only takes a sequence number, so a ticket on it
    /// fails.
    pub(super) fn stage(&self, op: StorageOp) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.staged_through += 1;
        let seq = queue.staged_through;
        if matches!(op, StorageOp::HardState(_)) {
            queue.hard_state_seq = seq;
        }
        if queue.stopped {
            return;
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
    /// closed and empty, or a write ends it.
    pub(super) fn run_writer(&self) {
        let mut failed = false;
        loop {
            let batch = {
                let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                while queue.ops.is_empty() && !queue.closed {
                    queue = self.work.wait(queue).unwrap_or_else(|p| p.into_inner());
                }
                queue.ops.drain(..).collect::<Vec<_>>()
            };
            if batch.is_empty() {
                break;
            }
            match self.apply_batch(batch) {
                CommitEnd::Durable => {}
                CommitEnd::Abandoned => {
                    self.stop_queue();
                    break;
                }
                CommitEnd::FailStopped => {
                    self.stop_queue();
                    failed = true;
                    break;
                }
            }
        }
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.closed = true;
        progress.failed = failed;
        self.progress_tx.send_replace(*progress);
        self.progressed.notify_all();
        drop(progress);
        self.signals.wake.notify_one();
    }

    /// Take no more writes: every later write depends on one that never
    /// becomes durable.
    fn stop_queue(&self) {
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        queue.stopped = true;
        queue.ops.clear();
    }

    /// Commit `batch` in order. Publishes the progress only for a commit that
    /// succeeded: a write that did not reach the disk is never durable.
    fn apply_batch(&self, batch: Vec<(u64, StorageOp)>) -> CommitEnd {
        let Some(through) = batch.last().map(|(seq, _)| *seq) else {
            return CommitEnd::Durable;
        };
        let mut stable = self.progress().stable;
        let ops = merge_appends(batch);
        for op in &ops {
            stable = next_stable(stable, op);
        }
        let log_ops: Vec<LogOp<'_>> = ops.iter().map(StorageOp::as_log_op).collect();
        let kinds: Vec<&'static str> = ops.iter().map(StorageOp::kind).collect();
        debug!(group_id = self.group_id, ops = ?kinds, "raft disk: commit");
        let end = self.commit_retrying(&log_ops, &kinds);
        if end == CommitEnd::Durable {
            let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
            progress.durable_seq = progress.durable_seq.max(through);
            progress.stable = stable;
            self.progress_tx.send_replace(*progress);
            self.progressed.notify_all();
            drop(progress);
            self.signals.wake.notify_one();
        }
        end
    }

    /// Commit `log_ops`, retrying an I/O error until it clears or the disk
    /// closes.
    fn commit_retrying(&self, log_ops: &[LogOp<'_>], kinds: &[&'static str]) -> CommitEnd {
        let group_id = self.group_id;
        let mut attempt = 0u32;
        let mut delay = RETRY_DELAY_MIN;
        loop {
            let failure = match self.try_commit(log_ops) {
                Ok(()) => return CommitEnd::Durable,
                Err(failure) => failure,
            };
            attempt = attempt.saturating_add(1);
            let detail = failure.to_string();
            if matches!(failure, LogWriteError::Deterministic { .. }) {
                error!(
                    group_id,
                    ops = ?kinds,
                    error = %failure,
                    "raft disk: a write failed with an error no retry clears; \
                     this replica of the group stops. Fix the cause and restart the node."
                );
                crate::diag::raft_disk_fail_stopped(group_id, kinds, &detail);
                self.signals.halts.record(group_id, detail);
                return CommitEnd::FailStopped;
            }
            if self.is_closing() {
                error!(
                    group_id,
                    ops = ?kinds,
                    error = %failure,
                    "raft disk: a write failed while the disk closes; it is abandoned, \
                     and its waiters get an error"
                );
                crate::diag::raft_disk_write_abandoned(group_id, kinds, &detail);
                return CommitEnd::Abandoned;
            }
            warn!(
                group_id,
                attempt,
                ops = ?kinds,
                error = %failure,
                "raft disk: a write failed; it is retried"
            );
            crate::diag::raft_disk_write_retried(group_id, attempt, kinds, &detail);
            self.wait_before_retry(delay);
            delay = delay.saturating_mul(2).min(RETRY_DELAY_MAX);
        }
    }

    /// One commit attempt. A handle whose attempt failed with an I/O error
    /// closes here, and the next attempt opens the log again.
    fn try_commit(&self, log_ops: &[LogOp<'_>]) -> Result<(), LogWriteError> {
        let mut slot = self.storage.lock().unwrap_or_else(|p| p.into_inner());
        let mut storage = match slot.take() {
            Some(storage) => storage,
            None => RedbLogStorage::reopen(&self.path)?,
        };
        let result = self
            .next_injected()
            .map_or_else(|| storage.commit_ops(log_ops), Err);
        if matches!(result, Err(LogWriteError::Transient { .. })) {
            drop(storage);
        } else {
            *slot = Some(storage);
        }
        result
    }

    #[cfg(test)]
    fn next_injected(&self) -> Option<LogWriteError> {
        self.injected
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop_front()
    }

    /// Make the next commit fail with `failure`, before it reaches redb.
    #[cfg(test)]
    pub(crate) fn inject_for_test(&self, failure: LogWriteError) {
        self.injected
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push_back(failure);
    }

    #[cfg(not(test))]
    #[inline]
    fn next_injected(&self) -> Option<LogWriteError> {
        None
    }

    fn is_closing(&self) -> bool {
        self.queue.lock().unwrap_or_else(|p| p.into_inner()).closed
    }

    /// Wait `delay`, or until the disk closes.
    fn wait_before_retry(&self, delay: Duration) {
        let deadline = Instant::now() + delay;
        let mut queue = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        while !queue.closed {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            queue = self
                .work
                .wait_timeout(queue, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
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

    fn transient() -> LogWriteError {
        LogWriteError::Transient {
            context: "commit",
            detail: "disk gone".to_owned(),
        }
    }

    fn deterministic() -> LogWriteError {
        LogWriteError::Deterministic {
            context: "serialize entry",
            detail: "bad bytes".to_owned(),
        }
    }

    fn disk_with(signals: DiskSignals) -> (tempfile::TempDir, std::sync::Arc<GroupDisk>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-5.redb");
        let disk = GroupDisk::open(5, &path, signals).expect("open");
        (dir, std::sync::Arc::new(disk))
    }

    fn inject(disk: &GroupDisk, failures: Vec<LogWriteError>) {
        disk.injected
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(failures);
    }

    fn start_writer(disk: &std::sync::Arc<GroupDisk>) -> std::thread::JoinHandle<()> {
        let disk = std::sync::Arc::clone(disk);
        std::thread::spawn(move || disk.run_writer())
    }

    /// A deterministic error ends the writer at once with nothing durable,
    /// records the halt, and refuses every later write.
    #[test]
    fn a_deterministic_error_fail_stops_the_disk() {
        let signals = DiskSignals::default();
        let (_dir, disk) = disk_with(signals.clone());
        inject(&disk, vec![deterministic()]);
        disk.stage(StorageOp::Append(vec![entry(1, 1)]));
        let writer = start_writer(&disk);
        assert!(!disk.wait_blocking(1), "the write is never durable");
        writer.join().expect("the writer ends");

        let progress = disk.progress();
        assert_eq!(progress.durable_seq, 0);
        assert_eq!(progress.stable, (0, 0));
        assert!(progress.closed && progress.failed);
        assert!(disk.is_failed());
        let halt = signals.halts.report().expect("the halt is recorded");
        assert_eq!(halt.group_id, 5);
        assert!(halt.error.contains("bad bytes"), "{}", halt.error);
        assert!(disk.check_running().is_err());

        // A later write takes a number no ticket ever sees durable, and
        // queues nothing.
        disk.stage(StorageOp::AppliedIndex(1));
        assert_eq!(disk.staged_through(), 2);
        assert!(disk.queue.lock().expect("queue").ops.is_empty());
        assert!(!disk.wait_blocking(2));
    }

    /// An I/O error is retried behind a reopen of the log, and the same run
    /// then commits and reads back.
    #[test]
    fn an_io_error_retries_through_a_reopen_and_commits() {
        let signals = DiskSignals::default();
        let (_dir, disk) = disk_with(signals.clone());
        inject(&disk, vec![transient(), transient()]);
        disk.stage(StorageOp::Append(vec![entry(1, 1), entry(2, 1)]));
        let writer = start_writer(&disk);
        assert!(disk.wait_blocking(1), "the retry makes the write durable");
        assert_eq!(disk.progress().stable, (2, 1));
        assert!(!disk.is_failed());
        assert!(!signals.halts.is_halted());
        disk.close();
        writer.join().expect("the writer ends");

        let entries = disk
            .read_storage(|storage| {
                nodedb_raft::storage::LogStorage::load_entries_after(storage, 0)
            })
            .expect("load");
        assert_eq!(entries.len(), 2);
    }

    /// A batch that fails on a closed disk publishes nothing, so a ticket on
    /// it fails and no floor reply acks it.
    #[test]
    fn a_write_abandoned_on_close_is_never_published_durable() {
        let signals = DiskSignals::default();
        let (_dir, disk) = disk_with(signals.clone());
        inject(&disk, vec![transient()]);
        disk.stage(StorageOp::HardState(HardState {
            current_term: 2,
            voted_for: 5,
            ordinal_floor: 900,
        }));
        disk.stage(StorageOp::Append(vec![entry(1, 2)]));
        let ticket = super::super::DurabilityTicket::for_staged(&disk).expect("two writes staged");
        disk.close();
        disk.run_writer();

        let progress = disk.progress();
        assert_eq!(progress.durable_seq, 0, "an abandoned write is not durable");
        assert_eq!(progress.stable, (0, 0));
        assert!(progress.closed);
        assert!(!progress.failed, "a close is no fail-stop");
        assert!(!signals.halts.is_halted());
        assert!(!ticket.wait_blocking());
        let awaited = futures::executor::block_on(ticket.durable());
        assert!(matches!(
            awaited,
            Err(crate::ClusterError::Storage { detail }) if detail.contains("closed")
        ));
    }

    /// A close reaches a writer that is retrying, so an unmount never hangs
    /// on a write the disk refuses.
    #[test]
    fn a_close_ends_a_writer_that_is_retrying() {
        let (_dir, disk) = disk_with(DiskSignals::default());
        inject(&disk, (0..1_000).map(|_| transient()).collect());
        disk.stage(StorageOp::AppliedIndex(1));
        let writer = start_writer(&disk);
        std::thread::sleep(Duration::from_millis(100));
        disk.close();
        writer.join().expect("the writer ends");
        let progress = disk.progress();
        assert_eq!(progress.durable_seq, 0);
        assert!(progress.closed);
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
