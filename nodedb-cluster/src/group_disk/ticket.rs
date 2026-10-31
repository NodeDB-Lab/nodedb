// SPDX-License-Identifier: BUSL-1.1

//! A claim on the durability of a group's staged writes.
//!
//! A caller takes a ticket under the `MultiRaft` lock, right after the Raft
//! call that staged the writes its reply depends on. It releases the lock,
//! awaits the ticket, and only then sends the reply or the vote request. No
//! other group, and no other caller of this group, waits on the disk
//! meanwhile.

use std::sync::Arc;

use super::writer::GroupDisk;

/// The durability of every write a group staged up to one sequence number.
#[derive(Clone)]
pub struct DurabilityTicket {
    disk: Arc<GroupDisk>,
    seq: u64,
}

impl std::fmt::Debug for DurabilityTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurabilityTicket")
            .field("group_id", &self.disk.group_id())
            .field("seq", &self.seq)
            .finish()
    }
}

impl DurabilityTicket {
    /// A ticket for every write `disk` staged so far, or `None` when all of
    /// them are durable.
    pub(super) fn for_staged(disk: &Arc<GroupDisk>) -> Option<Self> {
        let seq = disk.staged_through();
        (disk.progress().durable_seq < seq).then(|| Self {
            disk: Arc::clone(disk),
            seq,
        })
    }

    /// A ticket for a reply staged after `mark`, or `None` when the writes it
    /// depends on are durable. `mark` is [`GroupDisk::staged_through`] read
    /// before the Raft call that built the reply.
    ///
    /// A reply depends on the latest hard state, whoever staged it: its term
    /// and vote must survive a restart. When the call staged writes, the reply
    /// depends on them too. Writes are durable in staging order, so the
    /// ticket then covers every write staged so far. An `AppliedIndex` write
    /// staged by the apply loop is no dependency of a reply that staged
    /// nothing.
    pub(super) fn for_reply(disk: &Arc<GroupDisk>, mark: u64) -> Option<Self> {
        let (staged, hard_state) = disk.staged_marks();
        let seq = if staged > mark { staged } else { hard_state };
        (disk.progress().durable_seq < seq).then(|| Self {
            disk: Arc::clone(disk),
            seq,
        })
    }

    /// The group the ticket belongs to.
    pub fn group_id(&self) -> u64 {
        self.disk.group_id()
    }

    /// Wait until the writes are durable. Fails when the group's disk closed
    /// first: the group was unmounted, and the writes will never be durable.
    pub async fn durable(self) -> crate::Result<()> {
        let mut rx = self.disk.subscribe();
        loop {
            let progress = *rx.borrow_and_update();
            if progress.durable_seq >= self.seq {
                return Ok(());
            }
            if progress.closed || rx.changed().await.is_err() {
                return Err(crate::ClusterError::Storage {
                    detail: format!(
                        "raft group {}: the disk closed before write {} was durable",
                        self.disk.group_id(),
                        self.seq
                    ),
                });
            }
        }
    }

    /// Block until the writes are durable. Returns whether they are. For
    /// callers off the async threads.
    pub fn wait_blocking(&self) -> bool {
        self.disk.wait_blocking(self.seq)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_raft::message::LogEntry;
    use nodedb_raft::state::HardState;

    use super::super::writer::StorageOp;
    use super::*;

    /// A disk with no writer thread: nothing staged becomes durable.
    fn idle_disk() -> (tempfile::TempDir, Arc<GroupDisk>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-3.redb");
        let disk = Arc::new(GroupDisk::open(3, &path, Arc::default()).expect("open"));
        (dir, disk)
    }

    #[test]
    fn a_reply_that_staged_nothing_waits_on_no_applied_index() {
        let (_dir, disk) = idle_disk();
        disk.stage(StorageOp::AppliedIndex(4));
        let mark = disk.staged_through();

        assert!(DurabilityTicket::for_reply(&disk, mark).is_none());
        assert!(
            DurabilityTicket::for_staged(&disk).is_some(),
            "the applied index is not durable"
        );
    }

    #[test]
    fn a_reply_waits_on_the_latest_hard_state_whoever_staged_it() {
        let (_dir, disk) = idle_disk();
        disk.stage(StorageOp::HardState(HardState {
            current_term: 2,
            voted_for: 5,
        }));
        disk.stage(StorageOp::AppliedIndex(4));
        let mark = disk.staged_through();

        let ticket =
            DurabilityTicket::for_reply(&disk, mark).expect("the hard state is not durable");
        assert_eq!(ticket.seq, 1, "the later applied index is no dependency");
    }

    #[test]
    fn a_reply_that_staged_writes_waits_on_every_staged_write() {
        let (_dir, disk) = idle_disk();
        disk.stage(StorageOp::AppliedIndex(4));
        let mark = disk.staged_through();
        disk.stage(StorageOp::Append(vec![LogEntry {
            term: 1,
            index: 5,
            data: Vec::new(),
        }]));

        let ticket = DurabilityTicket::for_reply(&disk, mark).expect("the append is not durable");
        assert_eq!(ticket.seq, 2);
    }
}
