// SPDX-License-Identifier: BUSL-1.1

//! What a group's disk tells the rest of the node: the wake of the Raft loop,
//! and the record of each disk that fail-stopped.
//!
//! A disk fail-stops on a write error no retry clears. The group's replica
//! here then stops: it leads nothing and answers no Raft request, and the
//! other replicas carry the group. The node keeps serving every other group,
//! so the record is what shows the lost replica on the health surfaces. It
//! never clears: only a restart reopens the disk.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// The handles one group's disk reports through.
#[derive(Debug, Clone, Default)]
pub struct DiskSignals {
    /// Woken after each batch the writer makes durable and when the writer
    /// ends, so the Raft loop acts on the disk's progress at once.
    pub wake: Arc<Notify>,
    /// Where the disk records why it fail-stopped.
    pub halts: Arc<GroupDiskHalts>,
}

/// One fail-stopped disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDiskHalt {
    pub group_id: u64,
    /// The write error that stopped the disk.
    pub error: String,
}

/// The fail-stopped disks of this node, the first error of each. Never clears.
#[derive(Debug, Default)]
pub struct GroupDiskHalts {
    halted: AtomicBool,
    by_group: Mutex<BTreeMap<u64, String>>,
}

impl GroupDiskHalts {
    /// Record that `group_id`'s disk stopped on `error`. A later report for
    /// the group keeps the first.
    pub fn record(&self, group_id: u64, error: String) {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(group_id)
            .or_insert(error);
        self.halted.store(true, Ordering::Release);
    }

    /// Whether any disk of this node fail-stopped. One atomic load.
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    /// The fail-stopped disk with the lowest group id.
    pub fn report(&self) -> Option<GroupDiskHalt> {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .next()
            .map(|(&group_id, error)| GroupDiskHalt {
                group_id,
                error: error.clone(),
            })
    }

    /// Every fail-stopped group, rising.
    pub fn halted_groups(&self) -> Vec<u64> {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// Number of groups whose disk fail-stopped.
    pub fn stopped_groups(&self) -> u64 {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len() as u64
    }

    /// Append the `nodedb_raft_group_disk_fail_stopped` gauge.
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(
            out,
            "# HELP nodedb_raft_group_disk_fail_stopped Raft groups whose replica on this \
             node stopped because its disk hit a write error no retry clears\n\
             # TYPE nodedb_raft_group_disk_fail_stopped gauge\n\
             nodedb_raft_group_disk_fail_stopped {}",
            self.stopped_groups()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gauge_counts_stopped_groups() {
        let halts = GroupDiskHalts::default();
        let mut out = String::new();
        halts.write_prometheus(&mut out);
        assert!(
            out.contains("nodedb_raft_group_disk_fail_stopped 0"),
            "{out}"
        );

        halts.record(7, "first".to_owned());
        halts.record(7, "second".to_owned());
        halts.record(3, "other group".to_owned());
        let mut out = String::new();
        halts.write_prometheus(&mut out);
        assert!(
            out.contains("nodedb_raft_group_disk_fail_stopped 2"),
            "{out}"
        );
    }

    #[test]
    fn the_first_error_of_each_group_stays() {
        let halts = GroupDiskHalts::default();
        assert!(!halts.is_halted());
        assert_eq!(halts.report(), None);

        halts.record(7, "first".to_owned());
        halts.record(7, "second".to_owned());
        halts.record(3, "other group".to_owned());

        assert!(halts.is_halted());
        assert_eq!(halts.halted_groups(), vec![3, 7]);
        assert_eq!(
            halts.report(),
            Some(GroupDiskHalt {
                group_id: 3,
                error: "other group".to_owned(),
            })
        );
    }
}
