// SPDX-License-Identifier: BUSL-1.1

//! Per-group state the apply loop keeps across batches: the highest index it
//! applied, to report a second apply of a committed entry, and the backup cut
//! floor, which raises the commit HLC of every entry after a cut barrier.

use std::collections::HashMap;

/// Per-group apply state.
#[derive(Debug, Default)]
pub(super) struct GroupWatch {
    highest_applied: HashMap<u64, u64>,
    /// Lowest commit HLC an entry after the group's latest cut barrier
    /// records: one above that barrier's watermark.
    cut_floor: HashMap<u64, u64>,
}

impl GroupWatch {
    /// Note that the apply loop applies `(group_id, log_index)`. Reports an
    /// index at or below one it already applied as a second apply.
    pub(super) fn note_apply(&mut self, group_id: u64, log_index: u64) {
        let highest = self.highest_applied.entry(group_id).or_insert(0);
        if log_index <= *highest {
            crate::diag::raft_entry_reapplied(group_id, log_index, *highest);
            return;
        }
        *highest = log_index;
    }

    /// Raise `group_id`'s cut floor above the watermark `cut_hlc` of a
    /// backup's cut barrier.
    pub(super) fn raise_cut(&mut self, group_id: u64, cut_hlc: u64) {
        let floor = self.cut_floor.entry(group_id).or_insert(0);
        *floor = (*floor).max(cut_hlc.saturating_add(1));
    }

    /// The commit HLC an entry of `group_id` stamped `write_hlc` records.
    ///
    /// An entry the log places after a cut barrier records at least the cut
    /// floor, however early its proposer stamped it: the backup that placed
    /// the barrier did not contain it, so a restore of that backup refuses
    /// it. `0` means the entry carries no stamp; its apply stamps its own
    /// append, which already follows every barrier before it.
    pub(super) fn commit_hlc(&self, group_id: u64, write_hlc: u64) -> u64 {
        if write_hlc == 0 {
            return 0;
        }
        self.cut_floor
            .get(&group_id)
            .map_or(write_hlc, |floor| write_hlc.max(*floor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_after_a_cut_records_above_the_cut() {
        let mut watch = GroupWatch::default();
        assert_eq!(watch.commit_hlc(1, 50), 50);
        watch.raise_cut(1, 100);
        assert_eq!(watch.commit_hlc(1, 50), 101);
        assert_eq!(watch.commit_hlc(1, 200), 200);
        assert_eq!(watch.commit_hlc(2, 50), 50, "a cut binds only its group");
        assert_eq!(
            watch.commit_hlc(1, 0),
            0,
            "an unstamped entry stamps its own append"
        );
    }

    #[test]
    fn a_second_apply_of_an_index_is_counted() {
        let before = crate::diag::raft_entries_reapplied();
        let mut watch = GroupWatch::default();
        watch.note_apply(3, 5);
        watch.note_apply(3, 6);
        watch.note_apply(3, 6);
        assert!(crate::diag::raft_entries_reapplied() > before);
    }
}
