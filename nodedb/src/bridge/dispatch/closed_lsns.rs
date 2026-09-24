// SPDX-License-Identifier: BUSL-1.1

//! The LSNs above the outcome floor whose last owner closed, kept as
//! disjoint ranges.
//!
//! A closed LSN's outcome is final, so a resend of it is refused. The floor
//! prunes every range at or below it. A held window keeps the floor down for
//! the rest of the process, so the set must stay bounded without the floor.
//!
//! A range grows across a gap of LSNs no live window owns. Such an LSN is
//! either closed already, or was never owned: its record was minted outside
//! a write window and must never reach a core, so a resend of it is refused
//! too. The ranges are therefore split only at LSNs a live window owns, and
//! their count is at most one more than the number of live owned LSNs.

use std::collections::BTreeMap;

/// Closed LSNs as `start -> end` ranges, both ends inclusive.
#[derive(Debug, Default)]
pub(super) struct ClosedLsns {
    ranges: BTreeMap<u64, u64>,
}

impl ClosedLsns {
    /// Record that `lsn`'s last owner closed. `owners` holds every LSN a live
    /// window owns.
    pub(super) fn insert(&mut self, lsn: u64, owners: &BTreeMap<u64, usize>) {
        if self.contains(lsn) {
            return;
        }
        let mut start = lsn;
        let mut end = lsn;
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..lsn).next_back()
            && owners
                .range(prev_end.saturating_add(1)..lsn)
                .next()
                .is_none()
        {
            self.ranges.remove(&prev_start);
            start = prev_start;
        }
        if let Some((&next_start, &next_end)) = self.ranges.range(lsn.saturating_add(1)..).next()
            && owners
                .range(lsn.saturating_add(1)..next_start)
                .next()
                .is_none()
        {
            self.ranges.remove(&next_start);
            end = next_end;
        }
        self.ranges.insert(start, end);
    }

    /// Whether `lsn` lies in a closed range.
    pub(super) fn contains(&self, lsn: u64) -> bool {
        self.ranges
            .range(..=lsn)
            .next_back()
            .is_some_and(|(_, end)| lsn <= *end)
    }

    /// Drop every LSN at or below `floor`: the floor refuses those itself.
    pub(super) fn prune_through(&mut self, floor: u64) {
        let above = floor.saturating_add(1);
        let straddling = self
            .ranges
            .range(..above)
            .next_back()
            .map(|(_, end)| *end)
            .filter(|end| *end >= above);
        self.ranges = self.ranges.split_off(&above);
        if let Some(end) = straddling {
            self.ranges.insert(above, end);
        }
    }
}

#[cfg(test)]
impl ClosedLsns {
    /// Number of stored ranges.
    pub(super) fn range_count(&self) -> usize {
        self.ranges.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_and_unowned_gaps_merge_into_one_range() {
        let owners = BTreeMap::new();
        let mut closed = ClosedLsns::default();
        for lsn in [5, 7, 6, 10] {
            closed.insert(lsn, &owners);
        }
        assert_eq!(closed.range_count(), 1);
        assert!(
            closed.contains(8),
            "an unowned gap LSN is refused like a closed one"
        );
        assert!(!closed.contains(11));
    }

    #[test]
    fn a_live_owned_lsn_splits_the_ranges() {
        let owners = BTreeMap::from([(8, 1)]);
        let mut closed = ClosedLsns::default();
        closed.insert(5, &owners);
        closed.insert(10, &owners);
        assert_eq!(closed.range_count(), 2);
        assert!(!closed.contains(8));
    }

    #[test]
    fn pruning_keeps_only_lsns_above_the_floor() {
        let owners = BTreeMap::new();
        let mut closed = ClosedLsns::default();
        closed.insert(5, &owners);
        closed.insert(9, &owners);
        closed.prune_through(7);
        assert!(!closed.contains(7));
        assert!(closed.contains(8));
        assert!(closed.contains(9));
        closed.prune_through(9);
        assert_eq!(closed.range_count(), 0);
    }
}
