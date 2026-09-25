// SPDX-License-Identifier: BUSL-1.1

//! A set of LSNs kept as disjoint inclusive ranges.
//!
//! Two ranges merge only when they touch: `[5, 7]` and `[8, 9]` become
//! `[5, 9]`. A gap is never bridged. The core cannot tell a gap LSN that
//! belongs to another core from one still on its way to this core, and
//! marking the second as applied would make restart replay skip a record no
//! checkpoint holds.

use std::collections::BTreeMap;

use super::stamp::LsnRange;

/// LSNs as `start -> end` ranges, both ends inclusive.
#[derive(Debug, Default, Clone)]
pub(in crate::data::executor) struct LsnRanges {
    ranges: BTreeMap<u64, u64>,
}

impl LsnRanges {
    /// Add `lsn`, merging it with the ranges it touches.
    pub(in crate::data::executor) fn insert(&mut self, lsn: u64) {
        if self.contains(lsn) {
            return;
        }
        let mut start = lsn;
        let mut end = lsn;
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..lsn).next_back()
            && prev_end.checked_add(1) == Some(lsn)
        {
            self.ranges.remove(&prev_start);
            start = prev_start;
        }
        if let Some(next) = lsn.checked_add(1)
            && let Some(next_end) = self.ranges.remove(&next)
        {
            end = next_end;
        }
        self.ranges.insert(start, end);
    }

    /// Whether `lsn` lies in a range.
    pub(in crate::data::executor) fn contains(&self, lsn: u64) -> bool {
        self.ranges
            .range(..=lsn)
            .next_back()
            .is_some_and(|(_, end)| lsn <= *end)
    }

    /// Drop every LSN at or below `floor`.
    pub(in crate::data::executor) fn prune_through(&mut self, floor: u64) {
        let Some(above) = floor.checked_add(1) else {
            self.ranges.clear();
            return;
        };
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

    /// The highest LSN in the set.
    pub(in crate::data::executor) fn max(&self) -> Option<u64> {
        self.ranges.last_key_value().map(|(_, end)| *end)
    }

    /// Number of stored ranges.
    pub(in crate::data::executor) fn range_count(&self) -> usize {
        self.ranges.len()
    }

    /// Remove every LSN.
    pub(in crate::data::executor) fn clear(&mut self) {
        self.ranges.clear();
    }

    /// The ranges in ascending order.
    pub(in crate::data::executor) fn to_ranges(&self) -> Vec<LsnRange> {
        self.ranges
            .iter()
            .map(|(&start, &end)| LsnRange { start, end })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touching_lsns_merge_and_gaps_stay_open() {
        let mut set = LsnRanges::default();
        for lsn in [5, 7, 6, 10] {
            set.insert(lsn);
        }
        assert_eq!(
            set.to_ranges(),
            vec![
                LsnRange { start: 5, end: 7 },
                LsnRange { start: 10, end: 10 }
            ]
        );
        assert!(!set.contains(8), "a gap is never bridged");
        assert!(!set.contains(9));
        set.insert(9);
        set.insert(8);
        assert_eq!(set.range_count(), 1);
        assert_eq!(set.max(), Some(10));
    }

    #[test]
    fn pruning_keeps_only_lsns_above_the_floor() {
        let mut set = LsnRanges::default();
        for lsn in [3, 4, 5, 9] {
            set.insert(lsn);
        }
        set.prune_through(4);
        assert!(!set.contains(4));
        assert!(set.contains(5));
        assert!(set.contains(9));
        set.prune_through(9);
        assert_eq!(set.range_count(), 0);
        set.insert(u64::MAX);
        set.prune_through(u64::MAX);
        assert_eq!(set.range_count(), 0);
    }
}
