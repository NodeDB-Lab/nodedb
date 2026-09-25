// SPDX-License-Identifier: BUSL-1.1

//! What a published engine artifact holds, stated so restart replay can
//! decide every record exactly.
//!
//! A checkpoint used to carry one LSN, the highest one applied. LSNs are
//! node-global and records reach a core out of mint order, so a record with a
//! lower LSN can still be on its way when a higher one applies. A stamp at the
//! higher LSN then claims the lower record, restart replay skips it, and the
//! write is gone.
//!
//! A [`ReplayStamp`] states two facts instead:
//!
//! - `prefix`: the core's outcome floor when the artifact was written. Every
//!   record at or below it has a final outcome. A record this core applied is
//!   in the artifact. A refused record carries a durable abort marker.
//! - `applied_above`: every record above `prefix` that this core applied
//!   before the artifact was written.
//!
//! Replay skips a record exactly when [`ReplayStamp::skips`] says so. Every
//! engine that publishes an artifact and gates replay on it uses this type
//! and that function, whether its artifact covers the whole engine or one
//! collection.

use serde::{Deserialize, Serialize};

/// An inclusive LSN range.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct LsnRange {
    pub start: u64,
    pub end: u64,
}

/// The records a published artifact holds.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplayStamp {
    /// The outcome floor when the artifact was written.
    pub prefix: u64,
    /// The LSNs above `prefix` applied before the artifact was written, as
    /// ascending, disjoint, non-touching ranges.
    pub applied_above: Vec<LsnRange>,
}

/// Why a decoded [`ReplayStamp`] cannot describe an artifact.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidReplayStamp {
    /// A range ends before it starts.
    #[error("applied range [{start}, {end}] ends before it starts")]
    Inverted { start: u64, end: u64 },
    /// A range lies at or below the prefix, which already holds it.
    #[error("applied range [{start}, {end}] is not above the prefix {prefix}")]
    NotAbovePrefix { start: u64, end: u64, prefix: u64 },
    /// Two ranges overlap, touch, or are out of order.
    #[error("applied range starting at {start} does not follow the range ending at {previous_end}")]
    OutOfOrder { previous_end: u64, start: u64 },
}

impl ReplayStamp {
    /// A stamp holding every record at or below `prefix` and nothing above.
    pub fn through(prefix: u64) -> Self {
        Self {
            prefix,
            applied_above: Vec::new(),
        }
    }

    /// A stamp naming the record at `lsn` and nothing else above LSN 0.
    pub fn naming(lsn: u64) -> Self {
        if lsn == 0 {
            return Self::through(0);
        }
        Self {
            prefix: 0,
            applied_above: vec![LsnRange {
                start: lsn,
                end: lsn,
            }],
        }
    }

    /// Whether restart replay skips the record at `record_lsn`: the artifact
    /// already holds it, or it has a final outcome that is not an apply.
    ///
    /// Every other record replays.
    pub fn skips(&self, record_lsn: u64) -> bool {
        if record_lsn <= self.prefix {
            return true;
        }
        let after = self
            .applied_above
            .partition_point(|range| range.start <= record_lsn);
        after > 0
            && self
                .applied_above
                .get(after - 1)
                .is_some_and(|range| record_lsn <= range.end)
    }

    /// The highest LSN the stamp names: the end of its last applied range, or
    /// its prefix when it names nothing above the prefix.
    pub fn highest(&self) -> u64 {
        self.applied_above
            .last()
            .map_or(self.prefix, |range| range.end.max(self.prefix))
    }

    /// A stamp naming every record `self` or `other` names.
    ///
    /// Two artifacts of one collection written at different times each name
    /// the records they hold. The union is what the collection holds across
    /// both.
    pub fn union(&self, other: &ReplayStamp) -> ReplayStamp {
        let mut prefix = self.prefix.max(other.prefix);
        let mut ranges: Vec<LsnRange> = self
            .applied_above
            .iter()
            .chain(&other.applied_above)
            .filter(|range| range.end > prefix)
            .map(|range| LsnRange {
                start: range.start.max(prefix.saturating_add(1)),
                end: range.end,
            })
            .collect();
        ranges.sort_unstable_by_key(|range| range.start);
        let mut merged: Vec<LsnRange> = Vec::with_capacity(ranges.len());
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end.saturating_add(1) => {
                    last.end = last.end.max(range.end);
                }
                _ => merged.push(range),
            }
        }
        // A range that starts right above the prefix extends it.
        if merged
            .first()
            .is_some_and(|first| first.start == prefix.saturating_add(1))
        {
            prefix = merged.remove(0).end;
        }
        ReplayStamp {
            prefix,
            applied_above: merged,
        }
    }

    /// Check the shape [`Self::skips`] relies on.
    pub fn validate(&self) -> Result<(), InvalidReplayStamp> {
        let mut previous_end: Option<u64> = None;
        for range in &self.applied_above {
            if range.end < range.start {
                return Err(InvalidReplayStamp::Inverted {
                    start: range.start,
                    end: range.end,
                });
            }
            if range.start <= self.prefix {
                return Err(InvalidReplayStamp::NotAbovePrefix {
                    start: range.start,
                    end: range.end,
                    prefix: self.prefix,
                });
            }
            if let Some(previous_end) = previous_end
                && range.start <= previous_end.saturating_add(1)
            {
                return Err(InvalidReplayStamp::OutOfOrder {
                    previous_end,
                    start: range.start,
                });
            }
            previous_end = Some(range.end);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(prefix: u64, ranges: &[(u64, u64)]) -> ReplayStamp {
        ReplayStamp {
            prefix,
            applied_above: ranges
                .iter()
                .map(|&(start, end)| LsnRange { start, end })
                .collect(),
        }
    }

    #[test]
    fn the_prefix_and_the_applied_ranges_skip_and_nothing_else_does() {
        let stamp = stamp(10, &[(12, 13), (20, 20)]);
        for lsn in [1, 10, 12, 13, 20] {
            assert!(stamp.skips(lsn), "{lsn} is held by the artifact");
        }
        for lsn in [11, 14, 19, 21, u64::MAX] {
            assert!(!stamp.skips(lsn), "{lsn} must replay");
        }
    }

    #[test]
    fn a_lower_record_in_flight_at_the_stamp_replays() {
        // Record 11 was still on its way when 12 applied and the checkpoint
        // was written. A max-applied stamp at 12 would skip it.
        let stamp = stamp(10, &[(12, 12)]);
        assert!(!stamp.skips(11));
        assert!(stamp.skips(12));
    }

    #[test]
    fn a_prefix_only_stamp_skips_through_its_prefix() {
        let stamp = ReplayStamp::through(100);
        assert!(stamp.skips(100));
        assert!(!stamp.skips(101));
        assert!(!ReplayStamp::default().skips(1));
    }

    #[test]
    fn the_highest_named_lsn_is_the_last_range_end_or_the_prefix() {
        assert_eq!(stamp(10, &[(12, 13), (20, 21)]).highest(), 21);
        assert_eq!(ReplayStamp::through(7).highest(), 7);
    }

    #[test]
    fn a_union_names_what_either_stamp_names_and_nothing_else() {
        let a = stamp(10, &[(14, 15), (30, 30)]);
        let b = stamp(12, &[(16, 18), (25, 26), (40, 41)]);
        let u = a.union(&b);
        assert_eq!(u, stamp(12, &[(14, 18), (25, 26), (30, 30), (40, 41)]));
        assert!(u.validate().is_ok());
        for lsn in 0..=45 {
            assert_eq!(u.skips(lsn), a.skips(lsn) || b.skips(lsn), "lsn {lsn}");
        }
    }

    #[test]
    fn a_naming_stamp_names_its_one_record() {
        let stamp = ReplayStamp::naming(9);
        assert!(stamp.validate().is_ok());
        assert!(stamp.skips(9));
        assert!(!stamp.skips(8) && !stamp.skips(10));
    }

    #[test]
    fn a_union_folds_ranges_that_reach_the_prefix_into_it() {
        let u = stamp(10, &[(11, 12), (20, 20)]).union(&stamp(5, &[(8, 14)]));
        assert_eq!(u, stamp(14, &[(20, 20)]));
        assert_eq!(
            ReplayStamp::through(9).union(&ReplayStamp::default()),
            ReplayStamp::through(9)
        );
    }

    #[test]
    fn a_malformed_stamp_is_refused() {
        assert!(stamp(10, &[(12, 13), (20, 20)]).validate().is_ok());
        assert_eq!(
            stamp(10, &[(13, 12)]).validate(),
            Err(InvalidReplayStamp::Inverted { start: 13, end: 12 })
        );
        assert!(matches!(
            stamp(10, &[(9, 12)]).validate(),
            Err(InvalidReplayStamp::NotAbovePrefix { .. })
        ));
        assert!(matches!(
            stamp(10, &[(12, 14), (15, 16)]).validate(),
            Err(InvalidReplayStamp::OutOfOrder { .. })
        ));
        assert!(matches!(
            stamp(10, &[(20, 21), (12, 13)]).validate(),
            Err(InvalidReplayStamp::OutOfOrder { .. })
        ));
    }
}
