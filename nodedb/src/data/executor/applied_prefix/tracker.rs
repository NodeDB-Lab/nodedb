// SPDX-License-Identifier: BUSL-1.1

//! What this core knows about the applied prefix of the WAL.
//!
//! Every request from the Control Plane carries the node's outcome floor F:
//! every record at or below F that any core receives has a final outcome. The
//! core keeps the highest F it has read, and the set of LSNs above F that it
//! applied itself. Together they are the [`ReplayStamp`] a checkpoint takes.
//!
//! ## Why a record at or below F is in every later artifact
//!
//! A record routed here has a final outcome only once this core answered it,
//! and the core answers after the apply. The request carrying F reaches the
//! core after that answer, so every record at or below F that this core
//! applies is applied before the core reads F.
//!
//! ## Bounded size
//!
//! The set holds ranges, pruned as F advances, so it holds only the records
//! this core applied while an older record was in flight. A window held until
//! restart keeps F down for the rest of the process, so the set can still
//! grow. Past [`MAX_APPLIED_RANGES`] ranges the tracker drops the set and
//! refuses to stamp until F passes the highest LSN it dropped. Every dropped
//! LSN is then at or below F. A refused stamp fails the checkpoint, which
//! costs WAL growth and never data.
//!
//! ## After boot
//!
//! Restart replay decides every record in the WAL before the core serves a
//! request: it applies the record, or a stamp, a tombstone or an abort marker
//! says it must not. [`AppliedPrefix::seed_replayed_through`] therefore
//! raises the floor to the highest LSN replay read, before replay starts.
//! Nothing checkpoints until replay ends, and every request after boot
//! carries an LSN minted after replay, above every record replay read.

use super::ranges::LsnRanges;
use super::stamp::ReplayStamp;
use crate::types::Lsn;

/// Most ranges the applied set holds before the tracker drops it.
pub(in crate::data::executor) const MAX_APPLIED_RANGES: usize = 65_536;

/// Why the tracker cannot state what an artifact written now holds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "the applied set above outcome floor {floor} passed {MAX_APPLIED_RANGES} ranges and was \
     dropped; a stamp is exact again once the floor reaches lsn {dropped_through}"
)]
pub(crate) struct StampUnavailable {
    pub floor: u64,
    pub dropped_through: u64,
}

impl From<StampUnavailable> for crate::Error {
    fn from(e: StampUnavailable) -> Self {
        crate::Error::Storage {
            engine: "checkpoint".to_string(),
            detail: e.to_string(),
        }
    }
}

/// The core's view of the node's outcome floor and of what it applied above
/// it.
#[derive(Debug)]
pub(in crate::data::executor) struct AppliedPrefix {
    outcome_floor: Lsn,
    /// LSNs above `outcome_floor` this core applied. Empty while `dropped`.
    applied_above: LsnRanges,
    /// Highest LSN applied since the set was dropped. `None` while the set is
    /// exact.
    dropped: Option<u64>,
}

impl AppliedPrefix {
    /// A core that has read no floor yet.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            outcome_floor: Lsn::ZERO,
            applied_above: LsnRanges::default(),
            dropped: None,
        }
    }

    /// Read the floor a request carried. The kept floor never decreases.
    pub(in crate::data::executor) fn observe_outcome_floor(&mut self, floor: Lsn) {
        if floor <= self.outcome_floor {
            return;
        }
        self.outcome_floor = floor;
        self.applied_above.prune_through(floor.as_u64());
        if self
            .dropped
            .is_some_and(|dropped| dropped <= floor.as_u64())
        {
            self.dropped = None;
        }
    }

    /// Raise the floor to `lsn`, the highest LSN restart replay reads. Called
    /// before replay starts.
    pub(in crate::data::executor) fn seed_replayed_through(&mut self, lsn: Lsn) {
        self.observe_outcome_floor(lsn);
    }

    /// Record that this core applied the record at `lsn`.
    pub(in crate::data::executor) fn note_applied(&mut self, lsn: Lsn) {
        let lsn = lsn.as_u64();
        if lsn <= self.outcome_floor.as_u64() {
            return;
        }
        if let Some(dropped) = self.dropped.as_mut() {
            *dropped = (*dropped).max(lsn);
            return;
        }
        self.applied_above.insert(lsn);
        if self.applied_above.range_count() > MAX_APPLIED_RANGES {
            self.dropped = Some(self.applied_above.max().unwrap_or(lsn).max(lsn));
            self.applied_above.clear();
        }
    }

    /// What an artifact written now holds.
    pub(in crate::data::executor) fn stamp(&self) -> Result<ReplayStamp, StampUnavailable> {
        if let Some(dropped_through) = self.dropped {
            return Err(StampUnavailable {
                floor: self.outcome_floor.as_u64(),
                dropped_through,
            });
        }
        let mut stamp = ReplayStamp::through(self.outcome_floor.as_u64());
        stamp.applied_above = self.applied_above.to_ranges();
        Ok(stamp)
    }

    /// The highest outcome floor this core has read.
    pub(in crate::data::executor) fn outcome_floor(&self) -> Lsn {
        self.outcome_floor
    }
}

#[cfg(test)]
mod tests {
    use super::super::stamp::LsnRange;
    use super::*;

    #[test]
    fn the_kept_floor_never_decreases() {
        let mut prefix = AppliedPrefix::new();
        assert_eq!(prefix.outcome_floor(), Lsn::ZERO);
        prefix.observe_outcome_floor(Lsn::new(9));
        prefix.observe_outcome_floor(Lsn::new(4));
        assert_eq!(prefix.outcome_floor(), Lsn::new(9));
        prefix.observe_outcome_floor(Lsn::new(12));
        assert_eq!(prefix.outcome_floor(), Lsn::new(12));
    }

    #[test]
    fn applied_lsns_above_the_floor_are_stamped_and_pruned_as_it_advances() {
        let mut prefix = AppliedPrefix::new();
        prefix.observe_outcome_floor(Lsn::new(10));
        for lsn in [12, 13, 20, 8] {
            prefix.note_applied(Lsn::new(lsn));
        }
        assert_eq!(
            prefix.stamp(),
            Ok(ReplayStamp {
                prefix: 10,
                applied_above: vec![
                    LsnRange { start: 12, end: 13 },
                    LsnRange { start: 20, end: 20 }
                ],
            })
        );
        prefix.observe_outcome_floor(Lsn::new(15));
        assert_eq!(
            prefix.stamp(),
            Ok(ReplayStamp {
                prefix: 15,
                applied_above: vec![LsnRange { start: 20, end: 20 }],
            })
        );
        prefix.observe_outcome_floor(Lsn::new(20));
        assert_eq!(prefix.stamp(), Ok(ReplayStamp::through(20)));
    }

    #[test]
    fn a_record_in_flight_below_an_applied_one_is_not_stamped() {
        let mut prefix = AppliedPrefix::new();
        prefix.observe_outcome_floor(Lsn::new(10));
        prefix.note_applied(Lsn::new(12));
        let stamp = prefix.stamp().expect("exact");
        assert!(!stamp.skips(11), "record 11 is still on its way");
        assert!(stamp.skips(12));
    }

    #[test]
    fn the_set_stays_bounded_and_refuses_to_stamp_until_the_floor_passes_it() {
        let mut prefix = AppliedPrefix::new();
        // Every other LSN: no two touch, so each is its own range.
        for i in 1..=(MAX_APPLIED_RANGES as u64 + 1) {
            prefix.note_applied(Lsn::new(i * 2));
        }
        let top = (MAX_APPLIED_RANGES as u64 + 1) * 2;
        assert_eq!(prefix.applied_above.range_count(), 0, "the set is dropped");
        assert_eq!(
            prefix.stamp(),
            Err(StampUnavailable {
                floor: 0,
                dropped_through: top,
            })
        );
        prefix.note_applied(Lsn::new(top + 10));
        prefix.observe_outcome_floor(Lsn::new(top));
        assert!(
            prefix.stamp().is_err(),
            "an LSN applied after the drop is still above the floor"
        );
        prefix.observe_outcome_floor(Lsn::new(top + 10));
        assert_eq!(prefix.stamp(), Ok(ReplayStamp::through(top + 10)));
        prefix.note_applied(Lsn::new(top + 12));
        assert_eq!(prefix.applied_above.range_count(), 1, "exact again");
    }

    #[test]
    fn seeding_after_boot_covers_every_replayed_record() {
        let mut prefix = AppliedPrefix::new();
        prefix.seed_replayed_through(Lsn::new(500));
        // Replay notes each record it applies; all are at or below the seed.
        for lsn in [3, 250, 500] {
            prefix.note_applied(Lsn::new(lsn));
        }
        assert_eq!(prefix.applied_above.range_count(), 0);
        assert_eq!(prefix.stamp(), Ok(ReplayStamp::through(500)));
        // A fresh Control Plane's floor starts low; the seed is kept.
        prefix.observe_outcome_floor(Lsn::new(0));
        prefix.note_applied(Lsn::new(502));
        assert_eq!(
            prefix.stamp().expect("exact").applied_above,
            vec![LsnRange {
                start: 502,
                end: 502
            }]
        );
    }
}
