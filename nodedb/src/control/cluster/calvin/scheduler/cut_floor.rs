// SPDX-License-Identifier: BUSL-1.1

//! Backup cut markers as one vShard's scheduler receives them.
//!
//! A cut marker arrives between two epoch batches, so every transaction of
//! an epoch at or below the highest epoch delivered before the marker came
//! before it, and every transaction of a later epoch came after it. A
//! transaction after a marker commits above the marker's watermark: a
//! restore of the backup that placed the marker refuses it. A transaction
//! before the marker finishes before the scheduler reports the marker, so the
//! backup holds it.
//!
//! The commit HLC of a transaction is the instant its epoch was created,
//! read once on the sequencer leader and replicated with the batch, raised to
//! the floor of every marker it came after. Replicas of a vShard receive the
//! same inputs in the same order, so they stamp every transaction alike.

use super::recovery::NOT_YET_APPLIED_EPOCH;

/// Nanoseconds per millisecond, to express an epoch's millisecond wall time
/// on the nanosecond HLC scale.
const NANOS_PER_MILLI: u64 = 1_000_000;

/// A marker whose floor applies to the epochs above `through`.
#[derive(Debug, Clone, Copy)]
struct Marker {
    through: u64,
    floor: u64,
}

/// A marker the scheduler has not reported yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitingCut {
    /// The marker's watermark.
    pub hlc: u64,
    /// The highest epoch delivered before the marker. The marker passes once
    /// every epoch at or below it is fully applied.
    pub through: u64,
}

/// The cut markers one scheduler received.
#[derive(Debug, Default)]
pub struct CutFloors {
    /// The floor every epoch above the folded markers records at least.
    base: u64,
    /// Markers not folded into `base`, in arrival order.
    markers: Vec<Marker>,
    /// Markers not reported yet.
    waiting: Vec<WaitingCut>,
}

impl CutFloors {
    /// Receive a marker carrying `hlc`, after epochs up to `highest_seen`
    /// were delivered (`None` when none was). Returns `true` when every
    /// transaction before it finished already: nothing came before it.
    pub fn receive(&mut self, hlc: u64, highest_seen: Option<u64>) -> bool {
        let floor = hlc.saturating_add(1);
        match highest_seen {
            None => {
                self.base = self.base.max(floor);
                true
            }
            Some(through) => {
                self.markers.push(Marker { through, floor });
                self.waiting.push(WaitingCut { hlc, through });
                false
            }
        }
    }

    /// The commit HLC of a transaction of `epoch` whose epoch was created at
    /// `epoch_system_ms`.
    pub fn commit_hlc(&self, epoch: u64, epoch_system_ms: i64) -> u64 {
        let created = u64::try_from(epoch_system_ms)
            .unwrap_or(0)
            .saturating_mul(NANOS_PER_MILLI);
        self.markers
            .iter()
            .filter(|marker| marker.through < epoch)
            .map(|marker| marker.floor)
            .fold(created.max(self.base), u64::max)
    }

    /// Take every waiting marker whose epochs are fully applied, given the
    /// test `fully_applied_through`. Returns their watermarks.
    pub fn take_passed(&mut self, fully_applied_through: impl Fn(u64) -> bool) -> Vec<u64> {
        let mut passed = Vec::new();
        self.waiting.retain(|cut| {
            if fully_applied_through(cut.through) {
                passed.push(cut.hlc);
                false
            } else {
                true
            }
        });
        passed
    }

    /// Fold every marker whose floor covers all epochs above `fully_applied`
    /// into the base floor. No transaction of an epoch at or below
    /// `fully_applied` commits again, so only the epochs above it need the
    /// markers told apart.
    pub fn fold(&mut self, fully_applied: u64) {
        if fully_applied == NOT_YET_APPLIED_EPOCH {
            return;
        }
        let base = &mut self.base;
        self.markers.retain(|marker| {
            if marker.through <= fully_applied {
                *base = (*base).max(marker.floor);
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transaction_after_a_marker_commits_above_its_watermark() {
        let mut floors = CutFloors::default();
        assert!(!floors.receive(5_000_000_000, Some(7)));
        assert_eq!(
            floors.commit_hlc(7, 1),
            NANOS_PER_MILLI,
            "epoch 7 came before"
        );
        assert_eq!(floors.commit_hlc(8, 1), 5_000_000_001, "epoch 8 came after");
        assert_eq!(
            floors.commit_hlc(8, 9_000),
            9_000 * NANOS_PER_MILLI,
            "a later creation instant stands"
        );
    }

    #[test]
    fn a_marker_passes_once_its_epochs_are_fully_applied() {
        let mut floors = CutFloors::default();
        floors.receive(100, Some(3));
        floors.receive(200, Some(5));
        assert_eq!(floors.take_passed(|through| through <= 4), vec![100]);
        assert_eq!(
            floors.take_passed(|through| through <= 4),
            Vec::<u64>::new()
        );
        assert_eq!(floors.take_passed(|through| through <= 5), vec![200]);
    }

    #[test]
    fn a_marker_before_any_epoch_passes_at_once_and_raises_every_epoch() {
        let mut floors = CutFloors::default();
        assert!(floors.receive(100, None));
        assert_eq!(floors.commit_hlc(0, 0), 101);
        assert!(floors.take_passed(|_| false).is_empty());
    }

    #[test]
    fn folding_keeps_every_later_stamp() {
        let mut floors = CutFloors::default();
        floors.receive(100, Some(3));
        floors.receive(50, Some(6));
        let before: Vec<u64> = (4..9).map(|epoch| floors.commit_hlc(epoch, 0)).collect();
        floors.fold(4);
        let after: Vec<u64> = (5..9).map(|epoch| floors.commit_hlc(epoch, 0)).collect();
        assert_eq!(&before[1..], &after[..]);
        floors.fold(NOT_YET_APPLIED_EPOCH);
        assert_eq!(floors.commit_hlc(7, 0), 101);
    }
}
