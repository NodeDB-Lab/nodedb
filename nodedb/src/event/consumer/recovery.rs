// SPDX-License-Identifier: BUSL-1.1

//! Rebuild dropped events from the WAL.
//!
//! The ring drops an event when it is full. The consumer learns of the drop
//! from a gap in the core's event numbers, or from the core's emitted counter
//! running past the last event it took. Every dropped event belongs to a
//! record already in the WAL: a write appends its record before its core
//! applies it and emits. So recovery replays the WAL up to the head it read
//! when it learned of the drop.
//!
//! Recovery replays only records whose outcome is final. A record above the
//! outcome floor may still be applying, or may yet be refused; its events
//! arrive from the ring once it applies. At boot every record in the WAL is
//! final: the cores replayed it before the Event Plane started.
//!
//! The guard refuses what was already delivered, so recovery may replay a
//! record the ring also delivers.

use crate::types::Lsn;

/// A recovery in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    /// Every dropped event belongs to a record at or below this LSN.
    target: Lsn,
    /// Every record at or below this LSN was replayed.
    replayed_through: Lsn,
}

impl Recovery {
    /// Recover every record above `safe` up to `target`.
    pub fn new(safe: Lsn, target: Lsn) -> Self {
        Self {
            target,
            replayed_through: safe,
        }
    }

    /// Widen the recovery to `target`, for a drop learned of while it runs.
    pub fn extend(&mut self, target: Lsn) {
        self.target = self.target.max(target);
    }

    /// Every record at or below this LSN was replayed.
    pub fn replayed_through(&self) -> Lsn {
        self.replayed_through
    }

    /// Whether every record a dropped event could belong to was replayed.
    pub fn is_done(&self) -> bool {
        self.replayed_through >= self.target
    }

    /// The records the next pass replays, `(from, upto)` inclusive, when the
    /// final-outcome bound lets it make progress.
    pub fn next_range(&self, final_bound: Lsn) -> Option<(Lsn, Lsn)> {
        let upto = self.target.min(final_bound);
        upto.is_ahead_of(self.replayed_through)
            .then(|| (self.replayed_through.next(), upto))
    }

    /// Record that a pass replayed every record up to `upto`.
    pub fn note_replayed(&mut self, upto: Lsn) {
        self.replayed_through = self.replayed_through.max(upto);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_stops_at_the_final_outcome_bound() {
        let mut recovery = Recovery::new(Lsn::new(10), Lsn::new(30));
        assert_eq!(
            recovery.next_range(Lsn::new(20)),
            Some((Lsn::new(11), Lsn::new(20)))
        );
        recovery.note_replayed(Lsn::new(20));
        assert!(!recovery.is_done());
        // The bound has not moved: nothing more is final yet.
        assert_eq!(recovery.next_range(Lsn::new(20)), None);
        assert_eq!(
            recovery.next_range(Lsn::new(99)),
            Some((Lsn::new(21), Lsn::new(30)))
        );
        recovery.note_replayed(Lsn::new(30));
        assert!(recovery.is_done());
    }

    #[test]
    fn a_later_drop_widens_the_target() {
        let mut recovery = Recovery::new(Lsn::new(10), Lsn::new(15));
        recovery.extend(Lsn::new(12));
        recovery.note_replayed(Lsn::new(15));
        assert!(recovery.is_done());
        recovery.extend(Lsn::new(40));
        assert!(!recovery.is_done());
        assert_eq!(
            recovery.next_range(Lsn::new(40)),
            Some((Lsn::new(16), Lsn::new(40)))
        );
    }

    #[test]
    fn a_target_at_or_below_the_prefix_is_already_done() {
        let recovery = Recovery::new(Lsn::new(10), Lsn::new(8));
        assert!(recovery.is_done());
        assert_eq!(recovery.next_range(Lsn::new(50)), None);
    }
}
