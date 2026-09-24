// SPDX-License-Identifier: BUSL-1.1

//! What this core knows about the applied prefix of the WAL.
//!
//! Every request from the Control Plane carries the node's outcome floor: every
//! record at or below it that any core receives has a final outcome. The core
//! keeps the highest floor it has read.

use crate::types::Lsn;

/// The core's view of the node's outcome floor.
#[derive(Debug)]
pub(in crate::data::executor) struct AppliedPrefix {
    outcome_floor: Lsn,
}

impl AppliedPrefix {
    /// A core that has read no floor yet.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            outcome_floor: Lsn::ZERO,
        }
    }

    /// Read the floor a request carried. The kept floor never decreases.
    pub(in crate::data::executor) fn observe_outcome_floor(&mut self, floor: Lsn) {
        if floor > self.outcome_floor {
            self.outcome_floor = floor;
        }
    }

    /// The highest outcome floor this core has read.
    pub(in crate::data::executor) fn outcome_floor(&self) -> Lsn {
        self.outcome_floor
    }
}

#[cfg(test)]
mod tests {
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
}
