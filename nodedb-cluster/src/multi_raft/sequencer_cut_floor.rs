// SPDX-License-Identifier: BUSL-1.1

//! The cut floors of the markers this node appends to its sequencer log.
//!
//! A read cut closes through a Calvin cut marker. The marker carries the
//! cut's clock part as its floor (`SequencerEntry::CutMarker::floor`). Every
//! epoch the log holds after the marker must carry an instant above that
//! floor, or a Calvin version lands at or below a cut a reader closed.
//!
//! Applying the marker folds the floor into the applied epoch instant, and
//! a leader mints above the applied instant. An epoch the leader mints
//! before it applies the marker can still enter the log after it. So the
//! leader also records each floor as it appends the marker, under this
//! struct's lock, and checks an epoch's instant against it under the same
//! lock as it appends the epoch. The log order of a marker and an epoch is
//! their append order under the lock, so every epoch after a marker in the
//! log carries an instant above the marker's floor.
//!
//! A new leader seeds only after it applied its whole log, every marker an
//! earlier leader appended included. No clock bound is assumed.

use crate::calvin::SEQUENCER_GROUP_ID;
use crate::error::Result;
use crate::multi_raft::core::MultiRaft;

impl MultiRaft {
    /// Propose the encoded sequencer entry `data` to the sequencer group.
    /// `cut_floor` is the floor of a cut marker, `0` for any other entry.
    /// The floor is recorded before the append, so an epoch appended after
    /// the marker sees it.
    pub fn propose_to_sequencer(&mut self, data: Vec<u8>, cut_floor: i64) -> Result<u64> {
        if cut_floor > 0 {
            self.sequencer_cut_floor_ms = self
                .sequencer_cut_floor_ms
                .max(nodedb_types::ordinal_to_ms(cut_floor));
        }
        self.propose_to_group(SEQUENCER_GROUP_ID, data)
    }

    /// The highest cut floor, in epoch milliseconds, of any cut marker this
    /// node appended to its sequencer log. `0` for none. An epoch this node
    /// appends must carry a later instant.
    pub fn sequencer_cut_floor_ms(&self) -> i64 {
        self.sequencer_cut_floor_ms
    }
}

#[cfg(test)]
mod tests {
    use crate::multi_raft::core::MultiRaft;
    use crate::routing::RoutingTable;

    /// A floor only rises. Proposing without a mounted group still records
    /// it: a higher floor never makes a later epoch wrong.
    #[test]
    fn the_cut_floor_only_rises() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut mr = MultiRaft::new(1, RoutingTable::uniform(1, &[1], 1), dir.path().into());
        assert_eq!(mr.sequencer_cut_floor_ms(), 0);
        let ahead = nodedb_types::ms_to_ordinal_upper(5_000);
        let _ = mr.propose_to_sequencer(Vec::new(), ahead);
        assert_eq!(mr.sequencer_cut_floor_ms(), 5_000);
        let _ = mr.propose_to_sequencer(Vec::new(), nodedb_types::ms_to_ordinal_upper(10));
        let _ = mr.propose_to_sequencer(Vec::new(), 0);
        assert_eq!(mr.sequencer_cut_floor_ms(), 5_000);
    }
}
