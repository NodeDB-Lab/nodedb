// SPDX-License-Identifier: BUSL-1.1

//! The undo log of a batched install, drained newest first one batch at a
//! time.
//!
//! A drain merges the two lanes of the log (see `undo`) back into the
//! reverse of write order. [`UndoDrain::drain_batch`] hands over the
//! entries of one batch and stops at the first entry of the batch before
//! it. That entry waits in the drain, so a rollback can make the reversal
//! durable at the boundary and give the core back before it goes on.

use crate::data::executor::handlers::spill::tiered::rev_spool::NewestFirst;
use crate::data::executor::handlers::transaction::undo::spilled::SpilledUndo;
use crate::data::executor::handlers::transaction::undo::{UndoEntry, UndoError};

use super::undo::{BatchAt, DrainStop, RamUndo, forward, ram_weight};

/// An entry read off the log: its forward position, the batch whose write
/// it reverses, and the entry.
type Drained = (usize, BatchAt, UndoEntry);

/// How one batch's drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor::handlers::transaction::redo_apply) enum BatchDrained {
    /// Every entry of the newest batch left went. `kept` is the batch the
    /// next entry reverses.
    Boundary { kept: BatchAt },
    /// The log is empty.
    Empty,
}

/// The undo log of one batched install, read newest first.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct UndoDrain {
    spilled: NewestFirst<(BatchAt, SpilledUndo)>,
    /// Ascending by `follows`. The drain pops the newest.
    ram: Vec<RamUndo>,
    /// RAM the RAM-lane entries left hold.
    ram_bytes: usize,
    /// The first entry of the batch the last drain stopped before.
    next: Option<Drained>,
}

impl UndoDrain {
    pub(super) fn new(spilled: NewestFirst<(BatchAt, SpilledUndo)>, ram: Vec<RamUndo>) -> Self {
        let ram_bytes = ram.iter().map(|ram| ram_weight(&ram.entry)).sum();
        Self {
            spilled,
            ram,
            ram_bytes,
            next: None,
        }
    }

    /// The RAM the drain holds: the spill lane's tail and read buffer, the
    /// RAM lane, and the entry that waits.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn held_bytes(&self) -> usize {
        let next = self
            .next
            .as_ref()
            .map_or(0, |(_, _, entry)| ram_weight(entry));
        self.spilled
            .held_bytes()
            .saturating_add(self.ram_bytes)
            .saturating_add(next)
    }

    /// Hand every entry of the newest batch left to `undo`, newest first,
    /// with its forward position in the log and its batch. The first error
    /// stops the drain.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn drain_batch(
        &mut self,
        mut undo: impl FnMut(usize, BatchAt, UndoEntry) -> Result<(), UndoError>,
    ) -> Result<BatchDrained, DrainStop> {
        let mut reversing: Option<BatchAt> = None;
        loop {
            let Some((position, at, entry)) = self.next_entry()? else {
                return Ok(BatchDrained::Empty);
            };
            if let Some(reversed) = reversing
                && reversed != at
            {
                self.next = Some((position, at, entry));
                return Ok(BatchDrained::Boundary { kept: at });
            }
            reversing = Some(at);
            undo(position, at, entry).map_err(DrainStop::Undo)?;
        }
    }

    /// The newest entry left, `None` once the log is empty. `Err` names the
    /// position of the spilled entry that could not be read back.
    fn next_entry(&mut self) -> Result<Option<Drained>, DrainStop> {
        if let Some(next) = self.next.take() {
            return Ok(Some(next));
        }
        // Every spill-lane entry at or above the newest RAM entry's
        // position was recorded after it, so it goes first. Its forward
        // position counts the RAM entries left: all came before it.
        let follows = self.ram.last().map_or(0, |last| last.follows);
        if self.spilled.remaining() > follows {
            let position = forward(self.spilled.remaining() - 1, self.ram.len());
            let read = self
                .spilled
                .next_newest()
                .map_err(|error| DrainStop::Read {
                    entry_index: position,
                    error,
                })?;
            if let Some((at, entry)) = read {
                return Ok(Some((position, at, UndoEntry::from(entry))));
            }
        }
        let Some(last) = self.ram.pop() else {
            return Ok(None);
        };
        self.ram_bytes = self.ram_bytes.saturating_sub(ram_weight(&last.entry));
        Ok(Some((
            forward(last.follows, self.ram.len()),
            last.at,
            last.entry,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::super::undo::SpillableUndo;
    use super::*;
    use crate::data::executor::handlers::spill::tiered::SpillSpace;
    use crate::wal::redo::replay_arm::RedoArm;

    fn stats(label: &str) -> UndoEntry {
        UndoEntry::StatsRestore {
            key: label.to_string(),
            prior: None,
        }
    }

    fn at(arm: RedoArm, batch: u32) -> BatchAt {
        BatchAt { arm, batch }
    }

    /// Each drain hands over one batch, newest first, and stops at the
    /// first entry of the batch before it, which the next drain hands over
    /// first.
    #[test]
    fn a_drain_stops_at_each_batch_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("undo"), "test", 0);
        let mut undo = SpillableUndo::default();
        for batch in [0u32, 2, 3] {
            undo.enter(at(RedoArm::Document, batch));
            undo.record([stats(&format!("{batch}-a")), stats(&format!("{batch}-b"))]);
            if batch == 2 {
                undo.spill(&mut space).expect("spill");
            }
        }
        let mut drain = undo.into_drain();
        let mut batches = Vec::new();
        loop {
            let mut entries = Vec::new();
            let drained = drain
                .drain_batch(|index, at, _| {
                    entries.push((index, at.batch));
                    Ok(())
                })
                .unwrap_or_else(|_| panic!("drain"));
            batches.push(entries);
            match drained {
                BatchDrained::Boundary { kept } => {
                    let reversed = batches
                        .last()
                        .and_then(|entries| entries.last())
                        .map(|(_, batch)| *batch);
                    assert!(reversed.is_some_and(|reversed| kept.batch < reversed));
                }
                BatchDrained::Empty => break,
            }
        }
        assert_eq!(
            batches,
            vec![
                vec![(5, 3), (4, 3)],
                vec![(3, 2), (2, 2)],
                vec![(1, 0), (0, 0)],
            ]
        );
        assert_eq!(drain.held_bytes(), 0, "an empty drain holds no RAM");
    }
}
