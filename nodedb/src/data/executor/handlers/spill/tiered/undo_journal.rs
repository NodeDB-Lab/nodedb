// SPDX-License-Identifier: BUSL-1.1

//! An undo journal whose entries all grow with staged rows: the GRAPH and
//! ARRAY overlays' journals.
//!
//! Each entry records the hot version a mutation overwrote and has a
//! position. A spill moves every in-RAM entry to a journal file. A rollback
//! replays the entries at or after its marker, newest first: the in-RAM
//! ones, then the spilled ones one block at a time.

use super::journal::SpilledJournal;
use super::space::SpillSpace;
use super::weight::SpillWeight;

/// An undo journal. See the module doc.
pub(in crate::data::executor) struct UndoJournal<E> {
    next_seq: u64,
    /// RAM entries, ascending seq.
    tail: Vec<(u64, E)>,
    tail_bytes: usize,
    spilled: SpilledJournal<E>,
}

impl<E> Default for UndoJournal<E> {
    fn default() -> Self {
        Self {
            next_seq: 0,
            tail: Vec::new(),
            tail_bytes: 0,
            spilled: SpilledJournal::default(),
        }
    }
}

impl<E> std::fmt::Debug for UndoJournal<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoJournal")
            .field("next_seq", &self.next_seq)
            .field("tail", &self.tail.len())
            .finish()
    }
}

impl<E> UndoJournal<E>
where
    E: SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
{
    /// The position the next entry takes: the savepoint marker.
    pub(in crate::data::executor) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// RAM the in-memory entries hold.
    pub(in crate::data::executor) fn tail_bytes(&self) -> usize {
        self.tail_bytes
    }

    /// Heap bytes of the spilled journal's RAM index.
    pub(in crate::data::executor) fn spilled_heap_bytes(&self) -> usize {
        self.spilled.heap_bytes()
    }

    /// Append `entry`. Returns its position.
    pub(in crate::data::executor) fn push(&mut self, entry: E) -> u64 {
        let seq = self.next_seq;
        self.tail_bytes += entry.spill_weight();
        self.tail.push((seq, entry));
        self.next_seq += 1;
        seq
    }

    /// Move every in-RAM entry to one journal file in `space`.
    pub(in crate::data::executor) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        if self.tail.is_empty() {
            return Ok(());
        }
        let path = space.next_path("journal")?;
        self.spilled
            .spill(path, self.tail.iter().map(|(seq, entry)| (*seq, entry)))?;
        self.tail.clear();
        self.tail_bytes = 0;
        Ok(())
    }

    /// Hand every entry at or after `marker` to `undo`, newest first, then
    /// drop them. The next entry takes position `marker`.
    pub(in crate::data::executor) fn rollback(
        &mut self,
        marker: u64,
        space: &SpillSpace,
        mut undo: impl FnMut(E),
    ) -> crate::Result<()> {
        while self.tail.last().is_some_and(|(seq, _)| *seq >= marker) {
            let Some((_, entry)) = self.tail.pop() else {
                break;
            };
            self.tail_bytes = self.tail_bytes.saturating_sub(entry.spill_weight());
            undo(entry);
        }
        self.spilled
            .for_each_rev(marker, space.reader(), |_, entry| {
                undo(entry);
                Ok(())
            })?;
        self.spilled.truncate(marker)?;
        self.next_seq = marker;
        Ok(())
    }
}
