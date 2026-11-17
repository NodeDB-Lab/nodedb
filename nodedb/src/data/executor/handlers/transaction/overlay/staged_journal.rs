// SPDX-License-Identifier: BUSL-1.1

//! The staging overlay's undo journal and `ROLLBACK TO SAVEPOINT`.
//!
//! Each journal entry has a position (its seq). A savepoint marker is a
//! position. Every version a mutation stages carries the mutation's
//! position, in the hot tier and in a spilled run.
//!
//! Rolling back to a marker drops every version written at or after it in
//! every tier, then replays the journal entries at or after the marker,
//! newest first. A slot entry restores the hot version the mutation
//! overwrote. A version that a spill had moved to a run is still in its run,
//! so the journal keeps only hot versions. Replaying every entry down to the
//! marker leaves each slot at its value at the marker.
//!
//! A spill moves the slot and tag entries, which grow with the staged rows,
//! to a journal file. Truncate markers, timeseries instants and preview
//! schemas grow with statements and stay in RAM.

use std::collections::HashMap;

use nodedb_types::RowIdentity;

use super::row_tags::{RowTags, TagUndo};
use super::staged::{CollKey, CollectionOverlay, Staged, TouchedSlot, TxnOverlay};
use super::staged_sidecar::StagedTtl;
use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpilledJournal};
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;

/// One overlay slot's hot state captured immediately before a staged
/// value/TTL mutation overwrote it, with each hot version's seq.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) struct OverlayUndo {
    pub(super) coll_key: CollKey,
    pub(super) surrogate: u32,
    pub(super) doc_id: RowIdentity,
    /// Prior hot `by_surrogate` version, or `None` if the hot slot was empty.
    pub(super) prev_value: Option<(Staged, u64)>,
    /// Prior hot `ttl_by_surrogate` version, or `None` if absent.
    pub(super) prev_ttl: Option<(StagedTtl, u64)>,
    /// Prior hot `doc_id_to_surrogate` binding, or `None` if unbound.
    pub(super) prev_doc_binding: Option<(u32, u64)>,
    /// Whether the row was tagged body-only before the mutation.
    pub(super) prev_body_tag: bool,
}

/// One undo-journal entry: a slot mutation, a truncate marker, an unkeyed
/// ingest instant, a preview schema, or a row-tag change.
#[derive(Debug, Clone)]
pub(super) enum JournalEntry {
    /// A slot's prior hot state, captured before a staged value/TTL mutation.
    Slot(OverlayUndo),
    /// A truncate marker set on `coll_key`. `prev` is the journal position
    /// of the marker it replaced, `None` when the collection was not
    /// truncated before.
    Truncated {
        coll_key: CollKey,
        prev: Option<usize>,
    },
    /// An unkeyed timeseries ingest instant appended to `coll_key`.
    UnkeyedIngest { coll_key: CollKey },
    /// A timeseries preview schema set on `coll_key`. `prev` is the schema
    /// it replaced.
    PreviewSchema {
        coll_key: CollKey,
        prev: Option<ColumnarSchema>,
    },
    /// A row-tag or collection-set change. See [`super::row_tags`].
    Tags(TagUndo),
}

/// The journal entries a spill moves to disk.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) enum SpilledEntry {
    Slot(OverlayUndo),
    Tags(TagUndo),
}

impl JournalEntry {
    /// RAM an entry holds.
    fn weight(&self) -> usize {
        let base = std::mem::size_of::<JournalEntry>();
        match self {
            JournalEntry::Slot(undo) => {
                base + undo.coll_key.2.len()
                    + undo.doc_id.as_str().len()
                    + match &undo.prev_value {
                        Some((Staged::Put(body), _)) => body.len(),
                        Some((Staged::Tombstone, _)) | None => 0,
                    }
            }
            JournalEntry::Tags(TagUndo::Row { coll_key, row, .. }) => {
                base + coll_key.2.len() + row.as_str().len()
            }
            JournalEntry::Tags(TagUndo::Staging { added, .. }) => {
                base + added.iter().map(|key| key.2.len()).sum::<usize>()
            }
            JournalEntry::Truncated { coll_key, .. }
            | JournalEntry::UnkeyedIngest { coll_key }
            | JournalEntry::PreviewSchema { coll_key, .. } => base + coll_key.2.len(),
        }
    }
}

/// The overlay's undo journal: entries in RAM, and the spilled ones.
#[derive(Debug, Default)]
pub(super) struct OverlayJournal {
    next_seq: usize,
    /// RAM entries, ascending seq.
    tail: Vec<(usize, JournalEntry)>,
    tail_bytes: usize,
    spilled: SpilledJournal<SpilledEntry>,
}

impl OverlayJournal {
    /// The position the next entry takes.
    pub(super) fn next_seq(&self) -> usize {
        self.next_seq
    }

    /// RAM the in-memory entries hold.
    pub(super) fn tail_bytes(&self) -> usize {
        self.tail_bytes
    }

    /// Heap bytes of the spilled journal's RAM index.
    pub(super) fn spilled_heap_bytes(&self) -> usize {
        self.spilled.heap_bytes()
    }

    /// Append `entry`. Returns its position.
    pub(super) fn push(&mut self, entry: JournalEntry) -> usize {
        let seq = self.next_seq;
        self.tail_bytes += entry.weight();
        self.tail.push((seq, entry));
        self.next_seq += 1;
        seq
    }

    /// Move the slot and tag entries to one journal file in `space`.
    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        if !self
            .tail
            .iter()
            .any(|(_, e)| matches!(e, JournalEntry::Slot(_) | JournalEntry::Tags(_)))
        {
            return Ok(());
        }
        let mut moved: Vec<(u64, SpilledEntry)> = Vec::new();
        let mut kept: Vec<(usize, JournalEntry)> = Vec::new();
        for (seq, entry) in std::mem::take(&mut self.tail) {
            match entry {
                JournalEntry::Slot(undo) => moved.push((seq as u64, SpilledEntry::Slot(undo))),
                JournalEntry::Tags(undo) => moved.push((seq as u64, SpilledEntry::Tags(undo))),
                other @ (JournalEntry::Truncated { .. }
                | JournalEntry::UnkeyedIngest { .. }
                | JournalEntry::PreviewSchema { .. }) => kept.push((seq, other)),
            }
        }
        self.tail_bytes = kept.iter().map(|(_, e)| e.weight()).sum();
        self.tail = kept;
        let path = space.next_path("journal")?;
        self.spilled
            .spill(path, moved.iter().map(|(seq, entry)| (*seq, entry)))
    }
}

/// Restore the hot state one spilled or in-RAM slot entry recorded, and the
/// row tag it held.
fn undo_slot(
    collections: &mut HashMap<CollKey, CollectionOverlay>,
    row_tags: &mut RowTags,
    undo: OverlayUndo,
) {
    row_tags.restore(&undo.coll_key, &undo.doc_id, undo.prev_body_tag);
    let Some(overlay) = collections.get_mut(&undo.coll_key) else {
        return;
    };
    overlay
        .by_surrogate
        .restore(undo.surrogate, undo.prev_value);
    overlay
        .ttl_by_surrogate
        .restore(undo.surrogate, undo.prev_ttl);
    overlay
        .doc_id_to_surrogate
        .restore(undo.doc_id, undo.prev_doc_binding);
}

impl TxnOverlay {
    /// Rewind every staged mutation, truncate marker and tag change recorded
    /// at or after `marker`. Each slot returns to its exact state at the
    /// marker, across the hot tier and the spilled runs. A `marker` at or
    /// beyond the current length is a no-op.
    ///
    /// A spill-file error part way leaves the overlay unusable: every later
    /// call fails and the transaction can only roll back.
    pub fn rollback_to(&mut self, marker: usize) -> crate::Result<()> {
        self.guard()?;
        if marker >= self.journal.next_seq() {
            return Ok(());
        }
        let result = self.rollback_tiers(marker);
        if let Err(e) = &result {
            self.poisoned = Some(e.to_string());
        }
        result
    }

    fn rollback_tiers(&mut self, marker: usize) -> crate::Result<()> {
        let bound = marker as u64;
        for overlay in self.collections.values_mut() {
            overlay.rollback(bound, &mut self.space)?;
        }
        while self
            .journal
            .tail
            .last()
            .is_some_and(|(seq, _)| *seq >= marker)
        {
            let Some((_, entry)) = self.journal.tail.pop() else {
                break;
            };
            self.journal.tail_bytes = self.journal.tail_bytes.saturating_sub(entry.weight());
            self.undo_entry(entry);
        }
        let collections = &mut self.collections;
        let row_tags = &mut self.row_tags;
        self.journal
            .spilled
            .for_each_rev(bound, self.space.reader(), |_, entry| {
                match entry {
                    SpilledEntry::Slot(undo) => undo_slot(collections, row_tags, undo),
                    SpilledEntry::Tags(undo) => row_tags.undo(undo),
                }
                Ok(())
            })?;
        self.journal.spilled.truncate(bound)?;
        self.journal.next_seq = marker;
        self.collections.retain(|_, overlay| !overlay.is_empty());
        Ok(())
    }

    fn undo_entry(&mut self, entry: JournalEntry) {
        match entry {
            JournalEntry::Slot(undo) => undo_slot(&mut self.collections, &mut self.row_tags, undo),
            JournalEntry::Truncated { coll_key, prev } => {
                match prev {
                    Some(position) => self.truncated.insert(coll_key, position),
                    None => self.truncated.remove(&coll_key),
                };
            }
            JournalEntry::UnkeyedIngest { coll_key } => {
                if let Some(overlay) = self.collections.get_mut(&coll_key) {
                    overlay.unkeyed_ingest_now.pop();
                }
            }
            JournalEntry::PreviewSchema { coll_key, prev } => {
                if let Some(overlay) = self.collections.get_mut(&coll_key) {
                    overlay.ts_preview_schema = prev;
                }
            }
            JournalEntry::Tags(undo) => self.row_tags.undo(undo),
        }
    }

    /// Every slot of `coll_key` that a staged value or TTL mutation touched
    /// at or after `marker`. Each slot appears once, in the order it was
    /// first touched.
    pub fn slots_touched_since(
        &self,
        marker: usize,
        coll_key: &CollKey,
    ) -> crate::Result<Vec<TouchedSlot>> {
        self.guard()?;
        let mut first: Vec<OverlayUndo> = Vec::new();
        let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mut first_touch =
            |undo: &OverlayUndo| &undo.coll_key == coll_key && seen.insert(undo.surrogate);
        self.journal
            .spilled
            .for_each_from(marker as u64, self.space.reader(), |_, entry| {
                if let SpilledEntry::Slot(undo) = entry
                    && first_touch(&undo)
                {
                    first.push(undo);
                }
                Ok(())
            })?;
        for (seq, entry) in &self.journal.tail {
            if *seq >= marker
                && let JournalEntry::Slot(undo) = entry
                && first_touch(undo)
            {
                first.push(undo.clone());
            }
        }
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(Vec::new());
        };
        let reader = self.space.reader();
        let mut slots = Vec::with_capacity(first.len());
        for undo in first {
            // The first mutation after the marker overwrote the hot version
            // it recorded. With none, the value at the marker is the newest
            // spilled version written before the marker.
            let before = match undo.prev_value {
                Some((value, _)) => Some(value),
                None => overlay
                    .by_surrogate
                    .get_before(&undo.surrogate, marker as u64, reader)?,
            };
            let after = overlay
                .by_surrogate
                .get(&undo.surrogate, reader)?
                .map(std::borrow::Cow::into_owned);
            slots.push(TouchedSlot {
                surrogate: undo.surrogate,
                doc_id: undo.doc_id,
                before,
                after,
            });
        }
        Ok(slots)
    }
}
