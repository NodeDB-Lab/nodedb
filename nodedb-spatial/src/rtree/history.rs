// SPDX-License-Identifier: Apache-2.0

//! System-time history of one R-tree.
//!
//! A write indexes a row's geometry at its system-time ordinal. An update or
//! a delete ends the indexed entry at the write's ordinal: the entry leaves
//! the live tree and moves to the history with its interval
//! `[sys_from, sys_to)`. The ended entries sit in their own R-tree, keyed by
//! a version id, so a read at a cut finds them by bbox. A current read never
//! touches the history.

use std::collections::HashMap;

use nodedb_types::BoundingBox;

use super::node::{EntryId, RTreeEntry};
use super::tree::RTree;

/// The ordinal of a write that changes no history: it records no start and
/// ends no version. An index rebuild and a checkpoint restore write this way.
pub const UNVERSIONED: i64 = i64::MIN;

/// An indexed entry a write ended, visible at the cuts in
/// `[sys_from, sys_to)`.
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct EndedEntry {
    /// The id the entry held in the live tree.
    pub entry_id: EntryId,
    /// The id of the row the entry indexed.
    pub document_id: String,
    pub bbox: BoundingBox,
    pub sys_from: i64,
    pub sys_to: i64,
}

impl EndedEntry {
    /// Whether a read at ordinal `at` sees this entry.
    pub fn visible_at(&self, at: i64) -> bool {
        self.sys_from <= at && at < self.sys_to
    }
}

/// The start ordinal of one live entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LiveStart {
    pub entry_id: EntryId,
    pub sys_from: i64,
}

/// One candidate of a read at a cut.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CutCandidate<'a> {
    /// A live entry visible at the cut. The row's id is in the caller's
    /// reverse map.
    Live(EntryId),
    /// An ended entry visible at the cut.
    Ended(&'a EndedEntry),
}

/// The history rows of one tree, as a checkpoint carries them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HistoryRows {
    pub live: Vec<LiveStart>,
    pub ended: Vec<EndedEntry>,
}

/// The start ordinals of the live entries and the ended entries of one tree.
#[derive(Default)]
pub(crate) struct SpatialHistory {
    /// The start of each live entry indexed at a versioned ordinal. An entry
    /// absent here is visible at every cut.
    live_from: HashMap<EntryId, i64>,
    /// Ended entries by version id.
    ended: HashMap<u64, EndedEntry>,
    /// The version id of each ended entry by its live id and end ordinal.
    by_end: HashMap<(EntryId, i64), u64>,
    /// The ended entries' bboxes, keyed by version id. Created with the
    /// first ended entry, in the live tree's SRID.
    ended_tree: Option<Box<RTree>>,
    next_version: u64,
    /// Bytes of every ended entry's document id, kept in step with every
    /// push and drop.
    doc_id_bytes: usize,
}

impl SpatialHistory {
    /// Whether the history holds no ended entry.
    pub(crate) fn has_no_ended(&self) -> bool {
        self.ended.is_empty()
    }

    /// Bytes the history holds resident: the live starts, the ended
    /// entries with their document ids, the end index, and the ended tree.
    /// Constant time: every push and drop keeps the counts.
    pub(crate) fn resident_bytes(&self) -> usize {
        use std::mem::size_of;
        let live = self.live_from.len() * (size_of::<EntryId>() + size_of::<i64>());
        let ended = self.ended.len() * (size_of::<u64>() + size_of::<EndedEntry>());
        let by_end = self.by_end.len() * (size_of::<(EntryId, i64)>() + size_of::<u64>());
        let tree = self.ended_tree.as_ref().map_or(0, |t| t.nodes_bytes());
        live + ended + self.doc_id_bytes + by_end + tree
    }

    pub(crate) fn ended_len(&self) -> usize {
        self.ended.len()
    }

    /// Record that live entry `id` starts at `sys_from`.
    pub(crate) fn record_live(&mut self, id: EntryId, sys_from: i64) {
        if sys_from == UNVERSIONED {
            self.live_from.remove(&id);
        } else {
            self.live_from.insert(id, sys_from);
        }
    }

    /// Remove and return the start of live entry `id`.
    pub(crate) fn take_live(&mut self, id: EntryId) -> i64 {
        self.live_from.remove(&id).unwrap_or(UNVERSIONED)
    }

    /// Whether a read at `at` sees live entry `id`.
    pub(crate) fn live_visible_at(&self, id: EntryId, at: i64) -> bool {
        self.live_from.get(&id).is_none_or(|from| *from <= at)
    }

    /// Keep `entry` as history. `live` is the tree the entry left: the
    /// ended tree takes its memory scope and SRID.
    pub(crate) fn push_ended(&mut self, live: &RTree, entry: EndedEntry) {
        let version = self.next_version;
        self.next_version += 1;
        let tree = self
            .ended_tree
            .get_or_insert_with(|| Box::new(RTree::new(live.memory().clone(), live.tree_srid())));
        tree.insert(RTreeEntry {
            id: version,
            bbox: entry.bbox,
        });
        self.by_end.insert((entry.entry_id, entry.sys_to), version);
        self.doc_id_bytes += entry.document_id.len();
        self.ended.insert(version, entry);
    }

    /// Remove and return the ended entry `id` that a write ended at `sys_to`.
    pub(crate) fn take_ended(&mut self, id: EntryId, sys_to: i64) -> Option<EndedEntry> {
        let version = *self.by_end.get(&(id, sys_to))?;
        self.drop_version(version)
    }

    fn drop_version(&mut self, version: u64) -> Option<EndedEntry> {
        let entry = self.ended.remove(&version)?;
        self.doc_id_bytes -= entry.document_id.len();
        self.by_end.remove(&(entry.entry_id, entry.sys_to));
        if let Some(tree) = self.ended_tree.as_mut() {
            tree.delete(version);
        }
        if self.ended.is_empty() {
            self.ended_tree = None;
        }
        Some(entry)
    }

    /// The ended entries whose bbox meets `query` and whose interval holds
    /// `at`.
    pub(crate) fn ended_at(&self, query: &BoundingBox, at: i64) -> Vec<&EndedEntry> {
        let Some(tree) = self.ended_tree.as_ref() else {
            return Vec::new();
        };
        tree.search(query)
            .into_iter()
            .filter_map(|e| self.ended.get(&e.id))
            .filter(|e| e.visible_at(at))
            .collect()
    }

    /// Drop every ended entry no cut at or above `horizon` reads, and every
    /// start at or below it. Returns the number of ended entries dropped.
    pub(crate) fn purge_below(&mut self, horizon: i64) -> usize {
        let doomed: Vec<u64> = self
            .ended
            .iter()
            .filter(|(_, e)| e.sys_to <= horizon)
            .map(|(v, _)| *v)
            .collect();
        for version in &doomed {
            self.drop_version(*version);
        }
        self.live_from.retain(|_, from| *from > horizon);
        doomed.len()
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn rows(&self) -> HistoryRows {
        let mut live: Vec<LiveStart> = self
            .live_from
            .iter()
            .map(|(id, from)| LiveStart {
                entry_id: *id,
                sys_from: *from,
            })
            .collect();
        live.sort_unstable_by_key(|s| s.entry_id);
        let mut versions: Vec<&u64> = self.ended.keys().collect();
        versions.sort_unstable();
        let ended = versions
            .into_iter()
            .filter_map(|v| self.ended.get(v).cloned())
            .collect();
        HistoryRows { live, ended }
    }
}
