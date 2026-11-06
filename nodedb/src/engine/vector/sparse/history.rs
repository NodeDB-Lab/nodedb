// SPDX-License-Identifier: BUSL-1.1

//! System-time history of a sparse inverted index.
//!
//! An overwrite or a delete of a document ends its live version at the
//! write's ordinal instead of dropping it. The ended version keeps its
//! posting entries here, tagged with its span, so a read at an earlier cut
//! still scores it. A version is visible at ordinal `S` iff
//! `sys_from <= S < sys_to`. A live version has no `sys_to`.
//!
//! A live version carries its start only while a cut can read below it. A
//! live version without a start is visible at every retained cut. A purge at
//! `horizon` drops every ended version with `sys_to <= horizon`, and the
//! start of every live version with `sys_from <= horizon`. The newest version
//! at or below the horizon stays.

use std::collections::HashMap;

/// One ended version of a document.
#[derive(Debug, Clone, PartialEq)]
pub struct EndedVersion {
    /// The document the version belongs to.
    pub doc_id: String,
    /// The dimensions the version has entries for.
    pub dims: Vec<u32>,
    /// The ordinal of the write that created the version.
    pub sys_from: i64,
    /// The ordinal of the write that ended the version.
    pub sys_to: i64,
}

impl EndedVersion {
    /// Whether the version is visible at ordinal `at`.
    pub fn visible_at(&self, at: i64) -> bool {
        self.sys_from <= at && at < self.sys_to
    }
}

/// The start ordinals of live versions and the ended versions of one index.
/// See the module docs.
/// One ended version with its `(dimension, weight)` entries.
pub type EndedVersionEntries<'a> = (u32, &'a EndedVersion, Vec<(u32, f32)>);

#[derive(Debug, Default)]
pub struct SparseHistory {
    /// Bytes the ended versions and their posting entries hold
    /// ([`ended_version_bytes`]), kept in step with every end and removal.
    ended_bytes: usize,
    /// Live internal id → `sys_from`, while a cut can read below it.
    live_from: HashMap<u32, i64>,
    /// Ended internal id → its version.
    ended: HashMap<u32, EndedVersion>,
    /// Dimension → `(ended internal id, weight)` entries.
    ended_postings: HashMap<u32, Vec<(u32, f32)>>,
}

impl SparseHistory {
    /// Record live version `id` as started at `sys_from`. `i64::MIN` records
    /// nothing: the version is visible at every retained cut.
    pub fn record_live(&mut self, id: u32, sys_from: i64) {
        if sys_from == i64::MIN {
            self.live_from.remove(&id);
        } else {
            self.live_from.insert(id, sys_from);
        }
    }

    /// The start of live version `id`. `i64::MIN` when it carries none.
    pub fn live_from(&self, id: u32) -> i64 {
        self.live_from.get(&id).copied().unwrap_or(i64::MIN)
    }

    /// Remove and return the start of live version `id`.
    pub fn take_live_from(&mut self, id: u32) -> i64 {
        self.live_from.remove(&id).unwrap_or(i64::MIN)
    }

    /// Whether live version `id` is visible at ordinal `at`.
    pub fn live_visible_at(&self, id: u32, at: i64) -> bool {
        self.live_from.get(&id).is_none_or(|from| *from <= at)
    }

    /// Keep version `id` of `doc_id` with `entries` as ended over
    /// `[sys_from, sys_to)`. A span no ordinal falls in is dropped: no cut
    /// reads it.
    pub fn end(
        &mut self,
        id: u32,
        doc_id: String,
        entries: &[(u32, f32)],
        sys_from: i64,
        sys_to: i64,
    ) {
        if sys_from >= sys_to {
            return;
        }
        // A prior version under this id leaves with its postings, so a read
        // never scores the new span with the old weights.
        self.remove_ended(id);
        let mut dims = Vec::with_capacity(entries.len());
        for &(dim, weight) in entries {
            self.ended_postings
                .entry(dim)
                .or_default()
                .push((id, weight));
            dims.push(dim);
        }
        let version = EndedVersion {
            doc_id,
            dims,
            sys_from,
            sys_to,
        };
        self.ended_bytes += ended_version_bytes(&version);
        self.ended.insert(id, version);
    }

    /// Remove ended version `id` and return it with its entries. `None`
    /// when `id` is no ended version.
    pub fn remove_ended(&mut self, id: u32) -> Option<(EndedVersion, Vec<(u32, f32)>)> {
        let version = self.ended.remove(&id)?;
        self.ended_bytes -= ended_version_bytes(&version);
        let mut entries = Vec::with_capacity(version.dims.len());
        for &dim in &version.dims {
            if let Some(list) = self.ended_postings.get_mut(&dim) {
                if let Some(at) = list.iter().position(|(entry, _)| *entry == id) {
                    entries.push((dim, list.swap_remove(at).1));
                }
                if list.is_empty() {
                    self.ended_postings.remove(&dim);
                }
            }
        }
        Some((version, entries))
    }

    /// The ended entries of dimension `dim`.
    pub fn ended_postings(&self, dim: u32) -> &[(u32, f32)] {
        self.ended_postings.get(&dim).map_or(&[], Vec::as_slice)
    }

    /// Whether ended version `id` is visible at ordinal `at`.
    pub fn ended_visible_at(&self, id: u32, at: i64) -> bool {
        self.ended.get(&id).is_some_and(|v| v.visible_at(at))
    }

    /// The document of ended version `id`.
    pub fn ended_doc_id(&self, id: u32) -> Option<&str> {
        self.ended.get(&id).map(|v| v.doc_id.as_str())
    }

    /// Every ended version with its entries, ascending by id.
    pub fn ended_versions(&self) -> Vec<EndedVersionEntries<'_>> {
        let mut versions: Vec<EndedVersionEntries<'_>> = self
            .ended
            .iter()
            .map(|(&id, version)| {
                let entries = version
                    .dims
                    .iter()
                    .filter_map(|dim| {
                        let list = self.ended_postings.get(dim)?;
                        let weight = list.iter().find(|(entry, _)| *entry == id)?.1;
                        Some((*dim, weight))
                    })
                    .collect();
                (id, version, entries)
            })
            .collect();
        versions.sort_unstable_by_key(|(id, _, _)| *id);
        versions
    }

    /// The newest ordinal the history names: a live start or an end.
    /// `None` when it names none.
    pub fn newest_ordinal(&self) -> Option<i64> {
        self.live_from
            .values()
            .copied()
            .chain(self.ended.values().map(|v| v.sys_to))
            .max()
    }

    /// Number of ended versions held.
    pub fn ended_count(&self) -> usize {
        self.ended.len()
    }

    /// Whether the history holds no ended version and no live start.
    pub fn is_empty(&self) -> bool {
        self.ended.is_empty() && self.live_from.is_empty()
    }

    /// Bytes the history holds: live starts, ended versions, and their
    /// posting entries. Constant time: every write keeps the count.
    pub fn resident_bytes(&self) -> usize {
        self.live_from.len() * LIVE_START_BYTES + self.ended_bytes
    }

    /// Drop every version no cut at or above `horizon` reads: ended versions
    /// with `sys_to <= horizon`. Drop the start of every live version with
    /// `sys_from <= horizon`: every retained cut reads it. Returns the number
    /// of ended versions dropped.
    pub fn purge_below(&mut self, horizon: i64) -> usize {
        self.live_from.retain(|_, from| *from > horizon);
        let dropped: Vec<u32> = self
            .ended
            .iter()
            .filter(|(_, v)| v.sys_to <= horizon)
            .map(|(id, _)| *id)
            .collect();
        for id in &dropped {
            self.remove_ended(*id);
        }
        dropped.len()
    }
}

/// Bytes one live start holds: its id and its ordinal.
const LIVE_START_BYTES: usize = std::mem::size_of::<u32>() + std::mem::size_of::<i64>();

/// Bytes one ended version holds: its id, its record, its document id, and
/// one posting entry and one dimension per entry.
fn ended_version_bytes(version: &EndedVersion) -> usize {
    use std::mem::size_of;
    size_of::<u32>()
        + size_of::<EndedVersion>()
        + version.doc_id.len()
        + version.dims.len() * (size_of::<u32>() + size_of::<(u32, f32)>())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An id that ends twice keeps only its newest postings. Older dimensions
    /// never score under the new span, and a purge leaves nothing behind.
    #[test]
    fn ending_an_id_again_replaces_its_postings_and_its_bytes() {
        let mut h = SparseHistory::default();
        h.end(1, "d".into(), &[(5, 0.5)], 10, 20);
        h.end(1, "d".into(), &[(6, 0.7), (7, 0.9)], 20, 30);
        assert!(h.ended_postings(5).is_empty());
        assert_eq!(h.ended_postings(6), &[(1, 0.7)]);
        assert_eq!(h.ended_count(), 1);

        h.purge_below(30);
        assert!(h.ended_postings(6).is_empty());
        assert_eq!(h.resident_bytes(), 0);
    }

    #[test]
    fn an_ended_version_is_visible_over_its_span_only() {
        let mut h = SparseHistory::default();
        h.end(1, "d".into(), &[(5, 0.5)], 10, 20);
        assert!(!h.ended_visible_at(1, 9));
        assert!(h.ended_visible_at(1, 10));
        assert!(h.ended_visible_at(1, 19));
        assert!(!h.ended_visible_at(1, 20));
        assert_eq!(h.ended_postings(5), &[(1, 0.5)]);
    }

    #[test]
    fn an_empty_span_is_not_kept() {
        let mut h = SparseHistory::default();
        h.end(1, "d".into(), &[(5, 0.5)], 20, 20);
        assert_eq!(h.ended_count(), 0);
        assert!(h.ended_postings(5).is_empty());
    }

    #[test]
    fn a_live_start_hides_the_version_below_it() {
        let mut h = SparseHistory::default();
        h.record_live(3, 15);
        assert!(!h.live_visible_at(3, 14));
        assert!(h.live_visible_at(3, 15));
        assert!(
            h.live_visible_at(4, i64::MIN),
            "no start: visible everywhere"
        );
    }

    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let mut h = SparseHistory::default();
        h.end(1, "d".into(), &[(5, 0.5)], 10, 20);
        h.end(2, "d".into(), &[(5, 0.6)], 20, 30);
        h.record_live(3, 30);
        assert_eq!(h.purge_below(25), 1);
        assert!(!h.ended_visible_at(1, 15), "successor started at 20 <= 25");
        assert!(h.ended_visible_at(2, 25), "visible at the horizon");
        assert_eq!(h.live_from(3), 30, "starts above the horizon");
        assert_eq!(h.purge_below(30), 1);
        assert_eq!(h.live_from(3), i64::MIN, "every retained cut reads it");
        assert!(h.is_empty());
    }
}
