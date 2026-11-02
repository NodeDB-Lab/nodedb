// SPDX-License-Identifier: Apache-2.0

//! System-time spans of a collection's nodes.
//!
//! An update or a delete of a row ends the row's node at the write's
//! system-time ordinal instead of dropping it. The node stays in its index
//! soft-deleted, so a current search skips it at no extra cost, and a search
//! at an earlier cut admits it again. A node is visible at ordinal `S` iff
//! `sys_from <= S < sys_to`, where a live node has no `sys_to`.
//!
//! A live node carries a span only while a snapshot can read below its
//! `sys_from`. A live node without a span is visible at every retained cut.
//! An ended node carries its span until a purge drops every version ended at
//! or below the purge horizon. A soft-deleted node without a span is gone:
//! no cut reads it, and compaction removes it.

use std::collections::HashMap;

use nodedb_types::Surrogate;
use roaring::RoaringBitmap;

/// The system-time span of one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeSpan {
    /// The row the node holds the vector of. [`Surrogate::ZERO`] for a
    /// headless node.
    pub surrogate: Surrogate,
    /// The ordinal of the write that inserted the node. `i64::MIN` for a
    /// node whose insert no retained cut reads below.
    pub sys_from: i64,
    /// The ordinal of the write that ended the node. `None` while live.
    pub sys_to: Option<i64>,
}

impl NodeSpan {
    /// Whether the node is visible at ordinal `at`.
    pub fn visible_at(&self, at: i64) -> bool {
        self.sys_from <= at && self.sys_to.is_none_or(|to| at < to)
    }
}

/// One persisted span: `(node id, surrogate, sys_from, sys_to)`.
pub type SpanEntry = (u32, u32, i64, Option<i64>);

/// The admission sets of a search at one cut, by global node id.
#[derive(Debug, Default)]
pub struct CutSets {
    /// Live nodes inserted after the cut.
    pub hide: RoaringBitmap,
    /// Ended nodes visible at the cut.
    pub revive: RoaringBitmap,
}

/// The spans of one collection's nodes. See the module docs.
#[derive(Debug, Default)]
pub struct VectorVersions {
    spans: HashMap<u32, NodeSpan>,
    /// Ended nodes by row, so a read of one row at a cut finds its version
    /// without a scan.
    ended_by_surrogate: HashMap<Surrogate, Vec<u32>>,
}

impl VectorVersions {
    /// Record node `id` as inserted for `surrogate` at `sys_from`.
    pub fn record_live(&mut self, id: u32, surrogate: Surrogate, sys_from: i64) {
        self.drop_ended_index(id);
        self.spans.insert(
            id,
            NodeSpan {
                surrogate,
                sys_from,
                sys_to: None,
            },
        );
    }

    /// End node `id` of `surrogate` at `sys_to`. A node with no span ends
    /// with `sys_from = i64::MIN`: no retained cut reads below its insert.
    pub fn end(&mut self, id: u32, surrogate: Surrogate, sys_to: i64) {
        let span = self.spans.entry(id).or_insert(NodeSpan {
            surrogate,
            sys_from: i64::MIN,
            sys_to: None,
        });
        if span.sys_to.is_some() {
            return;
        }
        span.sys_to = Some(sys_to);
        let owner = span.surrogate;
        self.ended_by_surrogate.entry(owner).or_default().push(id);
    }

    /// Make the ended node `id` live again: the undo of the write that ended
    /// it. Returns `false` when `id` carries no ended span.
    pub fn revive(&mut self, id: u32) -> bool {
        let Some(span) = self.spans.get_mut(&id) else {
            return false;
        };
        if span.sys_to.take().is_none() {
            return false;
        }
        self.drop_ended_index(id);
        true
    }

    /// Drop every trace of node `id`: no cut reads it again.
    pub fn forget(&mut self, id: u32) {
        self.drop_ended_index(id);
        self.spans.remove(&id);
    }

    /// Drop every span.
    pub fn clear(&mut self) {
        self.spans.clear();
        self.ended_by_surrogate.clear();
    }

    /// The span of node `id`, when it carries one.
    pub fn span(&self, id: u32) -> Option<&NodeSpan> {
        self.spans.get(&id)
    }

    /// The row an ended node held, so a search hit on it resolves.
    pub fn ended_surrogate(&self, id: u32) -> Option<Surrogate> {
        self.spans
            .get(&id)
            .filter(|span| span.sys_to.is_some() && span.surrogate != Surrogate::ZERO)
            .map(|span| span.surrogate)
    }

    /// Whether node `id` is an ended node a cut still reads. Compaction keeps
    /// such a node.
    pub fn is_retained(&self, id: u32) -> bool {
        self.spans
            .get(&id)
            .is_some_and(|span| span.sys_to.is_some())
    }

    /// The ended nodes a cut still reads among global ids `[base, base +
    /// len)`, as ids relative to `base`.
    pub fn retained_in(&self, base: u32, len: usize) -> RoaringBitmap {
        let end = u64::from(base) + len as u64;
        self.spans
            .iter()
            .filter(|(id, span)| {
                span.sys_to.is_some() && u64::from(**id) >= u64::from(base) && u64::from(**id) < end
            })
            .map(|(id, _)| id - base)
            .collect()
    }

    /// Whether a live node `id` is visible at `at`. A live node without a
    /// span is visible at every retained cut.
    pub fn live_visible_at(&self, id: u32, at: i64) -> bool {
        self.spans.get(&id).is_none_or(|span| span.sys_from <= at)
    }

    /// The ended nodes of `surrogate` visible at `at`.
    pub fn ended_visible_at(&self, surrogate: Surrogate, at: i64) -> Vec<u32> {
        self.ended_by_surrogate
            .get(&surrogate)
            .into_iter()
            .flatten()
            .copied()
            .filter(|id| self.spans.get(id).is_some_and(|s| s.visible_at(at)))
            .collect()
    }

    /// The newest `sys_to` of the ended nodes of `surrogate`. `None` when
    /// it holds no ended node.
    pub fn newest_end_of(&self, surrogate: Surrogate) -> Option<i64> {
        self.ended_by_surrogate
            .get(&surrogate)
            .into_iter()
            .flatten()
            .filter_map(|id| self.spans.get(id).and_then(|span| span.sys_to))
            .max()
    }

    /// The admission sets of a search at `at`. Cost is the number of spans:
    /// the writes no purge dropped yet.
    pub fn cut_sets(&self, at: i64) -> CutSets {
        let mut sets = CutSets::default();
        for (&id, span) in &self.spans {
            match span.sys_to {
                None if span.sys_from > at => {
                    sets.hide.insert(id);
                }
                None => {}
                Some(_) if span.visible_at(at) => {
                    sets.revive.insert(id);
                }
                Some(_) => {}
            }
        }
        sets
    }

    /// Drop every version a cut at or above `horizon` cannot read: ended
    /// nodes with `sys_to <= horizon`, and the span of a live node with
    /// `sys_from <= horizon` (every retained cut reads it). The newest version
    /// at or below the horizon stays. Returns the ids of the dropped ended
    /// nodes. They stay soft-deleted, and compaction removes them.
    pub fn purge_below(&mut self, horizon: i64) -> Vec<u32> {
        let mut dropped: Vec<(u32, Surrogate)> = Vec::new();
        self.spans.retain(|&id, span| match span.sys_to {
            Some(to) if to <= horizon => {
                dropped.push((id, span.surrogate));
                false
            }
            Some(_) => true,
            None => span.sys_from > horizon,
        });
        for (id, surrogate) in &dropped {
            if let Some(ids) = self.ended_by_surrogate.get_mut(surrogate) {
                ids.retain(|n| n != id);
                if ids.is_empty() {
                    self.ended_by_surrogate.remove(surrogate);
                }
            }
        }
        let mut ids: Vec<u32> = dropped.into_iter().map(|(id, _)| id).collect();
        ids.sort_unstable();
        ids
    }

    /// Renumber nodes after compaction: `remap(old)` is the new id, `None`
    /// for a removed node.
    pub fn remap(&mut self, remap: impl Fn(u32) -> Option<u32>) {
        let spans = std::mem::take(&mut self.spans);
        self.ended_by_surrogate.clear();
        for (old, span) in spans {
            let Some(new) = remap(old) else {
                continue;
            };
            if span.sys_to.is_some() {
                self.ended_by_surrogate
                    .entry(span.surrogate)
                    .or_default()
                    .push(new);
            }
            self.spans.insert(new, span);
        }
    }

    /// Every span, ordered by node id, for a checkpoint.
    pub fn to_entries(&self) -> Vec<SpanEntry> {
        let mut entries: Vec<SpanEntry> = self
            .spans
            .iter()
            .map(|(&id, s)| (id, s.surrogate.as_u32(), s.sys_from, s.sys_to))
            .collect();
        entries.sort_unstable_by_key(|e| e.0);
        entries
    }

    /// The spans a checkpoint recorded.
    pub fn from_entries(entries: Vec<SpanEntry>) -> Self {
        let mut versions = Self::default();
        for (id, surrogate, sys_from, sys_to) in entries {
            let surrogate = Surrogate::new(surrogate);
            versions.spans.insert(
                id,
                NodeSpan {
                    surrogate,
                    sys_from,
                    sys_to,
                },
            );
            if sys_to.is_some() {
                versions
                    .ended_by_surrogate
                    .entry(surrogate)
                    .or_default()
                    .push(id);
            }
        }
        versions
    }

    /// Bytes the spans and the ended-node index hold: each span with a slot
    /// for its id in the ended index, and each row the index keys. Constant
    /// time.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        self.spans.len() * (2 * size_of::<u32>() + size_of::<NodeSpan>())
            + self.ended_by_surrogate.len() * (size_of::<Surrogate>() + size_of::<Vec<u32>>())
    }

    /// Number of spans held.
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    /// Whether no span is held.
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    fn drop_ended_index(&mut self, id: u32) {
        let Some(span) = self.spans.get(&id) else {
            return;
        };
        if let Some(ids) = self.ended_by_surrogate.get_mut(&span.surrogate) {
            ids.retain(|n| *n != id);
            if ids.is_empty() {
                self.ended_by_surrogate.remove(&span.surrogate);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Surrogate = Surrogate(7);

    #[test]
    fn a_span_is_visible_from_its_insert_until_its_end() {
        let mut v = VectorVersions::default();
        v.record_live(1, S, 10);
        v.end(1, S, 20);
        v.record_live(2, S, 20);
        let span = *v.span(1).expect("span");
        assert!(!span.visible_at(9));
        assert!(span.visible_at(10));
        assert!(span.visible_at(19));
        assert!(!span.visible_at(20), "the update at 20 replaces it");
        assert_eq!(v.ended_visible_at(S, 15), vec![1]);
        assert!(v.ended_visible_at(S, 25).is_empty());
        assert!(!v.live_visible_at(2, 19));
        assert!(v.live_visible_at(2, 20));
        assert!(v.live_visible_at(99, i64::MIN), "a spanless live node");
    }

    #[test]
    fn cut_sets_hide_later_inserts_and_revive_later_ends() {
        let mut v = VectorVersions::default();
        v.record_live(1, S, 10);
        v.end(1, S, 20);
        v.record_live(2, S, 20);
        v.record_live(3, Surrogate(8), 30);
        let at15 = v.cut_sets(15);
        assert!(at15.revive.contains(1));
        assert!(at15.hide.contains(2) && at15.hide.contains(3));
        let at25 = v.cut_sets(25);
        assert!(at25.revive.is_empty());
        assert!(!at25.hide.contains(2));
        assert!(at25.hide.contains(3));
    }

    #[test]
    fn revive_undoes_an_end_and_forget_drops_the_node() {
        let mut v = VectorVersions::default();
        v.record_live(1, S, 10);
        v.end(1, S, 20);
        assert!(v.is_retained(1));
        assert!(v.revive(1));
        assert!(!v.is_retained(1));
        assert!(!v.revive(1), "a live node has no end to undo");
        assert!(v.ended_visible_at(S, 15).is_empty());
        v.forget(1);
        assert!(v.span(1).is_none());
    }

    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let mut v = VectorVersions::default();
        v.record_live(1, S, 10);
        v.end(1, S, 20);
        v.record_live(2, S, 20);
        v.end(2, S, 30);
        v.record_live(3, S, 30);
        let dropped = v.purge_below(25);
        assert_eq!(dropped, vec![1]);
        assert!(v.span(2).is_some_and(|s| s.visible_at(25)));
        assert!(v.span(3).is_some(), "a live span above the horizon stays");
        let dropped = v.purge_below(35);
        assert_eq!(dropped, vec![2]);
        assert!(v.span(3).is_none(), "every retained cut reads node 3");
        assert!(v.is_empty());
    }

    #[test]
    fn entries_round_trip_and_remap_follows_compaction() {
        let mut v = VectorVersions::default();
        v.record_live(4, S, 10);
        v.end(4, S, 20);
        v.record_live(5, S, 20);
        let back = VectorVersions::from_entries(v.to_entries());
        assert_eq!(back.to_entries(), v.to_entries());
        assert_eq!(back.ended_visible_at(S, 15), vec![4]);

        let mut moved = back;
        moved.remap(|old| Some(old - 4));
        assert_eq!(moved.ended_visible_at(S, 15), vec![0]);
        assert_eq!(moved.span(1).map(|s| s.sys_from), Some(20));
    }
}
