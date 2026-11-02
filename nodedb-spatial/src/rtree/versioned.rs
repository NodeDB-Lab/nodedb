// SPDX-License-Identifier: Apache-2.0

//! Versioned writes and reads at a system-time cut (see `history`).

use nodedb_types::BoundingBox;

use super::history::{CutCandidate, EndedEntry, HistoryRows, UNVERSIONED};
use super::node::{EntryId, RTreeEntry};
use super::tree::RTree;

impl RTree {
    /// Insert `entry`, live from ordinal `sys_from`. [`UNVERSIONED`] makes
    /// the entry visible at every cut.
    pub fn insert_at(&mut self, entry: RTreeEntry, sys_from: i64) {
        self.history.record_live(entry.id, sys_from);
        self.insert(entry);
    }

    /// The live entry `id`. A linear walk of the tree.
    pub fn get(&self, id: EntryId) -> Option<&RTreeEntry> {
        self.entries().into_iter().find(|e| e.id == id)
    }

    /// Remove live entry `id`, which indexes row `document_id`, and keep it
    /// as history ended at `sys_to`. Returns the removed bbox, `None` when
    /// no live entry has the id.
    ///
    /// An end at [`UNVERSIONED`] keeps no history. An end at the entry's own
    /// start keeps none either: the write that indexed the entry replaced it
    /// at the same ordinal, or a WAL replay re-applies that write.
    pub fn end_at(&mut self, id: EntryId, document_id: &str, sys_to: i64) -> Option<BoundingBox> {
        let bbox = self.get(id)?.bbox;
        self.delete(id);
        let sys_from = self.history.take_live(id);
        if sys_to != UNVERSIONED && sys_from < sys_to {
            let mut history = std::mem::take(&mut self.history);
            history.push_ended(
                self,
                EndedEntry {
                    entry_id: id,
                    document_id: document_id.to_string(),
                    bbox,
                    sys_from,
                    sys_to,
                },
            );
            self.history = history;
        }
        Some(bbox)
    }

    /// Remove live entry `id` with its start: the reversal of an insert.
    /// Returns whether the entry was live.
    pub fn forget(&mut self, id: EntryId) -> bool {
        self.history.take_live(id);
        self.delete(id)
    }

    /// Put entry `id` back live with `bbox`: the reversal of an end at
    /// `sys_to`. The ended entry leaves the history, and the live entry
    /// starts at its start again. Without such an ended entry the entry is
    /// live at every cut.
    pub fn restore_ended(&mut self, id: EntryId, bbox: BoundingBox, sys_to: i64) {
        let sys_from = self
            .history
            .take_ended(id, sys_to)
            .map_or(UNVERSIONED, |e| e.sys_from);
        self.insert_at(RTreeEntry { id, bbox }, sys_from);
    }

    /// The candidates a read at ordinal `at` sees whose bbox meets `query`:
    /// the live entries inserted at or below `at`, then the ended entries
    /// whose interval holds `at`.
    pub fn search_at(&self, query: &BoundingBox, at: i64) -> Vec<CutCandidate<'_>> {
        let mut out: Vec<CutCandidate<'_>> = self
            .search(query)
            .into_iter()
            .filter(|e| self.history.live_visible_at(e.id, at))
            .map(|e| CutCandidate::Live(e.id))
            .collect();
        out.extend(
            self.history
                .ended_at(query, at)
                .into_iter()
                .map(CutCandidate::Ended),
        );
        out
    }

    /// Drop every ended entry no cut at or above `horizon` reads: the
    /// entries ended at or below `horizon`. The entry of each row visible at
    /// the horizon stays. Returns the number of ended entries dropped.
    pub fn purge_versions_below(&mut self, horizon: i64) -> usize {
        self.history.purge_below(horizon)
    }

    /// The number of ended entries the history holds.
    pub fn ended_len(&self) -> usize {
        self.history.ended_len()
    }

    /// Drop the whole history.
    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    /// The history as a checkpoint carries it.
    pub fn history_rows(&self) -> HistoryRows {
        self.history.rows()
    }

    /// Install `rows` as the history of a tree restored from a checkpoint.
    pub fn restore_history(&mut self, rows: HistoryRows) {
        self.history.clear();
        for start in rows.live {
            self.history.record_live(start.entry_id, start.sys_from);
        }
        let mut history = std::mem::take(&mut self.history);
        for entry in rows.ended {
            history.push_ended(self, entry);
        }
        self.history = history;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtree::TreeSrid;
    use crate::test_support::test_memory;

    fn point(x: f64) -> BoundingBox {
        BoundingBox::from_point(x, 0.0)
    }

    fn everywhere() -> BoundingBox {
        BoundingBox::new(-180.0, -90.0, 180.0, 90.0)
    }

    /// The row ids a read at `at` sees, sorted.
    fn rows_at(tree: &RTree, at: i64) -> Vec<String> {
        let mut rows: Vec<String> = tree
            .search_at(&everywhere(), at)
            .into_iter()
            .map(|c| match c {
                CutCandidate::Live(id) => format!("live:{id}"),
                CutCandidate::Ended(e) => format!("ended:{}@{}", e.entry_id, e.sys_from),
            })
            .collect();
        rows.sort();
        rows
    }

    /// Insert at 10, move at 20, delete at 30, re-insert at 40: each cut
    /// sees the entry visible there, and the current search the live one.
    #[test]
    fn each_cut_sees_the_entry_visible_there() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        assert_eq!(tree.end_at(1, "d1", 20), Some(point(1.0)));
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(2.0),
            },
            20,
        );
        tree.end_at(1, "d1", 30);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(3.0),
            },
            40,
        );

        assert!(rows_at(&tree, 5).is_empty());
        assert_eq!(rows_at(&tree, 15), vec!["ended:1@10"]);
        assert_eq!(rows_at(&tree, 25), vec!["ended:1@20"]);
        assert!(rows_at(&tree, 35).is_empty(), "deleted at 30");
        assert_eq!(rows_at(&tree, 45), vec!["live:1"]);
        assert_eq!(tree.search(&everywhere()).len(), 1);

        let at15 = tree.search_at(&point(1.0), 15);
        assert!(matches!(at15.as_slice(), [CutCandidate::Ended(e)] if e.document_id == "d1"));
        assert!(tree.search_at(&point(2.0), 15).is_empty(), "bbox prunes");
    }

    #[test]
    fn an_end_at_the_entry_start_keeps_no_history() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        tree.end_at(1, "d1", 10);
        tree.insert_at(
            RTreeEntry {
                id: 2,
                bbox: point(2.0),
            },
            10,
        );
        tree.end_at(2, "d2", UNVERSIONED);
        assert_eq!(tree.ended_len(), 0);
    }

    #[test]
    fn undo_restores_the_entry_and_its_start() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        tree.end_at(1, "d1", 20);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(2.0),
            },
            20,
        );
        // Roll the update back: forget the new entry, restore the old one.
        assert!(tree.forget(1));
        tree.restore_ended(1, point(1.0), 20);
        assert_eq!(tree.ended_len(), 0);
        assert!(rows_at(&tree, 5).is_empty());
        assert_eq!(rows_at(&tree, 25), vec!["live:1"]);
    }

    #[test]
    fn a_purge_drops_ended_entries_below_the_horizon() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        tree.end_at(1, "d1", 20);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(2.0),
            },
            20,
        );
        tree.end_at(1, "d1", 30);
        assert_eq!(tree.purge_versions_below(20), 1);
        assert_eq!(rows_at(&tree, 25), vec!["ended:1@20"]);
        assert_eq!(tree.purge_versions_below(30), 1);
        assert_eq!(tree.ended_len(), 0);
    }

    #[test]
    fn history_rows_round_trip() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        tree.end_at(1, "d1", 20);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(2.0),
            },
            20,
        );
        let rows = tree.history_rows();

        let mut restored = RTree::new(test_memory(), TreeSrid::WGS84);
        restored.insert(RTreeEntry {
            id: 1,
            bbox: point(2.0),
        });
        restored.restore_history(rows.clone());
        assert_eq!(restored.history_rows(), rows);
        assert_eq!(rows_at(&restored, 15), vec!["ended:1@10"]);
        assert_eq!(rows_at(&restored, 25), vec!["live:1"]);
    }

    /// A tree holding history keeps its SRID while its live entries are
    /// gone: the history's bboxes are in it.
    #[test]
    fn history_pins_the_tree_srid() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(1.0),
            },
            10,
        );
        tree.end_at(1, "d1", 20);
        assert!(tree.is_empty());
        let utm = crate::test_support::utm48n();
        assert!(!tree.accepts_srid(utm.srid));
        assert!(tree.bind_srid(utm).is_err());
        tree.purge_versions_below(20);
        assert!(tree.accepts_srid(utm.srid));
    }

    /// History raises the resident bytes, and a purge lowers them again.
    #[test]
    fn resident_bytes_follow_the_history() {
        let mut tree = RTree::new(test_memory(), TreeSrid::WGS84);
        tree.insert_at(
            RTreeEntry {
                id: 1,
                bbox: point(0.0),
            },
            1,
        );
        let live_only = tree.resident_bytes();
        for at in 2..50 {
            tree.end_at(1, "d1", at);
            tree.insert_at(
                RTreeEntry {
                    id: 1,
                    bbox: point(at as f64),
                },
                at,
            );
        }
        let with_history = tree.resident_bytes();
        assert!(with_history > live_only);
        assert_eq!(tree.purge_versions_below(50), 48);
        assert!(tree.resident_bytes() < with_history);
    }
}
