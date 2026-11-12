// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay for GRAPH writes.
//!
//! Graph is the first engine whose unit-of-mutation is NOT a single
//! surrogate-addressed row: an edge's identity is the string tuple
//! `(src_id, label, dst_id)` (the same identity the durable `EdgeStore` /
//! `ShardedCsrIndex` key by), and a node-label mutation touches a bitset
//! keyed by a raw node id, not a surrogate. Neither fits `super::TxnOverlay`
//! (which is keyed by `u32` surrogate), so this is a parallel, independent
//! overlay type held alongside it on `CoreLoop` (`graph_txn_overlays`).
//!
//! Scope: this overlay serves read-your-own-writes for Neighbors, Hop and
//! the `NeighborsMulti` hops of the walk coordinators, and COMMIT resolves
//! the staged edges and labels into the transaction's redo record, which
//! the redo install applies. It is dropped at commit or rollback, same
//! lifecycle as `super::TxnOverlay`.
//!
//! Edges and label deltas live in tiered maps: a hot tier in RAM and sorted
//! runs in the transaction's spill directory. Past `[tuning.query]
//! max_txn_overlay_bytes` of hot entries, the overlay spills.
//!
//! This file owns the type itself plus the savepoint undo journal
//! (`record_edge_undo` / `record_labels_undo` / `rollback_to`). Edge staging
//! and node-label staging live in the sibling `edges` and `labels` modules;
//! hot-tier accounting lives in `memory`.

use std::collections::HashMap;

use super::super::lease::{LeaseStamp, SystemFromLatch};
use super::types::{EdgeKey, EdgeSlot, GraphCollKey, GraphCollectionOverlay, NodeLabelDelta};
use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, UndoJournal};

/// One staged slot's hot state captured immediately before a mutation
/// overwrote it, with the hot version's seq. A rollback restores it.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) enum GraphOverlayUndo {
    Edge {
        coll_key: GraphCollKey,
        key: EdgeKey,
        prev: Option<(EdgeSlot, u64)>,
    },
    NodeLabels {
        coll_key: GraphCollKey,
        node_id: String,
        prev: Option<(NodeLabelDelta, u64)>,
    },
}

impl SpillWeight for GraphOverlayUndo {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<GraphOverlayUndo>()
            + match self {
                GraphOverlayUndo::Edge {
                    coll_key,
                    key,
                    prev,
                } => {
                    coll_key.2.len()
                        + key.spill_weight()
                        + prev.as_ref().map_or(0, |(slot, _)| slot.spill_weight())
                }
                GraphOverlayUndo::NodeLabels {
                    coll_key,
                    node_id,
                    prev,
                } => {
                    coll_key.2.len()
                        + node_id.len()
                        + prev.as_ref().map_or(0, |(delta, _)| delta.spill_weight())
                }
            }
    }
}

/// Per-transaction GRAPH staging overlay: holds not-yet-durable edge puts,
/// tombstones and node-label deltas for every collection touched by the
/// transaction.
#[derive(Debug)]
pub struct GraphTxnOverlay {
    pub(super) collections: HashMap<GraphCollKey, GraphCollectionOverlay>,
    /// The undo journal. Every mutator appends here before it stages, so no
    /// mutation escapes it.
    pub(super) journal: UndoJournal<GraphOverlayUndo>,
    /// Ordinal-clock stamp of the last time this transaction touched its
    /// GRAPH overlay. Read by the lease reaper alongside the other overlays'
    /// stamps: a refresh on ANY overlay keeps the transaction alive.
    last_touch: LeaseStamp,
    /// Frozen system-time ordinal used by both live transaction apply and WAL
    /// redo. Separate from lease liveness so refreshes cannot change history.
    resolved_system_from: SystemFromLatch,
    /// The spill directory, the hot-tier threshold, and the lookup reader.
    pub(super) space: SpillSpace,
    /// Set when a spill or a rollback failed part way: every later call
    /// fails with this text.
    pub(super) poisoned: Option<String>,
}

#[cfg(test)]
impl Default for GraphTxnOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphTxnOverlay {
    /// An overlay that spills into `space`.
    pub(in crate::data::executor) fn with_spill(space: SpillSpace) -> Self {
        Self {
            collections: HashMap::new(),
            journal: UndoJournal::default(),
            last_touch: LeaseStamp::default(),
            resolved_system_from: SystemFromLatch::default(),
            space,
            poisoned: None,
        }
    }

    /// An overlay that never spills, for tests.
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_spill(SpillSpace::never_spills())
    }

    /// Refresh the graph overlay's lease stamp to `ord`. See
    /// `super::super::staged::TxnOverlay::touch`.
    pub fn touch(&self, ord: i64) {
        self.last_touch.touch(ord);
    }

    /// The graph overlay's last lease stamp (0 if never touched).
    pub fn last_touch(&self) -> i64 {
        self.last_touch.last_touch()
    }

    /// Freeze the graph transaction's system-time ordinal on first resolve.
    /// Retries return the same value byte-for-byte.
    pub fn freeze_system_from(&self, candidate: i64) -> i64 {
        self.resolved_system_from.freeze(candidate)
    }

    pub fn resolved_system_from(&self) -> Option<i64> {
        self.resolved_system_from.resolved()
    }

    /// Fail when an earlier spill or rollback left the overlay unusable.
    pub(super) fn guard(&self) -> crate::Result<()> {
        match &self.poisoned {
            None => Ok(()),
            Some(reason) => Err(crate::Error::Internal {
                detail: format!(
                    "graph staging overlay is unusable after a spill error: {reason}; \
                     roll the transaction back"
                ),
            }),
        }
    }

    /// Record an edge slot's hot state before a staged mutation overwrites
    /// it. Returns the journal position the mutation's version carries.
    pub(super) fn record_edge_undo(
        &mut self,
        coll_key: &GraphCollKey,
        key: &EdgeKey,
    ) -> crate::Result<u64> {
        self.guard()?;
        let prev = self.collections.get(coll_key).and_then(|overlay| {
            overlay
                .edges
                .hot(key)
                .map(|hot| (hot.value.clone(), hot.seq))
        });
        Ok(self.journal.push(GraphOverlayUndo::Edge {
            coll_key: coll_key.clone(),
            key: key.clone(),
            prev,
        }))
    }

    /// Record a node's hot label-delta state before a staged mutation
    /// overwrites it. Returns the journal position.
    pub(super) fn record_labels_undo(
        &mut self,
        coll_key: &GraphCollKey,
        node_id: &str,
    ) -> crate::Result<u64> {
        self.guard()?;
        let prev = self.collections.get(coll_key).and_then(|overlay| {
            overlay
                .node_labels
                .hot(node_id)
                .map(|hot| (hot.value.clone(), hot.seq))
        });
        Ok(self.journal.push(GraphOverlayUndo::NodeLabels {
            coll_key: coll_key.clone(),
            node_id: node_id.to_string(),
            prev,
        }))
    }

    /// Current length of the graph overlay undo journal: the savepoint
    /// marker a later `rollback_to` rewinds toward.
    pub fn journal_len(&self) -> usize {
        self.journal.next_seq() as usize
    }

    /// Revert every staged graph mutation recorded at or after `marker`.
    /// Every version written since is dropped from the hot tier and the
    /// runs, and the journal restores the hot versions the mutations
    /// overwrote, newest first. A `marker` at or beyond the current length
    /// is a no-op. A spill-file error part way leaves the overlay unusable.
    pub fn rollback_to(&mut self, marker: usize) -> crate::Result<()> {
        self.guard()?;
        if marker >= self.journal_len() {
            return Ok(());
        }
        let result = self.rollback_tiers(marker as u64);
        if let Err(e) = &result {
            self.poisoned = Some(e.to_string());
        }
        result
    }

    fn rollback_tiers(&mut self, bound: u64) -> crate::Result<()> {
        for overlay in self.collections.values_mut() {
            overlay.rollback(bound, &mut self.space)?;
        }
        let collections = &mut self.collections;
        self.journal
            .rollback(bound, &self.space, |undo| match undo {
                GraphOverlayUndo::Edge {
                    coll_key,
                    key,
                    prev,
                } => {
                    if let Some(overlay) = collections.get_mut(&coll_key) {
                        overlay.edges.restore(key, prev);
                    }
                }
                GraphOverlayUndo::NodeLabels {
                    coll_key,
                    node_id,
                    prev,
                } => {
                    if let Some(overlay) = collections.get_mut(&coll_key) {
                        overlay.node_labels.restore(node_id, prev);
                    }
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId};

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn rollback_to_restores_edge_put_added_after_marker() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "c", vec![2])
            .unwrap();

        // Before rollback both edges are visible.
        assert_eq!(overlay.edges_for_src(&key("g"), "a").unwrap().len(), 2);

        overlay.rollback_to(marker).unwrap();

        // Post-savepoint edge A→C is gone; A→B (staged before the marker)
        // remains.
        let out = overlay.edges_for_src(&key("g"), "a").unwrap();
        assert_eq!(out, vec![("knows".to_string(), "b".to_string(), vec![1u8])]);
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn rollback_to_restores_prior_props_for_reoverwritten_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        // Overwrite the SAME edge slot after the marker: a naive drop would
        // lose the pre-marker props entirely.
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![9, 9])
            .unwrap();

        overlay.rollback_to(marker).unwrap();

        let out = overlay.edges_for_src(&key("g"), "a").unwrap();
        assert_eq!(out, vec![("knows".to_string(), "b".to_string(), vec![1u8])]);
    }

    #[test]
    fn rollback_to_restores_tombstone_cleared_by_reput() {
        // Cross-set clearing: tombstone an edge, mark, re-add it (which clears
        // the tombstone). Rollback must restore BOTH sets: the edge put goes
        // away AND the tombstone comes back.
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_delete(key("g"), "a", "knows", "b")
            .unwrap();
        let marker = overlay.journal_len();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![7])
            .unwrap();
        assert!(
            !overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap()
        );

        overlay.rollback_to(marker).unwrap();

        assert!(
            overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap(),
            "rollback must restore the pre-marker tombstone the re-put cleared"
        );
        assert_eq!(overlay.edges_for_src(&key("g"), "a").unwrap().len(), 0);
    }

    #[test]
    fn rollback_to_restores_put_cleared_by_delete() {
        // Symmetric cross-set case: put an edge, mark, delete it (clears the
        // put, adds a tombstone). Rollback restores the put and drops the
        // tombstone.
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![5])
            .unwrap();
        let marker = overlay.journal_len();
        overlay
            .stage_edge_delete(key("g"), "a", "knows", "b")
            .unwrap();

        overlay.rollback_to(marker).unwrap();

        assert!(
            !overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap()
        );
        let out = overlay.edges_for_src(&key("g"), "a").unwrap();
        assert_eq!(out, vec![("knows".to_string(), "b".to_string(), vec![5u8])]);
    }

    #[test]
    fn rollback_to_restores_prior_node_label_delta() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_node_labels_set(key("g"), "n1", &["Person".to_string()])
            .unwrap();
        let marker = overlay.journal_len();
        // After the marker: remove Person and add Robot.
        overlay
            .stage_node_labels_remove(key("g"), "n1", &["Person".to_string()])
            .unwrap();
        overlay
            .stage_node_labels_set(key("g"), "n1", &["Robot".to_string()])
            .unwrap();

        overlay.rollback_to(marker).unwrap();

        let delta = overlay.labels_delta(&key("g"), "n1").unwrap().unwrap();
        assert!(delta.added.contains("Person"));
        assert!(!delta.added.contains("Robot"));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn rollback_to_removes_node_delta_absent_at_marker() {
        let mut overlay = GraphTxnOverlay::new();
        let marker = overlay.journal_len();
        overlay
            .stage_node_labels_set(key("g"), "n1", &["Person".to_string()])
            .unwrap();

        overlay.rollback_to(marker).unwrap();

        assert!(overlay.labels_delta(&key("g"), "n1").unwrap().is_none());
    }

    #[test]
    fn rollback_to_current_len_is_noop() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![1])
            .unwrap();
        let marker = overlay.journal_len();
        overlay.rollback_to(marker).unwrap();
        assert_eq!(overlay.edges_for_src(&key("g"), "a").unwrap().len(), 1);
    }
}
