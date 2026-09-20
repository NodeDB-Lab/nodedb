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
//! Scope: this overlay only serves read-your-own-writes for Neighbors / Hop
//! (single-hop reads). COMMIT durability is unchanged -- the buffered
//! `GraphOp` plan is still replayed through the real `execute_edge_put` /
//! `execute_edge_delete` / ... handlers inside the COMMIT `TransactionBatch`.
//! This overlay is in-memory only and is dropped at commit or rollback, same
//! lifecycle as `super::TxnOverlay`.
//!
//! This file owns the type itself plus the savepoint undo journal
//! (`record_edge_undo` / `record_labels_undo` / `rollback_to`). Edge staging
//! and node-label staging live in the sibling `edges` and `labels` modules;
//! memory accounting lives in `memory`.

use std::collections::HashMap;

use super::super::lease::{LeaseStamp, SystemFromLatch};
use super::types::{EdgeKey, GraphCollKey, GraphCollectionOverlay, NodeLabelDelta};

/// One graph-overlay slot's state captured immediately before a staged edge
/// or node-label mutation overwrote it. The undo journal of these entries is
/// what makes `ROLLBACK TO SAVEPOINT` correct for the GRAPH overlay: every
/// mutator is last-writer-wins with CROSS-SET CLEARING (staging an edge put
/// removes the identity from the tombstone set and vice-versa; a node-label
/// add removes it from the delta's removed-set), so a naive "drop
/// post-savepoint keys" rewind would lose an earlier same-slot write AND
/// leave the opposite set in its post-mutation state. Restoring the recorded
/// prior membership of BOTH affected sets rewinds without either loss.
#[derive(Debug, Clone)]
enum GraphOverlayUndo {
    /// Prior state of an edge identity across BOTH edge sets: its properties
    /// in `pending_edges` (or `None` if absent) and whether it was present in
    /// `pending_edge_tombstones`. Both `stage_edge_put` and `stage_edge_delete`
    /// touch both sets, so both record this.
    Edge {
        coll_key: GraphCollKey,
        key: EdgeKey,
        /// Prior `pending_edges` entry, or `None` if the slot was absent.
        prev_props: Option<Vec<u8>>,
        /// Prior membership in `pending_edge_tombstones`.
        prev_tombstoned: bool,
    },
    /// Prior `pending_node_labels` delta for a node, or `None` if the node had
    /// no staged delta before this mutation.
    NodeLabels {
        coll_key: GraphCollKey,
        node_id: String,
        prev_delta: Option<NodeLabelDelta>,
    },
}

/// Per-transaction GRAPH staging overlay: holds not-yet-durable edge/label
/// writes for every collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug, Default)]
pub struct GraphTxnOverlay {
    pub(super) collections: HashMap<GraphCollKey, GraphCollectionOverlay>,
    /// Append-only undo journal recording each edge/label slot's prior state
    /// before a staged mutation overwrote it. `journal_len` reads its length
    /// (the graph savepoint marker); `rollback_to` replays it in reverse down
    /// to a marker. The four mutators are the ONLY writers of the private
    /// edge/tombstone/label sets and each appends here first, so no mutation
    /// escapes the journal — the guarantee `ROLLBACK TO SAVEPOINT` relies on.
    /// Dropped with the overlay when the transaction resolves.
    journal: Vec<GraphOverlayUndo>,
    /// Ordinal-clock stamp of the last time this transaction touched its GRAPH
    /// overlay — the parallel of `super::super::staged::TxnOverlay`'s stamp,
    /// read alongside it by the lease reaper (a refresh on EITHER overlay keeps
    /// the transaction alive). `Cell` for the same single-threaded `!Send`
    /// interior-mutability reason.
    last_touch: LeaseStamp,
    /// Frozen system-time ordinal used by both live transaction apply and WAL
    /// redo. Separate from lease liveness so refreshes cannot change history.
    resolved_system_from: SystemFromLatch,
}

impl GraphTxnOverlay {
    pub fn new() -> Self {
        Self::default()
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

    /// Record an edge identity's prior state across BOTH edge sets before a
    /// staged edge mutation overwrites it. Single chokepoint shared by
    /// `stage_edge_put` and `stage_edge_delete` (and the batch forms, which
    /// call those per-edge), so no edge-set mutation escapes the journal.
    pub(super) fn record_edge_undo(&mut self, coll_key: &GraphCollKey, key: &EdgeKey) {
        let (prev_props, prev_tombstoned) = match self.collections.get(coll_key) {
            Some(overlay) => (
                overlay.pending_edges.get(key).cloned(),
                overlay.pending_edge_tombstones.contains(key),
            ),
            None => (None, false),
        };
        self.journal.push(GraphOverlayUndo::Edge {
            coll_key: coll_key.clone(),
            key: key.clone(),
            prev_props,
            prev_tombstoned,
        });
    }

    /// Record a node's prior label delta before a staged label mutation
    /// overwrites it. Single chokepoint shared by `stage_node_labels_set` and
    /// `stage_node_labels_remove`.
    pub(super) fn record_labels_undo(&mut self, coll_key: &GraphCollKey, node_id: &str) {
        let prev_delta = self
            .collections
            .get(coll_key)
            .and_then(|overlay| overlay.pending_node_labels.get(node_id).cloned());
        self.journal.push(GraphOverlayUndo::NodeLabels {
            coll_key: coll_key.clone(),
            node_id: node_id.to_string(),
            prev_delta,
        });
    }

    /// Current length of the graph overlay undo journal — the savepoint marker
    /// a later `rollback_to` rewinds toward. Returned to the Control Plane by
    /// `MetaOp::MarkSavepoint` alongside the value overlay's marker.
    pub fn journal_len(&self) -> usize {
        self.journal.len()
    }

    /// Revert every staged edge/label mutation recorded after `marker`,
    /// restoring each slot's prior membership across BOTH affected sets (or
    /// removing it when the prior slot was absent), then truncate the journal
    /// to `marker`.
    ///
    /// Entries are replayed strictly in reverse so repeated writes to one slot
    /// unwind to the exact state present at the marked point, and the
    /// cross-set clearing each mutator performed is undone in lockstep. A
    /// `marker` at or beyond the current length is a no-op.
    pub fn rollback_to(&mut self, marker: usize) {
        while self.journal.len() > marker {
            let Some(undo) = self.journal.pop() else {
                break;
            };
            match undo {
                GraphOverlayUndo::Edge {
                    coll_key,
                    key,
                    prev_props,
                    prev_tombstoned,
                } => {
                    let Some(overlay) = self.collections.get_mut(&coll_key) else {
                        continue;
                    };
                    match prev_props {
                        Some(props) => {
                            overlay.pending_edges.insert(key.clone(), props);
                        }
                        None => {
                            overlay.pending_edges.remove(&key);
                        }
                    }
                    if prev_tombstoned {
                        overlay.pending_edge_tombstones.insert(key);
                    } else {
                        overlay.pending_edge_tombstones.remove(&key);
                    }
                }
                GraphOverlayUndo::NodeLabels {
                    coll_key,
                    node_id,
                    prev_delta,
                } => {
                    let Some(overlay) = self.collections.get_mut(&coll_key) else {
                        continue;
                    };
                    match prev_delta {
                        Some(delta) => {
                            overlay.pending_node_labels.insert(node_id, delta);
                        }
                        None => {
                            overlay.pending_node_labels.remove(&node_id);
                        }
                    }
                }
            }
        }
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
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1]);
        let marker = overlay.journal_len();
        overlay.stage_edge_put(key("g"), "a", "knows", "c", vec![2]);

        // Before rollback both edges are visible.
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 2);

        overlay.rollback_to(marker);

        // Post-savepoint edge A→C is gone; A→B (staged before the marker)
        // remains.
        let out: Vec<_> = overlay.edges_for_src(&key("g"), "a").collect();
        assert_eq!(out, vec![("knows", "b", &[1u8][..])]);
        assert_eq!(overlay.journal_len(), marker);
    }

    #[test]
    fn rollback_to_restores_prior_props_for_reoverwritten_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1]);
        let marker = overlay.journal_len();
        // Overwrite the SAME edge slot after the marker: a naive drop would
        // lose the pre-marker props entirely.
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![9, 9]);

        overlay.rollback_to(marker);

        let out: Vec<_> = overlay.edges_for_src(&key("g"), "a").collect();
        assert_eq!(out, vec![("knows", "b", &[1u8][..])]);
    }

    #[test]
    fn rollback_to_restores_tombstone_cleared_by_reput() {
        // Cross-set clearing: tombstone an edge, mark, re-add it (which clears
        // the tombstone). Rollback must restore BOTH sets: the edge put goes
        // away AND the tombstone comes back.
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");
        let marker = overlay.journal_len();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![7]);
        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));

        overlay.rollback_to(marker);

        assert!(
            overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"),
            "rollback must restore the pre-marker tombstone the re-put cleared"
        );
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 0);
    }

    #[test]
    fn rollback_to_restores_put_cleared_by_delete() {
        // Symmetric cross-set case: put an edge, mark, delete it (clears the
        // put, adds a tombstone). Rollback restores the put and drops the
        // tombstone.
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![5]);
        let marker = overlay.journal_len();
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");

        overlay.rollback_to(marker);

        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
        let out: Vec<_> = overlay.edges_for_src(&key("g"), "a").collect();
        assert_eq!(out, vec![("knows", "b", &[5u8][..])]);
    }

    #[test]
    fn rollback_to_restores_prior_node_label_delta() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(key("g"), "n1", &["Person".to_string()]);
        let marker = overlay.journal_len();
        // After the marker: remove Person and add Robot.
        overlay.stage_node_labels_remove(key("g"), "n1", &["Person".to_string()]);
        overlay.stage_node_labels_set(key("g"), "n1", &["Robot".to_string()]);

        overlay.rollback_to(marker);

        let delta = overlay.labels_delta(&key("g"), "n1").unwrap();
        assert!(delta.added.contains("Person"));
        assert!(!delta.added.contains("Robot"));
        assert!(delta.removed.is_empty());
    }

    #[test]
    fn rollback_to_removes_node_delta_absent_at_marker() {
        let mut overlay = GraphTxnOverlay::new();
        let marker = overlay.journal_len();
        overlay.stage_node_labels_set(key("g"), "n1", &["Person".to_string()]);

        overlay.rollback_to(marker);

        assert!(overlay.labels_delta(&key("g"), "n1").is_none());
    }

    #[test]
    fn rollback_to_current_len_is_noop() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1]);
        let marker = overlay.journal_len();
        overlay.rollback_to(marker);
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 1);
    }
}
