// SPDX-License-Identifier: Apache-2.0

//! Collection-scoped read paths.
//!
//! A CSR partition holds every collection's edges under one shared node space
//! (nodes, node-labels and surrogates are shared across collections; only edges
//! carry a collection id). These methods filter traversal to a single
//! collection so a `MATCH ... IN '<collection>'` / GraphRAG read never crosses
//! a collection boundary.

use super::types::{CsrIndex, Direction};

impl CsrIndex {
    /// Collection-scoped neighbor lookup by string name.
    ///
    /// Like [`Self::neighbors`], with the same `label_filter` set semantics,
    /// but only traverses edges tagged with
    /// `collection`. A collection with no edges in this partition yields an
    /// empty result (never falls back to the merged, cross-collection view).
    /// Used by collection-scoped GraphRAG BFS so expansion never crosses a
    /// collection boundary.
    pub fn neighbors_in_collection(
        &self,
        node: &str,
        label_filter: &[&str],
        direction: Direction,
        collection: &str,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        let Some(collection_id) = self.collection_id(collection) else {
            return Vec::new();
        };
        self.record_access(node_id);
        let labels = self.label_filter(label_filter);
        let keep = |lid: u32| labels.keeps(lid);

        let mut result = Vec::new();
        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.iter_out_edges_raw_in(node_id, collection_id) {
                if keep(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.iter_in_edges_raw_in(node_id, collection_id) {
                if keep(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }
        result
    }

    /// Collection-scoped raw outbound iteration: yields `(label_id, dst)` for
    /// edges inserted under `collection_id` only (dense + buffer, minus
    /// deleted). Used by the MATCH / RAG collection-scoped read paths so a
    /// `... IN '<collection>'` query never traverses another collection's
    /// edges even though all collections share one CSR partition.
    pub fn iter_out_edges_raw_in(
        &self,
        node: u32,
        collection_id: u32,
    ) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.dense_scoped(node, collection_id, true).into_iter()
    }

    /// Collection-scoped raw inbound iteration (see `iter_out_edges_raw_in`).
    pub fn iter_in_edges_raw_in(
        &self,
        node: u32,
        collection_id: u32,
    ) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.dense_scoped(node, collection_id, false).into_iter()
    }

    /// Build the collection-filtered `(label, neighbor)` list for a node.
    /// `outbound = true` selects out-edges, `false` selects in-edges.
    fn dense_scoped(&self, node: u32, collection_id: u32, outbound: bool) -> Vec<(u32, u32)> {
        let idx = node as usize;
        let (offsets, targets, labels, collections) = if outbound {
            (
                &self.out_offsets,
                &self.out_targets,
                &self.out_labels,
                &self.out_collections,
            )
        } else {
            (
                &self.in_offsets,
                &self.in_targets,
                &self.in_labels,
                &self.in_collections,
            )
        };
        let mut result = Vec::new();
        if idx + 1 < offsets.len() {
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            for i in start..end {
                if collections.get(i).copied().unwrap_or(0) != collection_id {
                    continue;
                }
                let lid = labels[i];
                let other = targets[i];
                // Surviving edges here all have `collection_id` (filtered
                // above), so deletion is keyed on the full identity.
                let deleted = if outbound {
                    self.deleted_edges
                        .contains(&(node, lid, other, collection_id))
                } else {
                    self.deleted_edges
                        .contains(&(other, lid, node, collection_id))
                };
                if !deleted {
                    result.push((lid, other));
                }
            }
        }
        // Buffer edges for this node, filtered by collection.
        let (buf, buf_coll) = if outbound {
            (
                self.buffer_out.get(idx),
                self.buffer_out_collections.get(idx),
            )
        } else {
            (self.buffer_in.get(idx), self.buffer_in_collections.get(idx))
        };
        if let (Some(buf), Some(buf_coll)) = (buf, buf_coll) {
            for (k, &(lid, other)) in buf.iter().enumerate() {
                if buf_coll.get(k).copied().unwrap_or(0) == collection_id {
                    result.push((lid, other));
                }
            }
        }
        result
    }
}
