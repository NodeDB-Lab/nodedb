// SPDX-License-Identifier: Apache-2.0

//! Edge insert / remove paths and node-edge cleanup.

use super::types::CsrIndex;

impl CsrIndex {
    /// Incrementally add an unweighted edge (goes into mutable buffer).
    /// Uses weight 1.0 if the graph already has weighted edges.
    ///
    /// Returns `Err(GraphError::LabelOverflow)` if the label id space is
    /// exhausted. Production callers should always surface this to the
    /// client; silently ignoring it reproduces the silent-wrap bug the
    /// `u32` widening was meant to fix.
    pub fn add_edge(&mut self, src: &str, label: &str, dst: &str) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, "", 1.0, false)
    }

    /// Add an unweighted edge scoped to `collection`.
    ///
    /// The collection tag is what lets a collection-scoped MATCH / RAG read
    /// (`... IN '<collection>'`) see only its own edges even though every
    /// collection's edges share one per-`(database, tenant)` CSR partition
    /// (nodes, node-labels and surrogates are shared across collections; only
    /// edges carry the collection axis).
    pub fn add_edge_in_collection(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
    ) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, collection, 1.0, false)
    }

    /// Add a weighted edge scoped to `collection`. See
    /// [`Self::add_edge_in_collection`] and [`Self::add_edge_weighted`].
    pub fn add_edge_weighted_in_collection(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
        weight: f64,
    ) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, collection, weight, weight != 1.0)
    }

    /// Incrementally add a weighted edge (goes into mutable buffer).
    ///
    /// If this is the first weighted edge (weight != 1.0), initializes
    /// the weight tracking infrastructure (backfills existing buffer
    /// entries with 1.0).
    pub fn add_edge_weighted(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        weight: f64,
    ) -> Result<(), crate::GraphError> {
        self.add_edge_internal(src, label, dst, "", weight, weight != 1.0)
    }

    fn add_edge_internal(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
        weight: f64,
        force_weights: bool,
    ) -> Result<(), crate::GraphError> {
        let src_id = self.ensure_node(src)?;
        let dst_id = self.ensure_node(dst)?;
        let label_id = self.ensure_label(label)?;
        let collection_id = self.ensure_collection(collection);

        // Check for duplicates in buffer. Edge identity is collection-aware:
        // the same `(label, dst)` under a DIFFERENT collection is a distinct
        // edge and must NOT be deduplicated away.
        let out = &self.buffer_out[src_id as usize];
        let out_colls = &self.buffer_out_collections[src_id as usize];
        if out
            .iter()
            .zip(out_colls.iter())
            .any(|(&(l, d), &c)| l == label_id && d == dst_id && c == collection_id)
        {
            return Ok(());
        }
        // A dense copy is the edge itself: a deleted one comes back.
        if self.dense_has_edge(src_id, label_id, dst_id, collection_id) {
            self.deleted_edges
                .remove(&(src_id, label_id, dst_id, collection_id));
            return Ok(());
        }

        // Initialize weight tracking on first non-default weight.
        if force_weights && !self.has_weights {
            self.enable_weights();
        }

        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));
        self.buffer_out_collections[src_id as usize].push(collection_id);
        self.buffer_in_collections[dst_id as usize].push(collection_id);

        if self.has_weights {
            self.buffer_out_weights[src_id as usize].push(weight);
            self.buffer_in_weights[dst_id as usize].push(weight);
        }

        // A node-edge removal marks buffered edges deleted too. With no dense
        // copy that mark names this edge only, so it goes.
        self.deleted_edges
            .remove(&(src_id, label_id, dst_id, collection_id));
        Ok(())
    }

    /// Incrementally remove an edge under the unscoped (`""`) collection.
    ///
    /// Kept for callers that operate on a single-collection CSR (NodeDB-Lite
    /// keys one `CsrIndex` per collection) or add edges via the collection-less
    /// [`Self::add_edge`]. Origin's collection-scoped delete path uses
    /// [`Self::remove_edge_in_collection`].
    pub fn remove_edge(&mut self, src: &str, label: &str, dst: &str) {
        self.remove_edge_in_collection(src, label, dst, "");
    }

    /// Incrementally remove the `(src, label, dst, collection)` edge.
    ///
    /// Only the copy tagged with `collection` is removed — an identical triple
    /// under a different collection is left intact.
    pub fn remove_edge_in_collection(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
    ) {
        let (Some(&src_id), Some(&dst_id)) = (self.node_to_id.get(src), self.node_to_id.get(dst))
        else {
            return;
        };
        let Some(&label_id) = self.label_to_id.get(label) else {
            return;
        };
        let Some(&collection_id) = self.collection_to_id.get(collection) else {
            return;
        };

        // Remove from buffer if present (keep parallel buffers in sync).
        let out_buf = &self.buffer_out[src_id as usize];
        let out_colls = &self.buffer_out_collections[src_id as usize];
        if let Some(pos) = out_buf
            .iter()
            .zip(out_colls.iter())
            .position(|(&(l, d), &c)| l == label_id && d == dst_id && c == collection_id)
        {
            self.buffer_out[src_id as usize].swap_remove(pos);
            self.buffer_out_collections[src_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_out_weights[src_id as usize].swap_remove(pos);
            }
        }
        let in_buf = &self.buffer_in[dst_id as usize];
        let in_colls = &self.buffer_in_collections[dst_id as usize];
        if let Some(pos) = in_buf
            .iter()
            .zip(in_colls.iter())
            .position(|(&(l, s), &c)| l == label_id && s == src_id && c == collection_id)
        {
            self.buffer_in[dst_id as usize].swap_remove(pos);
            self.buffer_in_collections[dst_id as usize].swap_remove(pos);
            if self.has_weights {
                self.buffer_in_weights[dst_id as usize].swap_remove(pos);
            }
        }

        // Mark as deleted in dense CSR.
        if self.dense_has_edge(src_id, label_id, dst_id, collection_id) {
            self.deleted_edges
                .insert((src_id, label_id, dst_id, collection_id));
        }
    }

    /// Remove ALL edges touching a node. Returns the number of edges removed.
    pub fn remove_node_edges(&mut self, node: &str) -> usize {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return 0;
        };
        let mut removed = 0;

        // Collect outgoing edges (collection-tagged) then remove reverse
        // references. Every collection's copy of an edge is removed — node
        // deletion cascades across all collections.
        let out_edges = self.dense_iter_out_coll(node_id);
        for (label_id, dst_id, coll_id) in &out_edges {
            let in_buf = &self.buffer_in[*dst_id as usize];
            let in_colls = &self.buffer_in_collections[*dst_id as usize];
            if let Some(pos) = in_buf
                .iter()
                .zip(in_colls.iter())
                .position(|(&(l, s), &c)| l == *label_id && s == node_id && c == *coll_id)
            {
                self.buffer_in[*dst_id as usize].swap_remove(pos);
                self.buffer_in_collections[*dst_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_in_weights[*dst_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges
                .insert((node_id, *label_id, *dst_id, *coll_id));
            removed += 1;
        }
        self.buffer_out[node_id as usize].clear();
        self.buffer_out_collections[node_id as usize].clear();
        if self.has_weights {
            self.buffer_out_weights[node_id as usize].clear();
        }

        // Collect incoming edges (collection-tagged) then remove reverse
        // references.
        let in_edges = self.dense_iter_in_coll(node_id);
        for (label_id, src_id, coll_id) in &in_edges {
            let out_buf = &self.buffer_out[*src_id as usize];
            let out_colls = &self.buffer_out_collections[*src_id as usize];
            if let Some(pos) = out_buf
                .iter()
                .zip(out_colls.iter())
                .position(|(&(l, d), &c)| l == *label_id && d == node_id && c == *coll_id)
            {
                self.buffer_out[*src_id as usize].swap_remove(pos);
                self.buffer_out_collections[*src_id as usize].swap_remove(pos);
                if self.has_weights {
                    self.buffer_out_weights[*src_id as usize].swap_remove(pos);
                }
            }
            self.deleted_edges
                .insert((*src_id, *label_id, node_id, *coll_id));
            removed += 1;
        }
        self.buffer_in[node_id as usize].clear();
        self.buffer_in_collections[node_id as usize].clear();
        if self.has_weights {
            self.buffer_in_weights[node_id as usize].clear();
        }

        removed
    }

    /// Remove all edges touching any node whose ID starts with `prefix`.
    ///
    /// Used for tenant purge: `prefix = "{tenant_id}:"` removes all
    /// edges belonging to that tenant.
    pub fn remove_nodes_with_prefix(&mut self, prefix: &str) {
        let matching_nodes: Vec<String> = self
            .node_to_id
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for node in &matching_nodes {
            self.remove_node_edges(node);
        }
    }
}
