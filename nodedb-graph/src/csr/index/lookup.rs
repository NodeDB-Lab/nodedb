// SPDX-License-Identifier: Apache-2.0

//! Read-side queries: neighbor lookup, counters, degree, iterators,
//! dense-array helpers, and the `add_node` / `build_dense` utilities.
//!
//! Public entry points take [`LocalNodeId`] so that cross-partition id
//! use panics at the boundary. Crate-internal iteration over dense
//! ranges uses raw `u32` via `dense_out_edges` / `dense_in_edges`.

use std::mem::size_of;

use nodedb_mem::ScopedMemory;

use super::types::{CsrIndex, Direction};
use crate::GraphError;
use crate::csr::LocalNodeId;
use crate::csr::rebuild::journal::{CsrWriteOp, OpOutcome};

/// Contiguous CSR adjacency arrays produced by [`CsrIndex::build_dense`].
pub(crate) struct DenseAdjacency {
    pub(crate) offsets: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    pub(crate) labels: Vec<u32>,
    pub(crate) collections: Vec<u32>,
}

impl CsrIndex {
    /// Partition tag assigned at construction. Embedded in every `LocalNodeId` this index produces.
    #[inline]
    pub fn partition_tag(&self) -> u32 {
        self.partition_tag
    }

    /// Mint a `LocalNodeId` for this partition from a raw dense index.
    /// Used by algorithm code that iterates `0..node_count` and needs
    /// to call `LocalNodeId`-taking APIs.
    #[inline]
    pub fn local(&self, id: u32) -> LocalNodeId {
        LocalNodeId::new(id, self.partition_tag)
    }

    /// Get immediate neighbors by string name. An empty `label_filter` keeps
    /// every edge. Otherwise an edge whose label is any listed label passes.
    pub fn neighbors(
        &self,
        node: &str,
        label_filter: &[&str],
        direction: Direction,
    ) -> Vec<(String, String)> {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return Vec::new();
        };
        self.record_access(node_id);
        let labels = self.label_filter(label_filter);

        let mut result = Vec::new();

        if matches!(direction, Direction::Out | Direction::Both) {
            for (lid, dst) in self.dense_iter_out(node_id) {
                if labels.keeps(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[dst as usize].clone(),
                    ));
                }
            }
        }
        if matches!(direction, Direction::In | Direction::Both) {
            for (lid, src) in self.dense_iter_in(node_id) {
                if labels.keeps(lid) {
                    result.push((
                        self.id_to_label[lid as usize].clone(),
                        self.id_to_node[src as usize].clone(),
                    ));
                }
            }
        }

        result
    }

    /// Add a node without any edges. Idempotent — returns the existing
    /// tagged id if the name is already present.
    ///
    /// Returns `Err(GraphError::NodeOverflow)` when the partition's node-id
    /// space is exhausted (more than `MAX_NODES_PER_CSR` distinct nodes).
    pub fn add_node(&mut self, name: &str) -> Result<LocalNodeId, crate::GraphError> {
        let result = self.ensure_node(name);
        self.journal_record(
            || CsrWriteOp::AddNode {
                name: name.to_string(),
            },
            OpOutcome::of(&result),
        );
        Ok(LocalNodeId::new(result?, self.partition_tag))
    }

    pub fn node_count(&self) -> usize {
        self.id_to_node.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }

    /// Get the string name for a tagged node id.
    pub fn node_name(&self, id: LocalNodeId) -> &str {
        &self.id_to_node[id.raw(self.partition_tag) as usize]
    }

    /// Look up the tagged node id for a string name.
    pub fn node_id(&self, name: &str) -> Option<LocalNodeId> {
        self.node_to_id
            .get(name)
            .copied()
            .map(|raw| LocalNodeId::new(raw, self.partition_tag))
    }

    /// Get the string label for a dense label index.
    pub fn label_name(&self, label_id: u32) -> &str {
        &self.id_to_label[label_id as usize]
    }

    /// Look up the dense label id for a string label.
    pub fn label_id(&self, name: &str) -> Option<u32> {
        self.label_to_id.get(name).copied()
    }

    /// Out-degree of a node (including buffer, excluding deleted).
    pub fn out_degree(&self, id: LocalNodeId) -> usize {
        self.dense_iter_out(id.raw(self.partition_tag)).count()
    }

    /// In-degree of a node.
    pub fn in_degree(&self, id: LocalNodeId) -> usize {
        self.dense_iter_in(id.raw(self.partition_tag)).count()
    }

    /// Total edge count (dense + buffer - deleted). O(V).
    pub fn edge_count(&self) -> usize {
        let n = self.id_to_node.len();
        (0..n as u32).map(|i| self.out_degree(self.local(i))).sum()
    }

    // ── Internal helpers ──

    /// Build contiguous offset/target/label arrays from per-node edge lists.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError::MemoryBudget`] if the reservation for the three
    /// output arrays exceeds the `Graph` engine budget.
    pub(crate) fn build_dense(
        edges: &[Vec<(u32, u32)>],
        collections: &[Vec<u32>],
        memory: &ScopedMemory,
    ) -> Result<DenseAdjacency, GraphError> {
        let n = edges.len();
        let total: usize = edges.iter().map(|e| e.len()).sum();
        // Reserve budget for offsets (n+1 u32s), targets/labels/collections (total u32s each).
        let reserve_bytes = (n + 1 + 3 * total) * size_of::<u32>();
        let _budget_guard = memory.reserve(reserve_bytes)?;
        let mut offsets = Vec::with_capacity(n + 1);
        let mut targets = Vec::with_capacity(total);
        let mut labels = Vec::with_capacity(total);
        let mut out_collections = Vec::with_capacity(total);

        let mut offset = 0u32;
        for (node, node_edges) in edges.iter().enumerate() {
            offsets.push(offset);
            let node_colls = collections.get(node);
            for (k, &(lid, target)) in node_edges.iter().enumerate() {
                targets.push(target);
                labels.push(lid);
                out_collections.push(node_colls.and_then(|c| c.get(k)).copied().unwrap_or(0));
            }
            offset += node_edges.len() as u32;
        }
        offsets.push(offset);

        Ok(DenseAdjacency {
            offsets,
            targets,
            labels,
            collections: out_collections,
        })
    }

    /// Check if a specific `(src, label, dst, collection)` edge exists in the
    /// dense CSR. Edge identity is collection-aware: the same triple under two
    /// collections is two distinct edges, so dedup / re-insert must key on the
    /// collection too.
    pub(crate) fn dense_has_edge(&self, src: u32, label: u32, dst: u32, collection: u32) -> bool {
        for (lid, target, coll) in self.dense_out_edges(src) {
            if lid == label && target == dst && coll == collection {
                return true;
            }
        }
        false
    }

    /// Iterate dense outbound edges for a node as `(label, dst, collection)`
    /// (raw u32, no tag check, no deletion filter).
    pub(crate) fn dense_out_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32, u32)> + '_ {
        let range = self
            .out_offsets
            .get(node as usize..)
            .and_then(|offsets| offsets.first().zip(offsets.get(1)))
            .map_or(0..0, |(&start, &end)| start as usize..end as usize);
        range.map(move |i| {
            (
                self.out_labels[i],
                self.out_targets[i],
                self.out_collections.get(i).copied().unwrap_or(0),
            )
        })
    }

    /// Iterate dense inbound edges for a node as `(label, src, collection)`
    /// (raw u32, no tag check, no deletion filter).
    pub(crate) fn dense_in_edges(&self, node: u32) -> impl Iterator<Item = (u32, u32, u32)> + '_ {
        let range = self
            .in_offsets
            .get(node as usize..)
            .and_then(|offsets| offsets.first().zip(offsets.get(1)))
            .map_or(0..0, |(&start, &end)| start as usize..end as usize);
        range.map(move |i| {
            (
                self.in_labels[i],
                self.in_targets[i],
                self.in_collections.get(i).copied().unwrap_or(0),
            )
        })
    }

    /// Raw u32 iteration over outbound edges (dense + buffer - deleted),
    /// yielding `(label, dst)`. The deletion filter is collection-aware: only
    /// the `(node, label, dst, collection)` copy that was deleted is hidden.
    /// Crate-internal: used by label-dispatching helpers and algorithms
    /// that already hold a validated partition borrow.
    pub(crate) fn dense_iter_out(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let dense = self
            .dense_out_edges(node)
            .filter(move |&(lid, dst, coll)| !self.deleted_edges.contains(&(node, lid, dst, coll)))
            .map(|(lid, dst, _coll)| (lid, dst));
        dense.chain(self.buffer_out_iter(node))
    }

    /// Raw u32 iteration over inbound edges (dense + buffer - deleted),
    /// yielding `(label, src)`. Collection-aware deletion filter.
    pub(crate) fn dense_iter_in(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let dense = self
            .dense_in_edges(node)
            .filter(move |&(lid, src, coll)| !self.deleted_edges.contains(&(src, lid, node, coll)))
            .map(|(lid, src, _coll)| (lid, src));
        dense.chain(self.buffer_in_iter(node))
    }

    /// Collection-tagged iteration over live outbound edges (dense + buffer -
    /// deleted), yielding `(label, dst, collection)`. Used by node-edge removal
    /// so each edge is tombstoned under its own collection identity.
    pub(crate) fn dense_iter_out_coll(&self, node: u32) -> Vec<(u32, u32, u32)> {
        let mut out: Vec<(u32, u32, u32)> = self
            .dense_out_edges(node)
            .filter(|&(lid, dst, coll)| !self.deleted_edges.contains(&(node, lid, dst, coll)))
            .collect();
        let idx = node as usize;
        if idx < self.buffer_out.len() {
            for (k, &(lid, dst)) in self.buffer_out[idx].iter().enumerate() {
                let coll = self
                    .buffer_out_collections
                    .get(idx)
                    .and_then(|c| c.get(k))
                    .copied()
                    .unwrap_or(0);
                out.push((lid, dst, coll));
            }
        }
        out
    }

    /// Collection-tagged iteration over live inbound edges (see
    /// [`Self::dense_iter_out_coll`]), yielding `(label, src, collection)`.
    pub(crate) fn dense_iter_in_coll(&self, node: u32) -> Vec<(u32, u32, u32)> {
        let mut out: Vec<(u32, u32, u32)> = self
            .dense_in_edges(node)
            .filter(|&(lid, src, coll)| !self.deleted_edges.contains(&(src, lid, node, coll)))
            .collect();
        let idx = node as usize;
        if idx < self.buffer_in.len() {
            for (k, &(lid, src)) in self.buffer_in[idx].iter().enumerate() {
                let coll = self
                    .buffer_in_collections
                    .get(idx)
                    .and_then(|c| c.get(k))
                    .copied()
                    .unwrap_or(0);
                out.push((lid, src, coll));
            }
        }
        out
    }

    /// Buffer-only iteration over outbound edges for a node.
    pub(crate) fn buffer_out_iter(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.buffer_out
            .get(node as usize)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .copied()
    }

    /// Buffer-only iteration over inbound edges for a node.
    pub(crate) fn buffer_in_iter(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.buffer_in
            .get(node as usize)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .copied()
    }

    /// Iterate all outbound edges for a tagged node. Yields
    /// `(label_id, dst)` with `dst` tagged to this partition.
    pub fn iter_out_edges(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId)> + '_ {
        let raw = node.raw(self.partition_tag);
        let tag = self.partition_tag;
        self.dense_iter_out(raw)
            .map(move |(lid, dst)| (lid, LocalNodeId::new(dst, tag)))
    }

    /// Iterate all inbound edges for a tagged node.
    pub fn iter_in_edges(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId)> + '_ {
        let raw = node.raw(self.partition_tag);
        let tag = self.partition_tag;
        self.dense_iter_in(raw)
            .map(move |(lid, src)| (lid, LocalNodeId::new(src, tag)))
    }

    // ── Raw u32 helpers for in-partition algorithm use ──
    //
    // The tagged `LocalNodeId` API catches cross-partition id leakage
    // at runtime. In-partition algorithms that iterate dense ranges
    // within a single `&CsrIndex` borrow cannot produce a cross-
    // partition id by construction — no other partition is reachable
    // from the borrow. These helpers expose the underlying raw u32
    // iteration at zero cost for that case.

    /// Raw dense out-edges iteration. In-partition algorithm use only.
    pub fn iter_out_edges_raw(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.dense_iter_out(node)
    }

    /// Raw dense in-edges iteration. In-partition algorithm use only.
    pub fn iter_in_edges_raw(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.dense_iter_in(node)
    }

    /// Raw out-degree by dense index.
    pub fn out_degree_raw(&self, node: u32) -> usize {
        self.dense_iter_out(node).count()
    }

    /// Raw in-degree by dense index.
    pub fn in_degree_raw(&self, node: u32) -> usize {
        self.dense_iter_in(node).count()
    }

    /// String name for a raw dense index. In-partition algorithm use only.
    pub fn node_name_raw(&self, id: u32) -> &str {
        &self.id_to_node[id as usize]
    }

    /// Whether a raw dense index names a node of this partition.
    ///
    /// Read paths that accept caller-supplied local ids gate on this: an id
    /// past the end is not a node, and admitting it would put an entry in a
    /// result set that resolves to no name at all.
    pub fn is_local_node(&self, id: u32) -> bool {
        (id as usize) < self.id_to_node.len()
    }

    /// String name for a raw dense index, or `None` when the index is out of
    /// range. Same domain as [`Self::node_name_raw`], without the panic: read
    /// paths that resolve a whole traversal frontier use this so one torn id
    /// narrows the answer instead of killing the core.
    pub fn node_name_checked(&self, id: u32) -> Option<&str> {
        self.id_to_node.get(id as usize).map(String::as_str)
    }

    /// Raw dense index lookup by name. In-partition algorithm use only.
    pub fn node_id_raw(&self, name: &str) -> Option<u32> {
        self.node_to_id.get(name).copied()
    }
}

#[cfg(test)]
mod tests {
    // Live adjacency iteration across dense and buffered edges.

    use super::CsrIndex;
    use crate::test_support::test_memory;

    fn names(csr: &CsrIndex, node: &str) -> Vec<(String, String)> {
        csr.iter_out_edges(csr.node_id(node).unwrap())
            .map(|(label, target)| (csr.label_name(label).into(), csr.node_name(target).into()))
            .collect()
    }

    #[test]
    fn live_iterators_preserve_dense_then_buffer_order() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "FIRST", "b").unwrap();
        assert_eq!(names(&csr, "a"), vec![("FIRST".into(), "b".into())]);
        csr.compact().unwrap();
        assert_eq!(names(&csr, "a"), vec![("FIRST".into(), "b".into())]);
        csr.add_edge("a", "SECOND", "c").unwrap();
        assert_eq!(
            names(&csr, "a"),
            vec![("FIRST".into(), "b".into()), ("SECOND".into(), "c".into())]
        );
        let a = csr.node_id_raw("a").unwrap();
        assert_eq!(csr.iter_out_edges_raw(a).count(), 2);
        for target in ["b", "c"] {
            let tagged = csr.node_id(target).unwrap();
            let inbound: Vec<_> = csr
                .iter_in_edges(tagged)
                .map(|(_, source)| csr.node_name(source))
                .collect();
            assert_eq!(inbound, vec!["a"]);
            assert_eq!(
                csr.iter_in_edges_raw(csr.node_id_raw(target).unwrap())
                    .count(),
                1
            );
        }
        assert_eq!(csr.iter_out_edges_raw(u32::MAX).count(), 0);
        assert_eq!(csr.iter_in_edges_raw(u32::MAX).count(), 0);
    }

    #[test]
    fn collection_tombstones_remove_only_the_matching_copy() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_in_collection("a", "LINK", "b", "first")
            .unwrap();
        csr.add_edge_in_collection("a", "LINK", "b", "second")
            .unwrap();
        csr.compact().unwrap();
        csr.remove_edge_in_collection("a", "LINK", "b", "first");
        csr.add_edge("a", "STAGED", "c").unwrap();
        csr.remove_edge("a", "STAGED", "c");
        assert_eq!(names(&csr, "a"), vec![("LINK".into(), "b".into())]);
        assert_eq!(csr.iter_in_edges(csr.node_id("b").unwrap()).count(), 1);
        assert_eq!(csr.iter_in_edges(csr.node_id("c").unwrap()).count(), 0);
    }
}
